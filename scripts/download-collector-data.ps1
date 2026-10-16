<#
.SYNOPSIS
    Downloads daily Zstandard (.jsonl.zst) market data from the AWS EC2 Data Collector instance to local disk,
    maintaining the identical directory structure, verifies file integrity, and deletes verified files from EC2.
    Also supports cleaning up remote files that were already downloaded previously.

.DESCRIPTION
    Connects to the Data Collector EC2 instance (via SSH/SSM tunnel), discovers all daily stream files (Binance Spot & Futures,
    Deribit Futures trades, Polymarket PolyBolt) and per-market Polymarket CLOB files for the specified UTC date (YYYY-MM-DD),
    streams them via tar directly into the local destination directory without creating intermediate archives on EC2, verifies
    every file's byte size locally, and upon 100% verification deletes the downloaded files from the remote EBS volume.

.PARAMETER Date
    Target UTC date in YYYY-MM-DD format (e.g. 2026-10-12, 2026-10-15). Optional when using -CleanAlreadyDownloaded.

.PARAMETER DestinationDir
    Local root directory for collector data. Defaults to 'D:\Crypto\data\Polymarket\collector'.

.PARAMETER Ec2Host
    EC2 SSH host alias configured in ~/.ssh/config. Defaults to 'i-08fe8a64d89f05ea9'.

.PARAMETER RemoteBaseDir
    Root directory of collector data on the EC2 host. Defaults to '/mnt/ebs/polymarket-data/collector'.

.PARAMETER DryRun
    If specified, displays matched files and sizes on EC2 without downloading or deleting anything.

.PARAMETER KeepRemoteFiles
    If specified, retains files on EC2 after successful download and verification instead of deleting them.

.PARAMETER CleanAlreadyDownloaded
    If specified, scans local files in DestinationDir, compares against remote EC2 files, and deletes any verified
    matching files on EC2 (useful for cleaning up files transferred via WinSCP or earlier downloads).

.EXAMPLE
    .\scripts\download-collector-data.ps1 -Date 2026-10-15
    .\scripts\download-collector-data.ps1 -Date 2026-10-15 -DryRun
    .\scripts\download-collector-data.ps1 -CleanAlreadyDownloaded
#>
[CmdletBinding(DefaultParameterSetName = "Download")]
param(
    [Parameter(ParameterSetName = "Download", Mandatory = $true, Position = 0, HelpMessage = "Target UTC date in YYYY-MM-DD format")]
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$Date,

    [Parameter(ParameterSetName = "CleanOnly", Mandatory = $true)]
    [switch]$CleanAlreadyDownloaded,

    [Parameter(Position = 1)]
    [string]$DestinationDir = "D:\Crypto\data\Polymarket\collector",

    [Parameter()]
    [string]$Ec2Host = "i-08fe8a64d89f05ea9",

    [Parameter()]
    [string]$RemoteBaseDir = "/mnt/ebs/polymarket-data/collector",

    [Parameter()]
    [switch]$DryRun,

    [Parameter(ParameterSetName = "Download")]
    [switch]$KeepRemoteFiles
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Ensure SessionManagerPlugin and SSH are in PATH
$pluginBin = "C:\Program Files\Amazon\SessionManagerPlugin\bin"
if ($env:PATH -notlike "*$pluginBin*") {
    $env:PATH += ";$pluginBin"
}

# --- MODE: CLEAN ALREADY DOWNLOADED ---
if ($CleanAlreadyDownloaded) {
    Write-Host "=================================================================" -ForegroundColor Cyan
    Write-Host " Polymarket Data Collector - Remote Reconcile & Cleanup" -ForegroundColor Cyan
    Write-Host " EC2 Instance Host : $Ec2Host" -ForegroundColor White
    Write-Host " Remote Base Dir   : $RemoteBaseDir" -ForegroundColor White
    Write-Host " Local Destination : $DestinationDir" -ForegroundColor White
    if ($DryRun) {
        Write-Host " Mode              : DRY-RUN (No remote files will be deleted)" -ForegroundColor Yellow
    } else {
        Write-Host " Mode              : VERIFY AND DELETE ALREADY DOWNLOADED FILES" -ForegroundColor Green
    }
    Write-Host "=================================================================" -ForegroundColor Cyan

    if (-not (Test-Path -LiteralPath $DestinationDir)) {
        throw "Local directory $DestinationDir does not exist."
    }

    Write-Host "[1/3] Scanning local files in $DestinationDir..." -ForegroundColor Cyan
    $localFiles = Get-ChildItem -Path $DestinationDir -Recurse -File -Filter "*.jsonl.zst"
    Write-Host "      Found $($localFiles.Count) local .jsonl.zst files." -ForegroundColor Green

    $manifestLines = $localFiles | ForEach-Object {
        $rel = $_.FullName.Substring($DestinationDir.Length).TrimStart('\', '/').Replace('\', '/')
        "$rel`t$($_.Length)"
    }
    $tempManifest = Join-Path $env:TEMP "reconcile_manifest_$([guid]::NewGuid().ToString()).tsv"
    [System.IO.File]::WriteAllLines($tempManifest, $manifestLines)

    $reconcilePy = @"
import os, sys

remote_base = "$RemoteBaseDir"
manifest_file = "/tmp/reconcile_manifest.tsv"

if not os.path.exists(manifest_file):
    print("ERROR: Manifest missing")
    sys.exit(1)

local_manifest = {}
with open(manifest_file, "r") as f:
    for line in f:
        line = line.strip()
        if not line: continue
        parts = line.split("\t")
        if len(parts) == 2:
            local_manifest[parts[0]] = int(parts[1])

matched = []
mismatch = []
remote_total = 0

for root, dirs, files in os.walk(remote_base):
    for f in files:
        if not f.endswith(".jsonl.zst"): continue
        remote_total += 1
        full_path = os.path.join(root, f)
        rel = os.path.relpath(full_path, remote_base).replace('\\\\', '/')
        rem_size = os.path.getsize(full_path)
        if rel in local_manifest:
            loc_size = local_manifest[rel]
            if loc_size == rem_size:
                matched.append((rel, full_path, rem_size))
            else:
                mismatch.append((rel, loc_size, rem_size))

total_mb = sum(s for _, _, s in matched) / (1024 * 1024)
print(f"RECONCILE\t{len(matched)}\t{total_mb:.2f}\t{len(mismatch)}\t{remote_total}")

with open("/tmp/reconcile_delete.txt", "w") as out:
    for _, full_path, _ in matched:
        out.write(full_path + "\n")
"@

    $tempPy = Join-Path $env:TEMP "reconcile_check_$([guid]::NewGuid().ToString()).py"
    [System.IO.File]::WriteAllText($tempPy, $reconcilePy)

    try {
        Write-Host "[2/3] Comparing with EC2 EBS volume..." -ForegroundColor Cyan
        scp -q -o BatchMode=yes "$tempManifest" "$Ec2Host`:/tmp/reconcile_manifest.tsv"
        scp -q -o BatchMode=yes "$tempPy" "$Ec2Host`:/tmp/reconcile_check.py"
        $recOutput = ssh -o BatchMode=yes $Ec2Host "python3 /tmp/reconcile_check.py"
    } finally {
        if (Test-Path $tempManifest) { Remove-Item -Force $tempManifest }
        if (Test-Path $tempPy) { Remove-Item -Force $tempPy }
    }

    if ($recOutput -notmatch 'RECONCILE\t(\d+)\t([\d\.]+)\t(\d+)\t(\d+)') {
        throw "Reconciliation script failed. Output: $recOutput"
    }

    $matchedCount = [int]$Matches[1]
    $matchedMb = [double]$Matches[2]
    $mismatchCount = [int]$Matches[3]
    $remoteTotal = [int]$Matches[4]

    Write-Host "      Matched & Verified Files : $matchedCount ($matchedMb MB)" -ForegroundColor Green
    Write-Host "      Size Mismatches (skipped): $mismatchCount" -ForegroundColor $(if ($mismatchCount -gt 0) { "Red" } else { "Gray" })
    Write-Host "      Total Remote Files       : $remoteTotal" -ForegroundColor Gray

    if ($matchedCount -eq 0) {
        Write-Host "[INFO] No matching files to delete on EC2." -ForegroundColor Yellow
        ssh -o BatchMode=yes $Ec2Host "rm -f /tmp/reconcile_manifest.tsv /tmp/reconcile_check.py /tmp/reconcile_delete.txt"
        return
    }

    if ($DryRun) {
        Write-Host "[DRY-RUN] Files that would be deleted from EC2:" -ForegroundColor Yellow
        ssh -o BatchMode=yes $Ec2Host "head -n 20 /tmp/reconcile_delete.txt"
        Write-Host "[DRY-RUN] Completed. No files deleted." -ForegroundColor Yellow
        ssh -o BatchMode=yes $Ec2Host "rm -f /tmp/reconcile_manifest.tsv /tmp/reconcile_check.py /tmp/reconcile_delete.txt"
        return
    }

    Write-Host "[3/3] Deleting $matchedCount verified files on EC2..." -ForegroundColor Cyan
    ssh -o BatchMode=yes $Ec2Host "cat /tmp/reconcile_delete.txt | xargs -r -d '\n' sudo rm -f && rm -f /tmp/reconcile_manifest.tsv /tmp/reconcile_check.py /tmp/reconcile_delete.txt"
    $dfOutput = ssh -o BatchMode=yes $Ec2Host "df -h $RemoteBaseDir"
    Write-Host "      Files deleted successfully. Remote EBS volume status:`n$dfOutput" -ForegroundColor Green
    Write-Host "`nSUCCESS: Reclaimed $matchedMb MB across $matchedCount files on EC2." -ForegroundColor Green
    return
}

# --- MODE: DOWNLOAD DAILY ARCHIVE ---
# Validate UTC today to prevent accidental deletion of currently open active streams
$utcToday = [DateTimeOffset]::UtcNow.ToString("yyyy-MM-dd")
if ($Date -eq $utcToday -and -not $KeepRemoteFiles) {
    Write-Warning "Requested date '$Date' matches today (UTC). The Data Collector daemon is actively writing to today's daily files."
    Write-Warning "Automatically enabling -KeepRemoteFiles to protect running streams from deletion."
    $KeepRemoteFiles = [switch]::Present
}

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host " Polymarket Data Collector - Daily Archive Downloader" -ForegroundColor Cyan
Write-Host " Target Date (UTC) : $Date" -ForegroundColor White
Write-Host " EC2 Instance Host : $Ec2Host" -ForegroundColor White
Write-Host " Remote Base Dir   : $RemoteBaseDir" -ForegroundColor White
Write-Host " Local Destination : $DestinationDir" -ForegroundColor White
if ($DryRun) {
    Write-Host " Mode              : DRY-RUN (No files will be downloaded or deleted)" -ForegroundColor Yellow
} elseif ($KeepRemoteFiles) {
    Write-Host " Mode              : DOWNLOAD & KEEP REMOTE FILES" -ForegroundColor Yellow
} else {
    Write-Host " Mode              : DOWNLOAD, VERIFY & DELETE FROM EC2" -ForegroundColor Green
}
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Generate remote manifest on EC2 via inline python script
$remotePythonScript = @"
import os, datetime

date_str = "$Date"
try:
    day_start = int(datetime.datetime.strptime(date_str + " 00:00:00+0000", "%Y-%m-%d %H:%M:%S%z").timestamp())
except Exception as e:
    print(f"ERROR: Invalid date format {e}")
    exit(1)
day_end = day_start + 86400

base = "$RemoteBaseDir"
if not os.path.exists(base):
    print(f"ERROR: Base directory {base} does not exist")
    exit(1)

manifest_lines = []
files_lines = []

for root, dirs, files in os.walk(base):
    for f in files:
        if not f.endswith(".jsonl.zst"):
            continue
        full_path = os.path.join(root, f)
        matched = False
        if ("-" + date_str + ".jsonl.zst") in f:
            matched = True
        else:
            parts = f.replace(".jsonl.zst", "").split("-")
            if parts and parts[-1].isdigit():
                try:
                    epoch = int(parts[-1])
                    if day_start <= epoch < day_end:
                        matched = True
                except ValueError:
                    pass
        if matched:
            rel = os.path.relpath(full_path, base).replace('\\\\', '/')
            size = os.path.getsize(full_path)
            manifest_lines.append(f"{rel}\t{size}")
            files_lines.append(rel)

manifest_path = f"/tmp/collector-{date_str}.manifest"
files_path = f"/tmp/collector-{date_str}.files"

with open(manifest_path, "w") as out:
    out.write("\n".join(manifest_lines) + ("\n" if manifest_lines else ""))

with open(files_path, "w") as out:
    out.write("\n".join(files_lines) + ("\n" if files_lines else ""))

total_size = sum(int(line.split("\t")[1]) for line in manifest_lines)
print(f"FOUND\t{len(manifest_lines)}\t{total_size}")
"@

Write-Host "[1/4] Scanning EC2 for files belonging to $Date..." -ForegroundColor Cyan
$scanResult = $remotePythonScript | ssh -o BatchMode=yes $Ec2Host "python3"
if ($LASTEXITCODE -ne 0 -or $scanResult -notmatch '^FOUND\t(\d+)\t(\d+)') {
    throw "Failed to scan remote files on EC2 host $Ec2Host. Output: $scanResult"
}

$matches = [regex]::Match($scanResult, '^FOUND\t(\d+)\t(\d+)')
$fileCount = [int]$matches.Groups[1].Value
$totalBytes = [int64]$matches.Groups[2].Value
$totalMb = [math]::Round($totalBytes / (1024 * 1024), 2)

if ($fileCount -eq 0) {
    Write-Host "[INFO] No files found on EC2 for date $Date." -ForegroundColor Yellow
    ssh -o BatchMode=yes $Ec2Host "rm -f /tmp/collector-$Date.manifest /tmp/collector-$Date.files"
    return
}

Write-Host "      Found $fileCount files ($totalMb MB) on EC2 for date $Date." -ForegroundColor Green

if ($DryRun) {
    Write-Host "[DRY-RUN] Remote file manifest:" -ForegroundColor Yellow
    $manifestContent = ssh -o BatchMode=yes $Ec2Host "cat /tmp/collector-$Date.manifest"
    $manifestContent -split "`n" | ForEach-Object {
        if (-not [string]::IsNullOrWhiteSpace($_)) {
            $p = $_ -split "`t"
            $sizeMb = [math]::Round([int64]$p[1] / (1024 * 1024), 2)
            Write-Host "  - $($p[0]) ($sizeMb MB)" -ForegroundColor Gray
        }
    }
    ssh -o BatchMode=yes $Ec2Host "rm -f /tmp/collector-$Date.manifest /tmp/collector-$Date.files"
    Write-Host "[DRY-RUN] Dry run completed. No files downloaded or deleted." -ForegroundColor Yellow
    return
}

# 2. Prepare local destination directory
Write-Host "[2/4] Preparing local directory: $DestinationDir..." -ForegroundColor Cyan
if (-not (Test-Path -LiteralPath $DestinationDir)) {
    New-Item -ItemType Directory -Force -Path $DestinationDir | Out-Null
}

# 3. Stream tarball from EC2 directly into local tar extraction
Write-Host "[3/4] Streaming $fileCount files ($totalMb MB) via SSH over SSM..." -ForegroundColor Cyan
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

$cmd = "ssh -o BatchMode=yes $Ec2Host ""tar -cf - -C $RemoteBaseDir -T /tmp/collector-$Date.files"" | tar -xf - -C ""$DestinationDir"""
& cmd.exe /c $cmd

if ($LASTEXITCODE -ne 0) {
    throw "Streaming transfer failed with exit code $LASTEXITCODE."
}
$stopwatch.Stop()
$elapsedSec = [math]::Round($stopwatch.Elapsed.TotalSeconds, 1)
$speedMbSec = if ($elapsedSec -gt 0) { [math]::Round($totalMb / $elapsedSec, 2) } else { 0 }
Write-Host "      Download completed in $elapsedSec s ($speedMbSec MB/s)." -ForegroundColor Green

# 4. Verify downloaded files locally against remote manifest
Write-Host "[4/4] Verifying all $fileCount downloaded files locally..." -ForegroundColor Cyan
$manifestLines = ssh -o BatchMode=yes $Ec2Host "cat /tmp/collector-$Date.manifest"

$verifiedCount = 0
$mismatches = @()

foreach ($line in ($manifestLines -split "`n")) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed)) { continue }
    $parts = $trimmed -split "`t"
    if ($parts.Length -lt 2) { continue }
    $relPath = $parts[0]
    $expectedBytes = [int64]$parts[1]

    $localFilePath = Join-Path $DestinationDir ($relPath -replace '/', '\')
    if (-not (Test-Path -LiteralPath $localFilePath -PathType Leaf)) {
        $mismatches += "Missing file: $relPath"
        continue
    }

    $localBytes = (Get-Item -LiteralPath $localFilePath).Length
    if ($localBytes -ne $expectedBytes) {
        $mismatches += "Size mismatch: $relPath (expected $expectedBytes bytes, found $localBytes bytes)"
        continue
    }

    $verifiedCount++
}

if ($verifiedCount -ne $fileCount -or $mismatches.Count -gt 0) {
    Write-Error "Verification failed! $verifiedCount of $fileCount files verified. Errors:`n$($mismatches -join "`n")"
    Write-Warning "NO FILES WERE DELETED ON EC2 due to verification failure."
    throw "Verification failed. Halting before deletion."
}

Write-Host "      Verification 100% SUCCESSFUL: All $fileCount files match expected sizes." -ForegroundColor Green

# 5. Delete verified files on EC2 if KeepRemoteFiles is not set
if (-not $KeepRemoteFiles) {
    Write-Host "[CLEANUP] Deleting $fileCount verified files from EC2 ($RemoteBaseDir)..." -ForegroundColor Cyan
    $deleteCmd = "cut -f1 /tmp/collector-$Date.manifest | sed 's|^|$RemoteBaseDir/|' | xargs -r -d '\n' sudo rm -f && rm -f /tmp/collector-$Date.manifest /tmp/collector-$Date.files"
    ssh -o BatchMode=yes $Ec2Host $deleteCmd
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Deletion command returned exit code $LASTEXITCODE. Please verify remote disk manually."
    } else {
        Write-Host "          Remote files successfully deleted from EC2." -ForegroundColor Green
    }
} else {
    Write-Host "[CLEANUP] Keeping remote files on EC2 (-KeepRemoteFiles was specified)." -ForegroundColor Yellow
    ssh -o BatchMode=yes $Ec2Host "rm -f /tmp/collector-$Date.manifest /tmp/collector-$Date.files"
}

Write-Host "`nSUCCESS: Successfully downloaded, verified, and cleaned $fileCount files ($totalMb MB) for $Date." -ForegroundColor Green
