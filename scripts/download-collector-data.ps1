<#
.SYNOPSIS
    Downloads daily Zstandard (.jsonl.zst) market data from the AWS EC2 Data Collector instance to local disk,
    maintaining the identical directory structure, verifies file integrity, and deletes verified files from EC2.

.DESCRIPTION
    Connects to the Data Collector EC2 instance (via SSH/SSM tunnel), discovers all daily stream files and per-market
    Polymarket files for the specified UTC date (YYYY-MM-DD), streams them via tar directly into the local destination
    directory without creating intermediate archives on EC2, verifies every file's byte size locally, and upon 100%
    verification deletes the downloaded files from the remote EBS volume.

.PARAMETER Date
    Target UTC date in YYYY-MM-DD format (e.g. 2026-10-12, 2026-10-15).

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

.EXAMPLE
    .\scripts\download-collector-data.ps1 -Date 2026-10-12
    .\scripts\download-collector-data.ps1 -Date 2026-10-15 -DryRun
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0, HelpMessage = "Target UTC date in YYYY-MM-DD format")]
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$Date,

    [Parameter(Position = 1)]
    [string]$DestinationDir = "D:\Crypto\data\Polymarket\collector",

    [Parameter()]
    [string]$Ec2Host = "i-08fe8a64d89f05ea9",

    [Parameter()]
    [string]$RemoteBaseDir = "/mnt/ebs/polymarket-data/collector",

    [Parameter()]
    [switch]$DryRun,

    [Parameter()]
    [switch]$KeepRemoteFiles
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Ensure SessionManagerPlugin and SSH are in PATH
$pluginBin = "C:\Program Files\Amazon\SessionManagerPlugin\bin"
if ($env:PATH -notlike "*$pluginBin*") {
    $env:PATH += ";$pluginBin"
}

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
