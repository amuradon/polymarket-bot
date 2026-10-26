<#
.SYNOPSIS
    Extracts downloaded Zstandard (.jsonl.zst) market data archives into plain .jsonl files in
    D:\Crypto\data\Polymarket\collector and deletes the original .zst archives upon successful extraction.

.DESCRIPTION
    Recursively scans the collector data directory (default 'D:\Crypto\data\Polymarket\collector')
    for all .jsonl.zst files (Binance, Deribit, Polymarket CLOB, and PolyBolt streams), decompresses
    each archive to .jsonl using zstd, verifies the integrity and non-zero size of the extracted file,
    and upon 100% verification removes the original .jsonl.zst archive.
    Includes support for preview mode (-DryRun), date filtering (-Date), overwrite mode (-Force),
    and preserving originals (-KeepOriginal).

.PARAMETER TargetDir
    Root directory containing collector archives. Defaults to 'D:\Crypto\data\Polymarket\collector'.

.PARAMETER Date
    Optional target UTC date in YYYY-MM-DD format (e.g. 2026-10-15) to extract only files belonging to that date.

.PARAMETER Filter
    Filename search pattern. Defaults to '*.jsonl.zst'.

.PARAMETER DryRun
    If specified, displays all files that would be extracted and deleted without modifying any disk contents.

.PARAMETER KeepOriginal
    If specified, retains the original .jsonl.zst files after extraction instead of deleting them.

.PARAMETER Force
    If specified, overwrites existing destination .jsonl files if they already exist.
    By default, existing non-empty .jsonl files are safely skipped.

.EXAMPLE
    .\scripts\extract-collector-data.ps1
    Extracts all .jsonl.zst files in D:\Crypto\data\Polymarket\collector and deletes originals.

.EXAMPLE
    .\scripts\extract-collector-data.ps1 -DryRun
    Previews all files that would be extracted and deleted without making any changes.

.EXAMPLE
    .\scripts\extract-collector-data.ps1 -Date 2026-10-15
    Extracts only archives matching the date 2026-10-15 and deletes their originals.

.EXAMPLE
    .\scripts\extract-collector-data.ps1 -KeepOriginal
    Extracts archives to .jsonl but retains the original .jsonl.zst files.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Position = 0)]
    [string]$TargetDir = "D:\Crypto\data\Polymarket\collector",

    [Parameter(Position = 1)]
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$Date,

    [Parameter()]
    [string]$Filter = "*.jsonl.zst",

    [Parameter()]
    [switch]$DryRun,

    [Parameter()]
    [switch]$KeepOriginal,

    [Parameter()]
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Verify zstd CLI is available
$zstdCmd = Get-Command "zstd" -ErrorAction SilentlyContinue
if (-not $zstdCmd) {
    throw "zstd CLI was not found in PATH. Please install Zstandard (e.g. 'winget install Facebook.Zstandard') or add it to PATH."
}

# Verify target directory exists
if (-not (Test-Path -LiteralPath $TargetDir)) {
    throw "Target directory '$TargetDir' does not exist."
}

$resolvedTargetDir = (Resolve-Path -LiteralPath $TargetDir).Path

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host " Polymarket Data Collector - Zstandard Archive Extractor" -ForegroundColor Cyan
Write-Host " Target Directory : $resolvedTargetDir" -ForegroundColor White
if ($Date) {
    Write-Host " Date Filter      : $Date" -ForegroundColor Yellow
}
Write-Host " Search Filter    : $Filter" -ForegroundColor White
if ($DryRun) {
    Write-Host " Mode             : DRY-RUN (No files will be extracted or deleted)" -ForegroundColor Yellow
} elseif ($KeepOriginal) {
    Write-Host " Mode             : EXTRACT AND KEEP ORIGINALS" -ForegroundColor Cyan
} else {
    Write-Host " Mode             : EXTRACT AND DELETE VERIFIED ORIGINALS" -ForegroundColor Green
}
Write-Host "=================================================================" -ForegroundColor Cyan

# Step 1: Discover archive files
Write-Host "[1/3] Scanning for matching archives in $resolvedTargetDir..." -ForegroundColor Cyan
$allFiles = Get-ChildItem -Path $resolvedTargetDir -Recurse -File -Filter $Filter

if ($Date) {
    $files = $allFiles | Where-Object { $_.Name -like "*$Date*" -or $_.FullName -like "*$Date*" }
} else {
    $files = $allFiles
}

$fileCount = if ($files) { @($files).Count } else { 0 }
if ($fileCount -eq 0) {
    Write-Host "No matching archives found in '$resolvedTargetDir'." -ForegroundColor Yellow
    return
}

$totalCompressedBytes = ($files | Measure-Object -Property Length -Sum).Sum
$totalCompressedMb = [math]::Round($totalCompressedBytes / 1MB, 2)
$totalCompressedGb = [math]::Round($totalCompressedBytes / 1GB, 2)

Write-Host "      Discovered $fileCount archives (Total: $totalCompressedMb MB / $totalCompressedGb GB)." -ForegroundColor Green

# Step 2: Extraction loop
Write-Host "[2/3] Processing archives..." -ForegroundColor Cyan

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$successCount = 0
$skippedCount = 0
$failedCount = 0
$totalExtractedBytes = 0L
$errors = [System.Collections.Generic.List[string]]::new()

for ($i = 0; $i -lt $fileCount; $i++) {
    $file = $files[$i]
    $index = $i + 1
    $srcPath = $file.FullName
    $relPath = $srcPath.Substring($resolvedTargetDir.Length).TrimStart('\', '/')
    $sizeMb = [math]::Round($file.Length / 1MB, 2)

    # Determine destination .jsonl filename (strip .zst)
    if ($srcPath.EndsWith(".zst", [System.StringComparison]::OrdinalIgnoreCase)) {
        $destPath = $srcPath.Substring(0, $srcPath.Length - 4)
    } else {
        $destPath = $srcPath + ".jsonl"
    }

    # Check if target .jsonl already exists
    if ((Test-Path -LiteralPath $destPath) -and (-not $Force)) {
        $existingItem = Get-Item -LiteralPath $destPath
        if ($existingItem.Length -gt 0) {
            Write-Host "  [$index/$fileCount] [SKIP] $relPath (Output already exists: $([math]::Round($existingItem.Length / 1MB, 2)) MB)" -ForegroundColor DarkGray
            $skippedCount++
            continue
        }
    }

    if ($DryRun) {
        Write-Host "  [$index/$fileCount] [DRY-RUN] Would extract $relPath ($sizeMb MB) -> $([System.IO.Path]::GetFileName($destPath))" -ForegroundColor Yellow
        $successCount++
        continue
    }

    Write-Host "  [$index/$fileCount] Extracting $relPath ($sizeMb MB)..." -NoNewline

    try {
        # Decompress using zstd to target .jsonl file
        # -d: decompress, -q: quiet, -f: overwrite destination if Force is set
        $zstdProcess = Start-Process -FilePath "zstd" -ArgumentList @("-d", "-q", "-f", "`"$srcPath`"", "-o", "`"$destPath`"") -NoNewWindow -Wait -PassThru

        if ($zstdProcess.ExitCode -ne 0) {
            throw "zstd exited with non-zero code ($($zstdProcess.ExitCode))."
        }

        # Verify extracted file exists and has non-zero size
        if (-not (Test-Path -LiteralPath $destPath)) {
            throw "Extracted file '$destPath' was not found on disk after zstd execution."
        }

        $destItem = Get-Item -LiteralPath $destPath
        if ($destItem.Length -le 0) {
            # Incomplete or empty file; clean up and abort deletion of original
            Remove-Item -LiteralPath $destPath -Force -ErrorAction SilentlyContinue
            throw "Extracted file '$destPath' is empty (0 bytes)."
        }

        $destMb = [math]::Round($destItem.Length / 1MB, 2)
        $totalExtractedBytes += $destItem.Length

        # Decompression succeeded and verified; safely remove source .zst file unless KeepOriginal is requested
        if (-not $KeepOriginal) {
            Remove-Item -LiteralPath $srcPath -Force
            if (Test-Path -LiteralPath $srcPath) {
                throw "Failed to delete original archive '$srcPath' after extraction."
            }
            Write-Host " OK ($destMb MB, archive deleted)" -ForegroundColor Green
        } else {
            Write-Host " OK ($destMb MB, archive kept)" -ForegroundColor Green
        }

        $successCount++
    } catch {
        $errMsg = "Failed to extract '$relPath': $($_.Exception.Message)"
        Write-Host " FAILED" -ForegroundColor Red
        Write-Host "        $errMsg" -ForegroundColor Red
        $errors.Add($errMsg)
        $failedCount++

        # Ensure corrupted or empty destination file is removed so it does not mask the archive
        if (Test-Path -LiteralPath $destPath) {
            $checkDest = Get-Item -LiteralPath $destPath -ErrorAction SilentlyContinue
            if ($checkDest -and $checkDest.Length -eq 0) {
                Remove-Item -LiteralPath $destPath -Force -ErrorAction SilentlyContinue
            }
        }
    }
}

$stopwatch.Stop()
$elapsed = $stopwatch.Elapsed

# Step 3: Summary Report
Write-Host "[3/3] Extraction Summary" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host " Total Archives Discovered : $fileCount" -ForegroundColor White
Write-Host " Successfully Extracted    : $successCount" -ForegroundColor $(if ($successCount -gt 0) { "Green" } else { "White" })
Write-Host " Skipped (Already Present) : $skippedCount" -ForegroundColor $(if ($skippedCount -gt 0) { "DarkGray" } else { "White" })
Write-Host " Failed Extractions        : $failedCount" -ForegroundColor $(if ($failedCount -gt 0) { "Red" } else { "White" })
Write-Host " Total Compressed Size     : $totalCompressedMb MB ($totalCompressedGb GB)" -ForegroundColor White
if (-not $DryRun) {
    $totalExtractedMb = [math]::Round($totalExtractedBytes / 1MB, 2)
    $totalExtractedGb = [math]::Round($totalExtractedBytes / 1GB, 2)
    Write-Host " Total Uncompressed Size   : $totalExtractedMb MB ($totalExtractedGb GB)" -ForegroundColor Green
}
Write-Host " Elapsed Time              : $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "=================================================================" -ForegroundColor Cyan

if ($errors.Count -gt 0) {
    Write-Host "Extraction errors encountered:" -ForegroundColor Red
    foreach ($err in $errors) {
        Write-Host " - $err" -ForegroundColor Red
    }
    exit 1
}
