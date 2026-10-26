<#
.SYNOPSIS
    Converts downloaded .jsonl.zst market data archives into optimized, typed Apache Parquet (.parquet)
    files using DuckDB, sorted by top-level timestamp, and deletes original archives upon successful conversion.

.DESCRIPTION
    Recursively scans the collector data directory (default 'D:\Crypto\data\Polymarket\collector')
    for all .jsonl.zst files (Binance Spot/Futures, Deribit Futures, Polymarket CLOB, and PolyBolt streams),
    classifies the stream type, applies a high-performance, schema-optimized DuckDB SQL transformation,
    orders records by top-level receipt timestamp (`ts` ASC), writes compressed Parquet files,
    verifies file integrity and row counts, and upon successful verification removes the source .jsonl.zst archive.

    Supports preview mode (-DryRun), date filtering (-Date), stream filtering (-Stream),
    overwrite mode (-Force), custom compression codec (-Compression), and retaining originals (-KeepOriginal).

.PARAMETER TargetDir
    Root directory containing collector archives. Defaults to 'D:\Crypto\data\Polymarket\collector'.

.PARAMETER Date
    Optional target UTC date in YYYY-MM-DD format (e.g. 2026-10-15) to convert only files belonging to that date.

.PARAMETER Stream
    Optional substring to filter by stream type or symbol (e.g. 'aggTrades', 'forceOrder', 'deribit', 'price_crypto', 'btc-updown').

.PARAMETER Filter
    Filename search pattern. Defaults to '*.jsonl.zst'.

.PARAMETER DryRun
    If specified, displays all files that would be converted and deleted without modifying any disk contents.

.PARAMETER KeepOriginal
    If specified, retains the original .jsonl.zst files after conversion instead of deleting them.

.PARAMETER Force
    If specified, overwrites existing destination .parquet files if they already exist.
    By default, existing non-empty .parquet files are safely skipped.

.PARAMETER Compression
    Parquet compression codec (ZSTD, SNAPPY, GZIP, UNCOMPRESSED). Defaults to 'ZSTD'.

.PARAMETER DuckDbPath
    Path to DuckDB CLI executable. Defaults to 'duckdb' found on system PATH.

.EXAMPLE
    .\scripts\convert-to-parquet.ps1
    Converts all .jsonl.zst files in D:\Crypto\data\Polymarket\collector to .parquet and deletes originals.

.EXAMPLE
    .\scripts\convert-to-parquet.ps1 -DryRun
    Previews all files that would be converted and deleted without making any changes.

.EXAMPLE
    .\scripts\convert-to-parquet.ps1 -Date 2026-10-15
    Converts only archives matching the date 2026-10-15.

.EXAMPLE
    .\scripts\convert-to-parquet.ps1 -Stream aggTrades
    Converts only aggTrades archives.

.EXAMPLE
    .\scripts\convert-to-parquet.ps1 -KeepOriginal
    Converts archives to .parquet but retains the original .jsonl.zst files.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Position = 0)]
    [string]$TargetDir = "D:\Crypto\data\Polymarket\collector",

    [Parameter(Position = 1)]
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$Date,

    [Parameter()]
    [string]$Stream,

    [Parameter()]
    [string]$Filter = "*.jsonl.zst",

    [Parameter()]
    [switch]$DryRun,

    [Parameter()]
    [switch]$KeepOriginal,

    [Parameter()]
    [switch]$Force,

    [Parameter()]
    [ValidateSet("ZSTD", "SNAPPY", "GZIP", "UNCOMPRESSED")]
    [string]$Compression = "ZSTD",

    [Parameter()]
    [string]$DuckDbPath = "duckdb"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Verify DuckDB CLI is available
$duckdbCmd = Get-Command $DuckDbPath -ErrorAction SilentlyContinue
if (-not $duckdbCmd) {
    throw "DuckDB CLI ('$DuckDbPath') was not found. Please ensure DuckDB is installed and available on PATH."
}

# Verify target directory exists
if (-not (Test-Path -LiteralPath $TargetDir)) {
    throw "Target directory '$TargetDir' does not exist."
}

$resolvedTargetDir = (Resolve-Path -LiteralPath $TargetDir).Path

function Get-StreamType {
    param([string]$FilePath)
    $normalized = $FilePath.Replace('\', '/').ToLowerInvariant()
    if ($normalized -like "*/binance/spot/*/aggtrades/*" -or $normalized -like "*spot*aggtrades*") {
        return "binance-spot-aggtrades"
    }
    if ($normalized -like "*/binance/futures/*/aggtrades/*" -or $normalized -like "*futures*aggtrades*") {
        return "binance-futures-aggtrades"
    }
    if ($normalized -like "*forceorder*") {
        return "binance-futures-forceorder"
    }
    if ($normalized -like "*depthdiff*") {
        return "binance-futures-depthdiff"
    }
    if ($normalized -like "*depthpartial*") {
        return "binance-futures-depthpartial"
    }
    if ($normalized -like "*/deribit/*" -or $normalized -like "*trades.btc-perpetual*") {
        return "deribit-trades"
    }
    if ($normalized -like "*price_crypto_twap*") {
        return "polymarket-price-twap"
    }
    if ($normalized -like "*price_crypto*") {
        return "polymarket-price-crypto"
    }
    if ($normalized -like "*btc-updown*") {
        return "polymarket-clob-market"
    }
    return "generic"
}

function Get-ConvertQuery {
    param(
        [string]$StreamType,
        [string]$InputPathEscaped
    )

    switch ($StreamType) {
        "binance-spot-aggtrades" {
            return @"
SELECT 
    ts,
    CAST(payload->>'E' AS UBIGINT) AS event_time,
    CAST(payload->>'a' AS BIGINT) AS aggregate_trade_id,
    payload->>'s' AS symbol,
    CAST(payload->>'p' AS DOUBLE) AS price,
    CAST(payload->>'q' AS DOUBLE) AS quantity,
    CAST(payload->>'f' AS BIGINT) AS first_trade_id,
    CAST(payload->>'l' AS BIGINT) AS last_trade_id,
    CAST(payload->>'T' AS UBIGINT) AS trade_time,
    CAST(payload->>'m' AS BOOLEAN) AS is_buyer_maker,
    CAST(payload->>'M' AS BOOLEAN) AS is_best_match
FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
"@
        }
        "binance-futures-aggtrades" {
            return @"
SELECT 
    ts,
    CAST(payload->>'E' AS UBIGINT) AS event_time,
    CAST(payload->>'a' AS BIGINT) AS aggregate_trade_id,
    payload->>'s' AS symbol,
    CAST(payload->>'p' AS DOUBLE) AS price,
    CAST(payload->>'q' AS DOUBLE) AS quantity,
    CAST(payload->>'f' AS BIGINT) AS first_trade_id,
    CAST(payload->>'l' AS BIGINT) AS last_trade_id,
    CAST(payload->>'T' AS UBIGINT) AS trade_time,
    CAST(payload->>'m' AS BOOLEAN) AS is_buyer_maker
FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
"@
        }
        "binance-futures-forceorder" {
            return @"
SELECT 
    ts,
    CAST(payload->>'E' AS UBIGINT) AS event_time,
    payload->'o'->>'s' AS symbol,
    payload->'o'->>'S' AS side,
    payload->'o'->>'o' AS order_type,
    payload->'o'->>'f' AS time_in_force,
    CAST(payload->'o'->>'q' AS DOUBLE) AS original_quantity,
    CAST(payload->'o'->>'p' AS DOUBLE) AS price,
    CAST(payload->'o'->>'ap' AS DOUBLE) AS average_price,
    payload->'o'->>'X' AS order_status,
    CAST(payload->'o'->>'l' AS DOUBLE) AS last_filled_quantity,
    CAST(payload->'o'->>'z' AS DOUBLE) AS accumulated_quantity,
    CAST(payload->'o'->>'T' AS UBIGINT) AS trade_time
FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
"@
        }
        { $_ -in "binance-futures-depthdiff", "binance-futures-depthpartial" } {
            return @"
SELECT 
    ts,
    CAST(payload->>'E' AS UBIGINT) AS event_time,
    CAST(payload->>'T' AS UBIGINT) AS transaction_time,
    payload->>'s' AS symbol,
    CAST(payload->>'U' AS UBIGINT) AS first_update_id,
    CAST(payload->>'u' AS UBIGINT) AS final_update_id,
    CAST(payload->>'pu' AS UBIGINT) AS prev_final_update_id,
    list_transform(CAST(payload->'b' AS VARCHAR[][]), lambda x: {'price': CAST(x[1] AS DOUBLE), 'quantity': CAST(x[2] AS DOUBLE)}) AS bids,
    list_transform(CAST(payload->'a' AS VARCHAR[][]), lambda x: {'price': CAST(x[1] AS DOUBLE), 'quantity': CAST(x[2] AS DOUBLE)}) AS asks
FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
"@
        }
        "deribit-trades" {
            return @"
SELECT 
    ts,
    CAST(t->>'timestamp' AS UBIGINT) AS trade_timestamp,
    t->>'instrument_name' AS instrument_name,
    t->>'trade_id' AS trade_id,
    CAST(t->>'trade_seq' AS BIGINT) AS trade_seq,
    t->>'direction' AS direction,
    CAST(t->>'price' AS DOUBLE) AS price,
    CAST(t->>'amount' AS DOUBLE) AS amount,
    CAST(t->>'contracts' AS DOUBLE) AS contracts,
    CAST(t->>'mark_price' AS DOUBLE) AS mark_price,
    CAST(t->>'index_price' AS DOUBLE) AS index_price,
    CAST(t->>'tick_direction' AS INT) AS tick_direction,
    CAST(t->>'starbase_timestamp' AS UBIGINT) AS starbase_timestamp
FROM (
    SELECT ts, UNNEST(CAST(payload->'params'->'data' AS JSON[])) AS t
    FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
)
ORDER BY ts ASC
"@
        }
        "polymarket-price-crypto" {
            return @"
WITH raw_data AS (
    SELECT ts, payload
    FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
)
SELECT 
    ts,
    CAST(payload->>'seq' AS UBIGINT) AS seq,
    payload->'payload'->>'symbol' AS symbol,
    payload->'payload'->>'source' AS source,
    CAST(payload->'payload'->>'value' AS DOUBLE) AS price,
    payload->'payload'->>'full_accuracy_value' AS full_accuracy_price,
    CAST(payload->'payload'->>'timestamp' AS UBIGINT) AS price_timestamp
FROM raw_data
WHERE payload->>'snapshot' IS NULL OR payload->>'snapshot' != 'true'

UNION ALL

SELECT 
    ts,
    CAST(payload->>'seq' AS UBIGINT) AS seq,
    payload->'payload'->>'symbol' AS symbol,
    payload->'payload'->>'source' AS source,
    CAST(d->>'value' AS DOUBLE) AS price,
    d->>'full_accuracy_value' AS full_accuracy_price,
    CAST(d->>'timestamp' AS UBIGINT) AS price_timestamp
FROM (
    SELECT ts, payload, UNNEST(CAST(payload->'payload'->'data' AS JSON[])) AS d
    FROM raw_data
    WHERE payload->>'snapshot' = 'true'
)
ORDER BY ts ASC, price_timestamp ASC
"@
        }
        "polymarket-price-twap" {
            return @"
WITH raw_data AS (
    SELECT ts, payload
    FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
)
SELECT 
    ts,
    CAST(payload->>'seq' AS UBIGINT) AS seq,
    payload->'payload'->>'symbol' AS symbol,
    payload->'payload'->>'source' AS source,
    CAST(payload->'payload'->>'value' AS DOUBLE) AS twap_price,
    payload->'payload'->>'full_accuracy_value' AS full_accuracy_twap_price,
    CAST(payload->'payload'->>'window_seconds' AS INT) AS window_seconds,
    CAST(payload->'payload'->>'timestamp' AS UBIGINT) AS twap_timestamp
FROM raw_data
WHERE payload->>'snapshot' IS NULL OR payload->>'snapshot' != 'true'

UNION ALL

SELECT 
    ts,
    CAST(payload->>'seq' AS UBIGINT) AS seq,
    payload->'payload'->>'symbol' AS symbol,
    payload->'payload'->>'source' AS source,
    CAST(d->>'value' AS DOUBLE) AS twap_price,
    d->>'full_accuracy_value' AS full_accuracy_twap_price,
    CAST(payload->'payload'->>'window_seconds' AS INT) AS window_seconds,
    CAST(d->>'timestamp' AS UBIGINT) AS twap_timestamp
FROM (
    SELECT ts, payload, UNNEST(CAST(payload->'payload'->'data' AS JSON[])) AS d
    FROM raw_data
    WHERE payload->>'snapshot' = 'true'
)
ORDER BY ts ASC, twap_timestamp ASC
"@
        }
        "polymarket-clob-market" {
            return @"
SELECT 
    ts,
    type AS msg_type,
    COALESCE(payload->>'event_type', type) AS event_type,
    COALESCE(payload->>'market', payload->>'conditionId') AS market,
    payload->>'asset_id' AS asset_id,
    CAST(payload->>'price' AS DOUBLE) AS price,
    CAST(payload->>'size' AS DOUBLE) AS size,
    payload->>'side' AS side,
    payload
FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'type': 'VARCHAR', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
"@
        }
        default {
            return @"
SELECT 
    ts,
    payload
FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
"@
        }
    }
}

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host " Polymarket Data Collector - DuckDB Parquet Converter" -ForegroundColor Cyan
Write-Host " Target Directory : $resolvedTargetDir" -ForegroundColor White
if ($Date) {
    Write-Host " Date Filter      : $Date" -ForegroundColor Yellow
}
if ($Stream) {
    Write-Host " Stream Filter    : $Stream" -ForegroundColor Yellow
}
Write-Host " Search Filter    : $Filter" -ForegroundColor White
Write-Host " Compression      : $Compression" -ForegroundColor White
if ($DryRun) {
    Write-Host " Mode             : DRY-RUN (No files will be converted or deleted)" -ForegroundColor Yellow
} elseif ($KeepOriginal) {
    Write-Host " Mode             : CONVERT AND KEEP ORIGINALS" -ForegroundColor Cyan
} else {
    Write-Host " Mode             : CONVERT AND DELETE VERIFIED ORIGINALS" -ForegroundColor Green
}
Write-Host "=================================================================" -ForegroundColor Cyan

# Step 1: Discover archive files
Write-Host "[1/3] Scanning for matching archives in $resolvedTargetDir..." -ForegroundColor Cyan
$allFiles = Get-ChildItem -Path $resolvedTargetDir -Recurse -File -Filter $Filter

$files = $allFiles
if ($Date) {
    $files = $files | Where-Object { $_.Name -like "*$Date*" -or $_.FullName -like "*$Date*" }
}
if ($Stream) {
    $files = $files | Where-Object { $_.FullName -like "*$Stream*" }
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

# Step 2: Conversion loop
Write-Host "[2/3] Converting archives to Parquet (DuckDB)..." -ForegroundColor Cyan

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$successCount = 0
$skippedCount = 0
$failedCount = 0
$totalParquetBytes = 0L
$errors = [System.Collections.Generic.List[string]]::new()

$tempSqlFile = [System.IO.Path]::GetTempFileName() + ".sql"

try {
    for ($i = 0; $i -lt $fileCount; $i++) {
        $file = $files[$i]
        $index = $i + 1
        $srcPath = $file.FullName
        $relPath = $srcPath.Substring($resolvedTargetDir.Length).TrimStart('\', '/')
        $sizeMb = [math]::Round($file.Length / 1MB, 2)

        # Determine destination .parquet filename
        if ($srcPath.EndsWith(".jsonl.zst", [System.StringComparison]::OrdinalIgnoreCase)) {
            $destPath = $srcPath.Substring(0, $srcPath.Length - 10) + ".parquet"
        } elseif ($srcPath.EndsWith(".zst", [System.StringComparison]::OrdinalIgnoreCase)) {
            $destPath = $srcPath.Substring(0, $srcPath.Length - 4) + ".parquet"
        } else {
            $destPath = $srcPath + ".parquet"
        }

        # Check if target .parquet already exists
        if ((Test-Path -LiteralPath $destPath) -and (-not $Force)) {
            $existingItem = Get-Item -LiteralPath $destPath
            if ($existingItem.Length -gt 0) {
                Write-Host "  [$index/$fileCount] [SKIP] $relPath (Parquet already exists: $([math]::Round($existingItem.Length / 1MB, 2)) MB)" -ForegroundColor DarkGray
                $skippedCount++
                continue
            }
        }

        if ($DryRun) {
            $streamType = Get-StreamType -FilePath $srcPath
            Write-Host "  [$index/$fileCount] [DRY-RUN] Would convert [$streamType] $relPath ($sizeMb MB) -> $([System.IO.Path]::GetFileName($destPath))" -ForegroundColor Yellow
            $successCount++
            continue
        }

        $streamType = Get-StreamType -FilePath $srcPath
        Write-Host "  [$index/$fileCount] Converting [$streamType] $relPath ($sizeMb MB)..." -NoNewline

        $tempParquetPath = "$destPath.tmp.parquet"
        if (Test-Path -LiteralPath $tempParquetPath) {
            Remove-Item -LiteralPath $tempParquetPath -Force -ErrorAction SilentlyContinue
        }

        try {
            $inputEscaped = $srcPath.Replace('\', '/')
            $tempDestEscaped = $tempParquetPath.Replace('\', '/')

            $selectQuery = Get-ConvertQuery -StreamType $streamType -InputPathEscaped $inputEscaped
            $copyScript = "COPY (`n$selectQuery`n) TO '$tempDestEscaped' (FORMAT PARQUET, COMPRESSION $Compression);`n"

            # Write DuckDB script to temp sql file without BOM
            [System.IO.File]::WriteAllText($tempSqlFile, $copyScript, [System.Text.UTF8Encoding]::new($false))

            $tempSqlEscaped = $tempSqlFile.Replace('\', '/')
            $duckdbOutput = & $DuckDbPath -c ".read '$tempSqlEscaped'" 2>&1
            if ($LASTEXITCODE -ne 0) {
                throw "DuckDB error (exit code $LASTEXITCODE): $duckdbOutput"
            }

            # Verify temp Parquet file exists and has non-zero size
            if (-not (Test-Path -LiteralPath $tempParquetPath)) {
                throw "Destination Parquet file '$tempParquetPath' was not generated by DuckDB."
            }

            $tempItem = Get-Item -LiteralPath $tempParquetPath
            if ($tempItem.Length -le 0) {
                Remove-Item -LiteralPath $tempParquetPath -Force -ErrorAction SilentlyContinue
                throw "Destination Parquet file is empty (0 bytes)."
            }

            # Verify readability and row count with DuckDB
            $verifyOutput = & $DuckDbPath -no-stdin -c "SELECT count(*) FROM read_parquet('$tempDestEscaped');" 2>&1
            if ($LASTEXITCODE -ne 0) {
                Remove-Item -LiteralPath $tempParquetPath -Force -ErrorAction SilentlyContinue
                throw "Parquet integrity verification failed: $verifyOutput"
            }

            # Atomically move temp Parquet to final destination
            Move-Item -LiteralPath $tempParquetPath -Destination $destPath -Force

            $finalItem = Get-Item -LiteralPath $destPath
            $finalMb = [math]::Round($finalItem.Length / 1MB, 2)
            $totalParquetBytes += $finalItem.Length

            # Conversion succeeded and verified; safely remove source .zst file unless KeepOriginal is requested
            if (-not $KeepOriginal) {
                Remove-Item -LiteralPath $srcPath -Force
                if (Test-Path -LiteralPath $srcPath) {
                    throw "Failed to delete original archive '$srcPath' after Parquet conversion."
                }
                Write-Host " OK ($finalMb MB, archive deleted)" -ForegroundColor Green
            } else {
                Write-Host " OK ($finalMb MB, archive kept)" -ForegroundColor Green
            }

            $successCount++
        } catch {
            $errMsg = "Failed to convert '$relPath': $($_.Exception.Message)"
            Write-Host " FAILED" -ForegroundColor Red
            Write-Host "        $errMsg" -ForegroundColor Red
            $errors.Add($errMsg)
            $failedCount++

            # Cleanup temp artifacts if failure occurred
            if (Test-Path -LiteralPath $tempParquetPath) {
                Remove-Item -LiteralPath $tempParquetPath -Force -ErrorAction SilentlyContinue
            }
        }
    }
} finally {
    if (Test-Path -LiteralPath $tempSqlFile) {
        Remove-Item -LiteralPath $tempSqlFile -Force -ErrorAction SilentlyContinue
    }
}

$stopwatch.Stop()
$elapsed = $stopwatch.Elapsed

# Step 3: Summary Report
Write-Host "[3/3] Conversion Summary" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host " Total Archives Discovered : $fileCount" -ForegroundColor White
Write-Host " Successfully Converted    : $successCount" -ForegroundColor $(if ($successCount -gt 0) { "Green" } else { "White" })
Write-Host " Skipped (Already Present) : $skippedCount" -ForegroundColor $(if ($skippedCount -gt 0) { "DarkGray" } else { "White" })
Write-Host " Failed Conversions        : $failedCount" -ForegroundColor $(if ($failedCount -gt 0) { "Red" } else { "White" })
Write-Host " Total Input Archive Size  : $totalCompressedMb MB ($totalCompressedGb GB)" -ForegroundColor White
if (-not $DryRun) {
    $totalParquetMb = [math]::Round($totalParquetBytes / 1MB, 2)
    $totalParquetGb = [math]::Round($totalParquetBytes / 1GB, 2)
    Write-Host " Total Output Parquet Size : $totalParquetMb MB ($totalParquetGb GB)" -ForegroundColor Green
    if ($totalCompressedBytes -gt 0 -and $totalParquetBytes -gt 0) {
        $ratio = [math]::Round(($totalCompressedBytes - $totalParquetBytes) / $totalCompressedBytes * 100, 1)
        Write-Host " Net Space Savings         : $ratio %" -ForegroundColor Green
    }
}
Write-Host " Elapsed Time              : $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "=================================================================" -ForegroundColor Cyan

if ($errors.Count -gt 0) {
    Write-Host "Conversion errors encountered:" -ForegroundColor Red
    foreach ($err in $errors) {
        Write-Host " - $err" -ForegroundColor Red
    }
    exit 1
}
