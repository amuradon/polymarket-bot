<#
.SYNOPSIS
    Converts downloaded .jsonl.zst market data archives into optimized, typed Apache Parquet (.parquet)
    files using DuckDB, sorted by top-level timestamp, and deletes original archives upon successful conversion.

.DESCRIPTION
    Recursively scans the collector data directory (default 'D:\Crypto\data\Polymarket\collector')
    for all .jsonl.zst files (Binance Spot/Futures bookTicker, Polymarket CLOB, and PolyBolt streams),
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
    Optional substring to filter by stream type or symbol (e.g. 'bookTicker', 'price_crypto', 'btc-updown').

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

.PARAMETER MemoryLimit
    DuckDB max memory limit (e.g. '5GB', '4GB'). Defaults to '5GB'.

.PARAMETER Threads
    Number of worker threads. Defaults to min(ProcessorCount, 4).

.PARAMETER RowGroupSize
    Maximum number of rows per Parquet row group. Defaults to 50000 (100000 for Polymarket CLOB, Kraken, Coinbase, and Binance bookTicker streams).

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
    [string]$DuckDbPath = "duckdb",

    [Parameter()]
    [string]$MemoryLimit = "5GB",

    [Parameter()]
    [int]$Threads = [Math]::Min([Environment]::ProcessorCount, 4),

    [Parameter()]
    [int]$RowGroupSize = 50000,

    [Parameter()]
    [string]$TempDir
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
    if ($normalized -like "*/binance/spot/*/bookticker/*" -or $normalized -like "*spot*bookticker*") {
        return "binance-spot-bookticker"
    }
    if ($normalized -like "*/binance/futures/*/bookticker/*" -or $normalized -like "*futures*bookticker*") {
        return "binance-futures-bookticker"
    }
    if ($normalized -like "*price_crypto_twap*") {
        return "polymarket-price-twap"
    }
    if ($normalized -like "*price_crypto*") {
        return "polymarket-price-crypto"
    }
    if ($normalized -like "*-updown-*") {
        return "polymarket-clob-market"
    }
    return $null
}

function Get-TypedClobSelectQuery {
    param([string]$RawSourceSql)
    return @"
WITH raw_numbered AS (
$RawSourceSql
),
tokens AS (
    SELECT
        CAST(payload->>'clobTokenIds' AS JSON)->>0 AS up_tok,
        CAST(payload->>'clobTokenIds' AS JSON)->>1 AS down_tok
    FROM raw_numbered
    WHERE event_type = 'market_discovered'
      AND payload->>'clobTokenIds' IS NOT NULL
    LIMIT 1
),
discovered_rows AS (
    SELECT
        r.ts,
        0::UBIGINT AS server_ts,
        0::UINTEGER AS frame_id,
        0::UTINYINT AS event_code,
        NULL::BOOLEAN AS is_up,
        NULL::BOOLEAN AS is_bid,
        NULL::USMALLINT AS tick,
        NULL::DOUBLE AS size,
        t.up_tok AS up_token_id,
        t.down_tok AS down_token_id
    FROM (SELECT MIN(ts) AS ts FROM raw_numbered WHERE event_type = 'market_discovered') r
    CROSS JOIN tokens t
    WHERE r.ts IS NOT NULL
),
unwrapped_objects AS (
    SELECT
        r.ts,
        r.frame_id,
        0 AS sub_idx,
        COALESCE(r.payload->>'event_type', r.event_type) AS obj_event_type,
        r.payload AS obj
    FROM raw_numbered r
    WHERE r.event_type != 'market_discovered'
      AND json_type(r.payload) = 'OBJECT'
    UNION ALL
    SELECT
        r.ts,
        r.frame_id,
        u.ord::INTEGER AS sub_idx,
        COALESCE(
            u.obj->>'event_type',
            CASE WHEN (u.obj->'bids') IS NOT NULL OR (u.obj->'asks') IS NOT NULL THEN 'book' ELSE '' END
        ) AS obj_event_type,
        u.obj AS obj
    FROM raw_numbered r, UNNEST(CAST(r.payload AS JSON[])) WITH ORDINALITY AS u(obj, ord)
    WHERE r.event_type != 'market_discovered'
      AND json_type(r.payload) = 'ARRAY'
      AND json_array_length(r.payload) > 0
),
book_objects AS (
    SELECT ts, frame_id, sub_idx, obj
    FROM unwrapped_objects
    WHERE obj_event_type = 'book'
),
book_rows AS (
    SELECT
        b.ts,
        COALESCE(TRY_CAST(b.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (b.frame_id * 8 + b.sub_idx * 2)::UINTEGER AS frame_id,
        1::UTINYINT AS event_code,
        (b.obj->>'asset_id' = t.up_tok) AS is_up,
        true AS is_bid,
        CAST(LEAST(10000, GREATEST(0, ROUND(CAST(lvl->>'price' AS DOUBLE) * 10000.0))) AS USMALLINT) AS tick,
        CAST(lvl->>'size' AS DOUBLE) AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM book_objects b CROSS JOIN tokens t, UNNEST(CAST(b.obj->'bids' AS JSON[])) AS l(lvl)
    WHERE TRY_CAST(lvl->>'size' AS DOUBLE) > 0.0
    UNION ALL
    SELECT
        b.ts,
        COALESCE(TRY_CAST(b.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (b.frame_id * 8 + b.sub_idx * 2)::UINTEGER AS frame_id,
        1::UTINYINT AS event_code,
        (b.obj->>'asset_id' = t.up_tok) AS is_up,
        false AS is_bid,
        CAST(LEAST(10000, GREATEST(0, ROUND(CAST(lvl->>'price' AS DOUBLE) * 10000.0))) AS USMALLINT) AS tick,
        CAST(lvl->>'size' AS DOUBLE) AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM book_objects b CROSS JOIN tokens t, UNNEST(CAST(b.obj->'asks' AS JSON[])) AS l(lvl)
    WHERE TRY_CAST(lvl->>'size' AS DOUBLE) > 0.0
    UNION ALL
    SELECT
        b.ts,
        COALESCE(TRY_CAST(b.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (b.frame_id * 8 + b.sub_idx * 2)::UINTEGER AS frame_id,
        1::UTINYINT AS event_code,
        (b.obj->>'asset_id' = t.up_tok) AS is_up,
        NULL::BOOLEAN AS is_bid,
        NULL::USMALLINT AS tick,
        0.0::DOUBLE AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM book_objects b CROSS JOIN tokens t
    WHERE COALESCE(json_array_length(b.obj->'bids'), 0) = 0
      AND COALESCE(json_array_length(b.obj->'asks'), 0) = 0
),
pc_objects AS (
    SELECT ts, frame_id, sub_idx, obj
    FROM unwrapped_objects
    WHERE obj_event_type = 'price_change'
),
price_change_rows AS (
    SELECT
        pco.ts,
        COALESCE(TRY_CAST(pco.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (pco.frame_id * 8 + pco.sub_idx * 2)::UINTEGER AS frame_id,
        2::UTINYINT AS event_code,
        (COALESCE(pc->>'asset_id', pco.obj->>'asset_id') = t.up_tok) AS is_up,
        (pc->>'side' IN ('BUY', 'buy', 'BID', 'bid')) AS is_bid,
        CAST(LEAST(10000, GREATEST(0, ROUND(CAST(pc->>'price' AS DOUBLE) * 10000.0))) AS USMALLINT) AS tick,
        CAST(pc->>'size' AS DOUBLE) AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM pc_objects pco CROSS JOIN tokens t, UNNEST(CAST(pco.obj->'price_changes' AS JSON[])) AS p(pc)
    WHERE TRY_CAST(pc->>'price' AS DOUBLE) > 0.0
),
bbo_hint_rows AS (
    SELECT DISTINCT
        pco.ts,
        COALESCE(TRY_CAST(pco.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (pco.frame_id * 8 + pco.sub_idx * 2 + 1)::UINTEGER AS frame_id,
        4::UTINYINT AS event_code,
        (COALESCE(pc->>'asset_id', pco.obj->>'asset_id') = t.up_tok) AS is_up,
        true AS is_bid,
        CAST(LEAST(10000, GREATEST(1, ROUND(CAST(pc->>'best_bid' AS DOUBLE) * 10000.0))) AS USMALLINT) AS tick,
        0.0::DOUBLE AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM pc_objects pco CROSS JOIN tokens t, UNNEST(CAST(pco.obj->'price_changes' AS JSON[])) AS p(pc)
    WHERE TRY_CAST(pc->>'best_bid' AS DOUBLE) > 0.0 AND TRY_CAST(pc->>'best_bid' AS DOUBLE) <= 1.0
    UNION ALL
    SELECT DISTINCT
        pco.ts,
        COALESCE(TRY_CAST(pco.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (pco.frame_id * 8 + pco.sub_idx * 2 + 1)::UINTEGER AS frame_id,
        4::UTINYINT AS event_code,
        (COALESCE(pc->>'asset_id', pco.obj->>'asset_id') = t.up_tok) AS is_up,
        false AS is_bid,
        CAST(LEAST(10000, GREATEST(1, ROUND(CAST(pc->>'best_ask' AS DOUBLE) * 10000.0))) AS USMALLINT) AS tick,
        0.0::DOUBLE AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM pc_objects pco CROSS JOIN tokens t, UNNEST(CAST(pco.obj->'price_changes' AS JSON[])) AS p(pc)
    WHERE TRY_CAST(pc->>'best_ask' AS DOUBLE) > 0.0 AND TRY_CAST(pc->>'best_ask' AS DOUBLE) <= 1.0
    UNION ALL
    SELECT
        bba.ts,
        COALESCE(TRY_CAST(bba.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (bba.frame_id * 8 + bba.sub_idx * 2)::UINTEGER AS frame_id,
        4::UTINYINT AS event_code,
        (bba.obj->>'asset_id' = t.up_tok) AS is_up,
        true AS is_bid,
        CAST(LEAST(10000, GREATEST(1, ROUND(CAST(bba.obj->>'best_bid' AS DOUBLE) * 10000.0))) AS USMALLINT) AS tick,
        0.0::DOUBLE AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM unwrapped_objects bba CROSS JOIN tokens t
    WHERE bba.obj_event_type = 'best_bid_ask'
      AND TRY_CAST(bba.obj->>'best_bid' AS DOUBLE) > 0.0 AND TRY_CAST(bba.obj->>'best_bid' AS DOUBLE) <= 1.0
    UNION ALL
    SELECT
        bba.ts,
        COALESCE(TRY_CAST(bba.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (bba.frame_id * 8 + bba.sub_idx * 2)::UINTEGER AS frame_id,
        4::UTINYINT AS event_code,
        (bba.obj->>'asset_id' = t.up_tok) AS is_up,
        false AS is_bid,
        CAST(LEAST(10000, GREATEST(1, ROUND(CAST(bba.obj->>'best_ask' AS DOUBLE) * 10000.0))) AS USMALLINT) AS tick,
        0.0::DOUBLE AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM unwrapped_objects bba CROSS JOIN tokens t
    WHERE bba.obj_event_type = 'best_bid_ask'
      AND TRY_CAST(bba.obj->>'best_ask' AS DOUBLE) > 0.0 AND TRY_CAST(bba.obj->>'best_ask' AS DOUBLE) <= 1.0
),
trade_rows AS (
    SELECT
        u.ts,
        COALESCE(TRY_CAST(u.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (u.frame_id * 8 + u.sub_idx * 2)::UINTEGER AS frame_id,
        3::UTINYINT AS event_code,
        NULL::BOOLEAN AS is_up,
        NULL::BOOLEAN AS is_bid,
        NULL::USMALLINT AS tick,
        NULL::DOUBLE AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM unwrapped_objects u
    WHERE u.obj_event_type = 'last_trade_price'
)
SELECT * FROM (
    SELECT * FROM discovered_rows
    UNION ALL SELECT * FROM book_rows
    UNION ALL SELECT * FROM price_change_rows
    UNION ALL SELECT * FROM bbo_hint_rows
    UNION ALL SELECT * FROM trade_rows
)
ORDER BY ts ASC, frame_id ASC
"@
}

function Get-ConvertQuery {
    param(
        [string]$StreamType,
        [string]$InputPathEscaped
    )

    switch ($StreamType) {
        "binance-spot-bookticker" {
            return @"
SELECT 
    CAST(json->>'u' AS UBIGINT) AS update_id,
    json->>'s' AS symbol,
    CAST(json->>'b' AS DOUBLE) AS best_bid_price,
    CAST(json->>'B' AS DOUBLE) AS best_bid_qty,
    CAST(json->>'a' AS DOUBLE) AS best_ask_price,
    CAST(json->>'A' AS DOUBLE) AS best_ask_qty
FROM read_json_objects('$InputPathEscaped', format='newline_delimited', maximum_object_size=67108864)
ORDER BY update_id ASC
"@
        }
        "binance-futures-bookticker" {
            return @"
SELECT 
    CAST(json->>'E' AS UBIGINT) AS event_time,
    CAST(json->>'T' AS UBIGINT) AS transaction_time,
    CAST(json->>'u' AS UBIGINT) AS update_id,
    json->>'s' AS symbol,
    CAST(json->>'b' AS DOUBLE) AS best_bid_price,
    CAST(json->>'B' AS DOUBLE) AS best_bid_qty,
    CAST(json->>'a' AS DOUBLE) AS best_ask_price,
    CAST(json->>'A' AS DOUBLE) AS best_ask_qty
FROM read_json_objects('$InputPathEscaped', format='newline_delimited', maximum_object_size=67108864)
ORDER BY event_time ASC, update_id ASC
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
            $rawSource = @"
    SELECT
        row_number() OVER () AS frame_id,
        ts,
        COALESCE(payload->>'event_type', type) AS event_type,
        payload
    FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'type': 'VARCHAR', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
"@
            return Get-TypedClobSelectQuery -RawSourceSql $rawSource
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
Write-Host " Memory Limit     : $MemoryLimit" -ForegroundColor White
Write-Host " Threads          : $Threads" -ForegroundColor White
Write-Host " Row Group Size   : $RowGroupSize" -ForegroundColor White
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
$allFiles = @(Get-ChildItem -Path $resolvedTargetDir -Recurse -File -Filter $Filter | Where-Object { $_.FullName -notmatch '([\\/]|^)\.duckdb_tmp([\\/]|$)' })

$files = $allFiles
if ($Date) {
    $dayStart = [DateTimeOffset]::ParseExact($Date, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::AssumeUniversal).ToUnixTimeSeconds()
    $dayEnd = $dayStart + 86400L
    $files = @($files | Where-Object {
        if ($_.Name -like "*$Date*" -or $_.FullName -like "*$Date*") {
            return $true
        }
        if ($_.Name -match '-updown-[^-]+-(\d+)\.(?:jsonl\.zst|jsonl|zst)$') {
            $ep = [int64]$Matches[1]
            return ($ep -ge $dayStart -and $ep -lt $dayEnd)
        }
        return $false
    })
}
if ($Stream) {
    $files = @($files | Where-Object { $_.FullName -like "*$Stream*" })
}
$files = @($files | Sort-Object -Property FullName)

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

if (-not $TempDir) {
    $duckdbTempDir = Join-Path $resolvedTargetDir ".duckdb_tmp"
} else {
    $duckdbTempDir = (Resolve-Path -LiteralPath $TempDir).Path
}

if (-not (Test-Path -LiteralPath $duckdbTempDir)) {
    New-Item -ItemType Directory -Path $duckdbTempDir -Force | Out-Null
}
$duckdbTempDirEscaped = $duckdbTempDir.Replace('\', '/')

try {
    for ($i = 0; $i -lt $fileCount; $i++) {
        $file = $files[$i]
        $index = $i + 1
        $srcPath = $file.FullName
        $relPath = $srcPath.Substring($resolvedTargetDir.Length).TrimStart('\', '/')
        $sizeMb = [math]::Round($file.Length / 1MB, 2)
        $inputEscaped = $srcPath.Replace('\', '/')

        $streamType = Get-StreamType -FilePath $srcPath
        if (-not $streamType) {
            Write-Host "  [$index/$fileCount] [WARN] Skipping unknown stream type: $relPath ($sizeMb MB)" -ForegroundColor Yellow
            $skippedCount++
            continue
        }

        # Determine destination .parquet filename
        if ($srcPath.EndsWith(".jsonl.zst", [System.StringComparison]::OrdinalIgnoreCase)) {
            $destPath = $srcPath.Substring(0, $srcPath.Length - 10) + ".parquet"
        } elseif ($srcPath.EndsWith(".jsonl", [System.StringComparison]::OrdinalIgnoreCase)) {
            $destPath = $srcPath.Substring(0, $srcPath.Length - 6) + ".parquet"
        } elseif ($srcPath.EndsWith(".zst", [System.StringComparison]::OrdinalIgnoreCase)) {
            $destPath = $srcPath.Substring(0, $srcPath.Length - 4) + ".parquet"
        } else {
            $destPath = $srcPath + ".parquet"
        }

        # Normalize Polymarket CLOB archives into yyyy-MM-dd/ even when converting legacy flat transition files
        if ($streamType -eq "polymarket-clob-market") {
            $srcParentDir = Split-Path -Parent $srcPath
            $srcParentName = Split-Path -Leaf $srcParentDir
            if ($srcParentName -notmatch '^\d{4}-\d{2}-\d{2}$') {
                $destLeaf = Split-Path -Leaf $destPath
                $destStem = [System.IO.Path]::GetFileNameWithoutExtension($destLeaf)
                if ($destStem -match '-(\d+)$') {
                    $utcDate = [DateTimeOffset]::FromUnixTimeSeconds([int64]$Matches[1]).UtcDateTime.ToString("yyyy-MM-dd")
                    $destPath = Join-Path (Join-Path $srcParentDir $utcDate) $destLeaf
                }
            }
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
            $relDest = $destPath.Substring($resolvedTargetDir.Length).TrimStart('\', '/')
            Write-Host "  [$index/$fileCount] [DRY-RUN] Would convert [$streamType] $relPath ($sizeMb MB) -> $relDest" -ForegroundColor Yellow
            $successCount++
            continue
        }

        Write-Host "  [$index/$fileCount] Converting [$streamType] $relPath ($sizeMb MB)..." -NoNewline

        $destDir = Split-Path -Parent $destPath
        if (-not (Test-Path -LiteralPath $destDir)) {
            New-Item -ItemType Directory -Path $destDir -Force | Out-Null
        }
        $tempParquetPath = "$destPath.tmp.parquet"
        if (Test-Path -LiteralPath $tempParquetPath) {
            Remove-Item -LiteralPath $tempParquetPath -Force -ErrorAction SilentlyContinue
        }

        try {
            $tempDestEscaped = $tempParquetPath.Replace('\', '/')

            $effectiveThreads = $Threads
            $effectiveRowGroupSize = $RowGroupSize
            if (($streamType -in "polymarket-clob-market", "binance-spot-bookticker", "binance-futures-bookticker") -and (-not $PSBoundParameters.ContainsKey('RowGroupSize'))) {
                $effectiveRowGroupSize = 100000
            }

            $selectQuery = Get-ConvertQuery -StreamType $streamType -InputPathEscaped $inputEscaped
            $copyScript = @"
SET max_memory = '$MemoryLimit';
SET temp_directory = '$duckdbTempDirEscaped';
SET threads = $effectiveThreads;
SET preserve_insertion_order = true;

COPY (
$selectQuery
) TO '$tempDestEscaped' (FORMAT PARQUET, COMPRESSION $Compression, ROW_GROUP_SIZE $effectiveRowGroupSize);
"@

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
            $verifyOutput = (& $DuckDbPath -csv -noheader -no-stdin -c "SELECT count(*) FROM read_parquet('$tempDestEscaped');" 2>&1 | Out-String).Trim()
            if ($LASTEXITCODE -ne 0) {
                Remove-Item -LiteralPath $tempParquetPath -Force -ErrorAction SilentlyContinue
                throw "Parquet integrity verification failed: $verifyOutput"
            }

            # Atomically move temp Parquet to final destination
            Move-Item -LiteralPath $tempParquetPath -Destination $destPath -Force

            $finalItem = Get-Item -LiteralPath $destPath
            $finalMb = [math]::Round($finalItem.Length / 1MB, 2)
            $totalParquetBytes += $finalItem.Length

            if (-not $KeepOriginal) {
                # Conversion succeeded and verified; safely remove source .zst file unless KeepOriginal is requested
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
    if ((Test-Path -LiteralPath $duckdbTempDir) -and (-not $TempDir)) {
        $rem = Get-ChildItem -LiteralPath $duckdbTempDir -Recurse -Force -ErrorAction SilentlyContinue
        if (-not $rem) {
            Remove-Item -LiteralPath $duckdbTempDir -Force -ErrorAction SilentlyContinue
        }
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
