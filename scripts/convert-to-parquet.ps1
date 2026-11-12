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

.PARAMETER MemoryLimit
    DuckDB max memory limit (e.g. '5GB', '4GB'). Defaults to '5GB'.

.PARAMETER Threads
    Number of worker threads. Defaults to min(ProcessorCount, 4). Automatically capped to 2 for heavy depth diff streams.

.PARAMETER RowGroupSize
    Maximum number of rows per Parquet row group. Defaults to 50000 (100000 for Polymarket CLOB markets, capped at 25000 for heavy depth diff streams).

.PARAMETER MigrateLegacyParquet
    In-place atomic migration mode for existing legacy Polymarket CLOB Parquet files (*-updown-*.parquet)
    that still contain the raw JSON 'payload' column after their original .jsonl.zst archives were deleted.
    Already-migrated typed CLOB Parquet files (containing 'event_code') are automatically skipped.

.EXAMPLE
    .\scripts\convert-to-parquet.ps1
    Converts all .jsonl.zst files in D:\Crypto\data\Polymarket\collector to .parquet and deletes originals.

.EXAMPLE
    .\scripts\convert-to-parquet.ps1 -MigrateLegacyParquet -Stream btc-updown-15m
    Atomically migrates existing legacy CLOB .parquet files in-place to the compact 10-column typed schema.

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
    [switch]$MigrateLegacyParquet,

    [Parameter()]
    [string]$TempDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($MigrateLegacyParquet -and (-not $PSBoundParameters.ContainsKey('Filter'))) {
    $Filter = "*-updown-*.parquet"
}

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
    if ($normalized -like "*-updown-*") {
        return "polymarket-clob-market"
    }
    return "generic"
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
book_objects AS (
    SELECT r.ts, r.frame_id, 0 AS sub_idx, r.payload AS obj
    FROM raw_numbered r
    WHERE r.event_type = 'book'
    UNION ALL
    SELECT r.ts, r.frame_id, u.ord::INTEGER AS sub_idx, u.obj
    FROM raw_numbered r, UNNEST(CAST(r.payload AS JSON[])) WITH ORDINALITY AS u(obj, ord)
    WHERE r.event_type = 'clob' AND json_type(r.payload) = 'ARRAY' AND json_array_length(r.payload) > 0
),
book_rows AS (
    SELECT
        b.ts,
        COALESCE(TRY_CAST(b.obj->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (b.frame_id * 4 + b.sub_idx)::UINTEGER AS frame_id,
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
        (b.frame_id * 4 + b.sub_idx)::UINTEGER AS frame_id,
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
        (b.frame_id * 4 + b.sub_idx)::UINTEGER AS frame_id,
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
price_change_rows AS (
    SELECT
        r.ts,
        COALESCE(TRY_CAST(r.payload->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (r.frame_id * 4)::UINTEGER AS frame_id,
        2::UTINYINT AS event_code,
        (pc->>'asset_id' = t.up_tok) AS is_up,
        (pc->>'side' IN ('BUY', 'buy')) AS is_bid,
        CAST(LEAST(10000, GREATEST(0, ROUND(CAST(pc->>'price' AS DOUBLE) * 10000.0))) AS USMALLINT) AS tick,
        CAST(pc->>'size' AS DOUBLE) AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM raw_numbered r CROSS JOIN tokens t, UNNEST(CAST(r.payload->'price_changes' AS JSON[])) AS p(pc)
    WHERE r.event_type = 'price_change'
),
trade_rows AS (
    SELECT
        r.ts,
        COALESCE(TRY_CAST(r.payload->>'timestamp' AS UBIGINT), 0::UBIGINT) AS server_ts,
        (r.frame_id * 4)::UINTEGER AS frame_id,
        3::UTINYINT AS event_code,
        NULL::BOOLEAN AS is_up,
        NULL::BOOLEAN AS is_bid,
        NULL::USMALLINT AS tick,
        NULL::DOUBLE AS size,
        NULL::VARCHAR AS up_token_id,
        NULL::VARCHAR AS down_token_id
    FROM raw_numbered r
    WHERE r.event_type = 'last_trade_price'
)
SELECT * FROM (
    SELECT * FROM discovered_rows
    UNION ALL SELECT * FROM book_rows
    UNION ALL SELECT * FROM price_change_rows
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
    list_transform(CAST(payload->'b' AS DOUBLE[][]), lambda x: {'price': x[1], 'quantity': x[2]}) AS bids,
    list_transform(CAST(payload->'a' AS DOUBLE[][]), lambda x: {'price': x[1], 'quantity': x[2]}) AS asks
FROM read_json('$InputPathEscaped', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
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
        "polymarket-clob-legacy-parquet" {
            $rawSource = @"
    SELECT
        row_number() OVER () AS frame_id,
        ts,
        COALESCE(event_type, msg_type) AS event_type,
        CAST(payload AS JSON) AS payload
    FROM read_parquet('$InputPathEscaped')
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
} elseif ($MigrateLegacyParquet) {
    Write-Host " Mode             : MIGRATE LEGACY CLOB PARQUET IN-PLACE (Atomic)" -ForegroundColor Cyan
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
if ($MigrateLegacyParquet) {
    $files = $files | Where-Object {
        (-not $_.Name.EndsWith(".tmp.parquet", [System.StringComparison]::OrdinalIgnoreCase)) -and
        (-not $_.Name.EndsWith(".legacy.parquet", [System.StringComparison]::OrdinalIgnoreCase))
    }
}
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

        if ($MigrateLegacyParquet) {
            $destPath = $srcPath
            $streamType = "polymarket-clob-legacy-parquet"

            $hasPayloadRaw = (& $DuckDbPath -csv -noheader -no-stdin -c "SELECT count(*) FROM parquet_schema('$inputEscaped') WHERE name = 'payload';" 2>&1 | Out-String).Trim()
            if ($LASTEXITCODE -ne 0) {
                $errMsg = "Failed to inspect Parquet schema for '$relPath': $hasPayloadRaw"
                Write-Host "  [$index/$fileCount] [FAILED] $errMsg" -ForegroundColor Red
                $errors.Add($errMsg)
                $failedCount++
                continue
            }
            if ($hasPayloadRaw -eq "0") {
                Write-Host "  [$index/$fileCount] [SKIP] $relPath (Already migrated to typed CLOB schema: $sizeMb MB)" -ForegroundColor DarkGray
                $skippedCount++
                continue
            }

            if ($DryRun) {
                Write-Host "  [$index/$fileCount] [DRY-RUN] Would migrate legacy CLOB Parquet $relPath ($sizeMb MB) -> typed 10-column schema" -ForegroundColor Yellow
                $successCount++
                continue
            }

            Write-Host "  [$index/$fileCount] Migrating legacy CLOB Parquet $relPath ($sizeMb MB)..." -NoNewline
        } else {
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

            # Check if target .parquet already exists
            if ((Test-Path -LiteralPath $destPath) -and (-not $Force)) {
                $existingItem = Get-Item -LiteralPath $destPath
                if ($existingItem.Length -gt 0) {
                    Write-Host "  [$index/$fileCount] [SKIP] $relPath (Parquet already exists: $([math]::Round($existingItem.Length / 1MB, 2)) MB)" -ForegroundColor DarkGray
                    $skippedCount++
                    continue
                }
            }

            $streamType = Get-StreamType -FilePath $srcPath
            if ($DryRun) {
                Write-Host "  [$index/$fileCount] [DRY-RUN] Would convert [$streamType] $relPath ($sizeMb MB) -> $([System.IO.Path]::GetFileName($destPath))" -ForegroundColor Yellow
                $successCount++
                continue
            }

            Write-Host "  [$index/$fileCount] Converting [$streamType] $relPath ($sizeMb MB)..." -NoNewline
        }

        $tempParquetPath = "$destPath.tmp.parquet"
        if (Test-Path -LiteralPath $tempParquetPath) {
            Remove-Item -LiteralPath $tempParquetPath -Force -ErrorAction SilentlyContinue
        }

        try {
            $tempDestEscaped = $tempParquetPath.Replace('\', '/')

            $effectiveThreads = $Threads
            $effectiveRowGroupSize = $RowGroupSize
            if ($streamType -in "binance-futures-depthdiff", "binance-futures-depthpartial") {
                $effectiveThreads = [Math]::Min($Threads, 2)
                $effectiveRowGroupSize = [Math]::Min($RowGroupSize, 25000)
            } elseif (($streamType -in "polymarket-clob-market", "polymarket-clob-legacy-parquet") -and (-not $PSBoundParameters.ContainsKey('RowGroupSize'))) {
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

            if ($MigrateLegacyParquet -and $KeepOriginal) {
                $legacyBackupPath = $srcPath.Substring(0, $srcPath.Length - 8) + ".legacy.parquet"
                Copy-Item -LiteralPath $srcPath -Destination $legacyBackupPath -Force
            }

            # Atomically move temp Parquet to final destination
            Move-Item -LiteralPath $tempParquetPath -Destination $destPath -Force

            $finalItem = Get-Item -LiteralPath $destPath
            $finalMb = [math]::Round($finalItem.Length / 1MB, 2)
            $totalParquetBytes += $finalItem.Length

            if ($MigrateLegacyParquet) {
                if ($KeepOriginal) {
                    Write-Host " OK ($finalMb MB, legacy backup kept)" -ForegroundColor Green
                } else {
                    Write-Host " OK ($finalMb MB, replaced in-place)" -ForegroundColor Green
                }
            } elseif (-not $KeepOriginal) {
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
