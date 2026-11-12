#!/usr/bin/env bash
#
# convert-to-parquet.sh
#
# Converts downloaded .jsonl.zst market data archives into optimized, typed Apache Parquet (.parquet)
# files using DuckDB, sorted by top-level timestamp, and deletes original archives upon successful conversion.
#

set -euo pipefail

# ANSI color codes
COLOR_RESET="\033[0m"
COLOR_CYAN="\033[1;36m"
COLOR_GREEN="\033[1;32m"
COLOR_YELLOW="\033[1;33m"
COLOR_RED="\033[1;31m"
COLOR_WHITE="\033[1;37m"
COLOR_GRAY="\033[0;90m"

log_info() {
    echo -e "${COLOR_CYAN}$*${COLOR_RESET}"
}
log_success() {
    echo -e "${COLOR_GREEN}$*${COLOR_RESET}"
}
log_warn() {
    echo -e "${COLOR_YELLOW}$*${COLOR_RESET}"
}
log_error() {
    echo -e "${COLOR_RED}$*${COLOR_RESET}"
}

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [options]

Options:
  -t, --target-dir <path>       Root directory containing collector archives.
                                (Default: /data/polymarket/collector or ./collector)
  -d, --date <YYYY-MM-DD>       Optional target UTC date filter.
  -s, --stream <pattern>        Optional substring to filter by stream type or symbol
                                (e.g. 'aggTrades', 'forceOrder', 'deribit', 'price_crypto', 'btc-updown').
  -f, --filter <pattern>        Filename search pattern (Default: *.jsonl.zst).
  -n, --dry-run                 Displays all files that would be converted without modifying disk.
  -k, --keep-original           Retain original .jsonl.zst files after conversion instead of deleting.
      --force                   Overwrite existing destination .parquet files.
      --migrate-legacy-parquet  Atomically migrate existing legacy CLOB (*-updown-*.parquet) files
                                in-place to the compact 10-column typed schema.
  -c, --compression <codec>     Parquet compression codec (ZSTD, SNAPPY, GZIP, UNCOMPRESSED). Default: ZSTD.
      --duckdb-path <path>      Path to DuckDB executable (Default: duckdb).
  -m, --memory-limit <limit>    DuckDB max memory limit (Default: 5GB).
      --threads <num>           DuckDB execution threads (Default: 4).
      --temp-dir <path>         Custom temporary directory for DuckDB operations.
      --help                    Display this help message.

Examples:
  $(basename "$0")
  $(basename "$0") --dry-run
  $(basename "$0") --migrate-legacy-parquet -s btc-updown-15m
  $(basename "$0") -d 2026-10-15
  $(basename "$0") -s aggTrades -k
  $(basename "$0") -t /data/polymarket/collector --force
EOF
    exit "${1:-0}"
}

# Defaults
if [ -d "/data/polymarket/collector" ]; then
    TARGET_DIR="/data/polymarket/collector"
else
    TARGET_DIR="${COLLECTOR_DATA_DIR:-./collector}"
fi

DATE_FILTER=""
STREAM_FILTER=""
FILE_FILTER="*.jsonl.zst"
CUSTOM_FILE_FILTER=false
DRY_RUN=false
KEEP_ORIGINAL=false
FORCE=false
MIGRATE_LEGACY_PARQUET=false
COMPRESSION="ZSTD"
DUCKDB_PATH="duckdb"
MEMORY_LIMIT="5GB"

# Detect core count
if command -v nproc >/dev/null 2>&1; then
    CPU_CORES=$(nproc)
elif command -v sysctl >/dev/null 2>&1; then
    CPU_CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo 4)
else
    CPU_CORES=4
fi
THREADS=$(( CPU_CORES > 4 ? 4 : CPU_CORES ))
TEMP_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--target-dir)
            TARGET_DIR="$2"
            shift 2
            ;;
        -d|--date)
            DATE_FILTER="$2"
            shift 2
            ;;
        -s|--stream)
            STREAM_FILTER="$2"
            shift 2
            ;;
        -f|--filter)
            FILE_FILTER="$2"
            CUSTOM_FILE_FILTER=true
            shift 2
            ;;
        -n|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -k|--keep-original)
            KEEP_ORIGINAL=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --migrate-legacy-parquet)
            MIGRATE_LEGACY_PARQUET=true
            shift
            ;;
        -c|--compression)
            COMPRESSION="$2"
            shift 2
            ;;
        --duckdb-path)
            DUCKDB_PATH="$2"
            shift 2
            ;;
        -m|--memory-limit)
            MEMORY_LIMIT="$2"
            shift 2
            ;;
        --threads)
            THREADS="$2"
            shift 2
            ;;
        --temp-dir)
            TEMP_DIR="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

if [ "$MIGRATE_LEGACY_PARQUET" = true ] && [ "$CUSTOM_FILE_FILTER" = false ]; then
    FILE_FILTER="*-updown-*.parquet"
fi

if ! command -v "$DUCKDB_PATH" >/dev/null 2>&1; then
    log_error "DuckDB CLI ('$DUCKDB_PATH') was not found on PATH. Please install DuckDB."
    exit 1
fi

if [ ! -d "$TARGET_DIR" ]; then
    log_error "Target directory '$TARGET_DIR' does not exist."
    exit 1
fi

TARGET_DIR_RESOLVED=$(cd "$TARGET_DIR" && pwd)

get_stream_type() {
    local path_lower
    path_lower=$(echo "$1" | tr '[:upper:]' '[:lower:]')
    if [[ "$path_lower" =~ /binance/spot/.*/aggtrades/ ]] || [[ "$path_lower" =~ spot.*aggtrades ]]; then
        echo "binance-spot-aggtrades"
    elif [[ "$path_lower" =~ /binance/futures/.*/aggtrades/ ]] || [[ "$path_lower" =~ futures.*aggtrades ]]; then
        echo "binance-futures-aggtrades"
    elif [[ "$path_lower" =~ forceorder ]]; then
        echo "binance-futures-forceorder"
    elif [[ "$path_lower" =~ depthdiff ]]; then
        echo "binance-futures-depthdiff"
    elif [[ "$path_lower" =~ depthpartial ]]; then
        echo "binance-futures-depthpartial"
    elif [[ "$path_lower" =~ /deribit/ ]] || [[ "$path_lower" =~ trades\.btc-perpetual ]]; then
        echo "deribit-trades"
    elif [[ "$path_lower" =~ price_crypto_twap ]]; then
        echo "polymarket-price-twap"
    elif [[ "$path_lower" =~ price_crypto ]]; then
        echo "polymarket-price-crypto"
    elif [[ "$path_lower" =~ -updown- ]]; then
        echo "polymarket-clob-market"
    else
        echo "generic"
    fi
}

get_typed_clob_select_query() {
    local raw_source_sql="$1"
    cat <<EOF
WITH raw_numbered AS (
$raw_source_sql
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
EOF
}

get_convert_query() {
    local stream_type="$1"
    local input_path="$2"

    case "$stream_type" in
        "binance-spot-aggtrades")
            cat <<EOF
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
FROM read_json('$input_path', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
EOF
            ;;
        "binance-futures-aggtrades")
            cat <<EOF
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
FROM read_json('$input_path', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
EOF
            ;;
        "binance-futures-forceorder")
            cat <<EOF
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
FROM read_json('$input_path', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
EOF
            ;;
        "binance-futures-depthdiff"|"binance-futures-depthpartial")
            cat <<EOF
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
FROM read_json('$input_path', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
EOF
            ;;
        "deribit-trades")
            cat <<EOF
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
    FROM read_json('$input_path', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
)
ORDER BY ts ASC
EOF
            ;;
        "polymarket-price-crypto")
            cat <<EOF
WITH raw_data AS (
    SELECT ts, payload
    FROM read_json('$input_path', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
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
EOF
            ;;
        "polymarket-price-twap")
            cat <<EOF
WITH raw_data AS (
    SELECT ts, payload
    FROM read_json('$input_path', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
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
EOF
            ;;
        "polymarket-clob-market")
            local raw_src
            raw_src=$(cat <<EOF
    SELECT
        row_number() OVER () AS frame_id,
        ts,
        COALESCE(payload->>'event_type', type) AS event_type,
        payload
    FROM read_json('$input_path', columns={'ts': 'UBIGINT', 'type': 'VARCHAR', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
EOF
)
            get_typed_clob_select_query "$raw_src"
            ;;
        "polymarket-clob-legacy-parquet")
            local raw_src
            raw_src=$(cat <<EOF
    SELECT
        row_number() OVER () AS frame_id,
        ts,
        COALESCE(event_type, msg_type) AS event_type,
        CAST(payload AS JSON) AS payload
    FROM read_parquet('$input_path')
EOF
)
            get_typed_clob_select_query "$raw_src"
            ;;
        *)
            cat <<EOF
SELECT 
    ts,
    payload
FROM read_json('$input_path', columns={'ts': 'UBIGINT', 'payload': 'JSON'}, format='newline_delimited', maximum_object_size=67108864)
ORDER BY ts ASC
EOF
            ;;
    esac
}

log_info "================================================================="
log_info " Polymarket Data Collector - DuckDB Parquet Converter (Linux)"
echo -e "${COLOR_WHITE} Target Directory : ${TARGET_DIR_RESOLVED}${COLOR_RESET}"
if [ -n "$DATE_FILTER" ]; then
    log_warn " Date Filter      : $DATE_FILTER"
fi
if [ -n "$STREAM_FILTER" ]; then
    log_warn " Stream Filter    : $STREAM_FILTER"
fi
echo -e "${COLOR_WHITE} Search Filter    : ${FILE_FILTER}${COLOR_RESET}"
echo -e "${COLOR_WHITE} Compression      : ${COMPRESSION}${COLOR_RESET}"
echo -e "${COLOR_WHITE} Memory Limit     : ${MEMORY_LIMIT}${COLOR_RESET}"
echo -e "${COLOR_WHITE} Threads          : ${THREADS}${COLOR_RESET}"
if [ "$DRY_RUN" = true ]; then
    log_warn " Mode             : DRY-RUN (No files will be converted or deleted)"
elif [ "$MIGRATE_LEGACY_PARQUET" = true ]; then
    log_info " Mode             : MIGRATE LEGACY CLOB PARQUET IN-PLACE (Atomic)"
elif [ "$KEEP_ORIGINAL" = true ]; then
    log_info " Mode             : CONVERT AND KEEP ORIGINALS"
else
    log_success " Mode             : CONVERT AND DELETE VERIFIED ORIGINALS"
fi
log_info "================================================================="

log_info "[1/3] Scanning for matching archives in $TARGET_DIR_RESOLVED..."

TEMP_FILE_LIST=$(mktemp /tmp/parquet_convert_files_XXXXXX.txt)
TEMP_SQL_FILE=$(mktemp /tmp/parquet_convert_sql_XXXXXX.sql)

if [ -z "$TEMP_DIR" ]; then
    DUCKDB_TEMP_DIR="${TARGET_DIR_RESOLVED}/.duckdb_tmp"
    CLEANUP_TEMP_DIR=true
else
    DUCKDB_TEMP_DIR="$TEMP_DIR"
    CLEANUP_TEMP_DIR=false
fi
mkdir -p "$DUCKDB_TEMP_DIR"

cleanup() {
    rm -f "$TEMP_FILE_LIST" "$TEMP_SQL_FILE"
    if [ "$CLEANUP_TEMP_DIR" = true ] && [ -d "$DUCKDB_TEMP_DIR" ]; then
        rm -rf "$DUCKDB_TEMP_DIR"
    fi
}
trap cleanup EXIT

# Scan files using python for exact matching
python3 - << EOF > "$TEMP_FILE_LIST"
import os, sys, fnmatch

target = "$TARGET_DIR_RESOLVED"
date_filter = "$DATE_FILTER"
stream_filter = "$STREAM_FILTER"
file_filter = "$FILE_FILTER"
migrate_legacy = "$MIGRATE_LEGACY_PARQUET" == "true"

for root, dirs, files in os.walk(target):
    for f in files:
        if migrate_legacy and (f.endswith(".tmp.parquet") or f.endswith(".legacy.parquet")):
            continue
        if fnmatch.fnmatch(f, file_filter):
            full = os.path.join(root, f)
            if date_filter and (date_filter not in f and date_filter not in full):
                continue
            if stream_filter and (stream_filter not in full):
                continue
            size = os.path.getsize(full)
            sys.stdout.write(f"{full}\t{size}\n")
EOF

TOTAL_ARCHIVES=$(wc -l < "$TEMP_FILE_LIST" | tr -d ' ')
if [ "$TOTAL_ARCHIVES" -eq 0 ]; then
    log_warn "No matching archives found in '$TARGET_DIR_RESOLVED'."
    exit 0
fi

TOTAL_INPUT_BYTES=$(awk -F'\t' '{sum += $2} END {print sum}' "$TEMP_FILE_LIST")
TOTAL_INPUT_MB=$(awk -v b="$TOTAL_INPUT_BYTES" 'BEGIN { printf "%.2f", b / (1024*1024) }')
TOTAL_INPUT_GB=$(awk -v b="$TOTAL_INPUT_BYTES" 'BEGIN { printf "%.2f", b / (1024*1024*1024) }')

log_success "      Discovered $TOTAL_ARCHIVES archives (Total: $TOTAL_INPUT_MB MB / $TOTAL_INPUT_GB GB)."

log_info "[2/3] Converting archives to Parquet (DuckDB)..."

START_TIME=$(date +%s)
SUCCESS_COUNT=0
SKIPPED_COUNT=0
FAILED_COUNT=0
TOTAL_PARQUET_BYTES=0

CURRENT_INDEX=0
while IFS=$'\t' read -r src_path src_size; do
    [ -z "$src_path" ] && continue
    CURRENT_INDEX=$(( CURRENT_INDEX + 1 ))
    rel_path="${src_path#"${TARGET_DIR_RESOLVED}/"}"
    size_mb=$(awk -v b="$src_size" 'BEGIN { printf "%.2f", b / (1024*1024) }')

    if [ "$MIGRATE_LEGACY_PARQUET" = true ]; then
        dest_path="$src_path"
        stream_type="polymarket-clob-legacy-parquet"
        has_payload=$("$DUCKDB_PATH" -csv -noheader -no-stdin -c "SELECT count(*) FROM parquet_schema('$src_path') WHERE name = 'payload';" 2>/dev/null | tr -d '[:space:]' || echo "ERR")
        if [ "$has_payload" = "ERR" ]; then
            echo -e "${COLOR_RED}  [$CURRENT_INDEX/$TOTAL_ARCHIVES] [FAILED] Failed to inspect Parquet schema for $rel_path${COLOR_RESET}"
            FAILED_COUNT=$(( FAILED_COUNT + 1 ))
            continue
        fi
        if [ "$has_payload" = "0" ]; then
            echo -e "${COLOR_GRAY}  [$CURRENT_INDEX/$TOTAL_ARCHIVES] [SKIP] $rel_path (Already migrated to typed CLOB schema: $size_mb MB)${COLOR_RESET}"
            SKIPPED_COUNT=$(( SKIPPED_COUNT + 1 ))
            continue
        fi
        if [ "$DRY_RUN" = true ]; then
            log_warn "  [$CURRENT_INDEX/$TOTAL_ARCHIVES] [DRY-RUN] Would migrate legacy CLOB Parquet $rel_path ($size_mb MB) -> typed 10-column schema"
            SUCCESS_COUNT=$(( SUCCESS_COUNT + 1 ))
            continue
        fi
        echo -ne "${COLOR_WHITE}  [$CURRENT_INDEX/$TOTAL_ARCHIVES] Migrating legacy CLOB Parquet $rel_path ($size_mb MB)...${COLOR_RESET}"
    else
        # Determine destination .parquet filename
        if [[ "$src_path" == *.jsonl.zst ]]; then
            dest_path="${src_path%.jsonl.zst}.parquet"
        elif [[ "$src_path" == *.jsonl ]]; then
            dest_path="${src_path%.jsonl}.parquet"
        elif [[ "$src_path" == *.zst ]]; then
            dest_path="${src_path%.zst}.parquet"
        else
            dest_path="${src_path}.parquet"
        fi

        # Check if target exists
        if [ -f "$dest_path" ] && [ "$FORCE" = false ]; then
            dest_size=$(stat -c%s "$dest_path" 2>/dev/null || stat -f%z "$dest_path" 2>/dev/null || echo 0)
            if [ "$dest_size" -gt 0 ]; then
                dest_mb=$(awk -v b="$dest_size" 'BEGIN { printf "%.2f", b / (1024*1024) }')
                echo -e "${COLOR_GRAY}  [$CURRENT_INDEX/$TOTAL_ARCHIVES] [SKIP] $rel_path (Parquet already exists: $dest_mb MB)${COLOR_RESET}"
                SKIPPED_COUNT=$(( SKIPPED_COUNT + 1 ))
                continue
            fi
        fi

        stream_type=$(get_stream_type "$src_path")

        if [ "$DRY_RUN" = true ]; then
            dest_base=$(basename "$dest_path")
            log_warn "  [$CURRENT_INDEX/$TOTAL_ARCHIVES] [DRY-RUN] Would convert [$stream_type] $rel_path ($size_mb MB) -> $dest_base"
            SUCCESS_COUNT=$(( SUCCESS_COUNT + 1 ))
            continue
        fi

        echo -ne "${COLOR_WHITE}  [$CURRENT_INDEX/$TOTAL_ARCHIVES] Converting [$stream_type] $rel_path ($size_mb MB)...${COLOR_RESET}"
    fi

    temp_parquet_path="${dest_path}.tmp.parquet"
    rm -f "$temp_parquet_path"

    SELECT_QUERY=$(get_convert_query "$stream_type" "$src_path")

    effective_threads=$THREADS
    effective_row_group_size=50000
    if [ "$stream_type" = "binance-futures-depthdiff" ] || [ "$stream_type" = "binance-futures-depthpartial" ]; then
        if [ "$effective_threads" -gt 2 ]; then
            effective_threads=2
        fi
        effective_row_group_size=25000
    elif [ "$stream_type" = "polymarket-clob-market" ] || [ "$stream_type" = "polymarket-clob-legacy-parquet" ]; then
        effective_row_group_size=100000
    fi

    cat << EOF > "$TEMP_SQL_FILE"
SET max_memory = '$MEMORY_LIMIT';
SET temp_directory = '$DUCKDB_TEMP_DIR';
SET threads = $effective_threads;
SET preserve_insertion_order = true;

COPY (
$SELECT_QUERY
) TO '$temp_parquet_path' (FORMAT PARQUET, COMPRESSION $COMPRESSION, ROW_GROUP_SIZE $effective_row_group_size);
EOF

    if ! "$DUCKDB_PATH" -c ".read '$TEMP_SQL_FILE'" >/dev/null 2>&1; then
        echo -e "${COLOR_RED} FAILED (DuckDB error)${COLOR_RESET}"
        FAILED_COUNT=$(( FAILED_COUNT + 1 ))
        rm -f "$temp_parquet_path"
        continue
    fi

    if [ ! -f "$temp_parquet_path" ]; then
        echo -e "${COLOR_RED} FAILED (Output file missing)${COLOR_RESET}"
        FAILED_COUNT=$(( FAILED_COUNT + 1 ))
        continue
    fi

    temp_size=$(stat -c%s "$temp_parquet_path" 2>/dev/null || stat -f%z "$temp_parquet_path" 2>/dev/null || echo 0)
    if [ "$temp_size" -le 0 ]; then
        echo -e "${COLOR_RED} FAILED (Output file is empty)${COLOR_RESET}"
        rm -f "$temp_parquet_path"
        FAILED_COUNT=$(( FAILED_COUNT + 1 ))
        continue
    fi

    # Verify integrity and row count with DuckDB
    if ! "$DUCKDB_PATH" -no-stdin -c "SELECT count(*) FROM read_parquet('$temp_parquet_path');" >/dev/null 2>&1; then
        echo -e "${COLOR_RED} FAILED (Parquet integrity verification failed)${COLOR_RESET}"
        rm -f "$temp_parquet_path"
        FAILED_COUNT=$(( FAILED_COUNT + 1 ))
        continue
    fi

    if [ "$MIGRATE_LEGACY_PARQUET" = true ] && [ "$KEEP_ORIGINAL" = true ]; then
        cp -f "$src_path" "${src_path%.parquet}.legacy.parquet"
    fi

    # Atomically move temp parquet to final destination
    mv "$temp_parquet_path" "$dest_path"
    final_size=$(stat -c%s "$dest_path" 2>/dev/null || stat -f%z "$dest_path" 2>/dev/null || echo 0)
    final_mb=$(awk -v b="$final_size" 'BEGIN { printf "%.2f", b / (1024*1024) }')
    TOTAL_PARQUET_BYTES=$(( TOTAL_PARQUET_BYTES + final_size ))

    if [ "$MIGRATE_LEGACY_PARQUET" = true ]; then
        if [ "$KEEP_ORIGINAL" = true ]; then
            echo -e "${COLOR_GREEN} OK ($final_mb MB, legacy backup kept)${COLOR_RESET}"
        else
            echo -e "${COLOR_GREEN} OK ($final_mb MB, replaced in-place)${COLOR_RESET}"
        fi
    elif [ "$KEEP_ORIGINAL" = false ]; then
        rm -f "$src_path"
        echo -e "${COLOR_GREEN} OK ($final_mb MB, archive deleted)${COLOR_RESET}"
    else
        echo -e "${COLOR_GREEN} OK ($final_mb MB, archive kept)${COLOR_RESET}"
    fi
    SUCCESS_COUNT=$(( SUCCESS_COUNT + 1 ))

done < "$TEMP_FILE_LIST"

END_TIME=$(date +%s)
ELAPSED_TOTAL=$(( END_TIME - START_TIME ))
ELAPSED_HOURS=$(( ELAPSED_TOTAL / 3600 ))
ELAPSED_MIN=$(( (ELAPSED_TOTAL % 3600) / 60 ))
ELAPSED_SEC=$(( ELAPSED_TOTAL % 60 ))
ELAPSED_FMT=$(printf "%02d:%02d:%02d" "$ELAPSED_HOURS" "$ELAPSED_MIN" "$ELAPSED_SEC")

log_info "[3/3] Conversion Summary"
log_info "================================================================="
echo -e "${COLOR_WHITE} Total Archives Discovered : ${TOTAL_ARCHIVES}${COLOR_RESET}"
if [ "$SUCCESS_COUNT" -gt 0 ]; then
    log_success " Successfully Converted    : ${SUCCESS_COUNT}"
else
    echo -e "${COLOR_WHITE} Successfully Converted    : 0${COLOR_RESET}"
fi
if [ "$SKIPPED_COUNT" -gt 0 ]; then
    echo -e "${COLOR_GRAY} Skipped (Already Present) : ${SKIPPED_COUNT}${COLOR_RESET}"
fi
if [ "$FAILED_COUNT" -gt 0 ]; then
    log_error " Failed Conversions        : ${FAILED_COUNT}"
else
    echo -e "${COLOR_WHITE} Failed Conversions        : 0${COLOR_RESET}"
fi
echo -e "${COLOR_WHITE} Total Input Archive Size  : ${TOTAL_INPUT_MB} MB (${TOTAL_INPUT_GB} GB)${COLOR_RESET}"
if [ "$DRY_RUN" = false ]; then
    TOTAL_PARQUET_MB=$(awk -v b="$TOTAL_PARQUET_BYTES" 'BEGIN { printf "%.2f", b / (1024*1024) }')
    TOTAL_PARQUET_GB=$(awk -v b="$TOTAL_PARQUET_BYTES" 'BEGIN { printf "%.2f", b / (1024*1024*1024) }')
    log_success " Total Output Parquet Size : ${TOTAL_PARQUET_MB} MB (${TOTAL_PARQUET_GB} GB)"
    if [ "$TOTAL_INPUT_BYTES" -gt 0 ] && [ "$TOTAL_PARQUET_BYTES" -gt 0 ]; then
        SAVINGS=$(awk -v inb="$TOTAL_INPUT_BYTES" -v outb="$TOTAL_PARQUET_BYTES" 'BEGIN { printf "%.1f", (inb - outb) / inb * 100 }')
        log_success " Net Space Savings         : ${SAVINGS} %"
    fi
fi
echo -e "${COLOR_WHITE} Elapsed Time              : ${ELAPSED_FMT}${COLOR_RESET}"
log_info "================================================================="

if [ "$FAILED_COUNT" -gt 0 ]; then
    exit 1
fi
exit 0
