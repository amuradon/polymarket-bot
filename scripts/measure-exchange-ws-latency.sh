#!/usr/bin/env bash
#
# measure-exchange-ws-latency.sh
#
# Measures one-way WebSocket network latency from Binance Spot, Coinbase Spot, Kraken Spot,
# Bitstamp Spot, Gemini Spot, LMAX Digital, and Crypto.com to an AWS EC2 instance (eu-west-1 Dublin).
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
  -h, --ec2-host <alias>        EC2 SSH host alias configured in ~/.ssh/config.
                                (Default: i-0e826b5eb507d87be)
  -s, --symbol <sym>            Cryptocurrency symbol to benchmark (e.g. BTC, ETH, SOL, BTCUSDT).
                                (Default: BTC)
  -c, --sample-count <num>      Target trade event samples per exchange (5-5000, Default: 100).
  -d, --duration <sec>          Maximum duration timeout in seconds (5-600, Default: 60).
  -e, --exchanges <list>        Comma-separated list of exchanges to benchmark.
                                Options: binance, coinbase, kraken, bitstamp, gemini, lmax, cryptocom.
                                (Default: all seven)
  -l, --log-dir <path>          Output directory for benchmark logs.
                                (Default: ./logs/latency)
      --clean-remote-env        Removes ~/.ws_bench_env on EC2 after benchmark execution.
      --help                    Display this help message.

Examples:
  $(basename "$0")
  $(basename "$0") -c 200 -d 30
  $(basename "$0") -s ETH -e cryptocom,lmax
  $(basename "$0") -s SOL -c 50 -e binance,coinbase,kraken
EOF
    exit "${1:-0}"
}

# Defaults
EC2_HOST="i-0e826b5eb507d87be"
SYMBOL="BTC"
SAMPLE_COUNT=100
DURATION_SECONDS=60
EXCHANGES_ARG="binance,coinbase,kraken,bitstamp,gemini,lmax,cryptocom"
LOG_DIR="./logs/latency"
CLEAN_REMOTE_ENV=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--ec2-host)
            EC2_HOST="$2"
            shift 2
            ;;
        -s|--symbol)
            SYMBOL="$2"
            shift 2
            ;;
        -c|--sample-count)
            SAMPLE_COUNT="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION_SECONDS="$2"
            shift 2
            ;;
        -e|--exchanges)
            EXCHANGES_ARG="$2"
            shift 2
            ;;
        -l|--log-dir)
            LOG_DIR="$2"
            shift 2
            ;;
        --clean-remote-env)
            CLEAN_REMOTE_ENV=true
            shift
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

# Ensure ssh and scp exist
for bin in ssh scp python3; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        log_error "Required command '$bin' is not installed or not in PATH."
        exit 1
    fi
done

# Normalize symbol
CLEAN_SYMBOL=$(echo "$SYMBOL" | tr '[:lower:]' '[:upper:]' | tr -d -- '-/')
if [[ "$CLEAN_SYMBOL" == *USDT ]]; then
    BASE_SYMBOL="${CLEAN_SYMBOL%USDT}"
elif [[ "$CLEAN_SYMBOL" == *USD ]]; then
    BASE_SYMBOL="${CLEAN_SYMBOL%USD}"
else
    BASE_SYMBOL="$CLEAN_SYMBOL"
fi

BASE_LOWER=$(echo "$BASE_SYMBOL" | tr '[:upper:]' '[:lower:]')

SYM_BINANCE="${BASE_LOWER}usdt"
SYM_COINBASE="${BASE_SYMBOL}-USD"
SYM_KRAKEN="${BASE_SYMBOL}/USD"
SYM_BITSTAMP="${BASE_LOWER}usd"
SYM_GEMINI="${BASE_LOWER}usd"
SYM_LMAX="${BASE_LOWER}-usd"
SYM_CRYPTOCOM="${BASE_SYMBOL}_USDT"

# Normalize exchanges
declare -a NORMALIZED_EXCHANGES=()
IFS=',' read -ra ADDR <<< "$EXCHANGES_ARG"
for i in "${ADDR[@]}"; do
    cleaned=$(echo "$i" | tr '[:upper:]' '[:lower:]' | tr -d ' ')
    if [ "$cleaned" = "crypto.com" ] || [ "$cleaned" = "cdc" ]; then
        cleaned="cryptocom"
    fi
    case "$cleaned" in
        binance|coinbase|kraken|bitstamp|gemini|lmax|cryptocom)
            NORMALIZED_EXCHANGES+=("$cleaned")
            ;;
        "")
            ;;
        *)
            log_error "Invalid exchange '$cleaned'. Supported: binance, coinbase, kraken, bitstamp, gemini, lmax, cryptocom"
            exit 1
            ;;
    esac
done

if [ ${#NORMALIZED_EXCHANGES[@]} -eq 0 ]; then
    log_error "At least one valid exchange must be specified."
    exit 1
fi

EXCHANGE_PYTHON_LIST=$(printf "'%s', " "${NORMALIZED_EXCHANGES[@]}")
EXCHANGE_PYTHON_LIST="[${EXCHANGE_PYTHON_LIST%, }]"

mkdir -p "$LOG_DIR"
RESOLVED_LOG_DIR=$(cd "$LOG_DIR" && pwd)
TIMESTAMP_STR=$(date +"%Y%m%d-%H%M%S")

log_info "================================================================="
log_info " Polymarket Bot - WebSocket One-Way Latency Benchmark (Linux)"
echo -e "${COLOR_WHITE} EC2 Instance Host : ${EC2_HOST}${COLOR_RESET}"
echo -e "${COLOR_WHITE} Base Symbol       : ${BASE_SYMBOL}${COLOR_RESET}"
echo -e "${COLOR_WHITE} Target Samples    : ${SAMPLE_COUNT} per exchange${COLOR_RESET}"
echo -e "${COLOR_WHITE} Max Timeout       : ${DURATION_SECONDS}s${COLOR_RESET}"
echo -e "${COLOR_WHITE} Target Exchanges  : ${EXCHANGES_ARG}${COLOR_RESET}"
echo -e "${COLOR_WHITE} Output Directory  : ${RESOLVED_LOG_DIR}${COLOR_RESET}"
log_info "================================================================="

# Step 1: Chrony clock verification
log_info "[1/3] Connecting to EC2 host and verifying time synchronization..."
CHRONY_RAW=$(ssh -o BatchMode=yes "$EC2_HOST" "chronyc tracking 2>&1")
REF_ID=$(echo "$CHRONY_RAW" | grep -E "Reference ID" | awk -F': ' '{print $2}' || echo "Unknown")
LAST_OFFSET=$(echo "$CHRONY_RAW" | grep -E "Last offset" | awk -F': ' '{print $2}' || echo "Unknown")
RMS_OFFSET=$(echo "$CHRONY_RAW" | grep -E "RMS offset" | awk -F': ' '{print $2}' || echo "Unknown")

log_success "      Chrony Reference   : $REF_ID (AWS Time Sync Service)"
log_success "      Chrony Last Offset : $LAST_OFFSET"
log_success "      Chrony RMS Offset  : $RMS_OFFSET"

# Step 2: Ensure remote environment on EC2
log_info "[2/3] Preparing remote benchmarking environment on EC2..."
ssh -o BatchMode=yes "$EC2_HOST" '
if [ ! -d "$HOME/.ws_bench_env" ]; then
    python3 -m venv "$HOME/.ws_bench_env" >/dev/null 2>&1
    "$HOME/.ws_bench_env/bin/pip" install websockets >/dev/null 2>&1
fi
'
log_success "      Remote environment ~/.ws_bench_env ready."

# Step 3: Run concurrent benchmark
log_info "[3/3] Running concurrent WebSocket latency measurement on EC2..."

TEMP_REMOTE_SCRIPT="/tmp/ws_bench_$(date +%s)_$$.py"
TEMP_LOCAL_SCRIPT=$(mktemp /tmp/ws_bench_local_XXXXXX.py)
trap 'rm -f "$TEMP_LOCAL_SCRIPT"; ssh -o BatchMode=yes "$EC2_HOST" "rm -f $TEMP_REMOTE_SCRIPT" 2>/dev/null || true' EXIT

cat << EOF > "$TEMP_LOCAL_SCRIPT"
import asyncio
import json
import time
import datetime
import math
import sys
import websockets

def parse_iso(ts_str):
    if ts_str.endswith('Z'): ts_str = ts_str[:-1] + '+00:00'
    return datetime.datetime.fromisoformat(ts_str).timestamp() * 1000.0

def calc_stats(samples):
    if not samples:
        return None
    s = sorted(samples)
    n = len(s)
    mean_val = sum(s) / n
    variance = sum((x - mean_val) ** 2 for x in s) / (n - 1) if n > 1 else 0.0
    def pct(p):
        idx = int(math.ceil(p / 100.0 * n)) - 1
        return s[max(0, min(idx, n - 1))]
    return {
        'count': n,
        'min_ms': round(s[0], 2),
        'max_ms': round(s[-1], 2),
        'mean_ms': round(mean_val, 2),
        'median_ms': round(pct(50), 2),
        'p90_ms': round(pct(90), 2),
        'p95_ms': round(pct(95), 2),
        'p99_ms': round(pct(99), 2),
        'std_dev_ms': round(math.sqrt(variance), 2),
        'samples': [round(x, 2) for x in samples]
    }

async def bench_binance(symbol, target, timeout):
    samples = []
    uri = f'wss://stream.binance.com:9443/ws/{symbol}@trade'
    try:
        async with websockets.connect(uri) as ws:
            deadline = time.time() + timeout
            while len(samples) < target and time.time() < deadline:
                rem = max(0.1, deadline - time.time())
                msg = await asyncio.wait_for(ws.recv(), timeout=rem)
                t_recv = time.time() * 1000.0
                data = json.loads(msg)
                if 'E' in data:
                    samples.append(t_recv - float(data['E']))
    except Exception:
        pass
    return {'exchange': 'Binance Spot', 'symbol': symbol.upper(), 'stats': calc_stats(samples)}

async def bench_coinbase(symbol, target, timeout):
    samples = []
    uri = 'wss://ws-feed.exchange.coinbase.com'
    try:
        async with websockets.connect(uri) as ws:
            sub = {'type': 'subscribe', 'product_ids': [symbol], 'channels': ['matches']}
            await ws.send(json.dumps(sub))
            deadline = time.time() + timeout
            while len(samples) < target and time.time() < deadline:
                rem = max(0.1, deadline - time.time())
                msg = await asyncio.wait_for(ws.recv(), timeout=rem)
                t_recv = time.time() * 1000.0
                data = json.loads(msg)
                if data.get('type') == 'match' and 'time' in data:
                    t_exch = parse_iso(data['time'])
                    samples.append(t_recv - t_exch)
    except Exception:
        pass
    return {'exchange': 'Coinbase Spot', 'symbol': symbol, 'stats': calc_stats(samples)}

async def bench_kraken(symbol, target, timeout):
    samples = []
    uri = 'wss://ws.kraken.com/v2'
    try:
        async with websockets.connect(uri) as ws:
            sub = {'method': 'subscribe', 'params': {'channel': 'trade', 'symbol': [symbol]}}
            await ws.send(json.dumps(sub))
            deadline = time.time() + timeout
            while len(samples) < target and time.time() < deadline:
                rem = max(0.1, deadline - time.time())
                msg = await asyncio.wait_for(ws.recv(), timeout=rem)
                t_recv = time.time() * 1000.0
                data = json.loads(msg)
                if data.get('channel') == 'trade' and data.get('type') == 'update':
                    for t in data.get('data', []):
                        if 'timestamp' in t:
                            t_exch = parse_iso(t['timestamp'])
                            samples.append(t_recv - t_exch)
    except Exception:
        pass
    return {'exchange': 'Kraken Spot', 'symbol': symbol, 'stats': calc_stats(samples)}

async def bench_bitstamp(symbol, target, timeout):
    samples = []
    uri = 'wss://ws.bitstamp.net'
    channel = f'live_trades_{symbol.lower()}'
    try:
        async with websockets.connect(uri) as ws:
            sub = {
                'event': 'bts:subscribe',
                'data': {'channel': channel}
            }
            await ws.send(json.dumps(sub))
            deadline = time.time() + timeout
            while len(samples) < target and time.time() < deadline:
                rem = max(0.1, deadline - time.time())
                msg = await asyncio.wait_for(ws.recv(), timeout=rem)
                t_recv = time.time() * 1000.0
                data = json.loads(msg)
                if data.get('event') == 'trade' and 'data' in data:
                    t_data = data['data']
                    micro_ts = t_data.get('microtimestamp')
                    if micro_ts is not None:
                        t_exch = float(micro_ts) / 1000.0
                        samples.append(t_recv - t_exch)
                    elif 'timestamp' in t_data:
                        t_exch = float(t_data['timestamp']) * 1000.0
                        samples.append(t_recv - t_exch)
    except Exception:
        pass
    return {'exchange': 'Bitstamp Spot', 'symbol': symbol.upper(), 'stats': calc_stats(samples)}

async def bench_gemini(symbol, target, timeout):
    samples = []
    uri = 'wss://ws.gemini.com'
    try:
        async with websockets.connect(uri) as ws:
            sub = {'id': '1', 'method': 'SUBSCRIBE', 'params': [f'{symbol.lower()}@trade', f'{symbol.lower()}@bookTicker']}
            await ws.send(json.dumps(sub))
            deadline = time.time() + timeout
            while len(samples) < target and time.time() < deadline:
                rem = max(0.1, deadline - time.time())
                msg = await asyncio.wait_for(ws.recv(), timeout=rem)
                t_recv = time.time() * 1000.0
                data = json.loads(msg)
                if 'E' in data:
                    e_val = float(data['E'])
                    t_exch = e_val / 1000000.0 if e_val > 1e16 else (e_val if e_val > 1e11 else e_val * 1000.0)
                    samples.append(t_recv - t_exch)
    except Exception:
        pass
    return {'exchange': 'Gemini Spot', 'symbol': symbol.upper(), 'stats': calc_stats(samples)}

async def bench_lmax(symbol, target, timeout):
    samples = []
    uri = 'wss://public-data-api.london-digital.lmax.com/v1/web-socket'
    try:
        async with websockets.connect(uri) as ws:
            sub = {'type': 'SUBSCRIBE', 'channels': [{'name': 'TICKER', 'instruments': [symbol.lower()]}]}
            await ws.send(json.dumps(sub))
            deadline = time.time() + timeout
            while len(samples) < target and time.time() < deadline:
                rem = max(0.1, deadline - time.time())
                msg = await asyncio.wait_for(ws.recv(), timeout=rem)
                t_recv = time.time() * 1000.0
                data = json.loads(msg)
                if data.get('type') == 'TICKER' and 'timestamp' in data:
                    t_exch = parse_iso(data['timestamp'])
                    samples.append(t_recv - t_exch)
    except Exception:
        pass
    return {'exchange': 'LMAX Digital', 'symbol': symbol.lower(), 'stats': calc_stats(samples)}

async def bench_cryptocom(symbol, target, timeout):
    samples = []
    uri = 'wss://stream.crypto.com/exchange/v1/market'
    channel = f'trade.{symbol.upper()}'
    try:
        async with websockets.connect(uri) as ws:
            sub = {'id': 1, 'method': 'subscribe', 'params': {'channels': [channel]}}
            await ws.send(json.dumps(sub))
            deadline = time.time() + timeout
            while len(samples) < target and time.time() < deadline:
                rem = max(0.1, deadline - time.time())
                msg = await asyncio.wait_for(ws.recv(), timeout=rem)
                t_recv = time.time() * 1000.0
                data = json.loads(msg)
                if data.get('method') == 'public/heartbeat':
                    await ws.send(json.dumps({'id': data.get('id'), 'method': 'public/respond-heartbeat'}))
                    continue
                if data.get('id') == 1:
                    continue
                res = data.get('result', {})
                if res.get('channel') == 'trade':
                    for item in res.get('data', []):
                        if 'd' in item:
                            e_val = float(item['d'])
                            t_exch = e_val / 1000000.0 if e_val > 1e16 else (e_val if e_val > 1e11 else e_val * 1000.0)
                            samples.append(t_recv - t_exch)
                        elif 't' in item:
                            samples.append(t_recv - float(item['t']))
    except Exception:
        pass
    return {'exchange': 'Crypto.com', 'symbol': symbol.upper(), 'stats': calc_stats(samples)}

async def main():
    tasks = []
    active_exchanges = $EXCHANGE_PYTHON_LIST
    if 'binance' in active_exchanges:
        tasks.append(bench_binance('$SYM_BINANCE', $SAMPLE_COUNT, $DURATION_SECONDS))
    if 'coinbase' in active_exchanges:
        tasks.append(bench_coinbase('$SYM_COINBASE', $SAMPLE_COUNT, $DURATION_SECONDS))
    if 'kraken' in active_exchanges:
        tasks.append(bench_kraken('$SYM_KRAKEN', $SAMPLE_COUNT, $DURATION_SECONDS))
    if 'bitstamp' in active_exchanges:
        tasks.append(bench_bitstamp('$SYM_BITSTAMP', $SAMPLE_COUNT, $DURATION_SECONDS))
    if 'gemini' in active_exchanges:
        tasks.append(bench_gemini('$SYM_GEMINI', $SAMPLE_COUNT, $DURATION_SECONDS))
    if 'lmax' in active_exchanges:
        tasks.append(bench_lmax('$SYM_LMAX', $SAMPLE_COUNT, $DURATION_SECONDS))
    if 'cryptocom' in active_exchanges:
        tasks.append(bench_cryptocom('$SYM_CRYPTOCOM', $SAMPLE_COUNT, $DURATION_SECONDS))
    
    results = await asyncio.gather(*tasks)
    print("---BENCHMARK_RESULT_START---")
    print(json.dumps(results))
    print("---BENCHMARK_RESULT_END---")

asyncio.run(main())
EOF

scp -q "$TEMP_LOCAL_SCRIPT" "${EC2_HOST}:${TEMP_REMOTE_SCRIPT}"

START_BENCH=$(date +%s)
BENCH_OUTPUT=$(ssh -o BatchMode=yes "$EC2_HOST" "~/.ws_bench_env/bin/python '$TEMP_REMOTE_SCRIPT'")
END_BENCH=$(date +%s)
BENCH_DURATION=$(( END_BENCH - START_BENCH ))

JSON_PAYLOAD=$(echo "$BENCH_OUTPUT" | sed -n '/---BENCHMARK_RESULT_START---/,/---BENCHMARK_RESULT_END---/{//!p}')

if [ -z "$JSON_PAYLOAD" ]; then
    log_error "Failed to retrieve JSON payload from EC2 benchmark output:\n$BENCH_OUTPUT"
    exit 1
fi

if [ "$CLEAN_REMOTE_ENV" = true ]; then
    ssh -o BatchMode=yes "$EC2_HOST" "rm -rf ~/.ws_bench_env" 2>/dev/null || true
fi

# Format output and write logs using Python formatter
LOG_FILE_TXT="${RESOLVED_LOG_DIR}/ws-latency-${BASE_SYMBOL}-${TIMESTAMP_STR}.log"
LOG_FILE_JSON="${RESOLVED_LOG_DIR}/ws-latency-${BASE_SYMBOL}-${TIMESTAMP_STR}.json"

python3 - << EOF
import json, sys

json_raw = '''$JSON_PAYLOAD'''
data = json.loads(json_raw)

header = "+---------------+-----------+---------+---------+---------+---------+---------+---------+---------+---------+---------+"
title  = "| Exchange      | Symbol    | Samples | Min(ms) | Med(ms) | Avg(ms) | P90(ms) | P95(ms) | P99(ms) | Max(ms) |  StdDev |"

lines = [header, title, header]

def fmt(val):
    if val is None:
        return "    N/A"
    return f"{float(val):7.2f}"

for item in data:
    exch = item['exchange']
    sym = item['symbol']
    st = item['stats']
    if st and st.get('count', 0) > 0:
        line = f"| {exch:<13} | {sym:<9} | {st['count']:7d} | {fmt(st['min_ms'])} | {fmt(st['median_ms'])} | {fmt(st['mean_ms'])} | {fmt(st['p90_ms'])} | {fmt(st['p95_ms'])} | {fmt(st['p99_ms'])} | {fmt(st['max_ms'])} | {fmt(st['std_dev_ms'])} |"
    else:
        line = f"| {exch:<13} | {sym:<9} |       0 |     N/A |     N/A |     N/A |     N/A |     N/A |     N/A |     N/A |     N/A |"
    lines.append(line)

lines.append(header)
table_str = "\n".join(lines)

log_content = f"""===========================================================================================================
 Polymarket Bot - WebSocket One-Way Latency Benchmark (Linux)
 Timestamp          : $(date -u +"%Y-%m-%d %H:%M:%S UTC")
 EC2 Instance Host  : $EC2_HOST (AWS eu-west-1 Dublin)
 Base Symbol        : $BASE_SYMBOL
 Target Samples     : $SAMPLE_COUNT per exchange
 Execution Duration : ${BENCH_DURATION}s
 Chrony Reference   : $REF_ID
 Chrony RMS Offset  : $RMS_OFFSET
===========================================================================================================

{table_str}
"""

print("\n===========================================================================================================")
print(" Benchmark Results: One-Way WebSocket Latency (t_recv - t_event)")
print(" EC2 Region: AWS eu-west-1 (Dublin) | Execution Time: ${BENCH_DURATION}s")
print("===========================================================================================================")
print(table_str)

with open("$LOG_FILE_TXT", "w") as f:
    f.write(log_content)

with open("$LOG_FILE_JSON", "w") as f:
    json.dump(data, f, indent=2)

EOF

log_info " Persistent Text Log : $LOG_FILE_TXT"
log_info " Raw JSON Metrics    : $LOG_FILE_JSON"
log_info "==========================================================================================================="
