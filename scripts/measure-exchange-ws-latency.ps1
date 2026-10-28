<#
.SYNOPSIS
    Measures one-way WebSocket network latency from Binance Spot, Coinbase Spot, Kraken Spot, and Bitstamp Spot
    to an AWS EC2 instance (eu-west-1 Dublin) and logs comprehensive statistical metrics.

.DESCRIPTION
    Connects to the specified AWS EC2 instance (configured via SSH/SSM tunnel), verifies system
    clock synchronization (chrony/AWS Time Sync Service), and executes a concurrent Python
    WebSocket benchmark across Binance, Coinbase, Kraken, and Bitstamp public trade streams.
    
    Measures the exact one-way transmission latency (t_recv - t_exchange_event) for each trade event,
    calculates key statistics (Sample Count, Min, Max, Mean, Median/P50, P90, P95, P99, StdDev),
    presents a formatted table in the console, and writes persistent human-readable and JSON logs
    to the designated log directory.

.PARAMETER Ec2Host
    EC2 SSH host alias configured in ~/.ssh/config. Defaults to 'i-058e25a0b3334934d'.

.PARAMETER Symbol
    Cryptocurrency symbol to benchmark (e.g. 'BTC', 'ETH', 'SOL', or 'BTCUSDT'). Defaults to 'BTC'.

.PARAMETER SampleCount
    Target number of trade event samples to collect per exchange. Defaults to 100.

.PARAMETER DurationSeconds
    Maximum timeout in seconds to wait for samples before concluding the benchmark. Defaults to 60.

.PARAMETER Exchanges
    List of exchanges to include in the benchmark ('binance', 'coinbase', 'kraken', 'bitstamp'). Defaults to all four.

.PARAMETER LogDir
    Directory path where latency logs will be saved. Defaults to '.\logs\latency'.

.PARAMETER CleanRemoteEnv
    If specified, removes the remote Python benchmarking environment (~/.ws_bench_env) on EC2 after execution.

.EXAMPLE
    .\scripts\measure-exchange-ws-latency.ps1
    Measures one-way latency for BTC across Binance, Coinbase, Kraken, and Bitstamp (100 samples each).

.EXAMPLE
    .\scripts\measure-exchange-ws-latency.ps1 -SampleCount 200 -DurationSeconds 30
    Collects up to 200 samples per exchange with a 30-second timeout.

.EXAMPLE
    .\scripts\measure-exchange-ws-latency.ps1 -Symbol ETH -Exchanges binance,coinbase,bitstamp
    Measures ETH trade streams on Binance, Coinbase, and Bitstamp.
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Ec2Host = "i-058e25a0b3334934d",

    [Parameter(Position = 1)]
    [string]$Symbol = "BTC",

    [Parameter()]
    [ValidateRange(5, 5000)]
    [int]$SampleCount = 100,

    [Parameter()]
    [ValidateRange(5, 600)]
    [int]$DurationSeconds = 60,

    [Parameter()]
    [ValidateSet("binance", "coinbase", "kraken", "bitstamp")]
    [string[]]$Exchanges = @("binance", "coinbase", "kraken", "bitstamp"),

    [Parameter()]
    [string]$LogDir = ".\logs\latency",

    [Parameter()]
    [switch]$CleanRemoteEnv
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Ensure SessionManagerPlugin and SSH are in PATH
$pluginBin = "C:\Program Files\Amazon\SessionManagerPlugin\bin"
if ((Test-Path -LiteralPath $pluginBin) -and ($env:PATH -notlike "*$pluginBin*")) {
    $env:PATH += ";$pluginBin"
}

$sshCmd = Get-Command "ssh" -ErrorAction SilentlyContinue
if (-not $sshCmd) {
    throw "SSH executable was not found on PATH. Please ensure OpenSSH client is installed."
}

# Normalize symbol for each exchange
function Get-NormalizedSymbols {
    param([string]$InputSymbol)
    $clean = $InputSymbol.ToUpperInvariant().Replace('-', '').Replace('/', '')
    if ($clean.EndsWith('USDT')) { $clean = $clean.Substring(0, $clean.Length - 4) }
    elseif ($clean.EndsWith('USD')) { $clean = $clean.Substring(0, $clean.Length - 3) }

    return [PSCustomObject]@{
        Base     = $clean
        Binance  = ($clean.ToLowerInvariant() + "usdt")
        Coinbase = ($clean + "-USD")
        Kraken   = ($clean + "/USD")
        Bitstamp = ($clean.ToLowerInvariant() + "usd")
    }
}

$symbols = Get-NormalizedSymbols -InputSymbol $Symbol
$timestampStr = (Get-Date).ToString("yyyyMMdd-HHmmss")
$resolvedLogDir = (Resolve-Path -Path (New-Item -ItemType Directory -Path $LogDir -Force)).Path

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host " Polymarket Bot - WebSocket One-Way Latency Benchmark (Ticket #44)" -ForegroundColor Cyan
Write-Host " EC2 Instance Host : $Ec2Host" -ForegroundColor White
Write-Host " Base Symbol       : $($symbols.Base)" -ForegroundColor Yellow
Write-Host " Target Samples    : $SampleCount per exchange" -ForegroundColor White
Write-Host " Max Timeout       : ${DurationSeconds}s" -ForegroundColor White
Write-Host " Target Exchanges  : $($Exchanges -join ', ')" -ForegroundColor White
Write-Host " Output Directory  : $resolvedLogDir" -ForegroundColor White
Write-Host "=================================================================" -ForegroundColor Cyan

# Step 1: Check EC2 connectivity and chrony clock synchronization
Write-Host "[1/3] Connecting to EC2 host and verifying time synchronization..." -ForegroundColor Cyan
$chronyRaw = & ssh $Ec2Host "chronyc tracking 2>&1"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to connect to EC2 host '$Ec2Host' via SSH. Details: $chronyRaw"
}

$refId = ($chronyRaw | Select-String "Reference ID\s*:\s*(.+)") -replace ".*:\s*", ""
$lastOffset = ($chronyRaw | Select-String "Last offset\s*:\s*(.+)") -replace ".*:\s*", ""
$rmsOffset = ($chronyRaw | Select-String "RMS offset\s*:\s*(.+)") -replace ".*:\s*", ""

Write-Host "      Chrony Reference   : $refId (AWS Time Sync Service)" -ForegroundColor Green
Write-Host "      Chrony Last Offset : $lastOffset" -ForegroundColor Green
Write-Host "      Chrony RMS Offset  : $rmsOffset" -ForegroundColor Green

# Step 2: Ensure remote benchmark virtual environment exists on EC2
Write-Host "[2/3] Preparing remote benchmarking environment on EC2..." -ForegroundColor Cyan
$setupRemoteCmd = @'
if [ ! -d "$HOME/.ws_bench_env" ]; then
    python3 -m venv "$HOME/.ws_bench_env" >/dev/null 2>&1
    "$HOME/.ws_bench_env/bin/pip" install websockets >/dev/null 2>&1
fi
'@
$setupOutput = & ssh $Ec2Host $setupRemoteCmd 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "Failed to initialize remote Python environment on EC2: $setupOutput"
}
Write-Host "      Remote environment ~/.ws_bench_env ready." -ForegroundColor Green

# Step 3: Run concurrent benchmark
Write-Host "[3/3] Running concurrent WebSocket latency measurement on EC2..." -ForegroundColor Cyan

$exchangesParam = ($Exchanges | ForEach-Object { "'$_'" }) -join ", "
$remoteScript = @"
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
    except Exception as e:
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
    except Exception as e:
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
    except Exception as e:
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
    except Exception as e:
        pass
    return {'exchange': 'Bitstamp Spot', 'symbol': symbol.upper(), 'stats': calc_stats(samples)}

async def main():
    tasks = []
    active_exchanges = [$exchangesParam]
    if 'binance' in active_exchanges:
        tasks.append(bench_binance('$($symbols.Binance)', $SampleCount, $DurationSeconds))
    if 'coinbase' in active_exchanges:
        tasks.append(bench_coinbase('$($symbols.Coinbase)', $SampleCount, $DurationSeconds))
    if 'kraken' in active_exchanges:
        tasks.append(bench_kraken('$($symbols.Kraken)', $SampleCount, $DurationSeconds))
    if 'bitstamp' in active_exchanges:
        tasks.append(bench_bitstamp('$($symbols.Bitstamp)', $SampleCount, $DurationSeconds))
    
    results = await asyncio.gather(*tasks)
    print("---BENCHMARK_RESULT_START---")
    print(json.dumps(results))
    print("---BENCHMARK_RESULT_END---")

asyncio.run(main())
"@

# Upload script to remote temp file and execute
$remoteScriptPath = "/tmp/ws_bench_$([guid]::NewGuid().ToString('N')).py"
$tempLocalScript = Join-Path $env:TEMP "ws_bench_$([guid]::NewGuid().ToString('N')).py"
[System.IO.File]::WriteAllText($tempLocalScript, $remoteScript, [System.Text.UTF8Encoding]::new($false))

try {
    # Copy script to EC2
    & scp -q $tempLocalScript "${Ec2Host}:${remoteScriptPath}"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to copy benchmark script to EC2."
    }

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $benchOutput = & ssh $Ec2Host "~/.ws_bench_env/bin/python $remoteScriptPath" 2>&1
    $sw.Stop()

    if ($LASTEXITCODE -ne 0) {
        throw "Benchmark execution on EC2 failed (code $LASTEXITCODE): $benchOutput"
    }

    # Extract JSON result block
    $outputStr = ($benchOutput -join "`n")
    $pattern = '(?s)---BENCHMARK_RESULT_START---\s*(.*?)\s*---BENCHMARK_RESULT_END---'
    if ($outputStr -match $pattern) {
        $jsonStr = $Matches[1]
    } else {
        throw "Could not find benchmark result JSON markers in EC2 output: $outputStr"
    }

    $benchmarkData = $jsonStr | ConvertFrom-Json

    # Optional cleanup of remote environment
    if ($CleanRemoteEnv) {
        Write-Host "      Cleaning up remote environment ~/.ws_bench_env..." -ForegroundColor DarkGray
        & ssh $Ec2Host "rm -rf ~/.ws_bench_env" 2>$null
    }
} finally {
    if (Test-Path -LiteralPath $tempLocalScript) {
        Remove-Item -LiteralPath $tempLocalScript -Force -ErrorAction SilentlyContinue
    }
    & ssh $Ec2Host "rm -f $remoteScriptPath" 2>$null
}

# Format and display results
Write-Host "`n===========================================================================================================" -ForegroundColor Cyan
Write-Host " Benchmark Results: One-Way WebSocket Latency (t_recv - t_event)" -ForegroundColor Green
Write-Host " EC2 Region: AWS eu-west-1 (Dublin) | Execution Time: $($sw.Elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "===========================================================================================================" -ForegroundColor Cyan

$inv = [System.Globalization.CultureInfo]::InvariantCulture

function Format-Num([object]$val) {
    if ($null -eq $val -or $val -eq "N/A") { return "   N/A" }
    return ([double]$val).ToString("F2", $inv).PadLeft(7)
}

$tableLines = [System.Collections.Generic.List[string]]::new()
$header = "+---------------+---------+---------+---------+---------+---------+---------+---------+---------+---------+---------+"
$title  = "| Exchange      | Symbol  | Samples | Min(ms) | Med(ms) | Avg(ms) | P90(ms) | P95(ms) | P99(ms) | Max(ms) |  StdDev |"
$tableLines.Add($header)
$tableLines.Add($title)
$tableLines.Add($header)

$reportRows = [System.Collections.Generic.List[PSCustomObject]]::new()

foreach ($item in $benchmarkData) {
    $exch = $item.exchange
    $sym = $item.symbol
    $st = $item.stats

    if ($null -ne $st -and $st.count -gt 0) {
        $row = [PSCustomObject]@{
            "Exchange"   = $exch
            "Symbol"     = $sym
            "Samples"    = $st.count
            "Min_ms"     = [math]::Round([double]$st.min_ms, 2)
            "Median_ms"  = [math]::Round([double]$st.median_ms, 2)
            "Mean_ms"    = [math]::Round([double]$st.mean_ms, 2)
            "P90_ms"     = [math]::Round([double]$st.p90_ms, 2)
            "P95_ms"     = [math]::Round([double]$st.p95_ms, 2)
            "P99_ms"     = [math]::Round([double]$st.p99_ms, 2)
            "Max_ms"     = [math]::Round([double]$st.max_ms, 2)
            "StdDev_ms"  = [math]::Round([double]$st.std_dev_ms, 2)
        }
        $reportRows.Add($row)

        $line = "| {0,-13} | {1,-7} | {2,7} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} |" -f `
            $exch, $sym, $st.count, `
            (Format-Num $st.min_ms), (Format-Num $st.median_ms), (Format-Num $st.mean_ms), `
            (Format-Num $st.p90_ms), (Format-Num $st.p95_ms), (Format-Num $st.p99_ms), `
            (Format-Num $st.max_ms), (Format-Num $st.std_dev_ms)
        $tableLines.Add($line)
    } else {
        $row = [PSCustomObject]@{
            "Exchange"   = $exch
            "Symbol"     = $sym
            "Samples"    = 0
            "Min_ms"     = $null
            "Median_ms"  = $null
            "Mean_ms"    = $null
            "P90_ms"     = $null
            "P95_ms"     = $null
            "P99_ms"     = $null
            "Max_ms"     = $null
            "StdDev_ms"  = $null
        }
        $reportRows.Add($row)
        $line = "| {0,-13} | {1,-7} | {2,7} |     N/A |     N/A |     N/A |     N/A |     N/A |     N/A |     N/A |     N/A |" -f $exch, $sym, 0
        $tableLines.Add($line)
    }
}
$tableLines.Add($header)

$tableStr = $tableLines -join "`n"
Write-Host $tableStr -ForegroundColor White

# Save human-readable log and JSON
$logFileTxt = Join-Path $resolvedLogDir "ws-latency-$($symbols.Base)-$timestampStr.log"
$logFileJson = Join-Path $resolvedLogDir "ws-latency-$($symbols.Base)-$timestampStr.json"

$logContent = @"
===========================================================================================================
 Polymarket Bot - WebSocket One-Way Latency Benchmark (Ticket #44)
 Timestamp          : $(Get-Date -Format "yyyy-MM-dd HH:mm:ss UTCzz")
 EC2 Instance Host  : $Ec2Host (AWS eu-west-1 Dublin)
 Base Symbol        : $($symbols.Base)
 Target Samples     : $SampleCount per exchange
 Execution Duration : $($sw.Elapsed.ToString('hh\:mm\:ss\.fff'))
 Chrony Reference   : $refId
 Chrony RMS Offset  : $rmsOffset
===========================================================================================================

$tableStr

"@

[System.IO.File]::WriteAllText($logFileTxt, $logContent, [System.Text.UTF8Encoding]::new($false))
[System.IO.File]::WriteAllText($logFileJson, ($benchmarkData | ConvertTo-Json -Depth 5), [System.Text.UTF8Encoding]::new($false))

Write-Host " Persistent Text Log : $logFileTxt" -ForegroundColor Cyan
Write-Host " Raw JSON Metrics    : $logFileJson" -ForegroundColor Cyan
Write-Host "===========================================================================================================" -ForegroundColor Cyan
