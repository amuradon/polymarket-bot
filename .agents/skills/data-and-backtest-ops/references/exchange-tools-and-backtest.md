# Exchange Latency Benchmark, Historical Trade Downloaders & Backtest CLI

## 1. Exchange WebSocket Latency Benchmark (`.\scripts\measure-exchange-ws-latency.ps1`)
Verifies `chrony` (AWS Time Sync Service) on the `eu-west-1` EC2 host and measures one-way WebSocket latency (`t_recv - t_exchange_event`) across 7 exchanges (`binance`, `coinbase`, `kraken`, `bitstamp`, `gemini`, `lmax`, `cryptocom`), outputting P50/P90/P95/P99 statistics and logs to `.\logs\latency`.

```powershell
# Benchmark all 7 exchanges for BTC (default 100 samples, 60s timeout):
.\scripts\measure-exchange-ws-latency.ps1

# Benchmark selected exchanges for ETH with custom sample count and timeout:
.\scripts\measure-exchange-ws-latency.ps1 -Symbol ETH -Exchanges cryptocom,lmax -SampleCount 200 -DurationSeconds 90
```

---

## 2. Historical Spot Trade Downloaders (Python in `.\scripts`)
Downloads historical trades via REST with HTTP rate-limit backoff and writes daily ZSTD-compressed Parquet files (`${Dir}/${Symbol}/trades/${Symbol}-trades-${yyyy-MM-dd}.parquet`).

```powershell
cd .\scripts
.\.venv\Scripts\activate
pip install -r requirements.txt

# Kraken Spot (/0/public/Trades -> default D:\Crypto\data\Polymarket\collector\kraken\spot):
python download_kraken_trades.py --symbol USDTUSD --from 2026-11-04 --to 2026-11-09
python download_kraken_trades.py --symbol BTCUSD --from 2026-11-10 --to 2026-11-10 --dir D:\Crypto\data\Polymarket\collector\kraken\spot

# Coinbase Exchange Spot (/products/<product-id>/trades with binary search trade_id -> default D:\Crypto\data\Polymarket\collector\coinbase\spot):
python download_coinbase_trades.py --symbol BTCUSD --from 2026-11-04 --to 2026-11-09 --dir D:\Crypto\data\Polymarket\collector\coinbase\spot
```

---

## 3. Backtest Engine Execution (`backtest` Module — Port `8083`)

### Configuration Properties (`application.properties` / `-D` Overrides)
| Property | Default | Description |
| :--- | :--- | :--- |
| `polymarket.backtest.batchMode` | `false` | `true` = run immediately on startup, export `backtest_<Strategy>_<yyyyMMdd_HHmmss>.json` + `latest.json`, and exit; `false` = start REST server on `:8083` |
| `polymarket.backtest.strategy` | `UtterParameciumStrategy` | Strategy class/name |
| `polymarket.backtest.initial-capital` | `10000.0` | Starting capital in USD |
| `polymarket.backtest.from` | `2026-10-12` | Inclusive start date (`yyyy-MM-dd`) |
| `polymarket.backtest.to` | Current UTC date | Inclusive end date (`yyyy-MM-dd`) |
| `polymarket.backtest.output-dir` | `D:/Crypto/data/Polymarket/backtesting` | Output directory for JSON reports |

### Batch Mode & Server Mode (PowerShell)
```powershell
# Batch Mode via Quarkus Dev with defaults:
.\mvnw quarkus:dev -pl backtest "-Dpolymarket.backtest.batchMode=true"

# Batch Mode from packaged JAR with custom parameters:
.\mvnw package -pl backtest -am -DskipTests
java `
  "-Dpolymarket.backtest.batchMode=true" `
  "-Dpolymarket.backtest.strategy=UtterParameciumStrategy" `
  "-Dpolymarket.backtest.initial-capital=25000.0" `
  "-Dpolymarket.backtest.from=2026-10-15" `
  "-Dpolymarket.backtest.to=2026-11-10" `
  "-Dpolymarket.backtest.output-dir=D:/Crypto/data/Polymarket/backtesting" `
  -jar backtest/target/quarkus-app/quarkus-run.jar

# Interactive REST Server Mode (GET /api/status, POST /api/backtest/run, GET /api/backtest/run/{jobId}):
.\mvnw quarkus:dev -pl backtest
```
