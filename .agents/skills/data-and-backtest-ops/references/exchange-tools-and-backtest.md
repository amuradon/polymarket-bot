# Exchange Latency Benchmark, Historical Trade Downloaders & Backtest CLI (Ubuntu Bash)

## 1. Exchange WebSocket Latency Benchmark (`./scripts/measure-exchange-ws-latency.sh`)
Verifies `chrony` (AWS Time Sync Service) on the `eu-west-1` EC2 host and measures one-way WebSocket latency (`t_recv - t_exchange_event`) across 7 exchanges (`binance`, `coinbase`, `kraken`, `bitstamp`, `gemini`, `lmax`, `cryptocom`), outputting P50/P90/P95/P99 statistics and logs to `./logs/latency`.

```bash
# Benchmark all 7 exchanges for BTC (default 100 samples, 60s timeout):
./scripts/measure-exchange-ws-latency.sh

# Benchmark selected exchanges for ETH with custom sample count and timeout:
./scripts/measure-exchange-ws-latency.sh -s ETH -e cryptocom,lmax -n 200 -t 90
```

---

## 2. Historical Spot Trade Downloaders (Python in `./scripts`)
Downloads historical trades via public archives / REST with HTTP rate-limit backoff and writes daily ZSTD-compressed Parquet files (`<dir>/<SYMBOL>/trades/<SYMBOL>-trades-<YYYY-MM-DD>.parquet` or `aggTrades`). All scripts default directly to `/data/polymarket/collector/...`.

```bash
cd ./scripts
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Binance Spot aggTrades (data.binance.vision ZIP primary + REST fallback -> default /data/polymarket/collector/binance/spot):
python3 download_binance_spot_aggtrades.py --symbol BTCUSDT --from 2026-11-24 --to 2026-11-25

# Kraken Spot (/0/public/Trades -> default /data/polymarket/collector/kraken/spot):
python3 download_kraken_trades.py --symbol USDTUSD --from 2026-11-24 --to 2026-11-25
python3 download_kraken_trades.py --symbol BTCUSD --from 2026-11-24 --to 2026-11-25

# Coinbase Exchange Spot (/products/<product-id>/trades -> default /data/polymarket/collector/coinbase/spot):
python3 download_coinbase_trades.py --symbol BTCUSD --from 2026-11-24 --to 2026-11-25
```

---

## 3. Backtest Engine Execution (`backtest` Module — Port `8083`)

### Configuration Properties (`application.properties` / `-D` Overrides)
| Property | Default | Description |
| :--- | :--- | :--- |
| `polymarket.backtest.batchMode` | `false` | `true` = execute simulation on startup, persist to DuckDB history, and exit; `false` = start REST & Web UI Studio on `:8083` |
| `polymarket.backtest.strategy` | `OraclePulseLockStrategy` | Strategy class/name |
| `polymarket.backtest.symbol` | `BTCUSDT` | Traded symbol |
| `polymarket.backtest.initial-capital` | `10000.0` | Starting capital in USD |
| `polymarket.backtest.from` | `2026-10-12` | Inclusive start date (`yyyy-MM-dd`) |
| `polymarket.backtest.to` | Current UTC date | Inclusive end date (`yyyy-MM-dd`) |
| `polymarket.backtest.latency-factor` | `1.0` | Scaling multiplier for simulated CLOB execution delay |
| `polymarket.backtest.order-gateway-delay-ms` | `220` | Two-phase Polymarket CLOB `POST /order` delay in ms (`~196 ms` speed bump + `~24 ms` return) |
| `polymarket.backtest.collector-dir` | `/data/polymarket/collector` | Root directory of converted Parquet archives |
| `polymarket.backtest.history-db-path` | `${user.home}/.local/share/polymarket-bot/backtest-history.duckdb` | Persistent DuckDB database on internal NVMe SSD |
| `polymarket.backtest.duckdb.memory-limit` | `6GB` | Native DuckDB C++ buffer pool limit (tuned for 32 GB RAM) |
| `polymarket.backtest.duckdb.threads` | `4` | DuckDB worker threads for Parquet ZSTD decompression & sorting |
| `polymarket.backtest.duckdb.temp-dir` | `/tmp/polymarket-duckdb` | Local `tmpfs` directory for DuckDB temporary spill files |

### Batch Mode & Server Mode (Ubuntu Bash)
```bash
# Interactive REST & Web UI Studio Mode (http://localhost:8083/):
./mvnw quarkus:dev -pl backtest

# Batch Mode via Quarkus Dev with defaults:
./mvnw quarkus:dev -pl backtest -Dpolymarket.backtest.batchMode=true

# Batch Mode from packaged JAR with ZGC and 4 GB heap ceiling (leaving ~22+ GB for Linux Page Cache & DuckDB):
./mvnw package -pl backtest -am -DskipTests
java --enable-native-access=ALL-UNNAMED -XX:+UseZGC -Xmx4g \
  -Dpolymarket.backtest.batchMode=true \
  -Dpolymarket.backtest.strategy=OraclePulseLockStrategy \
  -Dpolymarket.backtest.from=2026-11-24 \
  -Dpolymarket.backtest.to=2026-11-25 \
  -jar backtest/target/quarkus-app/quarkus-run.jar
```
