---
name: project-architecture
description: >-
  Detailed architecture reference for Polymarket Bot's 7 Maven submodules,
  exchange and Polymarket WebSocket stream contracts, data collector Zstandard
  recording and Blue-Green cutover endpoints, 3-stage trading lifecycle,
  frontend stack, and local PowerShell startup commands. Use when designing
  cross-module features, adding connectors/streams/strategies, working on
  collector or trading runtime internals, or starting applications locally.
---

# Polymarket Bot — Detailed Architecture Reference

## 1. Platform Scope & 3-Stage Lifecycle

Automated low-latency algorithmic trading platform for **Polymarket binary prediction markets** (Up/Down crypto price resolution contracts: core `BTC/USD` 5m/15m, extensible to `ETH`, `SOL`, 1h/4h/1d timeframes, and multi-strategy alpha such as TWAP divergence, order book imbalance, latency arbitrage, and mean-reversion):

1. **`backtest` (`:8083`)**: High-throughput historical simulation over 1s snapshots and L2 tick data (Parquet/DuckDB) modeling slippage, market impact, exchange latency, and taker fees; evaluates Sharpe, Sortino, Win Rate, Profit Factor, and Max Drawdown.
2. **`paper` (`:8082`)**: Connects to live real-time exchange and Polymarket feeds and routes orders to a virtual order book modeling queue position and fills against live CLOB updates.
3. **`live` (`:8080`)**: Real-money execution on Polymarket CLOB (Polygon wallet, pre-allocated EIP-712 order signing, `TCP_NODELAY` / native Netty `epoll`, automated circuit breakers, kill-switches, and per-candle position caps).

---

## 2. Submodule Internals & Stream Contracts

### `common` (Pure Domain & Strategy Library)
- **Domain Models**: Compact, allocation-friendly immutable structures for `Timeframe`, candles, order book quotes, and trade records.
- **Quantitative Calculators**: Rolling calculated Spot & 60s TWAP (`CalculatedSpotTwapEngine` replicating Polymarket settlement rules) and performance metrics.
- **Strategy & Execution Contracts**: `TradingStrategy`, `StrategyContext`, `ExecutionRouter`, strategy registry, and shared strategy implementations (e.g. `UtterParameciumStrategy`).

### `connector` (Exchange & Polymarket Connectivity Library — Zero `common` Dependency)
- **Resilient WebSocket Core**: Reconnecting WebSocket abstractions, connection state tracking, and raw/parsed listener contracts.
- **Binance Connectors**: Spot and USD-M Futures streams: `SPOT_AGG_TRADES`, `SPOT_BOOK_TICKER`, `FUTURES_AGG_TRADES`, `FUTURES_BOOK_TICKER`.
- **Kraken Connector**: Kraken Spot v2 WebSocket (`wss://ws.kraken.com/v2`) `trade` channel updates (`USDTUSD` and configured symbol such as `BTCUSD`, `ETHUSD`, `SOLUSD`) using a dedicated event-loop I/O reader and zero-decode frame filtering.
- **Coinbase Connector**: Coinbase Exchange Spot WebSocket (`wss://ws-feed.exchange.coinbase.com`) `matches` trade streams (`BTC-USD`, `ETH-USD`, `SOL-USD`) using a dedicated event-loop I/O reader and zero-decode frame filtering.
- **Polymarket Connectors**:
  - Gamma market discovery and resolution polling.
  - PolyBolt v2 (`wss://ws-live-v2.polymarket.com/ws`) for `price.crypto` and `price.crypto.twap` (60s TWAP).
  - Per-market CLOB order book and trade WebSocket streams.

### `data` (Market Data Collector Quarkus App — `:8084`, Zero `common`/`trading` Dependency)
- **Storage Format**: Appends raw WebSocket JSON payloads paired with local receipt timestamps (`ts`) directly to disk in **JSONL + Zstandard Streaming (`.jsonl.zst`, `zstd-jni`)** with zero WebSocket JSON parsing overhead.
- **UTC Daily Stream Files**: Records the 4 Binance streams, Kraken Spot `trade` (`<SYMBOL>` plus optional `USDTUSD` controlled by `polymarket.collector.kraken.usdtusd-enabled`), Coinbase Spot `matches` (`<SYMBOL>`), and PolyBolt (`price.crypto`, `price.crypto.twap`) into separate UTC daily files.
- **Per-Market Lifecycle Files**: Records the active Polymarket market plus `lookahead-count` (default `5`) upcoming markets (Gamma discovery, CLOB book/trade events, and market resolution), closing the file and opening the next lookahead stream once resolution is recorded.
- **Status & In-Host Blue-Green Cutover API**:
  - `GET /api/collector/status`: Reports active streams, `writeMode` (`ACTIVE`, `STANDBY`, `FROZEN`), and `readyForCutover`.
  - `POST /api/collector/cutover/freeze`: Flushes and closes active Zstd frames at cutover timestamp `T_cutover`.
  - `POST /api/collector/cutover/activate?afterTimestampMs={ts}`: Replays buffered `STANDBY` frames with `ts > afterTimestampMs` and switches to `ACTIVE` with zero data loss and zero duplicate records.

### `trading` (Shared Runtime Layer for `paper` & `live`)
- **Stream Bridge**: Zero-allocation `CalculatedSpotStreamProcessor` bridging Binance Spot (`aggTrades`), Kraken Spot (`USDTUSD` + `<SYMBOL>` `trade` updates), and Coinbase Spot (`matches`) frames into `CalculatedSpotTwapEngine` and `TradingStrategy`.
- **Web Stack**: Quarkus REST (`quarkus-rest`, `quarkus-rest-jackson`), Qute server-side templates (`quarkus-rest-qute`), reactive WebSockets (`quarkus-websockets-next`), Vert.x (`quarkus-vertx`), Scheduler (`quarkus-scheduler`), and TradingView Lightweight Charts v5 + Vanilla JS/CSS.

### `live` (`:8080`), `paper` (`:8082`), `backtest` (`:8083`)
- **`live`**: `polymarket.trading.mode=live`; streams CLOB, PolyBolt, Binance, Kraken, and Coinbase feeds into `CalculatedSpotTwapEngine` and `UtterParameciumStrategy` with live EIP-712 CLOB order submission.
- **`paper`**: `polymarket.trading.mode=paper`; identical feed pipeline routed to paper execution simulator.
- **`backtest`**: `polymarket.trading.mode=backtest`; supports startup batch execution (`polymarket.backtest.batchMode=true`) or interactive REST server (`GET /api/status`, `POST /api/backtest/run`, `GET /api/backtest/run/{jobId}`). For full batch CLI flags, load the `data-and-backtest-ops` skill.

---

## 3. Local Development Commands (PowerShell)

```powershell
# Start applications in Quarkus Dev Mode:
.\mvnw quarkus:dev -pl live       # http://localhost:8080/
.\mvnw quarkus:dev -pl paper      # http://localhost:8082/
.\mvnw quarkus:dev -pl backtest   # http://localhost:8083/api/status
.\mvnw quarkus:dev -pl data       # http://localhost:8084/api/collector/status

# Run full multi-module test suite (when Dev Mode is not running):
.\mvnw test
```
