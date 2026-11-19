---
name: project-architecture
description: >-
  Detailed architecture reference for Polymarket Bot's 4 Maven submodules
  (common, connector, bot, backtest), exchange and Polymarket WebSocket stream
  contracts, continuous Zstandard data collector, unified LIVE/PAPER/OFF
  strategy runtime with state persistence, frontend stack, and local PowerShell
  startup commands. Use when designing cross-module features, adding
  connectors/streams/strategies, working on collector or trading runtime
  internals, or starting applications locally.
---

# Polymarket Bot — Detailed Architecture Reference

## 1. Platform Scope & 2 Executable Applications

Automated low-latency algorithmic trading platform for **Polymarket binary prediction markets** (Up/Down crypto price resolution contracts: core `BTC/USD` 15m, extensible to `ETH`, `SOL`, and additional timeframes):

1. **`backtest` (`:8083`)**: High-throughput historical simulation over 1s snapshots and L2 tick data (Parquet/DuckDB) modeling slippage, market impact, exchange latency, and taker fees; evaluates Sharpe, Sortino, Win Rate, Profit Factor, and Max Drawdown.
2. **`bot` (`:8080` locally / `:443` in AWS prod)**: Unified production Quarkus application combining:
   - **Continuous Data Collector (`DataCollectorService`)**: Always records Polymarket PolyBolt (`price.crypto`, `price.crypto.twap`) and deduplicated per-market CLOB streams (`current + lookahead-count`, default `lookahead-count = 1`) into `.jsonl.zst` archives on `CPU #0` (with optional Binance Spot/Futures `bookTicker` preserved and disabled by default).
   - **Unified Strategy Engine (`StrategyTradingEngine`)**: Switchable at startup (`polymarket.strategy.mode`) or at runtime (`POST /api/mode`) between:
     - **`LIVE`**: Real-money execution on Polymarket CLOB (EIP-712 order signing, `POST /order`, `/ws/user` balance stream, `GET /balance-allowance`).
     - **`PAPER`**: Identical live market feeds routed to virtual order execution (`orderLatencyMs`, bestAsk price improvement / slippage checks, `PaperTradeRepository` persistence to `trades.jsonl` + `metrics.json`).
     - **`OFF`**: Stops all strategy worker threads and disconnects strategy streams; zero strategy CLOB or PolyBolt queues accumulate frames while Data Collector continues recording standalone.
   - **State Persistence (`RuntimeStateStore`)**: Saves `runtime-state.json` on shutdown (`SIGTERM`) and periodically off-hot-path on `CPU #0` so newly deployed or rolled-back instances seamlessly restore `twapOpenPrice` (active 15m target price), `StrategyMode`, and open `Position`s.

---

## 2. Submodule Internals & Stream Contracts

### `common` (Pure Domain & Strategy Library)
- **Domain Models**: Compact, allocation-friendly immutable structures for `Timeframe`, `OrderBookQuote`, and `TradeRecord`.
- **Quantitative Calculators**: Rolling calculated Spot & 60s TWAP (`CalculatedSpotTwapEngine` replicating Polymarket settlement rules) and `PerformanceMetricsCalculator`.
- **Strategy & Execution Contracts**: `TradingStrategy`, `StrategyContext`, `ExecutionRouter`, `StrategyRegistry`, and shared strategy implementations (`UtterParameciumStrategy`).

### `connector` (Exchange & Polymarket Connectivity Library — Zero `common` Dependency)
- **Consolidated 1-EventLoop Reader (`MarketWebSocketRuntime`)**: Single `epoll` + BoringSSL `market-ws-io-reader` (`CPU #1`) servicing all WebSocket sockets into lock-free MPSC queues.
- **Polymarket Connectors**:
  - `PolymarketGammaClient`: Market discovery via `GET /markets/slug/{slug}`.
  - `PolymarketPolyBoltClient`: Shared single-connection `price.crypto` and `price.crypto.twap` streams feeding Data Collector on `CPU #0` and lock-free `strategyQueue` on `CPU #2` when attached (`detachStrategyConsumer()` clears and bypasses `strategyQueue` in `OFF` mode).
  - `PolymarketClobClient`: Unifies per-market CLOB subscriptions (`POLYMARKET-CLOB-<slug>` + `-BACKUP` for the active strategy market, single-leg `POLYMARKET-CLOB-<slug>` for the `1` lookahead/resolving market -> max `3` CLOB market streams total) plus `POLYMARKET-CLOB-USER`, using independent lock-free queues and deduplicators on `CPU #0` (Collector) and `CPU #2` (Strategy) so zero duplicate records ever enter `.jsonl.zst`.
- **Spot Exchange Connectors**: Dual multi-path `BinanceStreamConnector` (`startSpotAggTradesBatch` for strategy; `startBookTickerStreams` preserved for optional collector recording), lock-free `KrakenStreamConnector` (`USDTUSD` + `<SYMBOL>`), and `CoinbaseStreamConnector` (`matches`).

### `bot` (Unified Production Quarkus App — `:8080`)
- **4-Core Pipeline (`c8gn.xlarge`)**:
  - `CPU #0`: OS/ENA IRQs, ZGC, Quarkus HTTP/scheduler, Data Collector Zstd writers, `telemetry-maintenance-worker`, and `clob-user-worker` (`LIVE`).
  - `CPU #1`: `market-ws-io-reader`.
  - `CPU #2`: `market-strategy-worker` (`PolymarketLocalBook` + `PolyBolt` + `CalculatedSpotStreamProcessor` + `TradingStrategy`).
  - `CPU #3`: `order-execution-io-worker` (EIP-712 signing + HTTP `POST /order` / `DELETE /order`).
- **Unified REST & Dashboard (`DashboardResource`)**: `GET /`, `GET /api/status`, `GET /api/health` (`200` / `503`), `POST /api/mode`, `GET /api/telemetry`, `GET /api/positions`, `GET /api/paper/metrics`, `GET /api/paper/trades`, `POST /api/reset`, `GET /api/collector/status`.

### `backtest` (`:8083`)
- Supports startup batch execution (`polymarket.backtest.batchMode=true`) or interactive REST server (`GET /api/status`, `POST /api/backtest/run`, `GET /api/backtest/run/{jobId}`). Load `data-and-backtest-ops` for CLI flags.

---

## 3. Local Development Commands (PowerShell)

```powershell
# Start unified Bot in Quarkus Dev Mode (LIVE | PAPER | OFF):
.\mvnw quarkus:dev -pl bot                                     # http://localhost:8080/
.\mvnw quarkus:dev -pl bot -Dpolymarket.strategy.mode=PAPER    # Paper Trading + Collector
.\mvnw quarkus:dev -pl bot -Dpolymarket.strategy.mode=OFF      # Data Collector only

# Start Backtest in Quarkus Dev Mode:
.\mvnw quarkus:dev -pl backtest                                # http://localhost:8083/api/status

# Run full 4-module test suite (when Dev Mode is not running):
.\mvnw test
```
