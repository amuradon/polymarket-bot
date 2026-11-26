---
name: project-architecture
description: >-
  Detailed architecture reference for Polymarket Bot's 4 Maven submodules
  (common, connector, bot, backtest), Polymarket CLOB/PolyBolt mechanics and
  end-to-end link latencies (including the ~180 ms taker speed bump), exchange
  WebSocket stream contracts, continuous Zstandard data collector, unified
  LIVE/PAPER/OFF strategy runtime with state persistence, and local Bash startup
  commands. Use when designing cross-module features, adding connectors/streams/strategies,
  modeling orderbook or execution latency, or starting applications locally.
---

# Polymarket Bot — Detailed Architecture Reference

## 1. Platform Scope & 2 Executable Applications

Automated low-latency algorithmic trading platform for **Polymarket binary prediction markets** (Up/Down crypto price resolution contracts: core `BTC/USD` 15m, extensible to `ETH`, `SOL`, and additional timeframes):

1. **`backtest` (`:8083`)**: Continuous chronological historical simulation powered by the shared `CoreTradingEngine`, `PolymarketOrderBook`, and `SimulatedClobExecutionEngine` in `common`, streaming raw CEX trades, full-accuracy PolyBolt oracle feeds, and typed Polymarket CLOB L2 + BBO hints directly from Parquet via DuckDB (`2.0.1`).
2. **`bot` (`:8080` locally / `:443` in AWS prod)**: Unified production Quarkus application combining:
   - **Continuous Data Collector (`DataCollectorService`)**: Always records Polymarket PolyBolt (`price.crypto`, `price.crypto.twap`) and deduplicated per-market CLOB streams (`current + lookahead-count`, default `lookahead-count = 1`) into `.jsonl.zst` archives on `CPU #0` (with optional Binance Spot/Futures `bookTicker` preserved and disabled by default).
   - **Unified Strategy Engine (`StrategyTradingEngine` + `CoreTradingEngine`)**: Switchable at startup (`polymarket.strategy.mode`) or at runtime (`POST /api/mode`) between:
     - **`LIVE`**: Real-money execution on Polymarket CLOB (EIP-712 order signing, `POST /order`, `/ws/user` balance stream, `GET /balance-allowance`).
     - **`PAPER`**: Identical live market feeds routed to shared `SimulatedClobExecutionEngine` (`polymarket.paper.order-latency-ms=220`, two-phase Polymarket speed bump + L2 liquidity depletion, `PaperTradeRepository` persistence to `trades.jsonl` + `metrics.json`).
     - **`OFF`**: Stops all strategy worker threads and disconnects strategy streams; zero strategy CLOB or PolyBolt queues accumulate frames while Data Collector continues recording standalone.
   - **State Persistence (`RuntimeStateStore`)**: Saves `runtime-state.json` on shutdown (`SIGTERM`) and periodically off-hot-path on `CPU #0` so newly deployed or rolled-back instances seamlessly restore `twapOpenPrice` (active 15m target price), `StrategyMode`, and open `Position`s.

---

## 2. Submodule Internals & Stream Contracts

### `common` (Shared Domain, L2 Order Book, Core Engine & Strategy Library)
- **Domain & Order Book**: Allocation-free `OrderBookQuote`, `TradeRecord`, `Position`, and canonical 1-bps bitset `PolymarketOrderBook` (with symmetric `UP` ↔ `DOWN` complementarity, per-side `server_ts` guards, non-fabricating `applyBboHint`, and on-demand L2 VWAP sweep queries).
- **Unified Engine & Simulator**: `CoreTradingEngine` (single-writer event engine shared across `LIVE`, `PAPER`, and `BACKTEST`), `OrderExecutionGateway`, and `SimulatedClobExecutionEngine` (`220 ms` two-phase CLOB latency calibrated from `eu-west-1`).
- **Indicators & Strategies**: `SpotTwapIndicator`, `FastSpotMicroIndicator`, `PerformanceMetricsCalculator`, `TradingStrategy`, `StrategyContext`, `ExecutionRouter`, `StrategyRegistry`, and strategy implementations (`OraclePulseLockStrategy`, `PricingModelStrategy`, `RoundMainstreamStrategy`, `UtterParameciumStrategy` v0–v4).

### `connector` (Exchange & Polymarket Connectivity Library — Zero `common` Dependency)
- **Consolidated 1-EventLoop Reader (`MarketWebSocketRuntime`)**: Single `epoll` + BoringSSL `market-ws-io-reader` (`CPU #1`) servicing all WebSocket sockets into lock-free MPSC queues.
- **Polymarket Connectors**:
  - `PolymarketGammaClient`: Market discovery via `GET /markets/slug/{slug}`.
  - `PolymarketPolyBoltClient`: Shared single-connection `price.crypto` and `price.crypto.twap` streams feeding Data Collector on `CPU #0` and lock-free `strategyQueue` on `CPU #2` when attached (`detachStrategyConsumer()` clears and bypasses `strategyQueue` in `OFF` mode).
  - `PolymarketClobClient` & `PolymarketStreamParser`: Zero-allocation L2/BBO parser writing through `ClobBookSink` (`POLYMARKET-CLOB-<slug>` + `-BACKUP` for the active strategy market, single-leg `POLYMARKET-CLOB-<slug>` for the `1` lookahead/resolving market -> max `3` CLOB market streams total) plus `POLYMARKET-CLOB-USER`.
- **Spot Exchange Connectors**: Dual multi-path `BinanceStreamConnector` (`startSpotAggTradesBatch` for strategy; `startBookTickerStreams` preserved for optional collector recording), lock-free `KrakenStreamConnector` (`USDTUSD` + `<SYMBOL>`), and `CoinbaseStreamConnector` (`matches`).

### `bot` (Unified Production Quarkus App — `:8080`)
- **4-Core Pipeline (`c8gn.xlarge`)**:
  - `CPU #0`: OS/ENA IRQs, ZGC, Quarkus HTTP/scheduler, Data Collector Zstd writers, `telemetry-maintenance-worker`, and `clob-user-worker` (`LIVE`).
  - `CPU #1`: `market-ws-io-reader`.
  - `CPU #2`: `market-strategy-worker` (`CoreTradingEngine` + `PolymarketOrderBook` + `PolyBolt` + `SpotTwapProcessor` + `TradingStrategy`).
  - `CPU #3`: `order-execution-io-worker` (EIP-712 signing + HTTP `POST /order` / `DELETE /order`).
- **Unified REST & Dashboard (`DashboardResource`)**: `GET /`, `GET /api/status`, `GET /api/health` (`200` / `503`), `POST /api/mode`, `GET /api/telemetry`, `GET /api/positions`, `GET /api/paper/metrics`, `GET /api/paper/trades`, `POST /api/reset`, `GET /api/collector/status`.

### `backtest` (`:8083`)
- Supports startup batch execution (`polymarket.backtest.batchMode=true`) or interactive REST & Web UI Studio (`:8083`) backed by persistent DuckDB run history (`~/.local/share/polymarket-bot/backtest-history.duckdb`). Load `data-and-backtest-ops` for CLI flags.

---

## 3. Local Development Commands (Ubuntu Bash)

```bash
# Start unified Bot in Quarkus Dev Mode (LIVE | PAPER | OFF):
./mvnw quarkus:dev -pl bot                                     # http://localhost:8080/
./mvnw quarkus:dev -pl bot -Dpolymarket.strategy.mode=PAPER    # Paper Trading + Collector
./mvnw quarkus:dev -pl bot -Dpolymarket.strategy.mode=OFF      # Data Collector only

# Start Backtest Studio in Quarkus Dev Mode:
./mvnw quarkus:dev -pl backtest                                # http://localhost:8083/

# Run full 4-module test suite (when Dev Mode is not running):
./mvnw test
```

---

## 4. Progressive Deep-Dive Reference

Load **only** when working on orderbook reconstruction, WebSocket stream parsing, strategy execution, or latency/backtest modeling:

- **[references/polymarket-mechanics-and-latencies.md](references/polymarket-mechanics-and-latencies.md)**: Binary CTF complementarity (`P_up + P_down = 1.0`), order constraints & parabolic fees, `FAK` matching, per-side `server_ts` & non-fabricating BBO hints, `/ws/user` reconciliation, and the empirically measured latency map across all CEX, PolyBolt, and Polymarket CLOB links from `eu-west-1` (including the `~180 ms` server-side Taker Speed Bump inside `~220 ms` `POST /order` RTT).
