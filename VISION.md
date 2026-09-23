# Product & Engineering Vision

This document outlines the strategic vision, architectural principles, and operational requirements for the **Polymarket Bot** trading platform.

All AI agents and engineers **MUST** study this document to ensure that all new code, refactoring, and architectural extensions align with this vision.

---

## 1. Platform Purpose & Scope

The platform is designed for automated, algorithmic trading on **Polymarket binary prediction markets** (Up/Down binary contracts, price resolution markets):
- **Core Markets**: BTC/USD Up/Down 15-minute and 5-minute markets.
- **Planned Markets**: 1-hour intervals, daily expiries, and additional cryptocurrencies (ETH, SOL, etc.).
- **Multi-Strategy**: Support for diverse alpha strategies including TWAP divergence, order book imbalance, latency arbitrage, and mean-reversion.

---

## 2. The 3-System Lifecycle (Backtest $\rightarrow$ Paper $\rightarrow$ Live)

To maximize profitability and eliminate production execution failures, every trading model moves through a strict 3-stage lifecycle:

```mermaid
flowchart LR
    BT["1. Backtest<br/>(Historical Simulation)"] --> PT["2. Paper Trading<br/>(Live Feed + Virtual Fills)"]
    PT --> LT["3. Live Trading<br/>(Real Funds & CLOB Execution)"]
```

1. **Backtesting (`backtest`)**:
   - High-throughput simulation across massive historical datasets (1s snapshots, L2 tick data, Parquet/DuckDB).
   - Strict realistic modeling of slippage, market impact, exchange latency, and taker fees.
   - Comprehensive performance evaluation: Sharpe ratio, Sortino ratio, Win Rate, Profit Factor, and Max Drawdown.

2. **Paper Trading (`paper`)**:
   - Validates execution mechanics against live real-time market feeds without financial risk.
   - Routes orders to a virtual order book that models queue position and fills against real Polymarket CLOB book changes.
   - Detects discrepancies between backtest assumptions and real live order book behavior.

3. **Live Trading (`live`)**:
   - Production execution on the Polymarket CLOB with real Polygon wallet funds.
   - Armed with automated risk guards (circuit breakers, kill-switches, maximum position sizes per candle).

---

## 3. Core Architectural Pillars

### Pillar A: Zero Discrepancy (Code Parity)
The trading strategy, signal generation, and indicator calculation engine **MUST be identical** across all three stages. 
- A strategy class written once in `common` or a strategy module must run unchanged in backtesting, paper trading, and live trading.
- Polymarket resolution TWAP algorithms must exactly replicate Polymarket settlement rules.

### Pillar B: Zero Leakage (Production Hygiene)
Production code in `live` and `paper` **MUST NOT** be bloated with:
- Offline data parsers, historical file loaders, or heavy visualization/plotting libraries.
- The live trading runtime must remain minimal, fast, and secure.

### Pillar C: Ultra-Low Latency & High Performance
The trading system competes on latency against global market makers:
1. **Physical & Network Proximity**:
   - Deployment target: **Google Cloud Platform (GCP) in Dublin (`europe-west1`)**.
   - Polymarket CLOB servers are hosted in AWS London (`eu-west-2`).
   - Round-trip time (RTT) between GCP Dublin and AWS London across the Irish Sea is typically **5–8 ms** over GCP's dedicated fiber backbone.
2. **Runtime & Language Efficiency (Java 25)**:
   - **GC-Free Critical Path**: Avoid heap allocations in the hot path (from WebSocket packet arrival to order submission). Avoid boxing (`Double`, `Long`, `BigDecimal`) in real-time execution loops.
   - **Modern Concurrency**: Asynchronous, non-blocking Vert.x event loops without thread contention or blocking locks.
   - **Network Tuning**: Enable `TCP_NODELAY`, native Netty epoll transports, and reusable HTTP/WebSocket connection pools.
   - **Optimized Signing**: Fast pre-allocated EIP-712 cryptographic signature generation for Polymarket order payloads.

---

## 4. Extensibility Guidelines for AI Agents

When implementing new features, agents must design for modular extensibility:
- **New Instruments**: Adding a new token (e.g. `ETH`, `SOL`) should require only configuration and registering feed adapters, without rewriting calculation or UI logic.
- **New Timeframes**: Support for 1h, 4h, or 1d candles must build upon the existing `Timeframe` domain enum and `TwapEngine` abstractions.
- **New Strategies**: Must implement the standard `TradingStrategy` interface from `common`.
