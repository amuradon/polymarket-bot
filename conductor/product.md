# Product Definition: Polymarket Crypto Bot

## Vision
Real-time cryptocurrency price aggregation, TWAP (Time-Weighted Average Price) calculation, and automated algorithmic trading platform designed for Polymarket binary prediction markets across BTC, ETH, and SOL.

## Core Architectural Pillars
- **Zero Strategy Discrepancy**: Strategies execute identically across backtest, paper, and live environments using shared interfaces in `common`.
- **Zero Backtest Leakage**: Production applications (`live`, `paper`) remain ultra-lightweight and devoid of backtesting or heavy data extraction dependencies.
- **Three-Stage Lifecycle**:
  - `backtest`: Simulation engine evaluating alpha on historical tick and order book datasets.
  - `paper`: Real-time order simulation matching against live Polymarket order books.
  - `live`: Real-money execution gateway connecting to Polymarket CLOB.
- **Microstructure & Latency**: Optimized for ultra-low latency on GCP Dublin, utilizing Vert.x non-blocking I/O and GC-free hot paths.

## Key Capabilities
- Aggregation across multiple tier-1 exchanges (Binance, Coinbase, Kraken).
- Live 1-second TWAP calculation and rolling hourly price caching.
- TradingView Lightweight Charts visualization with real-time WebSocket updates.
- Historical data ingestion and backtesting for spot/futures trades and order books.
