# Product Guidelines: Polymarket Crypto Bot

## Engineering & Operational Philosophy
- **Financial Precision & Integrity**: Exact arithmetic for financial calculations, strict timestamp alignment, and zero heuristic fallbacks.
- **Fail-Fast Mechanics**: Any unexpected data corruption or invariant violation must immediately throw clear, descriptive exceptions rather than silently degrading.
- **Deterministic & Idempotent Operations**: Offline and historical data ingestion must be strictly idempotent—checking existing local datasets (CSV, Parquet) before downloading, logging `INFO` when skipping already present files.

## API & Interface Principles
- **RESTful Endpoints**: Versioned endpoints under `/api/1/...` providing deterministic JSON responses, input validation, and clear error diagnostics.
- **WebSocket Streaming**: Ultra-low-latency binary/JSON streaming for price updates and state synchronizations.
- **Visualization**: Light-themed, responsive TradingView Lightweight Charts with crisp readability and zero heavy frontend framework overhead.

## Observability & Logging
- Uniform log formatting across modules: timestamp, level, logger name, and structured message.
- Informative and clean progress logging for long-running batch operations (e.g. data ingestion).
