---
name: data-and-backtest-ops
description: >-
  Operational guide for market data tooling in scripts/ (downloading .jsonl.zst
  archives from EC2 Data Collector, DuckDB conversion and Parquet schemas,
  exchange WebSocket latency benchmarking, historical Binance/Kraken/Coinbase
  trade downloaders) and running backtest simulations in batch or server mode on
  Ubuntu Linux. Use when downloading or converting market data, inspecting
  Parquet schemas, measuring exchange latency, or executing backtests.
---

# Data Pipeline Scripts & Backtest Operations (Ubuntu Bash)

Read **only** the reference file required for your current task:

1. **[references/collector-and-parquet.md](references/collector-and-parquet.md)**:
   - Downloading daily `.jsonl.zst` archives from the AWS EC2 Data Collector (`./scripts/download-collector-data.sh`) with size verification and remote EBS cleanup.
   - Converting `.jsonl.zst` archives to typed Apache Parquet via DuckDB (`./scripts/convert-to-parquet.sh`), including exact column schemas and sort keys for **Binance Spot/Futures**, **Kraken Spot**, **Coinbase Spot**, and **Polymarket CLOB** (`book`, `price_change`, `last_trade_price`, `bbo_hint`). *(PowerShell `.ps1` counterparts remain available in `scripts/`.)*
2. **[references/exchange-tools-and-backtest.md](references/exchange-tools-and-backtest.md)**:
   - Measuring one-way WebSocket latency (`t_recv - t_exchange_event`) across 7 exchanges on `eu-west-1` EC2 (`./scripts/measure-exchange-ws-latency.sh`).
   - Downloading historical spot trades to daily ZSTD Parquet via Python (`download_binance_spot_aggtrades.py`, `download_kraken_trades.py`, `download_coinbase_trades.py`).
   - Executing the `backtest` module in startup **Batch Mode** (`polymarket.backtest.batchMode=true`) or **REST & Web UI Studio Mode** (`:8083`).
3. **[../project-architecture/references/polymarket-mechanics-and-latencies.md](../project-architecture/references/polymarket-mechanics-and-latencies.md)**:
   - Polymarket L2 orderbook rules, `event_code = 4` (`bbo_hint`) pruning semantics, CEX feed delays (`50/50/100 ms`), and the `~220 ms` two-phase CLOB `POST /order` speed-bump latency model.
