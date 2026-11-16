# Collector Download & DuckDB Parquet Conversion (PowerShell)

## 1. Download Data from EC2 Collector (`.\scripts\download-collector-data.ps1`)
Streams daily `.jsonl.zst` archives (Binance, Kraken including `USDTUSD`, Coinbase, PolyBolt) and per-market Polymarket CLOB files from `/mnt/ebs/polymarket-data/collector` on EC2 to local disk (default `D:\Crypto\data\Polymarket\collector`), verifies byte sizes, and deletes verified remote files unless `-KeepRemoteFiles` is passed.

```powershell
# Preview matched files on EC2 without downloading or deleting:
.\scripts\download-collector-data.ps1 -Date 2026-10-30 -DryRun

# Download all streams for a UTC date and remove verified files from EC2:
.\scripts\download-collector-data.ps1 -Date 2026-10-30

# Filter by stream/exchange substring and retain files on EC2:
.\scripts\download-collector-data.ps1 -Date 2026-10-30 -Stream "kraken" -KeepRemoteFiles

# Download only Polymarket directory to custom path:
.\scripts\download-collector-data.ps1 -Date 2026-10-30 `
  -RemoteBaseDir /mnt/ebs/polymarket-data/collector/polymarket `
  -DestinationDir D:\Crypto\data\Polymarket\collector\polymarket

# Reconcile and delete remote EC2 files already verified in local DestinationDir:
.\scripts\download-collector-data.ps1 -CleanAlreadyDownloaded -DestinationDir D:\Crypto\data\Polymarket\collector
```

---

## 2. Convert `.jsonl.zst` to Apache Parquet (`.\scripts\convert-to-parquet.ps1`)
Uses DuckDB (`ROW_GROUP_SIZE = 100000`, default `ZSTD` compression) to transform `.jsonl.zst` archives into sorted, typed `.parquet` files, verifies row counts, and deletes original `.jsonl.zst` files unless `-KeepOriginal` is specified.

### Output Parquet Schemas & Ordering
- **Binance Spot `bookTicker`** (`binance/spot/<SYMBOL>/bookTicker/<SYMBOL>-bookTicker-<YYYY-MM-DD>.parquet`):
  - 7 primitive columns: `ts`, `update_id`, `symbol`, `best_bid_price`, `best_bid_qty`, `best_ask_price`, `best_ask_qty`
  - Ordered by: `ts ASC, update_id ASC`
- **Binance Futures `bookTicker`** (`binance/futures/<SYMBOL>/bookTicker/<SYMBOL>-bookTicker-<YYYY-MM-DD>.parquet`):
  - 9 primitive columns: `ts`, `event_time`, `transaction_time`, `update_id`, `symbol`, `best_bid_price`, `best_bid_qty`, `best_ask_price`, `best_ask_qty`
  - Ordered by: `ts ASC, update_id ASC`
- **Kraken Spot Trades** (`kraken/spot/<SYMBOL>/trades/<SYMBOL>-trades-<YYYY-MM-DD>.parquet`):
  - Unnests `payload.data[]` from `channel="trade", type="update"` into 9 columns: `ts`, `trade_timestamp`, `symbol`, `trade_id`, `side`, `ord_type`, `price`, `qty`, `timestamp`
  - Ordered by: `ts ASC, trade_id ASC`
- **Coinbase Spot Trades** (`coinbase/spot/<SYMBOL>/trades/<SYMBOL>-trades-<YYYY-MM-DD>.parquet`):
  - Maps `type="match"` events into 8 columns: `ts`, `trade_timestamp`, `symbol`, `trade_id`, `side`, `price`, `qty`, `timestamp`
  - Ordered by: `ts ASC, trade_id ASC`
- **Polymarket CLOB** (`*-updown-*.parquet`):
  - Compact 10-column primitive schema: `ts`, `server_ts`, `frame_id`, `event_code`, `is_up`, `is_bid`, `tick`, `size`, `up_token_id`, `down_token_id` (excludes raw `payload` JSON and `market_lifecycle` / `market_resolved` events)
  - Ordered by: `ts ASC, frame_id ASC`

### PowerShell Usage
```powershell
# Preview files to convert without modifying disk:
.\scripts\convert-to-parquet.ps1 -DryRun

# Convert all archives in target directory:
.\scripts\convert-to-parquet.ps1 -TargetDir D:\Crypto\data\Polymarket\collector

# Filter by date or stream type and keep source .jsonl.zst archives:
.\scripts\convert-to-parquet.ps1 -Date 2026-10-30 -Stream "aggTrades" -KeepOriginal
```
