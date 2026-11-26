# Collector Download & DuckDB Parquet Conversion (Ubuntu Bash)

## 1. Download Data from EC2 Collector (`./scripts/download-collector-data.sh`)
Streams daily `.jsonl.zst` archives (Polymarket PolyBolt + optional Binance `bookTicker`) and per-market Polymarket CLOB files from `/mnt/ebs/polymarket-bot/collector` on the unified Polymarket Bot EC2 instance (`polymarket-bot-ec2` in `eu-west-1`) to local disk (default `/data/polymarket/collector`, symlinked to `/mnt/t7/Crypto/data/Polymarket/collector`), verifies byte sizes, and deletes verified remote files unless `-k` / `--keep-remote-files` is passed.

```bash
# Preview matched files on EC2 without downloading or deleting:
./scripts/download-collector-data.sh -d 2026-11-25 --dry-run

# Download all streams for a UTC date and remove verified files from EC2:
./scripts/download-collector-data.sh -d 2026-11-25

# Filter by stream/exchange substring and retain files on EC2:
./scripts/download-collector-data.sh -d 2026-11-25 -s price_crypto -k

# Download only Polymarket directory to custom path:
./scripts/download-collector-data.sh -d 2026-11-25 \
  -r /mnt/ebs/polymarket-bot/collector/polymarket \
  -o /data/polymarket/collector/polymarket

# Reconcile and delete remote EC2 files already verified in local destination-dir:
./scripts/download-collector-data.sh --clean-already-downloaded -o /data/polymarket/collector
```

---

## 2. Convert `.jsonl.zst` to Apache Parquet (`./scripts/convert-to-parquet.sh`)
Uses DuckDB CLI (`v2.0.1`, default `MEMORY_LIMIT=16GB`, `THREADS=min(nproc, 8)`, local NVMe spill dir `/var/tmp/polymarket-parquet-convert-<pid>`, `ROW_GROUP_SIZE=100000` for CLOB/bookTicker, `ZSTD` compression) to transform `.jsonl.zst` archives into sorted, typed `.parquet` files, verifies row counts, and deletes original `.jsonl.zst` files unless `-k` / `--keep-original` is specified.

### Output Parquet Schemas & Ordering
- **Binance Spot `bookTicker`** (`binance/spot/<SYMBOL>/bookTicker/<SYMBOL>-bookTicker-<YYYY-MM-DD>.parquet`):
  - Columns: `update_id`, `symbol`, `best_bid_price`, `best_bid_qty`, `best_ask_price`, `best_ask_qty`
  - Ordered by: `update_id ASC`
- **Binance Futures `bookTicker`** (`binance/futures/<SYMBOL>/bookTicker/<SYMBOL>-bookTicker-<YYYY-MM-DD>.parquet`):
  - Columns: `event_time`, `transaction_time`, `update_id`, `symbol`, `best_bid_price`, `best_bid_qty`, `best_ask_price`, `best_ask_qty`
  - Ordered by: `event_time ASC, update_id ASC`
- **Polymarket PolyBolt `price_crypto` & `price_crypto_twap_60s`**:
  - Preserves both `DOUBLE` (`price` / `twap_price`) and full-precision `VARCHAR` (`full_accuracy_price` / `full_accuracy_twap_price`), ordered by `ts ASC, price_timestamp/twap_timestamp ASC`.
- **Polymarket CLOB** (`polymarket/<asset>-updown-15m/<asset>-updown-15m-<epoch>.parquet`):
  - Unwraps both standalone JSON objects and JSON array batches `[{...}]` into a compact 10-column primitive schema:
    `ts`, `server_ts`, `frame_id`, `event_code`, `is_up`, `is_bid`, `tick`, `size`, `up_token_id`, `down_token_id`
  - `event_code` values: `0 = market_discovered`, `1 = book`, `2 = price_change`, `3 = last_trade_price`, `4 = bbo_hint` (deduplicated non-fabricating `best_bid` / `best_ask` ticks from `price_change` and `best_bid_ask`).
  - Ordered by: `ts ASC, frame_id ASC`

### Bash Usage
```bash
# Preview files to convert without modifying disk:
./scripts/convert-to-parquet.sh --dry-run

# Convert all archives in default /data/polymarket/collector:
./scripts/convert-to-parquet.sh

# Filter by date or stream type and keep source .jsonl.zst archives:
./scripts/convert-to-parquet.sh -d 2026-11-25 -s btc-updown -k

# Force re-conversion overwriting existing .parquet files:
./scripts/convert-to-parquet.sh -d 2026-11-25 --force -k
```
