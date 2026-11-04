#!/usr/bin/env python3
"""
Downloads historical spot trades from Kraken REST API (/0/public/Trades) and writes
daily ZSTD-compressed Apache Parquet files matching the Kraken Spot WebSocket v2
trade schema and the Polymarket Bot collector conventions.

Target path layout:
  <output_dir>/<PAIR>-trades-<YYYY-MM-DD>.parquet
Default output_dir:
  D:\\Crypto\\data\\Polymarket\\collector\\kraken\\spot\\USDTUSD\\trades

Parquet schema (aligned with Kraken WebSocket v2 `trade` channel + collector `ts`):
  - ts: uint64              (trade_timestamp + latency_ms, default +15ms)
  - trade_timestamp: uint64 (exchange trade time in epoch milliseconds)
  - symbol: string          (Kraken WS v2 symbol, e.g. "USDT/USD")
  - trade_id: int64         (unique sequential Kraken trade ID)
  - side: string            ("buy" or "sell")
  - ord_type: string        ("limit" or "market")
  - price: double           (trade execution price)
  - qty: double             (executed quantity in base asset)
  - timestamp: string       (RFC3339 UTC timestamp with microsecond precision)
"""

import argparse
from collections import defaultdict
import datetime
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable
import urllib.error
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq


KRAKEN_TRADES_PARQUET_SCHEMA = pa.schema(
    [
        ("ts", pa.uint64()),
        ("trade_timestamp", pa.uint64()),
        ("symbol", pa.string()),
        ("trade_id", pa.int64()),
        ("side", pa.string()),
        ("ord_type", pa.string()),
        ("price", pa.float64()),
        ("qty", pa.float64()),
        ("timestamp", pa.string()),
    ]
)

SIDE_MAP = {
    "b": "buy",
    "buy": "buy",
    "s": "sell",
    "sell": "sell",
}

ORD_TYPE_MAP = {
    "l": "limit",
    "limit": "limit",
    "m": "market",
    "market": "market",
}


def normalize_pair_folder(pair: str) -> str:
    """Normalizes a trading pair to uppercase alphanumeric folder/file prefix (e.g. 'USDT/USD' -> 'USDTUSD')."""
    clean = pair.strip().upper().replace("/", "").replace("-", "").replace("_", "")
    if not clean:
        raise ValueError("Trading pair cannot be empty.")
    return clean


def normalize_ws_symbol(pair: str) -> str:
    """Normalizes a trading pair to Kraken WebSocket v2 symbol format (e.g. 'USDTUSD' -> 'USDT/USD')."""
    raw = pair.strip().upper().replace("-", "/").replace("_", "/")
    if "/" in raw:
        return raw
    for quote in ("USDT", "USDC", "USD", "EUR", "GBP"):
        if raw.endswith(quote) and len(raw) > len(quote):
            base = raw[: -len(quote)]
            return f"{base}/{quote}"
    return raw


def format_rfc3339_micros(epoch_seconds: float) -> str:
    """Formats a floating-point Unix epoch timestamp into RFC3339 UTC string with microsecond precision."""
    dt = datetime.datetime.fromtimestamp(epoch_seconds, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def transform_rest_trade(raw_trade: list[Any], ws_symbol: str, latency_ms: int = 15) -> dict[str, Any]:
    """
    Transforms a single Kraken REST /0/public/Trades entry:
      [price, volume, time, side, orderType, misc, tradeId]
    into a dictionary matching the Kraken Spot WebSocket v2 trade schema + collector `ts`.
    """
    if len(raw_trade) < 7:
        raise ValueError(f"Expected at least 7 elements in Kraken trade record, got: {raw_trade}")

    price_str, qty_str, time_val, side_code, ord_type_code, _misc, trade_id_val = raw_trade[:7]
    epoch_sec = float(time_val)
    # Use integer microseconds to avoid floating-point drift when computing milliseconds
    epoch_us = int(round(epoch_sec * 1_000_000))
    trade_ts_ms = epoch_us // 1_000
    ts_ms = trade_ts_ms + int(latency_ms)

    side = SIDE_MAP.get(str(side_code).lower())
    if side is None:
        raise ValueError(f"Unsupported Kraken trade side code: '{side_code}'")

    ord_type = ORD_TYPE_MAP.get(str(ord_type_code).lower())
    if ord_type is None:
        raise ValueError(f"Unsupported Kraken order type code: '{ord_type_code}'")

    return {
        "ts": ts_ms,
        "trade_timestamp": trade_ts_ms,
        "symbol": ws_symbol,
        "trade_id": int(trade_id_val),
        "side": side,
        "ord_type": ord_type,
        "price": float(price_str),
        "qty": float(qty_str),
        "timestamp": format_rfc3339_micros(epoch_us / 1_000_000.0),
    }


def fetch_kraken_trades_page(
    pair: str,
    since_ns: int,
    count: int = 1000,
    max_retries: int = 10,
) -> dict[str, Any]:
    """Fetches a single page of trades from Kraken public REST API with retry/backoff."""
    url = f"https://api.kraken.com/0/public/Trades?pair={pair}&since={since_ns}&count={count}"
    headers = {"User-Agent": "PolymarketBot-HistoricalDownloader/1.0"}

    backoff = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            errors = payload.get("error", [])
            if errors:
                err_text = ", ".join(str(e) for e in errors)
                err_lower = err_text.lower()
                if "rate limit" in err_lower or "too many" in err_lower or "toomany" in err_lower or "temporary lockout" in err_lower:
                    if attempt < max_retries:
                        time.sleep(backoff)
                        backoff = min(backoff * 1.8, 30.0)
                        continue
                raise RuntimeError(f"Kraken API returned error: {err_text}")
            return payload
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            if attempt >= max_retries:
                raise RuntimeError(f"Failed to fetch Kraken trades after {max_retries} attempts: {exc}") from exc
            time.sleep(backoff)
            backoff = min(backoff * 1.8, 30.0)

    raise RuntimeError(f"Exhausted retries fetching {url}")


def write_daily_parquet_files(
    records_by_date: dict[str, list[dict[str, Any]]],
    output_dir: Path,
    pair_folder: str,
    overwrite: bool = False,
) -> list[Path]:
    """Writes sorted daily Parquet files compressed with ZSTD and returns written file paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    written_files: list[Path] = []

    for date_str in sorted(records_by_date.keys()):
        day_records = records_by_date[date_str]
        if not day_records:
            continue

        dest_path = output_dir / f"{pair_folder}-trades-{date_str}.parquet"
        if dest_path.exists() and not overwrite and dest_path.stat().st_size > 0:
            written_files.append(dest_path)
            continue

        day_records.sort(key=lambda r: (r["ts"], r["trade_id"]))

        columns = {
            "ts": pa.array([r["ts"] for r in day_records], type=pa.uint64()),
            "trade_timestamp": pa.array([r["trade_timestamp"] for r in day_records], type=pa.uint64()),
            "symbol": pa.array([r["symbol"] for r in day_records], type=pa.string()),
            "trade_id": pa.array([r["trade_id"] for r in day_records], type=pa.int64()),
            "side": pa.array([r["side"] for r in day_records], type=pa.string()),
            "ord_type": pa.array([r["ord_type"] for r in day_records], type=pa.string()),
            "price": pa.array([r["price"] for r in day_records], type=pa.float64()),
            "qty": pa.array([r["qty"] for r in day_records], type=pa.float64()),
            "timestamp": pa.array([r["timestamp"] for r in day_records], type=pa.string()),
        }
        table = pa.Table.from_pydict(columns, schema=KRAKEN_TRADES_PARQUET_SCHEMA)

        tmp_path = dest_path.with_suffix(".tmp.parquet")
        if tmp_path.exists():
            tmp_path.unlink()

        pq.write_table(table, tmp_path, compression="ZSTD")
        tmp_path.replace(dest_path)
        written_files.append(dest_path)

    return written_files


def download_and_save_kraken_trades(
    pair: str,
    start_date: str,
    end_date: str,
    output_dir: Path | str,
    latency_ms: int = 15,
    overwrite: bool = False,
    fetch_fn: Callable[[str, int], dict[str, Any]] | None = None,
    sleep_sec: float = 1.05,
) -> list[Path]:
    """
    Downloads Kraken spot trades for [start_date, end_date] UTC and saves them
    as daily Parquet files partitioned by the UTC date of `ts = trade_timestamp + latency_ms`.
    """
    pair_folder = normalize_pair_folder(pair)
    ws_symbol = normalize_ws_symbol(pair)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    start_dt = datetime.date.fromisoformat(start_date.strip())
    end_dt = datetime.date.fromisoformat(end_date.strip())
    if end_dt < start_dt:
        raise ValueError(f"end_date ({end_date}) cannot be earlier than start_date ({start_date})")

    # Check which days actually need downloading if overwrite is False
    all_days: list[datetime.date] = []
    curr = start_dt
    while curr <= end_dt:
        all_days.append(curr)
        curr += datetime.timedelta(days=1)

    if not overwrite:
        missing_days = [
            d
            for d in all_days
            if not (out_path / f"{pair_folder}-trades-{d.isoformat()}.parquet").exists()
            or (out_path / f"{pair_folder}-trades-{d.isoformat()}.parquet").stat().st_size == 0
        ]
        if not missing_days:
            print(f"All {len(all_days)} daily Parquet files already exist in {out_path}.")
            return [out_path / f"{pair_folder}-trades-{d.isoformat()}.parquet" for d in all_days]
        effective_start_dt = min(missing_days)
        effective_end_dt = max(missing_days)
    else:
        effective_start_dt = start_dt
        effective_end_dt = end_dt

    window_start_ms = int(
        datetime.datetime.combine(effective_start_dt, datetime.time.min, tzinfo=datetime.timezone.utc).timestamp()
        * 1000
    )
    window_end_exclusive_ms = int(
        datetime.datetime.combine(
            effective_end_dt + datetime.timedelta(days=1),
            datetime.time.min,
            tzinfo=datetime.timezone.utc,
        ).timestamp()
        * 1000
    )

    # Start querying slightly before window_start_ms to capture trades whose +latency_ms crosses midnight
    query_start_ns = max(0, (window_start_ms - max(latency_ms, 1000)) * 1_000_000)

    if fetch_fn is None:
        fetch_fn = lambda p, s: fetch_kraken_trades_page(p, s, count=1000)

    since_ns = query_start_ns
    seen_trade_ids: set[int] = set()
    records_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    written_files: list[Path] = []
    completed_dates: set[str] = set()

    page_count = 0
    total_collected = 0
    t0 = time.time()

    while True:
        payload = fetch_fn(pair_folder, since_ns)
        result = payload.get("result", {})
        data_keys = [k for k in result.keys() if k != "last"]
        if not data_keys:
            break

        raw_trades = result[data_keys[0]]
        last_cursor_ns = int(result.get("last", since_ns))
        page_count += 1

        if not raw_trades:
            break

        max_ts_in_page = 0
        for raw_trade in raw_trades:
            rec = transform_rest_trade(raw_trade, ws_symbol=ws_symbol, latency_ms=latency_ms)
            tid = rec["trade_id"]
            if tid in seen_trade_ids:
                continue
            seen_trade_ids.add(tid)

            ts_ms = rec["ts"]
            if ts_ms > max_ts_in_page:
                max_ts_in_page = ts_ms

            if window_start_ms <= ts_ms < window_end_exclusive_ms:
                utc_date_str = datetime.datetime.fromtimestamp(
                    ts_ms / 1000.0, tz=datetime.timezone.utc
                ).strftime("%Y-%m-%d")
                records_by_date[utc_date_str].append(rec)
                total_collected += 1

        # Flush any UTC day that has completely passed (buffer of 5 seconds to be safe)
        if max_ts_in_page > 0:
            cutoff_date_str = datetime.datetime.fromtimestamp(
                (max_ts_in_page - 5000) / 1000.0, tz=datetime.timezone.utc
            ).strftime("%Y-%m-%d")
            ready_dates = [
                d for d in list(records_by_date.keys()) if d < cutoff_date_str and d not in completed_dates
            ]
            for d in sorted(ready_dates):
                day_list = records_by_date.pop(d)
                day_written = write_daily_parquet_files(
                    {d: day_list},
                    output_dir=out_path,
                    pair_folder=pair_folder,
                    overwrite=overwrite,
                )
                written_files.extend(day_written)
                completed_dates.add(d)
                elapsed = time.time() - t0
                print(
                    f"  [Saved] {pair_folder}-trades-{d}.parquet "
                    f"({len(day_list):,} trades, page {page_count}, {elapsed:.1f}s)"
                )

        if max_ts_in_page >= window_end_exclusive_ms or last_cursor_ns <= since_ns:
            break

        since_ns = last_cursor_ns
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    # Flush remaining days
    remaining_days = {d: recs for d, recs in records_by_date.items() if d not in completed_dates and recs}
    if remaining_days:
        rem_written = write_daily_parquet_files(
            remaining_days,
            output_dir=out_path,
            pair_folder=pair_folder,
            overwrite=overwrite,
        )
        for d in sorted(remaining_days.keys()):
            print(
                f"  [Saved] {pair_folder}-trades-{d}.parquet "
                f"({len(remaining_days[d]):,} trades, total pages {page_count})"
            )
        written_files.extend(rem_written)

    written_files.sort()
    return written_files


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download historical Kraken Spot trades and store daily Parquet files."
    )
    parser.add_argument(
        "--pair",
        type=str,
        default="USDTUSD",
        help="Kraken spot trading pair (default: USDTUSD)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-10-12",
        help="Start UTC date in YYYY-MM-DD format (inclusive, default: 2026-10-12)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2026-11-03",
        help="End UTC date in YYYY-MM-DD format (inclusive, default: 2026-11-03)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=r"D:\Crypto\data\Polymarket\collector\kraken\spot\USDTUSD\trades",
        help=r"Target directory for daily Parquet files (default: D:\Crypto\data\Polymarket\collector\kraken\spot\USDTUSD\trades)",
    )
    parser.add_argument(
        "--latency-ms",
        type=int,
        default=15,
        help="Simulated one-way transmission latency in milliseconds added to trade_timestamp for ts (default: 15)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing daily Parquet files if present",
    )
    return parser.parse_args(args)


def main() -> None:
    args = parse_args()
    print("=================================================================")
    print(" Kraken Historical Spot Trades Downloader -> Daily Parquet")
    print(f" Pair       : {args.pair} (WS symbol: {normalize_ws_symbol(args.pair)})")
    print(f" Date Range : {args.start_date} .. {args.end_date} (UTC)")
    print(f" Latency    : +{args.latency_ms} ms (ts = trade_timestamp + {args.latency_ms})")
    print(f" Output Dir : {args.output_dir}")
    print("=================================================================")
    try:
        written = download_and_save_kraken_trades(
            pair=args.pair,
            start_date=args.start_date,
            end_date=args.end_date,
            output_dir=args.output_dir,
            latency_ms=args.latency_ms,
            overwrite=args.overwrite,
        )
        print("=================================================================")
        print(f" Completed! Total daily Parquet files ready: {len(written)}")
        print("=================================================================")
    except Exception as exc:
        print(f"Error downloading Kraken trades: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
