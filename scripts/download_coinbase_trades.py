#!/usr/bin/env python3
"""
Downloads historical spot trades from Coinbase Exchange REST API
(https://api.exchange.coinbase.com/products/<product-id>/trades) and writes
daily ZSTD-compressed Apache Parquet files matching the Polymarket Bot collector conventions.

Target path layout:
  <dir>/<SYMBOL>/trades/<SYMBOL>-trades-<YYYY-MM-DD>.parquet
Default dir:
  D:\\Crypto\\data\\Polymarket\\collector\\coinbase\\spot

Parquet schema:
  - ts: uint64              (trade_timestamp + latency_ms, default +15ms)
  - trade_timestamp: uint64 (exchange trade time in epoch milliseconds)
  - symbol: string          (Coinbase product ID, e.g. "BTC-USD")
  - trade_id: int64         (unique sequential Coinbase trade ID)
  - side: string            ("buy" or "sell")
  - price: double           (trade execution price)
  - qty: double             (executed quantity in base asset)
  - timestamp: string       (RFC3339 UTC timestamp with microsecond precision)
"""

import argparse
from collections import defaultdict
import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
import sys
import time
from typing import Any, Mapping

import httpx
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_ROOT_DIR = r"D:\Crypto\data\Polymarket\collector\coinbase\spot"
COINBASE_API_BASE_URL = "https://api.exchange.coinbase.com"

COINBASE_TRADES_PARQUET_SCHEMA = pa.schema(
    [
        ("ts", pa.uint64()),
        ("trade_timestamp", pa.uint64()),
        ("symbol", pa.string()),
        ("trade_id", pa.int64()),
        ("side", pa.string()),
        ("price", pa.float64()),
        ("qty", pa.float64()),
        ("timestamp", pa.string()),
    ]
)

BASE_ALIAS_MAP = {
    "XBT": "BTC",
    "XXBT": "BTC",
    "XDG": "DOGE",
}


class DailyColumnBuffer:
    """
    High-performance columnar in-memory buffer for a single UTC day's trades.
    Avoids per-row Python dictionary allocations and converts directly into
    typed PyArrow arrays and ZSTD-compressed Parquet.
    """

    __slots__ = (
        "ts",
        "trade_timestamp",
        "trade_id",
        "side",
        "price",
        "qty",
        "timestamp",
    )

    def __init__(self) -> None:
        self.ts: list[int] = []
        self.trade_timestamp: list[int] = []
        self.trade_id: list[int] = []
        self.side: list[str] = []
        self.price: list[float] = []
        self.qty: list[float] = []
        self.timestamp: list[str] = []

    def append(
        self,
        ts: int,
        trade_timestamp: int,
        trade_id: int,
        side: str,
        price: float,
        qty: float,
        timestamp: str,
    ) -> None:
        self.ts.append(ts)
        self.trade_timestamp.append(trade_timestamp)
        self.trade_id.append(trade_id)
        self.side.append(side)
        self.price.append(price)
        self.qty.append(qty)
        self.timestamp.append(timestamp)

    def __len__(self) -> int:
        return len(self.ts)

    def to_sorted_arrow_table(self, product_id: str) -> pa.Table:
        """Constructs a PyArrow Table sorted by (ts ASC, trade_id ASC)."""
        n = len(self.ts)
        if n == 0:
            return pa.Table.from_pydict(
                {field.name: [] for field in COINBASE_TRADES_PARQUET_SCHEMA},
                schema=COINBASE_TRADES_PARQUET_SCHEMA,
            )

        # Since Coinbase pagination walks backwards (newest to oldest), reversing
        # often yields an already ascending sequence in O(N) before checking sort order.
        if n > 1 and self.trade_id[0] > self.trade_id[-1]:
            self.ts.reverse()
            self.trade_timestamp.reverse()
            self.trade_id.reverse()
            self.side.reverse()
            self.price.reverse()
            self.qty.reverse()
            self.timestamp.reverse()

        is_sorted = True
        ts_list = self.ts
        tid_list = self.trade_id
        for i in range(1, n):
            prev_ts = ts_list[i - 1]
            curr_ts = ts_list[i]
            if curr_ts < prev_ts or (curr_ts == prev_ts and tid_list[i] < tid_list[i - 1]):
                is_sorted = False
                break

        if is_sorted:
            ts_col = self.ts
            trade_ts_col = self.trade_timestamp
            tid_col = self.trade_id
            side_col = self.side
            price_col = self.price
            qty_col = self.qty
            iso_col = self.timestamp
        else:
            order = sorted(range(n), key=lambda i: (ts_list[i], tid_list[i]))
            ts_col = [self.ts[i] for i in order]
            trade_ts_col = [self.trade_timestamp[i] for i in order]
            tid_col = [self.trade_id[i] for i in order]
            side_col = [self.side[i] for i in order]
            price_col = [self.price[i] for i in order]
            qty_col = [self.qty[i] for i in order]
            iso_col = [self.timestamp[i] for i in order]

        return pa.Table.from_arrays(
            [
                pa.array(ts_col, type=pa.uint64()),
                pa.array(trade_ts_col, type=pa.uint64()),
                pa.repeat(pa.scalar(product_id, type=pa.string()), n),
                pa.array(tid_col, type=pa.int64()),
                pa.array(side_col, type=pa.string()),
                pa.array(price_col, type=pa.float64()),
                pa.array(qty_col, type=pa.float64()),
                pa.array(iso_col, type=pa.string()),
            ],
            schema=COINBASE_TRADES_PARQUET_SCHEMA,
        )


def normalize_symbol_folder(symbol: str) -> str:
    """Normalizes a trading symbol to uppercase alphanumeric folder/file prefix (e.g. 'BTC-USD' -> 'BTCUSD')."""
    clean = symbol.strip().upper().replace("/", "").replace("-", "").replace("_", "")
    if not clean:
        raise ValueError("Symbol cannot be empty.")
    return clean


def normalize_coinbase_product_id(symbol: str) -> str:
    """Normalizes a trading symbol to Coinbase product ID format (e.g. 'BTCUSD' -> 'BTC-USD', 'USDTUSD' -> 'USDT-USD')."""
    raw = symbol.strip().upper().replace("/", "-").replace("_", "-")
    if "-" in raw:
        base, quote = raw.split("-", 1)
        base = BASE_ALIAS_MAP.get(base, base)
        return f"{base}-{quote}"
    for quote in ("USDT", "USDC", "PYUSD", "EUR", "USD", "GBP", "JPY", "CHF", "CAD", "AUD", "BTC", "ETH"):
        if raw.endswith(quote) and len(raw) > len(quote):
            base = raw[: -len(quote)]
            base = BASE_ALIAS_MAP.get(base, base)
            return f"{base}-{quote}"
    return raw


def parse_coinbase_time_to_ms(iso_str: str, latency_ms: int) -> tuple[int, int, str]:
    """
    Parses Coinbase ISO-8601 UTC timestamp string (e.g. '2026-11-10T00:00:00.139360Z')
    into (ts_ms, trade_timestamp_ms, canonical_iso_str).
    """
    s = iso_str.strip()
    if s.endswith("Z"):
        dt = datetime.datetime.fromisoformat(s[:-1] + "+00:00")
    else:
        dt = datetime.datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)

    epoch_us = int(round(dt.timestamp() * 1_000_000))
    trade_ts_ms = epoch_us // 1_000
    ts_ms = trade_ts_ms + latency_ms
    canonical_iso = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return ts_ms, trade_ts_ms, canonical_iso


def compute_rate_limit_delay(headers: Mapping[str, str], default_delay: float = 0.10) -> float:
    """
    Computes the delay in seconds before the next request based on HTTP response headers.
    Inspects Retry-After, RateLimit-Remaining / X-RateLimit-Remaining,
    and RateLimit-Reset / X-RateLimit-Reset.
    """
    lower_headers = {k.lower(): v.strip() for k, v in headers.items() if v}
    delay = default_delay

    # 1. Retry-After (seconds or HTTP-date)
    retry_after = lower_headers.get("retry-after")
    if retry_after:
        try:
            delay = max(delay, float(retry_after))
        except ValueError:
            try:
                retry_dt = parsedate_to_datetime(retry_after)
                if retry_dt.tzinfo is None:
                    retry_dt = retry_dt.replace(tzinfo=datetime.timezone.utc)
                wait_sec = (retry_dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
                if wait_sec > 0:
                    delay = max(delay, wait_sec)
            except Exception:
                pass

    # 2. X-RateLimit-Reset / RateLimit-Reset & Remaining
    reset_val = (
        lower_headers.get("x-ratelimit-reset")
        or lower_headers.get("ratelimit-reset")
        or lower_headers.get("cf-ratelimit-reset")
    )
    remaining_val = (
        lower_headers.get("x-ratelimit-remaining")
        or lower_headers.get("ratelimit-remaining")
        or lower_headers.get("cf-ratelimit-remaining")
    )

    remaining: float | None = None
    if remaining_val is not None:
        try:
            remaining = float(remaining_val)
        except ValueError:
            remaining = None

    if reset_val is not None:
        try:
            reset_num = float(reset_val)
            now_ts = time.time()
            reset_wait = (reset_num - now_ts) if reset_num > 1_000_000_000 else reset_num
            if remaining is not None and remaining <= 1:
                delay = max(delay, max(0.5, reset_wait))
            elif remaining is not None and remaining > 1 and reset_wait > 0:
                delay = max(delay, reset_wait / remaining)
        except ValueError:
            pass
    elif remaining is not None and remaining <= 1:
        delay = max(delay, 1.0)

    return delay


def fetch_coinbase_trades_page(
    client: httpx.Client,
    product_id: str,
    limit: int = 1000,
    after_trade_id: int | None = None,
    max_retries: int = 12,
    base_delay: float = 0.10,
) -> tuple[list[dict[str, Any]], Mapping[str, str], float]:
    """
    Fetches a single page of trades from Coinbase Exchange public REST API,
    respecting HTTP rate limit headers and retrying on transient errors or HTTP 429/5xx.
    Returns (trades_list, response_headers, recommended_sleep_seconds).
    """
    url = f"{COINBASE_API_BASE_URL}/products/{product_id}/trades"
    params: dict[str, str] = {"limit": str(limit)}
    if after_trade_id is not None:
        params["after"] = str(after_trade_id)

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.get(url, params=params)
            recommended_sleep = compute_rate_limit_delay(resp.headers, default_delay=base_delay)

            if resp.status_code in (429, 500, 502, 503, 504):
                wait_time = max(recommended_sleep, backoff)
                if attempt < max_retries:
                    print(
                        f"  [RateLimit/Server] HTTP {resp.status_code} received, waiting {wait_time:.2f}s "
                        f"(attempt {attempt}/{max_retries})...",
                        flush=True,
                    )
                    time.sleep(wait_time)
                    backoff = min(backoff * 1.8, 30.0)
                    continue
                resp.raise_for_status()

            if resp.status_code == 404:
                raise RuntimeError(f"Coinbase product '{product_id}' not found (HTTP 404): {resp.text}")

            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise RuntimeError(f"Unexpected Coinbase API response for {product_id}: {data}")

            return data, resp.headers, recommended_sleep

        except (httpx.HTTPError, TimeoutError, ConnectionError) as exc:
            if attempt >= max_retries:
                raise RuntimeError(f"Failed to fetch Coinbase trades after {max_retries} attempts: {exc}") from exc
            print(
                f"  [Retry] Network error ({exc}), waiting {backoff:.2f}s "
                f"(attempt {attempt}/{max_retries})...",
                flush=True,
            )
            time.sleep(backoff)
            backoff = min(backoff * 1.8, 30.0)

    raise RuntimeError(f"Exhausted retries fetching Coinbase trades for {product_id}")


def find_trade_id_at_or_after_ts(
    client: httpx.Client,
    product_id: str,
    target_ts_ms: int,
    latest_trade_id: int,
    latency_ms: int,
    base_delay: float = 0.05,
) -> int:
    """
    Uses binary search over sequential Coinbase trade_id in [1, latest_trade_id]
    to find the smallest trade_id whose `ts = trade_timestamp + latency_ms >= target_ts_ms`.
    Requires only ~30 lightweight (limit=1) requests instead of scanning millions of trades.
    """
    lo = 1
    hi = latest_trade_id

    while lo < hi:
        mid = (lo + hi) // 2
        trades, _headers, rec_delay = fetch_coinbase_trades_page(
            client=client,
            product_id=product_id,
            limit=1,
            after_trade_id=mid + 1,
            base_delay=base_delay,
        )
        if rec_delay > 0:
            time.sleep(min(rec_delay, 0.05))

        if not trades:
            lo = mid + 1
            continue

        t_obj = trades[0]
        ts_ms, _trade_ts_ms, _iso = parse_coinbase_time_to_ms(t_obj["time"], latency_ms=latency_ms)
        actual_tid = int(t_obj["trade_id"])

        if ts_ms < target_ts_ms:
            lo = max(mid + 1, actual_tid + 1)
        else:
            hi = mid

    return lo


def write_daily_parquet_file(
    date_str: str,
    buffer: DailyColumnBuffer,
    trades_dir: Path,
    symbol_folder: str,
    product_id: str,
) -> Path:
    """
    Converts a DailyColumnBuffer into a sorted PyArrow Table and atomically writes
    (or overwrites) the daily ZSTD-compressed Parquet file:
      ${Dir}/${Symbol}/trades/${Symbol}-trades-${yyyy-MM-dd}.parquet
    """
    trades_dir.mkdir(parents=True, exist_ok=True)
    dest_path = trades_dir / f"{symbol_folder}-trades-{date_str}.parquet"
    tmp_path = dest_path.with_suffix(".tmp.parquet")

    if tmp_path.exists():
        tmp_path.unlink()

    table = buffer.to_sorted_arrow_table(product_id)
    pq.write_table(table, tmp_path, compression="ZSTD", row_group_size=100_000)
    tmp_path.replace(dest_path)
    return dest_path


def download_coinbase_spot_trades(
    symbol: str,
    from_date: str,
    to_date: str,
    root_dir: Path | str = DEFAULT_ROOT_DIR,
    latency_ms: int = 15,
    base_delay: float = 0.10,
) -> list[Path]:
    """
    Downloads Coinbase spot trades for [from_date, to_date] UTC (inclusive) and writes
    daily Parquet files into <root_dir>/<SYMBOL>/trades/<SYMBOL>-trades-<YYYY-MM-DD>.parquet.
    Always overwrites existing daily Parquet files in the requested range.
    """
    symbol_folder = normalize_symbol_folder(symbol)
    product_id = normalize_coinbase_product_id(symbol)
    trades_dir = Path(root_dir) / symbol_folder / "trades"
    trades_dir.mkdir(parents=True, exist_ok=True)

    start_dt = datetime.date.fromisoformat(from_date.strip())
    end_dt = datetime.date.fromisoformat(to_date.strip())
    if end_dt < start_dt:
        raise ValueError(f"To date ({to_date}) cannot be earlier than From date ({from_date}).")

    window_start_ms = int(
        datetime.datetime.combine(start_dt, datetime.time.min, tzinfo=datetime.timezone.utc).timestamp() * 1000
    )
    window_end_exclusive_ms = int(
        datetime.datetime.combine(
            end_dt + datetime.timedelta(days=1),
            datetime.time.min,
            tzinfo=datetime.timezone.utc,
        ).timestamp()
        * 1000
    )

    headers = {
        "User-Agent": "PolymarketBot-CoinbaseSpotDownloader/1.0",
        "Accept": "application/json",
    }

    written_files: list[Path] = []
    t0 = time.time()

    with httpx.Client(headers=headers, timeout=30.0, http2=False) as client:
        # 1. Fetch latest trade to determine upper bound
        latest_trades, _hdrs, _delay = fetch_coinbase_trades_page(
            client=client,
            product_id=product_id,
            limit=1,
            after_trade_id=None,
            base_delay=base_delay,
        )
        if not latest_trades:
            print(f"No trades returned for product {product_id}.", flush=True)
            return []

        latest_trade = latest_trades[0]
        latest_tid = int(latest_trade["trade_id"])
        latest_ts_ms, _, _ = parse_coinbase_time_to_ms(latest_trade["time"], latency_ms=latency_ms)

        if latest_ts_ms < window_start_ms:
            print(f"Latest trade on exchange ({latest_trade['time']}) is older than From date ({from_date}).", flush=True)
            return []

        # 2. Binary search upper bound trade_id (first trade at or after window_end_exclusive_ms)
        if latest_ts_ms <= window_end_exclusive_ms:
            start_after_tid = latest_tid + 1
        else:
            print("  [Index] Locating upper trade_id boundary via binary search...", flush=True)
            upper_tid = find_trade_id_at_or_after_ts(
                client=client,
                product_id=product_id,
                target_ts_ms=window_end_exclusive_ms,
                latest_trade_id=latest_tid,
                latency_ms=latency_ms,
                base_delay=0.04,
            )
            # Add small safety margin (+50 trades) in case of microsecond out-of-order timestamps
            start_after_tid = upper_tid + 50

        print(f"  [Index] Starting backward pagination from trade_id < {start_after_tid:,}...", flush=True)

        after_cursor = start_after_tid
        seen_trade_ids: set[int] = set()
        buffers_by_date: dict[str, DailyColumnBuffer] = defaultdict(DailyColumnBuffer)
        completed_dates: set[str] = set()

        page_count = 0
        total_collected = 0

        # 3. Paginate backward from start_after_tid down to window_start_ms
        while after_cursor > 1:
            t_req_start = time.monotonic()
            raw_trades, resp_headers, recommended_delay = fetch_coinbase_trades_page(
                client=client,
                product_id=product_id,
                limit=1000,
                after_trade_id=after_cursor,
                base_delay=base_delay,
            )

            if not raw_trades:
                break

            page_count += 1
            min_ts_in_page = None
            min_tid_in_page = after_cursor

            for raw_trade in raw_trades:
                tid = int(raw_trade["trade_id"])
                if tid < min_tid_in_page:
                    min_tid_in_page = tid

                if tid in seen_trade_ids:
                    continue
                seen_trade_ids.add(tid)

                ts_ms, trade_ts_ms, canonical_iso = parse_coinbase_time_to_ms(
                    raw_trade["time"], latency_ms=latency_ms
                )

                if min_ts_in_page is None or ts_ms < min_ts_in_page:
                    min_ts_in_page = ts_ms

                if window_start_ms <= ts_ms < window_end_exclusive_ms:
                    side_raw = str(raw_trade.get("side", "")).lower()
                    side = "buy" if side_raw == "buy" else "sell"
                    price = float(raw_trade["price"])
                    qty = float(raw_trade["size"])

                    utc_date_str = datetime.datetime.fromtimestamp(
                        ts_ms / 1000.0, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d")

                    buffers_by_date[utc_date_str].append(
                        ts=ts_ms,
                        trade_timestamp=trade_ts_ms,
                        trade_id=tid,
                        side=side,
                        price=price,
                        qty=qty,
                        timestamp=canonical_iso,
                    )
                    total_collected += 1

            if page_count % 25 == 0:
                elapsed = time.time() - t0
                cursor_dt_str = (
                    datetime.datetime.fromtimestamp(min_ts_in_page / 1000.0, tz=datetime.timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    if min_ts_in_page is not None
                    else "N/A"
                )
                print(
                    f"  [Progress] Page {page_count} | Collected: {total_collected:,} trades | "
                    f"Cursor UTC: {cursor_dt_str} (trade_id {min_tid_in_page:,}) | Elapsed: {elapsed:.1f}s",
                    flush=True,
                )

            # Since pagination walks backward in time, any UTC day strictly greater than
            # the date of (min_ts_in_page + 5000ms) is completely downloaded and can be flushed immediately!
            if min_ts_in_page is not None:
                cutoff_date_str = datetime.datetime.fromtimestamp(
                    (min_ts_in_page + 5000) / 1000.0, tz=datetime.timezone.utc
                ).strftime("%Y-%m-%d")
                ready_dates = [
                    d for d in list(buffers_by_date.keys()) if d > cutoff_date_str and d not in completed_dates
                ]
                for d in sorted(ready_dates, reverse=True):
                    day_buf = buffers_by_date.pop(d)
                    if len(day_buf) > 0:
                        pq_path = write_daily_parquet_file(
                            date_str=d,
                            buffer=day_buf,
                            trades_dir=trades_dir,
                            symbol_folder=symbol_folder,
                            product_id=product_id,
                        )
                        written_files.append(pq_path)
                        completed_dates.add(d)
                        elapsed = time.time() - t0
                        size_kb = pq_path.stat().st_size / 1024.0
                        print(
                            f"  [Saved] {pq_path} ({len(day_buf):,} trades, {size_kb:.1f} KB, "
                            f"page {page_count}, {elapsed:.1f}s)",
                            flush=True,
                        )

            # Stop once we have walked past window_start_ms (with a 2-second safety margin)
            if min_ts_in_page is not None and min_ts_in_page < (window_start_ms - 2000):
                break

            cb_after_hdr = resp_headers.get("cb-after") or resp_headers.get("CB-AFTER")
            next_after = int(cb_after_hdr) if cb_after_hdr and cb_after_hdr.isdigit() else min_tid_in_page
            if next_after >= after_cursor:
                next_after = min_tid_in_page
            if next_after >= after_cursor or next_after <= 1:
                break
            after_cursor = next_after

            elapsed_req = time.monotonic() - t_req_start
            sleep_needed = max(0.0, recommended_delay - elapsed_req)
            if sleep_needed > 0:
                time.sleep(sleep_needed)

    # Flush remaining days in buffer
    for d in sorted(buffers_by_date.keys()):
        if d in completed_dates:
            continue
        day_buf = buffers_by_date[d]
        if len(day_buf) > 0:
            pq_path = write_daily_parquet_file(
                date_str=d,
                buffer=day_buf,
                trades_dir=trades_dir,
                symbol_folder=symbol_folder,
                product_id=product_id,
            )
            written_files.append(pq_path)
            completed_dates.add(d)
            elapsed = time.time() - t0
            size_kb = pq_path.stat().st_size / 1024.0
            print(
                f"  [Saved] {pq_path} ({len(day_buf):,} trades, {size_kb:.1f} KB, "
                f"total pages {page_count}, {elapsed:.1f}s)",
                flush=True,
            )

    written_files.sort()
    return written_files


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download historical Coinbase Spot trades and write daily Parquet files."
    )
    parser.add_argument(
        "--symbol",
        "-s",
        "--Symbol",
        "-Symbol",
        dest="symbol",
        type=str,
        required=True,
        help="Symbol to download trades for (e.g. BTCUSD). Required.",
    )
    parser.add_argument(
        "--from",
        "-f",
        "--From",
        "-From",
        "--start-date",
        dest="from_date",
        type=str,
        required=True,
        help="Start UTC date inclusive in YYYY-MM-DD format (e.g. 2026-10-12). Required.",
    )
    parser.add_argument(
        "--to",
        "-t",
        "--To",
        "-To",
        "--end-date",
        dest="to_date",
        type=str,
        required=True,
        help="End UTC date inclusive in YYYY-MM-DD format (e.g. 2026-10-12). Required.",
    )
    parser.add_argument(
        "--dir",
        "-d",
        "--Dir",
        "-Dir",
        "--output-dir",
        dest="root_dir",
        type=str,
        default=DEFAULT_ROOT_DIR,
        help=f"Root output directory (default: {DEFAULT_ROOT_DIR}).",
    )
    parser.add_argument(
        "--latency-ms",
        type=int,
        default=50,
        help="Simulated one-way latency in ms added to trade_timestamp for ts (default: 50).",
    )
    return parser.parse_args(args)


def main() -> None:
    args = parse_args()
    symbol_folder = normalize_symbol_folder(args.symbol)
    product_id = normalize_coinbase_product_id(args.symbol)
    target_trades_dir = Path(args.root_dir) / symbol_folder / "trades"

    print("=================================================================", flush=True)
    print(" Coinbase Spot Historical Trades Downloader -> Daily Parquet", flush=True)
    print(f" Symbol     : {symbol_folder} (Coinbase Product ID: {product_id})", flush=True)
    print(f" From       : {args.from_date} (inclusive, UTC)", flush=True)
    print(f" To         : {args.to_date} (inclusive, UTC)", flush=True)
    print(f" Root Dir   : {args.root_dir}", flush=True)
    print(f" Target Dir : {target_trades_dir}", flush=True)
    print(f" Latency    : +{args.latency_ms} ms (ts = trade_timestamp + {args.latency_ms})", flush=True)
    print("=================================================================", flush=True)

    try:
        written = download_coinbase_spot_trades(
            symbol=args.symbol,
            from_date=args.from_date,
            to_date=args.to_date,
            root_dir=args.root_dir,
            latency_ms=args.latency_ms,
        )
        print("=================================================================", flush=True)
        print(f" Completed! Daily Parquet files written: {len(written)}", flush=True)
        for p in written:
            print(f"   -> {p}", flush=True)
        print("=================================================================", flush=True)
    except Exception as exc:
        print(f"Error downloading Coinbase spot trades: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
