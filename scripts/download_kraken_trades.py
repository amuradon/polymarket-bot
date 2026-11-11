#!/usr/bin/env python3
"""
Downloads historical spot trades from Kraken REST API (/0/public/Trades) and writes
daily ZSTD-compressed Apache Parquet files matching the Kraken Spot WebSocket v2
trade schema and the Polymarket Bot collector conventions.

Target path layout:
  <dir>/<SYMBOL>/trades/<SYMBOL>-trades-<YYYY-MM-DD>.parquet
Default dir:
  D:\\Crypto\\data\\Polymarket\\collector\\kraken\\spot

Parquet schema (aligned with Kraken WebSocket v2 `trade` channel + collector `ts`):
  - ts: uint64              (trade_timestamp + latency_ms, default +15ms)
  - trade_timestamp: uint64 (exchange trade time in epoch milliseconds)
  - symbol: string          (Kraken WS v2 symbol, e.g. "BTC/USD", "USDT/USD")
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
from email.utils import parsedate_to_datetime
from pathlib import Path
import re
import sys
import time
from typing import Any, Mapping

import httpx
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_ROOT_DIR = r"D:\Crypto\data\Polymarket\collector\kraken\spot"
KRAKEN_TRADES_URL = "https://api.kraken.com/0/public/Trades"

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
        "ord_type",
        "price",
        "qty",
        "timestamp",
    )

    def __init__(self) -> None:
        self.ts: list[int] = []
        self.trade_timestamp: list[int] = []
        self.trade_id: list[int] = []
        self.side: list[str] = []
        self.ord_type: list[str] = []
        self.price: list[float] = []
        self.qty: list[float] = []
        self.timestamp: list[str] = []

    def append(
        self,
        ts: int,
        trade_timestamp: int,
        trade_id: int,
        side: str,
        ord_type: str,
        price: float,
        qty: float,
        timestamp: str,
    ) -> None:
        self.ts.append(ts)
        self.trade_timestamp.append(trade_timestamp)
        self.trade_id.append(trade_id)
        self.side.append(side)
        self.ord_type.append(ord_type)
        self.price.append(price)
        self.qty.append(qty)
        self.timestamp.append(timestamp)

    def __len__(self) -> int:
        return len(self.ts)

    def to_sorted_arrow_table(self, ws_symbol: str) -> pa.Table:
        """Constructs a PyArrow Table sorted by (ts ASC, trade_id ASC)."""
        n = len(self.ts)
        if n == 0:
            return pa.Table.from_pydict(
                {field.name: [] for field in KRAKEN_TRADES_PARQUET_SCHEMA},
                schema=KRAKEN_TRADES_PARQUET_SCHEMA,
            )

        # Check if already sorted in ascending order (common case for Kraken REST pagination)
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
            ord_col = self.ord_type
            price_col = self.price
            qty_col = self.qty
            iso_col = self.timestamp
        else:
            order = sorted(range(n), key=lambda i: (ts_list[i], tid_list[i]))
            ts_col = [self.ts[i] for i in order]
            trade_ts_col = [self.trade_timestamp[i] for i in order]
            tid_col = [self.trade_id[i] for i in order]
            side_col = [self.side[i] for i in order]
            ord_col = [self.ord_type[i] for i in order]
            price_col = [self.price[i] for i in order]
            qty_col = [self.qty[i] for i in order]
            iso_col = [self.timestamp[i] for i in order]

        return pa.Table.from_arrays(
            [
                pa.array(ts_col, type=pa.uint64()),
                pa.array(trade_ts_col, type=pa.uint64()),
                pa.repeat(pa.scalar(ws_symbol, type=pa.string()), n),
                pa.array(tid_col, type=pa.int64()),
                pa.array(side_col, type=pa.string()),
                pa.array(ord_col, type=pa.string()),
                pa.array(price_col, type=pa.float64()),
                pa.array(qty_col, type=pa.float64()),
                pa.array(iso_col, type=pa.string()),
            ],
            schema=KRAKEN_TRADES_PARQUET_SCHEMA,
        )


def normalize_symbol_folder(symbol: str) -> str:
    """Normalizes a trading symbol to uppercase alphanumeric folder/file prefix (e.g. 'BTC/USD' -> 'BTCUSD')."""
    clean = symbol.strip().upper().replace("/", "").replace("-", "").replace("_", "")
    if not clean:
        raise ValueError("Symbol cannot be empty.")
    return clean


def normalize_ws_symbol(symbol: str) -> str:
    """Normalizes a trading symbol to Kraken WebSocket v2 format (e.g. 'BTCUSD' -> 'BTC/USD', 'USDTUSD' -> 'USDT/USD')."""
    raw = symbol.strip().upper().replace("-", "/").replace("_", "/")
    if "/" in raw:
        base, quote = raw.split("/", 1)
        base = BASE_ALIAS_MAP.get(base, base)
        return f"{base}/{quote}"
    for quote in ("USDT", "USDC", "PYUSD", "EUR", "USD", "GBP", "JPY", "CHF", "CAD", "AUD"):
        if raw.endswith(quote) and len(raw) > len(quote):
            base = raw[: -len(quote)]
            base = BASE_ALIAS_MAP.get(base, base)
            return f"{base}/{quote}"
    return raw


def format_rfc3339_micros(epoch_us: int) -> str:
    """Formats integer Unix epoch microseconds into RFC3339 UTC string with microsecond precision."""
    dt = datetime.datetime.fromtimestamp(epoch_us / 1_000_000.0, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def compute_rate_limit_delay(headers: Mapping[str, str], default_delay: float = 1.0) -> float:
    """
    Computes the delay in seconds before the next request based on HTTP response headers.
    Inspects Retry-After, RateLimit-Remaining / X-RateLimit-Remaining,
    RateLimit-Reset / X-RateLimit-Reset, and Cache-Control.
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

    # 2. X-RateLimit-Reset / RateLimit-Reset
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
            # Determine if reset_num is a Unix epoch timestamp or delta seconds
            now_ts = time.time()
            reset_wait = (reset_num - now_ts) if reset_num > 1_000_000_000 else reset_num
            if remaining is not None and remaining <= 1:
                delay = max(delay, max(1.0, reset_wait))
            elif remaining is not None and remaining > 1 and reset_wait > 0:
                delay = max(delay, reset_wait / remaining)
        except ValueError:
            pass
    elif remaining is not None and remaining <= 1:
        delay = max(delay, 2.0)

    # 3. Cache-Control: max-age=<sec>
    cache_control = lower_headers.get("cache-control", "")
    if cache_control and default_delay > 0:
        m = re.search(r"max-age=(\d+)", cache_control)
        if m:
            max_age = float(m.group(1))
            # Respect public API pacing (around 1s per call)
            delay = max(delay, min(max_age, 1.05))

    return delay


def fetch_kraken_trades_page(
    client: httpx.Client,
    pair: str,
    since_ns: int,
    count: int = 1000,
    max_retries: int = 12,
    base_delay: float = 1.0,
) -> tuple[dict[str, Any], float]:
    """
    Fetches a single page of trades from Kraken public REST API using an HTTP client,
    respecting HTTP rate limit headers and retrying on transient errors or 429 / EGeneral:Too many requests.
    Returns (json_payload, recommended_sleep_seconds).
    """
    params = {"pair": pair, "since": str(since_ns), "count": str(count)}
    backoff = 2.0

    for attempt in range(1, max_retries + 1):
        try:
            resp = client.get(KRAKEN_TRADES_URL, params=params)
            recommended_sleep = compute_rate_limit_delay(resp.headers, default_delay=base_delay)

            if resp.status_code == 429 or resp.status_code == 503:
                wait_time = max(recommended_sleep, backoff)
                if attempt < max_retries:
                    print(
                        f"  [RateLimit] HTTP {resp.status_code} received, waiting {wait_time:.2f}s "
                        f"(attempt {attempt}/{max_retries})...",
                        flush=True,
                    )
                    time.sleep(wait_time)
                    backoff = min(backoff * 1.8, 60.0)
                    continue
                resp.raise_for_status()

            resp.raise_for_status()
            payload = resp.json()

            errors = payload.get("error", [])
            if errors:
                err_text = ", ".join(str(e) for e in errors)
                err_lower = err_text.lower()
                if (
                    "rate limit" in err_lower
                    or "too many" in err_lower
                    or "toomany" in err_lower
                    or "temporary lockout" in err_lower
                    or "service:unavailable" in err_lower
                ):
                    wait_time = max(recommended_sleep, backoff)
                    if attempt < max_retries:
                        print(
                            f"  [RateLimit] Kraken API '{err_text}', waiting {wait_time:.2f}s "
                            f"(attempt {attempt}/{max_retries})...",
                            flush=True,
                        )
                        time.sleep(wait_time)
                        backoff = min(backoff * 1.8, 60.0)
                        continue
                raise RuntimeError(f"Kraken API returned error: {err_text}")

            return payload, recommended_sleep

        except (httpx.HTTPError, TimeoutError, ConnectionError) as exc:
            if attempt >= max_retries:
                raise RuntimeError(f"Failed to fetch Kraken trades after {max_retries} attempts: {exc}") from exc
            print(
                f"  [Retry] Network error ({exc}), waiting {backoff:.2f}s "
                f"(attempt {attempt}/{max_retries})...",
                flush=True,
            )
            time.sleep(backoff)
            backoff = min(backoff * 1.8, 60.0)

    raise RuntimeError(f"Exhausted retries fetching Kraken trades for pair={pair}, since={since_ns}")


def write_daily_parquet_file(
    date_str: str,
    buffer: DailyColumnBuffer,
    trades_dir: Path,
    symbol_folder: str,
    ws_symbol: str,
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

    table = buffer.to_sorted_arrow_table(ws_symbol)
    pq.write_table(table, tmp_path, compression="ZSTD", row_group_size=100_000)
    tmp_path.replace(dest_path)
    return dest_path


def download_kraken_spot_trades(
    symbol: str,
    from_date: str,
    to_date: str,
    root_dir: Path | str = DEFAULT_ROOT_DIR,
    latency_ms: int = 15,
    base_delay: float = 1.0,
) -> list[Path]:
    """
    Downloads Kraken spot trades for [from_date, to_date] UTC (inclusive) and writes
    daily Parquet files into <root_dir>/<SYMBOL>/trades/<SYMBOL>-trades-<YYYY-MM-DD>.parquet.
    Always overwrites existing daily Parquet files in the requested range.
    """
    symbol_folder = normalize_symbol_folder(symbol)
    ws_symbol = normalize_ws_symbol(symbol)
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

    # Query slightly before window_start_ms so trades whose +latency_ms crosses 00:00:00 UTC are captured
    query_start_ns = max(0, (window_start_ms - max(latency_ms, 1000)) * 1_000_000)

    since_ns = query_start_ns
    seen_trade_ids: set[int] = set()
    buffers_by_date: dict[str, DailyColumnBuffer] = defaultdict(DailyColumnBuffer)
    written_files: list[Path] = []
    completed_dates: set[str] = set()

    page_count = 0
    total_collected = 0
    t0 = time.time()

    headers = {
        "User-Agent": "PolymarketBot-KrakenSpotDownloader/1.0",
        "Accept": "application/json",
    }

    with httpx.Client(headers=headers, timeout=30.0, http2=False) as client:
        while True:
            t_req_start = time.monotonic()
            payload, recommended_delay = fetch_kraken_trades_page(
                client=client,
                pair=symbol_folder,
                since_ns=since_ns,
                count=1000,
                base_delay=base_delay,
            )

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
                if len(raw_trade) < 7:
                    raise ValueError(f"Expected at least 7 elements in Kraken trade entry, got: {raw_trade}")

                price_str, qty_str, time_val, side_code, ord_type_code, _misc, trade_id_val = raw_trade[:7]
                tid = int(trade_id_val)
                if tid in seen_trade_ids:
                    continue
                seen_trade_ids.add(tid)

                epoch_sec = float(time_val)
                epoch_us = int(round(epoch_sec * 1_000_000))
                trade_ts_ms = epoch_us // 1_000
                ts_ms = trade_ts_ms + int(latency_ms)

                if ts_ms > max_ts_in_page:
                    max_ts_in_page = ts_ms

                if window_start_ms <= ts_ms < window_end_exclusive_ms:
                    side = SIDE_MAP.get(str(side_code).lower())
                    if side is None:
                        raise ValueError(f"Unsupported Kraken trade side code: '{side_code}'")

                    ord_type = ORD_TYPE_MAP.get(str(ord_type_code).lower())
                    if ord_type is None:
                        raise ValueError(f"Unsupported Kraken order type code: '{ord_type_code}'")

                    utc_date_str = datetime.datetime.fromtimestamp(
                        ts_ms / 1000.0, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d")

                    buffers_by_date[utc_date_str].append(
                        ts=ts_ms,
                        trade_timestamp=trade_ts_ms,
                        trade_id=tid,
                        side=side,
                        ord_type=ord_type,
                        price=float(price_str),
                        qty=float(qty_str),
                        timestamp=format_rfc3339_micros(epoch_us),
                    )
                    total_collected += 1

            if page_count % 10 == 0:
                elapsed = time.time() - t0
                current_dt_str = (
                    datetime.datetime.fromtimestamp(max_ts_in_page / 1000.0, tz=datetime.timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    if max_ts_in_page > 0
                    else "N/A"
                )
                print(
                    f"  [Progress] Page {page_count} | Collected: {total_collected:,} trades | "
                    f"Cursor UTC: {current_dt_str} | Elapsed: {elapsed:.1f}s",
                    flush=True,
                )

            # Flush any UTC day that has completely passed (5-second safety buffer)
            if max_ts_in_page > 0:
                cutoff_date_str = datetime.datetime.fromtimestamp(
                    (max_ts_in_page - 5000) / 1000.0, tz=datetime.timezone.utc
                ).strftime("%Y-%m-%d")
                ready_dates = [
                    d for d in list(buffers_by_date.keys()) if d < cutoff_date_str and d not in completed_dates
                ]
                for d in sorted(ready_dates):
                    day_buf = buffers_by_date.pop(d)
                    if len(day_buf) > 0:
                        pq_path = write_daily_parquet_file(
                            date_str=d,
                            buffer=day_buf,
                            trades_dir=trades_dir,
                            symbol_folder=symbol_folder,
                            ws_symbol=ws_symbol,
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

            if max_ts_in_page >= window_end_exclusive_ms or last_cursor_ns <= since_ns:
                break

            since_ns = last_cursor_ns

            # Respect rate limit delay minus time already spent processing the response
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
                ws_symbol=ws_symbol,
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
        description="Download historical Kraken Spot trades and write daily Parquet files."
    )
    parser.add_argument(
        "--symbol",
        "-s",
        "--Symbol",
        "-Symbol",
        dest="symbol",
        type=str,
        required=True,
        help="Symbol to download trades for (e.g. BTCUSD, USDTUSD). Required.",
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
        default=15,
        help="Simulated one-way latency in ms added to trade_timestamp for ts (default: 15).",
    )
    return parser.parse_args(args)


def main() -> None:
    args = parse_args()
    symbol_folder = normalize_symbol_folder(args.symbol)
    ws_symbol = normalize_ws_symbol(args.symbol)
    target_trades_dir = Path(args.root_dir) / symbol_folder / "trades"

    print("=================================================================", flush=True)
    print(" Kraken Spot Historical Trades Downloader -> Daily Parquet", flush=True)
    print(f" Symbol     : {symbol_folder} (WS v2 symbol: {ws_symbol})", flush=True)
    print(f" From       : {args.from_date} (inclusive, UTC)", flush=True)
    print(f" To         : {args.to_date} (inclusive, UTC)", flush=True)
    print(f" Root Dir   : {args.root_dir}", flush=True)
    print(f" Target Dir : {target_trades_dir}", flush=True)
    print(f" Latency    : +{args.latency_ms} ms (ts = trade_timestamp + {args.latency_ms})", flush=True)
    print("=================================================================", flush=True)

    try:
        written = download_kraken_spot_trades(
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
        print(f"Error downloading Kraken spot trades: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
