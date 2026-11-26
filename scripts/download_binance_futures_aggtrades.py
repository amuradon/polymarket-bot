#!/usr/bin/env python3
"""
Downloads historical Binance USD-M Futures aggTrades and writes daily ZSTD-compressed Apache Parquet files
matching the Polymarket Bot collector schema and directory layout.

Primary source: Binance Public Data Vision daily ZIP archive
  https://data.binance.vision/data/futures/um/daily/aggTrades/<SYMBOL>/<SYMBOL>-aggTrades-<YYYY-MM-DD>.zip
Fallback (for recent dates not yet published on data.binance.vision, e.g. today): Binance Futures REST API
  https://fapi.binance.com/fapi/v1/aggTrades

Target path layout:
  <dir>/<SYMBOL>/aggTrades/<SYMBOL>-aggTrades-<YYYY-MM-DD>.parquet
Default dir:
  /data/polymarket/collector/binance/futures

Parquet schema:
  - event_time: uint64         (nullable; null for historical Vision/REST downloads)
  - aggregate_trade_id: int64  (unique sequential aggregate trade ID)
  - symbol: string             (trading pair symbol, e.g. "BTCUSDT")
  - price: double              (trade execution price)
  - quantity: double           (executed quantity in base asset)
  - first_trade_id: int64      (first underlying trade ID)
  - last_trade_id: int64       (last underlying trade ID)
  - trade_time: uint64         (exchange trade time in epoch milliseconds)
  - is_buyer_maker: bool       (true if buyer was the maker)
"""

import argparse
import csv
import datetime
import io
from pathlib import Path
import sys
import time
from typing import Any
import zipfile

import httpx
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_ROOT_DIR = "/data/polymarket/collector/binance/futures"
BINANCE_VISION_FUTURES_BASE_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"
BINANCE_FUTURES_REST_URL = "https://fapi.binance.com/fapi/v1/aggTrades"

FUTURES_AGGTRADES_PARQUET_SCHEMA = pa.schema(
    [
        ("event_time", pa.uint64()),
        ("aggregate_trade_id", pa.int64()),
        ("symbol", pa.string()),
        ("price", pa.float64()),
        ("quantity", pa.float64()),
        ("first_trade_id", pa.int64()),
        ("last_trade_id", pa.int64()),
        ("trade_time", pa.uint64()),
        ("is_buyer_maker", pa.bool_()),
    ]
)


class FuturesAggTradesBuffer:
    """
    Columnar in-memory buffer for a single UTC day's Binance USD-M Futures aggTrades.
    Converts directly into typed PyArrow arrays and ZSTD-compressed Parquet.
    """

    __slots__ = (
        "aggregate_trade_id",
        "price",
        "quantity",
        "first_trade_id",
        "last_trade_id",
        "trade_time",
        "is_buyer_maker",
    )

    def __init__(self) -> None:
        self.aggregate_trade_id: list[int] = []
        self.price: list[float] = []
        self.quantity: list[float] = []
        self.first_trade_id: list[int] = []
        self.last_trade_id: list[int] = []
        self.trade_time: list[int] = []
        self.is_buyer_maker: list[bool] = []

    def append(
        self,
        aggregate_trade_id: int,
        price: float,
        quantity: float,
        first_trade_id: int,
        last_trade_id: int,
        trade_time: int,
        is_buyer_maker: bool,
    ) -> None:
        self.aggregate_trade_id.append(aggregate_trade_id)
        self.price.append(price)
        self.quantity.append(quantity)
        self.first_trade_id.append(first_trade_id)
        self.last_trade_id.append(last_trade_id)
        self.trade_time.append(trade_time)
        self.is_buyer_maker.append(is_buyer_maker)

    def __len__(self) -> int:
        return len(self.aggregate_trade_id)

    def to_sorted_arrow_table(self, symbol: str) -> pa.Table:
        """Constructs a PyArrow Table sorted by (trade_time ASC, aggregate_trade_id ASC)."""
        n = len(self.aggregate_trade_id)
        if n == 0:
            return pa.Table.from_pydict(
                {field.name: [] for field in FUTURES_AGGTRADES_PARQUET_SCHEMA},
                schema=FUTURES_AGGTRADES_PARQUET_SCHEMA,
            )

        is_sorted = True
        t_list = self.trade_time
        aid_list = self.aggregate_trade_id
        for i in range(1, n):
            prev_t = t_list[i - 1]
            curr_t = t_list[i]
            if curr_t < prev_t or (curr_t == prev_t and aid_list[i] < aid_list[i - 1]):
                is_sorted = False
                break

        if is_sorted:
            aid_col = self.aggregate_trade_id
            price_col = self.price
            qty_col = self.quantity
            first_col = self.first_trade_id
            last_col = self.last_trade_id
            t_col = self.trade_time
            maker_col = self.is_buyer_maker
        else:
            order = sorted(range(n), key=lambda i: (t_list[i], aid_list[i]))
            aid_col = [self.aggregate_trade_id[i] for i in order]
            price_col = [self.price[i] for i in order]
            qty_col = [self.quantity[i] for i in order]
            first_col = [self.first_trade_id[i] for i in order]
            last_col = [self.last_trade_id[i] for i in order]
            t_col = [self.trade_time[i] for i in order]
            maker_col = [self.is_buyer_maker[i] for i in order]

        return pa.Table.from_arrays(
            [
                pa.nulls(n, type=pa.uint64()),
                pa.array(aid_col, type=pa.int64()),
                pa.repeat(pa.scalar(symbol, type=pa.string()), n),
                pa.array(price_col, type=pa.float64()),
                pa.array(qty_col, type=pa.float64()),
                pa.array(first_col, type=pa.int64()),
                pa.array(last_col, type=pa.int64()),
                pa.array(t_col, type=pa.uint64()),
                pa.array(maker_col, type=pa.bool_()),
            ],
            schema=FUTURES_AGGTRADES_PARQUET_SCHEMA,
        )


def normalize_symbol(symbol: str) -> str:
    """Normalizes a trading symbol to uppercase alphanumeric Binance format (e.g. 'btc/usdt' -> 'BTCUSDT')."""
    clean = symbol.strip().upper().replace("/", "").replace("-", "").replace("_", "")
    if not clean:
        raise ValueError("Symbol cannot be empty.")
    return clean


def normalize_epoch_ms(raw_ts: int) -> int:
    """Normalizes Binance Vision CSV timestamps (ms, us, or ns) into epoch milliseconds."""
    if raw_ts >= 100_000_000_000_000_000:
        return raw_ts // 1_000_000
    if raw_ts >= 100_000_000_000_000:
        return raw_ts // 1_000
    return raw_ts


def parse_bool(val: str | bool) -> bool:
    if isinstance(val, bool):
        return val
    return val.strip().lower() in ("true", "1", "t", "yes")


def write_daily_parquet_file(
    date_str: str,
    buffer: FuturesAggTradesBuffer,
    aggtrades_dir: Path,
    symbol: str,
) -> Path:
    """Atomically writes a sorted ZSTD-compressed daily Parquet file."""
    aggtrades_dir.mkdir(parents=True, exist_ok=True)
    dest_path = aggtrades_dir / f"{symbol}-aggTrades-{date_str}.parquet"
    tmp_path = dest_path.with_suffix(".tmp.parquet")

    if tmp_path.exists():
        tmp_path.unlink()

    table = buffer.to_sorted_arrow_table(symbol)
    pq.write_table(table, tmp_path, compression="ZSTD", row_group_size=100_000)
    tmp_path.replace(dest_path)
    return dest_path


def download_day_from_vision(
    client: httpx.Client,
    symbol: str,
    date_str: str,
) -> FuturesAggTradesBuffer | None:
    """
    Attempts to download a single UTC day's USD-M Futures aggTrades ZIP from data.binance.vision.
    Returns None if HTTP 404 (not yet published).
    """
    url = f"{BINANCE_VISION_FUTURES_BASE_URL}/{symbol}/{symbol}-aggTrades-{date_str}.zip"
    backoff = 1.0
    for attempt in range(5):
        try:
            resp = client.get(url)
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                print(f"  [Vision HTTP {resp.status_code}] Retrying in {backoff:.1f}s...", flush=True)
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 16.0)
                continue
            resp.raise_for_status()

            buf = FuturesAggTradesBuffer()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                if not csv_names:
                    raise RuntimeError(f"Empty zip archive at {url}")
                with zf.open(csv_names[0]) as raw_csv:
                    reader = csv.reader(io.TextIOWrapper(raw_csv, encoding="utf-8", newline=""))
                    for row in reader:
                        if not row or not row[0].strip().lstrip("-").isdigit():
                            continue
                        aid = int(row[0])
                        price = float(row[1])
                        qty = float(row[2])
                        first_id = int(row[3])
                        last_id = int(row[4])
                        trade_ms = normalize_epoch_ms(int(row[5]))
                        is_maker = parse_bool(row[6])
                        buf.append(
                            aggregate_trade_id=aid,
                            price=price,
                            quantity=qty,
                            first_trade_id=first_id,
                            last_trade_id=last_id,
                            trade_time=trade_ms,
                            is_buyer_maker=is_maker,
                        )
            return buf
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if attempt == 4:
                raise RuntimeError(f"Vision download failed for {url}: {exc}") from exc
            print(f"  [Vision Network] {exc}; retrying in {backoff:.1f}s...", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 16.0)
    return None


def fetch_rest_aggtrades_page(
    client: httpx.Client,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    """Fetches a single page from Binance USD-M Futures /fapi/v1/aggTrades with rate-limit backoff."""
    backoff = 1.0
    for attempt in range(8):
        try:
            resp = client.get(BINANCE_FUTURES_REST_URL, params=params)
            if resp.status_code in (418, 429):
                retry_after = float(resp.headers.get("Retry-After", backoff))
                wait_s = max(retry_after, backoff)
                print(f"  [REST RateLimit {resp.status_code}] Waiting {wait_s:.1f}s...", flush=True)
                time.sleep(wait_s)
                backoff = min(backoff * 2.0, 60.0)
                continue
            if resp.status_code in (500, 502, 503, 504):
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
                continue
            resp.raise_for_status()

            used_weight = resp.headers.get("X-MBX-USED-WEIGHT-1M")
            if used_weight and int(used_weight) >= 2000:
                time.sleep(1.0)
            return resp.json()
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if attempt == 7:
                raise RuntimeError(f"REST fetch failed: {exc}") from exc
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
    raise RuntimeError("Exhausted retries fetching Binance Futures aggTrades")


def download_day_from_rest(
    client: httpx.Client,
    symbol: str,
    day_date: datetime.date,
) -> FuturesAggTradesBuffer:
    """Downloads a single UTC day's USD-M Futures aggTrades via Binance REST API /fapi/v1/aggTrades."""
    day_start_ms = int(
        datetime.datetime.combine(day_date, datetime.time.min, tzinfo=datetime.timezone.utc).timestamp() * 1000
    )
    day_end_exclusive_ms = int(
        datetime.datetime.combine(
            day_date + datetime.timedelta(days=1), datetime.time.min, tzinfo=datetime.timezone.utc
        ).timestamp()
        * 1000
    )

    buf = FuturesAggTradesBuffer()
    seen_ids: set[int] = set()

    # Locate initial aggregate_trade_id using a 1-hour window probe starting at day_start_ms
    probe_end = min(day_start_ms + 3_600_000, day_end_exclusive_ms - 1)
    first_page = fetch_rest_aggtrades_page(
        client,
        {"symbol": symbol, "startTime": day_start_ms, "endTime": probe_end, "limit": 1000},
    )
    if not first_page:
        cursor = probe_end + 1
        while cursor < day_end_exclusive_ms and not first_page:
            next_end = min(cursor + 3_600_000, day_end_exclusive_ms - 1)
            first_page = fetch_rest_aggtrades_page(
                client,
                {"symbol": symbol, "startTime": cursor, "endTime": next_end, "limit": 1000},
            )
            cursor = next_end + 1

    if not first_page:
        return buf

    next_from_id: int | None = None
    for item in first_page:
        aid = int(item["a"])
        trade_ms = int(item["T"])
        if day_start_ms <= trade_ms < day_end_exclusive_ms and aid not in seen_ids:
            seen_ids.add(aid)
            buf.append(
                aggregate_trade_id=aid,
                price=float(item["p"]),
                quantity=float(item["q"]),
                first_trade_id=int(item["f"]),
                last_trade_id=int(item["l"]),
                trade_time=trade_ms,
                is_buyer_maker=bool(item["m"]),
            )
        next_from_id = aid + 1

    page_count = 1
    while next_from_id is not None:
        page = fetch_rest_aggtrades_page(
            client,
            {"symbol": symbol, "fromId": next_from_id, "limit": 1000},
        )
        if not page:
            break

        page_count += 1
        reached_end = False
        last_aid = next_from_id
        for item in page:
            aid = int(item["a"])
            trade_ms = int(item["T"])
            last_aid = aid
            if trade_ms >= day_end_exclusive_ms:
                reached_end = True
                continue
            if day_start_ms <= trade_ms and aid not in seen_ids:
                seen_ids.add(aid)
                buf.append(
                    aggregate_trade_id=aid,
                    price=float(item["p"]),
                    quantity=float(item["q"]),
                    first_trade_id=int(item["f"]),
                    last_trade_id=int(item["l"]),
                    trade_time=trade_ms,
                    is_buyer_maker=bool(item["m"]),
                )

        if reached_end or len(page) < 1000:
            break
        next_from_id = last_aid + 1
        if page_count % 50 == 0:
            print(f"    [REST {day_date.isoformat()}] {page_count} pages | {len(buf):,} trades...", flush=True)

    return buf


def download_binance_futures_aggtrades(
    symbol: str,
    from_date: str,
    to_date: str,
    root_dir: Path | str = DEFAULT_ROOT_DIR,
) -> list[Path]:
    """
    Downloads Binance USD-M Futures aggTrades for [from_date, to_date] UTC (inclusive) and writes
    daily Parquet files into <root_dir>/<SYMBOL>/aggTrades/<SYMBOL>-aggTrades-<YYYY-MM-DD>.parquet.
    """
    norm_symbol = normalize_symbol(symbol)
    aggtrades_dir = Path(root_dir) / norm_symbol / "aggTrades"
    aggtrades_dir.mkdir(parents=True, exist_ok=True)

    start_dt = datetime.date.fromisoformat(from_date.strip())
    end_dt = datetime.date.fromisoformat(to_date.strip())
    if end_dt < start_dt:
        raise ValueError(f"To date ({to_date}) cannot be earlier than From date ({from_date}).")

    written_files: list[Path] = []
    headers = {"User-Agent": "PolymarketBot-BinanceFuturesDownloader/1.0"}

    with httpx.Client(headers=headers, timeout=60.0, follow_redirects=True) as client:
        current = start_dt
        while current <= end_dt:
            date_str = current.isoformat()
            t0 = time.time()
            buf = download_day_from_vision(client, norm_symbol, date_str)
            source = "data.binance.vision"
            if buf is None:
                source = "REST /fapi/v1/aggTrades"
                buf = download_day_from_rest(client, norm_symbol, current)

            out_path = write_daily_parquet_file(date_str, buf, aggtrades_dir, norm_symbol)
            written_files.append(out_path)
            elapsed = time.time() - t0
            print(
                f"  [Saved] {out_path.name} ({len(buf):,} rows, source={source}, {elapsed:.1f}s)",
                flush=True,
            )
            current += datetime.timedelta(days=1)

    return written_files


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download historical Binance USD-M Futures aggTrades to daily ZSTD Parquet files.",
    )
    parser.add_argument(
        "--symbol",
        "-s",
        "-Symbol",
        type=str,
        default="BTCUSDT",
        help="Trading symbol (e.g. BTCUSDT, ETHUSDT). Default: BTCUSDT.",
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
    return parser.parse_args(args)


def main() -> None:
    args = parse_args()
    norm_symbol = normalize_symbol(args.symbol)
    target_dir = Path(args.root_dir) / norm_symbol / "aggTrades"

    print("=================================================================", flush=True)
    print(" Binance Futures Historical aggTrades Downloader -> Daily Parquet", flush=True)
    print(f" Symbol     : {norm_symbol}", flush=True)
    print(f" From       : {args.from_date} (inclusive, UTC)", flush=True)
    print(f" To         : {args.to_date} (inclusive, UTC)", flush=True)
    print(f" Root Dir   : {args.root_dir}", flush=True)
    print(f" Target Dir : {target_dir}", flush=True)
    print("=================================================================", flush=True)

    try:
        written = download_binance_futures_aggtrades(
            symbol=args.symbol,
            from_date=args.from_date,
            to_date=args.to_date,
            root_dir=args.root_dir,
        )
        print("=================================================================", flush=True)
        print(f" Completed! Daily Parquet files written: {len(written)}", flush=True)
        for p in written:
            print(f"   -> {p}", flush=True)
        print("=================================================================", flush=True)
    except Exception as exc:
        print(f"Error downloading Binance Futures aggTrades: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
