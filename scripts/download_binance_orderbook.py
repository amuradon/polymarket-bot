"""
Script to download Binance Futures orderbook data using cryptohftdata.

Usage:
    python scripts/download_binance_orderbook.py [OPTIONS]

Example:
    python scripts/download_binance_orderbook.py \\
        --symbol BTCUSDT \\
        --exchange binance_futures \\
        --start-date 2026-08-07 \\
        --end-date 2026-09-06 \\
        --data-dir "D:\\Crypto\\data\\Polymarket\\Binance\\futures\\BTCUSDT\\orderBook"
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import cryptohftdata as chd
from cryptohftdata.bulk import BulkDownloadResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("download_binance_orderbook")


def parse_args(args: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Binance Futures orderbook data using cryptohftdata"
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="BTCUSDT",
        help="Trading pair symbol (default: BTCUSDT)",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="binance_futures",
        help="Exchange identifier (default: binance_futures)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-08-07",
        help="Start date YYYY-MM-DD (default: 2026-08-07)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2026-09-06",
        help="End date YYYY-MM-DD (default: 2026-09-06)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=r"D:\Crypto\data\Polymarket\Binance\futures\BTCUSDT\orderBook",
        help=r"Target directory for downloaded data (default: D:\Crypto\data\Polymarket\Binance\futures\BTCUSDT\orderBook)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=os.environ.get("CRYPTOHFTDATA_API_KEY"),
        help="CryptoHFTData API key (optional, uses CRYPTOHFTDATA_API_KEY env if set)",
    )
    parser.add_argument(
        "--layout",
        type=str,
        choices=["mirror", "hive"],
        default="mirror",
        help="Directory layout for downloaded data: mirror or hive (default: mirror)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum concurrent download workers",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the download plan without downloading files",
    )
    return parser.parse_args(args)


def download_orderbook(
    symbol: str = "BTCUSDT",
    exchange: str = "binance_futures",
    start_date: str = "2026-08-07",
    end_date: str = "2026-09-06",
    data_dir: str = r"D:\Crypto\data\Polymarket\Binance\futures\BTCUSDT\orderBook",
    api_key: Optional[str] = None,
    layout: str = "mirror",
    max_workers: Optional[int] = None,
    dry_run: bool = False,
    client: Optional[chd.CryptoHFTDataClient] = None,
) -> BulkDownloadResult:
    """
    Download orderbook data for a trading pair via cryptohftdata bulk download.
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    if start_dt > end_dt:
        raise ValueError(
            f"start_date ({start_date}) must not be after end_date ({end_date})"
        )

    target_path = Path(data_dir)
    if not dry_run:
        target_path.mkdir(parents=True, exist_ok=True)

    if client is None:
        client = chd.CryptoHFTDataClient(api_key=api_key)

    bulk_kwargs = {
        "exchange": exchange,
        "data_type": "orderbook",
        "start": start_date,
        "end": end_date,
        "symbols": [symbol],
        "dest": str(target_path),
        "layout": layout,
        "confirm": True,
        "decompress": True,
        "dry_run": dry_run,
    }
    if max_workers is not None:
        bulk_kwargs["max_workers"] = max_workers

    logger.info(
        "Starting bulk download for %s %s from %s to %s -> %s (layout=%s, dry_run=%s)",
        exchange,
        symbol,
        start_date,
        end_date,
        target_path,
        layout,
        dry_run,
    )

    result = client.download_bulk(**bulk_kwargs)

    if result.failed:
        raise RuntimeError(
            f"Failed to download {len(result.failed)} files: {result.failed}"
        )

    if dry_run:
        logger.info(
            "Dry run complete: %d planned files, %d planned bytes",
            result.planned_files,
            result.planned_bytes,
        )
    else:
        logger.info(
            "Download complete: %d files downloaded (%d skipped), %.2f MB in %.1fs",
            result.files,
            result.skipped,
            result.bytes / (1024 * 1024),
            result.elapsed_seconds,
        )

    return result


def main() -> None:
    args = parse_args()
    try:
        download_orderbook(
            symbol=args.symbol,
            exchange=args.exchange,
            start_date=args.start_date,
            end_date=args.end_date,
            data_dir=args.data_dir,
            api_key=args.api_key,
            layout=args.layout,
            max_workers=args.max_workers,
            dry_run=args.dry_run,
        )
    except Exception as e:
        logger.error("Error executing download: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
