#!/usr/bin/env python3
"""
Script to export Polymarket historical series data using MarketLens SDK.
Documentation: https://marketlens.trade/docs/exports
"""

import argparse
import os
import sys
from pathlib import Path
from marketlens import MarketLens


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="Export Polymarket series historical data via MarketLens."
    )
    parser.add_argument(
        "--series-id",
        type=str,
        default="btc-up-or-down-15m",
        help="Series slug or UUID (default: btc-up-or-down-15m)"
    )
    parser.add_argument(
        "--after",
        type=str,
        default="2026-08-07T00:00:00Z",
        help="Window start in ISO 8601 (default: 2026-08-07T00:00:00Z)"
    )
    parser.add_argument(
        "--before",
        type=str,
        default="2026-09-07T00:00:00Z",
        help="Window end in ISO 8601 (default: 2026-09-07T00:00:00Z)"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=r"D:\Crypto\data\Polymarket\btc-up-or-down-15m",
        help=r"Directory to save parquet files (default: D:\Crypto\data\Polymarket\btc-up-or-down-15m)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview export cost without downloading"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of concurrent downloads (default: 4)"
    )
    return parser.parse_args(args)


def export_series(
    series_id: str,
    after: str,
    before: str,
    data_dir: str,
    dry_run: bool = False,
    concurrency: int = 4,
    client: MarketLens | None = None
):
    path = Path(data_dir)
    path.mkdir(parents=True, exist_ok=True)

    if client is None:
        api_key = os.environ.get("MARKETLENS_API_KEY")
        if not api_key:
            raise ValueError("MARKETLENS_API_KEY environment variable is not set.")
        client = MarketLens(api_key=api_key)

    print(f"Starting series export:")
    print(f"  Series ID:   {series_id}")
    print(f"  After:       {after}")
    print(f"  Before:      {before}")
    print(f"  Data dir:    {data_dir}")
    print(f"  Dry run:     {dry_run}")
    print(f"  Concurrency: {concurrency}")

    result = client.exports.download_series(
        series_id,
        after=after,
        before=before,
        data_dir=data_dir,
        dry_run=dry_run,
        concurrency=concurrency
    )

    print("\nExport completed successfully!")
    print(f"  Ready markets:       {len(result.ready)}")
    print(f"  Pending markets:     {len(result.pending)}")
    print(f"  Failed markets:      {len(result.failed)}")
    print(f"  Rate-limited markets:{len(result.rate_limited)}")
    print(f"  Rows charged:        {result.rows_charged}")
    print(f"  Events charged:      {result.events_charged}")

    return result


def main():
    args = parse_args()
    try:
        export_series(
            series_id=args.series_id,
            after=args.after,
            before=args.before,
            data_dir=args.data_dir,
            dry_run=args.dry_run,
            concurrency=args.concurrency
        )
    except Exception as e:
        print(f"Error during export: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
