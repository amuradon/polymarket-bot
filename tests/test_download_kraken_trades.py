import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

import pyarrow.parquet as pq

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.download_kraken_trades import (
    format_rfc3339_micros,
    normalize_pair_folder,
    normalize_ws_symbol,
    parse_args,
    transform_rest_trade,
    download_and_save_kraken_trades,
    write_daily_parquet_files,
)


class TestDownloadKrakenTrades(unittest.TestCase):

    def test_parse_args_defaults(self):
        args = parse_args([])
        self.assertEqual(args.pair, "USDTUSD")
        self.assertEqual(args.start_date, "2026-10-12")
        self.assertEqual(args.end_date, "2026-11-03")
        self.assertEqual(
            Path(args.output_dir),
            Path(r"D:\Crypto\data\Polymarket\collector\kraken\spot\USDTUSD\trades"),
        )
        self.assertEqual(args.latency_ms, 15)
        self.assertFalse(args.overwrite)

    def test_normalize_helpers(self):
        self.assertEqual(normalize_pair_folder("USDT/USD"), "USDTUSD")
        self.assertEqual(normalize_pair_folder("usdtusd"), "USDTUSD")
        self.assertEqual(normalize_ws_symbol("USDTUSD"), "USDT/USD")
        self.assertEqual(normalize_ws_symbol("USDT/USD"), "USDT/USD")
        self.assertEqual(normalize_ws_symbol("USDCUSD"), "USDC/USD")

    def test_format_rfc3339_micros(self):
        # 2026-10-12T00:00:00.358160Z -> epoch 1791763200.358160
        ts_str = format_rfc3339_micros(1791763200.358160)
        self.assertEqual(ts_str, "2026-10-12T00:00:00.358160Z")

    def test_transform_rest_trade_buy_market_and_sell_limit(self):
        # Synthetic Kraken REST trade entry:
        # [price, volume, time, side, orderType, misc, tradeId]
        raw_sell_limit = ["0.99889000", "100.50000000", 1791763200.358160, "s", "l", "", 84454466]
        rec1 = transform_rest_trade(raw_sell_limit, ws_symbol="USDT/USD", latency_ms=15)

        self.assertEqual(rec1["trade_timestamp"], 1791763200358)
        self.assertEqual(rec1["ts"], 1791763200358 + 15)
        self.assertEqual(rec1["symbol"], "USDT/USD")
        self.assertEqual(rec1["trade_id"], 84454466)
        self.assertEqual(rec1["side"], "sell")
        self.assertEqual(rec1["ord_type"], "limit")
        self.assertAlmostEqual(rec1["price"], 0.99889, places=8)
        self.assertAlmostEqual(rec1["qty"], 100.5, places=8)
        self.assertEqual(rec1["timestamp"], "2026-10-12T00:00:00.358160Z")

        raw_buy_market = ["0.99901000", "2500.00000000", 1791763210.999900, "b", "m", "", 84454467]
        rec2 = transform_rest_trade(raw_buy_market, ws_symbol="USDT/USD", latency_ms=15)

        self.assertEqual(rec2["trade_timestamp"], 1791763210999)
        self.assertEqual(rec2["ts"], 1791763210999 + 15)
        self.assertEqual(rec2["side"], "buy")
        self.assertEqual(rec2["ord_type"], "market")
        self.assertEqual(rec2["trade_id"], 84454467)
        self.assertAlmostEqual(rec2["price"], 0.99901, places=8)
        self.assertAlmostEqual(rec2["qty"], 2500.0, places=8)

    def test_write_daily_parquet_files_schema_and_sorting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            # Two synthetic trades for 2026-10-12 out of order
            t1 = transform_rest_trade(
                ["0.99890000", "50.0", 1791763205.100000, "b", "l", "", 1002],
                ws_symbol="USDT/USD",
                latency_ms=15,
            )
            t2 = transform_rest_trade(
                ["0.99885000", "25.0", 1791763201.200000, "s", "m", "", 1001],
                ws_symbol="USDT/USD",
                latency_ms=15,
            )

            written = write_daily_parquet_files(
                records_by_date={"2026-10-12": [t1, t2]},
                output_dir=out_dir,
                pair_folder="USDTUSD",
                overwrite=True,
            )

            self.assertEqual(len(written), 1)
            pq_file = out_dir / "USDTUSD-trades-2026-10-12.parquet"
            self.assertTrue(pq_file.exists())

            table = pq.read_table(pq_file)
            self.assertEqual(
                table.schema.names,
                [
                    "ts",
                    "trade_timestamp",
                    "symbol",
                    "trade_id",
                    "side",
                    "ord_type",
                    "price",
                    "qty",
                    "timestamp",
                ],
            )
            data = table.to_pydict()
            # Sorted by ts ASC, trade_id ASC -> trade 1001 first, then 1002
            self.assertEqual(data["trade_id"], [1001, 1002])
            self.assertEqual(data["ts"], [1791763201215, 1791763205115])
            self.assertEqual(data["trade_timestamp"], [1791763201200, 1791763205100])
            self.assertEqual(data["side"], ["sell", "buy"])
            self.assertEqual(data["ord_type"], ["market", "limit"])

    def test_download_and_save_pagination_and_boundary_handling(self):
        # Day range: 2026-10-12 to 2026-10-12
        # Start of 2026-10-12 UTC = 1791763200
        # End of 2026-10-12 UTC = 1791849600
        page1 = {
            "error": [],
            "result": {
                "USDTZUSD": [
                    # Trade just before midnight whose +15ms receipt lands in 2026-10-12 (1791763199.990 -> +15ms = 1791763200005)
                    ["0.99880000", "10.0", 1791763199.990000, "b", "l", "", 500],
                    # Normal trade inside 2026-10-12
                    ["0.99885000", "20.0", 1791763250.000000, "s", "m", "", 501],
                ],
                "last": "1791763250000000000",
            },
        }
        page2 = {
            "error": [],
            "result": {
                "USDTZUSD": [
                    # Duplicate trade 501 (should be deduplicated)
                    ["0.99885000", "20.0", 1791763250.000000, "s", "m", "", 501],
                    # Trade in next day 2026-10-13 (should stop pagination and not be included in 2026-10-12)
                    ["0.99890000", "30.0", 1791849605.000000, "b", "l", "", 502],
                ],
                "last": "1791849605000000000",
            },
        }

        fetch_mock = MagicMock(side_effect=[page1, page2])

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            written = download_and_save_kraken_trades(
                pair="USDTUSD",
                start_date="2026-10-12",
                end_date="2026-10-12",
                output_dir=out_dir,
                latency_ms=15,
                overwrite=True,
                fetch_fn=fetch_mock,
                sleep_sec=0.0,
            )

            self.assertEqual(len(written), 1)
            table = pq.read_table(out_dir / "USDTUSD-trades-2026-10-12.parquet")
            rows = table.to_pydict()
            self.assertEqual(rows["trade_id"], [500, 501])
            self.assertEqual(rows["ts"], [1791763200005, 1791763250015])


if __name__ == "__main__":
    unittest.main()
