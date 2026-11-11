import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

import pyarrow.parquet as pq

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.download_kraken_trades import (
    DailyColumnBuffer,
    compute_rate_limit_delay,
    download_kraken_spot_trades,
    format_rfc3339_micros,
    normalize_symbol_folder,
    normalize_ws_symbol,
    parse_args,
    write_daily_parquet_file,
)


class TestDownloadKrakenTrades(unittest.TestCase):

    def test_parse_args(self):
        args = parse_args(["--symbol", "USDTUSD", "--from", "2026-10-12", "--to", "2026-11-03"])
        self.assertEqual(args.symbol, "USDTUSD")
        self.assertEqual(args.from_date, "2026-10-12")
        self.assertEqual(args.to_date, "2026-11-03")
        self.assertEqual(
            Path(args.root_dir),
            Path(r"D:\Crypto\data\Polymarket\collector\kraken\spot"),
        )
        self.assertEqual(args.latency_ms, 15)

    def test_normalize_helpers(self):
        self.assertEqual(normalize_symbol_folder("USDT/USD"), "USDTUSD")
        self.assertEqual(normalize_symbol_folder("usdtusd"), "USDTUSD")
        self.assertEqual(normalize_ws_symbol("USDTUSD"), "USDT/USD")
        self.assertEqual(normalize_ws_symbol("USDT/USD"), "USDT/USD")
        self.assertEqual(normalize_ws_symbol("XBTUSD"), "BTC/USD")
        self.assertEqual(normalize_ws_symbol("USDCUSD"), "USDC/USD")

    def test_format_rfc3339_micros(self):
        # 2026-10-12T00:00:00.358160Z -> epoch_us = 1791763200358160
        ts_str = format_rfc3339_micros(1791763200358160)
        self.assertEqual(ts_str, "2026-10-12T00:00:00.358160Z")

    def test_compute_rate_limit_delay(self):
        self.assertAlmostEqual(compute_rate_limit_delay({}, default_delay=1.0), 1.0)
        self.assertAlmostEqual(compute_rate_limit_delay({"Retry-After": "3.5"}, default_delay=1.0), 3.5)
        self.assertAlmostEqual(
            compute_rate_limit_delay({"X-RateLimit-Remaining": "1"}, default_delay=1.0),
            2.0,
        )

    def test_write_daily_parquet_file_schema_and_sorting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trades_dir = Path(tmpdir)
            buf = DailyColumnBuffer()
            # Two synthetic trades for 2026-10-12 out of order
            buf.append(
                ts=1791763205115,
                trade_timestamp=1791763205100,
                trade_id=1002,
                side="buy",
                ord_type="limit",
                price=0.99890,
                qty=50.0,
                timestamp="2026-10-12T00:00:05.100000Z",
            )
            buf.append(
                ts=1791763201215,
                trade_timestamp=1791763201200,
                trade_id=1001,
                side="sell",
                ord_type="market",
                price=0.99885,
                qty=25.0,
                timestamp="2026-10-12T00:00:01.200000Z",
            )

            pq_file = write_daily_parquet_file(
                date_str="2026-10-12",
                buffer=buf,
                trades_dir=trades_dir,
                symbol_folder="USDTUSD",
                ws_symbol="USDT/USD",
            )

            self.assertTrue(pq_file.exists())
            self.assertEqual(pq_file, trades_dir / "USDTUSD-trades-2026-10-12.parquet")

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
            self.assertEqual(data["symbol"], ["USDT/USD", "USDT/USD"])
            self.assertEqual(data["side"], ["sell", "buy"])
            self.assertEqual(data["ord_type"], ["market", "limit"])

    @patch("scripts.download_kraken_trades.fetch_kraken_trades_page")
    def test_download_kraken_spot_trades_pagination_and_boundary_handling(self, fetch_mock: MagicMock):
        # Day range: 2026-10-12 to 2026-10-12
        # Start of 2026-10-12 UTC = 1791763200
        # End of 2026-10-12 UTC = 1791849600
        page1 = (
            {
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
            },
            0.0,
        )
        page2 = (
            {
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
            },
            0.0,
        )
        fetch_mock.side_effect = [page1, page2]

        with tempfile.TemporaryDirectory() as tmpdir:
            root_dir = Path(tmpdir)
            written = download_kraken_spot_trades(
                symbol="USDTUSD",
                from_date="2026-10-12",
                to_date="2026-10-12",
                root_dir=root_dir,
                latency_ms=15,
                base_delay=0.0,
            )

            self.assertEqual(len(written), 1)
            expected_path = root_dir / "USDTUSD" / "trades" / "USDTUSD-trades-2026-10-12.parquet"
            self.assertEqual(written[0], expected_path)
            table = pq.read_table(expected_path)
            rows = table.to_pydict()
            self.assertEqual(rows["trade_id"], [500, 501])
            self.assertEqual(rows["ts"], [1791763200005, 1791763250015])


if __name__ == "__main__":
    unittest.main()
