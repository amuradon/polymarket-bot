import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.download_binance_orderbook import (
    parse_args,
    download_orderbook,
)


class TestDownloadBinanceOrderbook(unittest.TestCase):

    def test_parse_args_defaults(self):
        args = parse_args([])
        self.assertEqual(args.symbol, "BTCUSDT")
        self.assertEqual(args.exchange, "binance_futures")
        self.assertEqual(args.start_date, "2026-08-07")
        self.assertEqual(args.end_date, "2026-09-06")
        self.assertEqual(
            args.data_dir,
            r"D:\Crypto\data\Polymarket\Binance\futures\BTCUSDT\orderBook",
        )
        self.assertEqual(args.layout, "mirror")
        self.assertFalse(args.dry_run)
        self.assertIsNone(args.max_workers)

    def test_parse_args_custom(self):
        custom_args = [
            "--symbol", "ETHUSDT",
            "--exchange", "binance_spot",
            "--start-date", "2026-01-01",
            "--end-date", "2026-01-02",
            "--data-dir", "custom_dir",
            "--layout", "hive",
            "--max-workers", "4",
            "--dry-run",
            "--api-key", "test_key",
        ]
        args = parse_args(custom_args)
        self.assertEqual(args.symbol, "ETHUSDT")
        self.assertEqual(args.exchange, "binance_spot")
        self.assertEqual(args.start_date, "2026-01-01")
        self.assertEqual(args.end_date, "2026-01-02")
        self.assertEqual(args.data_dir, "custom_dir")
        self.assertEqual(args.layout, "hive")
        self.assertEqual(args.max_workers, 4)
        self.assertTrue(args.dry_run)
        self.assertEqual(args.api_key, "test_key")

    def test_download_orderbook_date_validation(self):
        with self.assertRaises(ValueError) as ctx:
            download_orderbook(
                start_date="2026-09-07",
                end_date="2026-08-07",
            )
        self.assertIn("must not be after end_date", str(ctx.exception))

    def test_download_orderbook_calls_download_bulk(self):
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.failed = []
        mock_result.files = 744
        mock_result.bytes = 1000000
        mock_client.download_bulk.return_value = mock_result

        with patch("scripts.download_binance_orderbook.Path.mkdir"):
            result = download_orderbook(
                symbol="BTCUSDT",
                exchange="binance_futures",
                start_date="2026-08-07",
                end_date="2026-09-06",
                data_dir=r"D:\Crypto\data\Polymarket\Binance\futures\BTCUSDT\orderBook",
                layout="mirror",
                max_workers=8,
                dry_run=False,
                client=mock_client,
            )

            mock_client.download_bulk.assert_called_once_with(
                exchange="binance_futures",
                data_type="orderbook",
                start="2026-08-07",
                end="2026-09-06",
                symbols=["BTCUSDT"],
                dest=r"D:\Crypto\data\Polymarket\Binance\futures\BTCUSDT\orderBook",
                layout="mirror",
                confirm=True,
                decompress=True,
                dry_run=False,
                max_workers=8,
            )
            self.assertEqual(result, mock_result)

    def test_download_orderbook_dry_run(self):
        mock_client = MagicMock()
        mock_result = MagicMock(dry_run=True, planned_files=744, failed=[])
        mock_client.download_bulk.return_value = mock_result

        result = download_orderbook(
            dry_run=True,
            client=mock_client,
        )

        mock_client.download_bulk.assert_called_once()
        call_kwargs = mock_client.download_bulk.call_args[1]
        self.assertTrue(call_kwargs["dry_run"])
        self.assertEqual(result, mock_result)

    def test_download_orderbook_failure_raises_error(self):
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.failed = [{"key": "bad_key", "error": "404"}]
        mock_client.download_bulk.return_value = mock_result

        with patch("scripts.download_binance_orderbook.Path.mkdir"):
            with self.assertRaises(RuntimeError) as ctx:
                download_orderbook(
                    client=mock_client,
                )
            self.assertIn("Failed to download 1 files", str(ctx.exception))

    def test_download_orderbook_initializes_client_if_none(self):
        with patch("scripts.download_binance_orderbook.chd.CryptoHFTDataClient") as mock_chd_cls:
            mock_client_instance = MagicMock()
            mock_client_instance.download_bulk.return_value = MagicMock(failed=[])
            mock_chd_cls.return_value = mock_client_instance

            with patch("scripts.download_binance_orderbook.Path.mkdir"):
                download_orderbook(api_key="secret123", dry_run=False)

            mock_chd_cls.assert_called_once_with(api_key="secret123")


if __name__ == "__main__":
    unittest.main()
