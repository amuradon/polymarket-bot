import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.export_series import parse_args, export_series


class TestExportSeries(unittest.TestCase):

    def test_parse_args_defaults(self):
        args = parse_args([])
        self.assertEqual(args.series_id, "btc-up-or-down-15m")
        self.assertEqual(args.after, "2026-08-07T00:00:00Z")
        self.assertEqual(args.before, "2026-09-07T00:00:00Z")
        self.assertEqual(args.data_dir, r"D:\Crypto\data\Polymarket\btc-up-or-down-15m")
        self.assertFalse(args.dry_run)

    def test_parse_args_custom(self):
        custom_args = [
            "--series-id", "eth-up-or-down-5m",
            "--after", "2026-01-01T00:00:00Z",
            "--before", "2026-01-02T00:00:00Z",
            "--data-dir", "test_data",
            "--dry-run"
        ]
        args = parse_args(custom_args)
        self.assertEqual(args.series_id, "eth-up-or-down-5m")
        self.assertEqual(args.after, "2026-01-01T00:00:00Z")
        self.assertEqual(args.before, "2026-01-02T00:00:00Z")
        self.assertEqual(args.data_dir, "test_data")
        self.assertTrue(args.dry_run)

    def test_export_series_calls_download_series(self):
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.ready = ["market-1", "market-2"]
        mock_result.pending = []
        mock_result.failed = []
        mock_result.rate_limited = []
        mock_result.rows_charged = 1000
        mock_result.events_charged = 1000
        mock_client.exports.download_series.return_value = mock_result

        with patch("scripts.export_series.Path.mkdir") as mock_mkdir:
            result = export_series(
                series_id="btc-up-or-down-15m",
                after="2026-08-07T00:00:00Z",
                before="2026-09-07T00:00:00Z",
                data_dir=r"D:\Crypto\data\Polymarket\btc-up-or-down-15m",
                dry_run=False,
                client=mock_client
            )

            mock_client.exports.download_series.assert_called_once_with(
                "btc-up-or-down-15m",
                after="2026-08-07T00:00:00Z",
                before="2026-09-07T00:00:00Z",
                data_dir=r"D:\Crypto\data\Polymarket\btc-up-or-down-15m",
                dry_run=False,
                concurrency=4
            )
            self.assertEqual(result, mock_result)

    def test_export_series_missing_api_key_raises_error(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch("scripts.export_series.MarketLens", side_effect=ValueError("No API key")):
                with self.assertRaises(ValueError):
                    export_series(
                        series_id="btc-up-or-down-15m",
                        after="2026-08-07T00:00:00Z",
                        before="2026-09-07T00:00:00Z",
                        data_dir=r"D:\Crypto\data\Polymarket\btc-up-or-down-15m",
                        client=None
                    )


if __name__ == "__main__":
    unittest.main()
