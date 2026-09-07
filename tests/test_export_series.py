import os
import sys
import unittest
import tempfile
from unittest.mock import MagicMock, patch
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.export_series import (
    parse_args,
    export_series,
    get_market_open_times,
    rename_market_files
)


class TestExportSeries(unittest.TestCase):

    def test_parse_args_defaults(self):
        args = parse_args([])
        self.assertEqual(args.series_id, "btc-up-or-down-15m")
        self.assertEqual(args.after, "2026-08-07T00:00:00Z")
        self.assertEqual(args.before, "2026-09-07T00:00:00Z")
        self.assertEqual(args.data_dir, r"D:\Crypto\data\Polymarket\btc-up-or-down-15m")
        self.assertFalse(args.dry_run)
        self.assertFalse(args.rename_only)
        self.assertTrue(args.rename)

    def test_parse_args_custom(self):
        custom_args = [
            "--series-id", "eth-up-or-down-5m",
            "--after", "2026-01-01T00:00:00Z",
            "--before", "2026-01-02T00:00:00Z",
            "--data-dir", "test_data",
            "--dry-run",
            "--rename-only",
            "--no-rename"
        ]
        args = parse_args(custom_args)
        self.assertEqual(args.series_id, "eth-up-or-down-5m")
        self.assertEqual(args.after, "2026-01-01T00:00:00Z")
        self.assertEqual(args.before, "2026-01-02T00:00:00Z")
        self.assertEqual(args.data_dir, "test_data")
        self.assertTrue(args.dry_run)
        self.assertTrue(args.rename_only)
        self.assertFalse(args.rename)

    def test_get_market_open_times_success(self):
        mock_client = MagicMock()
        m1 = MagicMock(id="market-1", open_time=1788681600000)
        m2 = MagicMock(id="market-2", open_time=1788682500000)
        mock_client.series.walk.return_value = [m1, m2]

        mapping = get_market_open_times(mock_client, "btc-up-or-down-15m", "after", "before")
        self.assertEqual(mapping, {
            "market-1": 1788681600,
            "market-2": 1788682500
        })

    def test_get_market_open_times_missing_open_time_raises(self):
        mock_client = MagicMock()
        m1 = MagicMock(id="market-1", open_time=None)
        mock_client.series.walk.return_value = [m1]

        with self.assertRaises(ValueError) as ctx:
            get_market_open_times(mock_client, "btc-up-or-down-15m", "after", "before")
        self.assertIn("missing open_time", str(ctx.exception))

    def test_rename_market_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            f1 = tmppath / "history-market-1-compact.parquet"
            f2 = tmppath / "history-market-2.parquet"
            f1.touch()
            f2.touch()

            mapping = {
                "market-1": 1788681600,
                "market-2": 1788682500
            }

            renamed = rename_market_files(tmppath, mapping)
            self.assertEqual(renamed, 2)
            self.assertFalse(f1.exists())
            self.assertFalse(f2.exists())
            self.assertTrue((tmppath / "1788681600.parquet").exists())
            self.assertTrue((tmppath / "1788682500.parquet").exists())

    def test_export_series_calls_download_series_and_renames(self):
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.ready = ["market-1", "market-2"]
        mock_result.pending = []
        mock_result.failed = []
        mock_result.rate_limited = []
        mock_result.rows_charged = 1000
        mock_result.events_charged = 1000
        mock_client.exports.download_series.return_value = mock_result

        m1 = MagicMock(id="market-1", open_time=1788681600000)
        m2 = MagicMock(id="market-2", open_time=1788682500000)
        mock_client.series.walk.return_value = [m1, m2]

        with patch("scripts.export_series.Path.mkdir") as mock_mkdir:
            with patch("scripts.export_series.rename_market_files") as mock_rename:
                mock_rename.return_value = 2
                result = export_series(
                    series_id="btc-up-or-down-15m",
                    after="2026-08-07T00:00:00Z",
                    before="2026-09-07T00:00:00Z",
                    data_dir=r"D:\Crypto\data\Polymarket\btc-up-or-down-15m",
                    dry_run=False,
                    client=mock_client,
                    rename_by_open_time=True
                )

                mock_client.exports.download_series.assert_called_once_with(
                    "btc-up-or-down-15m",
                    after="2026-08-07T00:00:00Z",
                    before="2026-09-07T00:00:00Z",
                    data_dir=r"D:\Crypto\data\Polymarket\btc-up-or-down-15m",
                    dry_run=False,
                    concurrency=4
                )
                mock_rename.assert_called_once()
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
