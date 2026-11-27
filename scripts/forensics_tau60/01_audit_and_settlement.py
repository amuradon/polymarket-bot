#!/usr/bin/env python3
"""
Step 01: 46-Day Dataset Audit & Exact Chainlink 60s TWAP Settlement Extraction.

Scans all `btc-updown-15m-<T_start>.parquet` markets alongside daily PolyBolt
`price_crypto` and `price_crypto_twap_60s` streams to produce:
  1. `/var/tmp/polymarket-forensics-tau60/markets_audit.parquet`: one row per 15m market
     with exact `twap_open`, `twap_close`, `outcome_up`, oracle tick completeness in
     `tau <= 60s`, verification of `mean(price_crypto_{last 60s}) - twap_close`,
     pre-tau60 realized 1s volatility, CLOB L2 completeness, and WS latency quantiles.
  2. `/var/tmp/polymarket-forensics-tau60/audit_summary.json`: aggregate forensic stats
     including outcome flip probabilities across remaining tau (60s, 45s, 30s, 15s, 5s).
"""

from __future__ import annotations

import bisect
import datetime as dt
import glob
import json
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

COLLECTOR_ROOT = Path(os.environ.get("COLLECTOR_DATA_DIR", "/data/polymarket/collector"))
WORK_DIR = Path(os.environ.get("FORENSICS_WORK_DIR", "/var/tmp/polymarket-forensics-tau60"))
DISCOVERY_END_DATE = "2026-11-12"
BOUNDARY_TWAP_MAX_OFFSET_MS = 10_000


def run_duckdb_parquet(sql: str, out_parquet: Path) -> None:
    """Executes a DuckDB COPY query writing to `out_parquet`."""
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    full_sql = f"SET threads=8; SET memory_limit='12GB'; COPY ({sql}) TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD);"
    subprocess.run(["duckdb", "-c", full_sql], check=True)


def week_bucket_label(utc_date: str) -> str:
    """Assigns each UTC date in [2026-10-12 .. 2026-11-26] to weekly cohorts W1..W7."""
    d = dt.date.fromisoformat(utc_date)
    anchor = dt.date(2026, 10, 12)  # Monday
    w_idx = max(0, (d - anchor).days // 7) + 1
    return f"W{w_idx}"


def main() -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    clob_dir = COLLECTOR_ROOT / "polymarket" / "btc-updown-15m"
    price_glob = str(COLLECTOR_ROOT / "polymarket" / "price_crypto" / "btcusd" / "*.parquet")
    twap_glob = str(COLLECTOR_ROOT / "polymarket" / "price_crypto_twap_60s" / "btcusd" / "*.parquet")

    market_files = sorted(glob.glob(str(clob_dir / "*" / "btc-updown-15m-*.parquet")))
    print(f"[01_audit] Found {len(market_files)} CLOB 15m market files.")

    # 1. Extract deduplicated monotonic PolyBolt price_crypto and twap_60s via DuckDB
    oracle_price_pq = WORK_DIR / "oracle_price_dedup.parquet"
    oracle_twap_pq = WORK_DIR / "oracle_twap_dedup.parquet"

    print("[01_audit] Deduplicating 46-day PolyBolt price.crypto & price.crypto.twap...")
    run_duckdb_parquet(
        f"""
        SELECT
            CAST(price_timestamp AS BIGINT) AS oracle_ts,
            MIN(CAST(ts AS BIGINT)) AS receipt_ts,
            FIRST(COALESCE(TRY_CAST(full_accuracy_price AS DOUBLE), price) ORDER BY ts ASC) AS px
        FROM read_parquet('{price_glob}')
        WHERE price_timestamp IS NOT NULL AND price > 0
        GROUP BY price_timestamp
        ORDER BY oracle_ts ASC
        """,
        oracle_price_pq,
    )
    run_duckdb_parquet(
        f"""
        SELECT
            CAST(twap_timestamp AS BIGINT) AS oracle_ts,
            MIN(CAST(ts AS BIGINT)) AS receipt_ts,
            FIRST(COALESCE(TRY_CAST(full_accuracy_twap_price AS DOUBLE), twap_price) ORDER BY ts ASC) AS twap_px
        FROM read_parquet('{twap_glob}')
        WHERE twap_timestamp IS NOT NULL AND twap_price > 0
        GROUP BY twap_timestamp
        ORDER BY oracle_ts ASC
        """,
        oracle_twap_pq,
    )

    # 2. Extract per-file CLOB summary stats in a single parallel DuckDB scan over all 4,378 files
    clob_stats_pq = WORK_DIR / "clob_file_stats.parquet"
    print("[01_audit] Scanning 4,378 CLOB Parquet files for L2 coverage, trade counts & WS latency...")
    run_duckdb_parquet(
        f"""
        WITH src AS (
            SELECT
                regexp_extract(filename, 'btc-updown-15m-([0-9]+)\\.parquet', 1)::BIGINT AS t_start_sec,
                CAST(ts AS BIGINT) AS ts,
                CAST(server_ts AS BIGINT) AS server_ts,
                event_code,
                is_up
            FROM read_parquet('{clob_dir}/*/btc-updown-15m-*.parquet', filename=true)
            WHERE event_code > 0
        )
        SELECT
            t_start_sec,
            COUNT(*) FILTER (
                WHERE ts >= (t_start_sec + 840) * 1000 AND ts < (t_start_sec + 900) * 1000
                  AND event_code IN (1, 2, 4)
            ) AS clob_events_tau60,
            COUNT(*) FILTER (
                WHERE ts >= (t_start_sec + 840) * 1000 AND ts < (t_start_sec + 900) * 1000
                  AND event_code = 3
            ) AS trade_events_tau60,
            BOOL_OR(
                event_code = 1 AND is_up = true AND ts <= (t_start_sec + 825) * 1000
            ) AS has_pre75_up_book,
            BOOL_OR(
                event_code = 1 AND is_up = false AND ts <= (t_start_sec + 825) * 1000
            ) AS has_pre75_down_book,
            QUANTILE_CONT(ts - server_ts, 0.50) FILTER (
                WHERE ts >= (t_start_sec + 840) * 1000 AND ts < (t_start_sec + 900) * 1000
                  AND server_ts > 0 AND ts >= server_ts AND (ts - server_ts) < 60000
            ) AS ws_lat_p50_ms,
            QUANTILE_CONT(ts - server_ts, 0.95) FILTER (
                WHERE ts >= (t_start_sec + 840) * 1000 AND ts < (t_start_sec + 900) * 1000
                  AND server_ts > 0 AND ts >= server_ts AND (ts - server_ts) < 60000
            ) AS ws_lat_p95_ms,
            QUANTILE_CONT(ts - server_ts, 0.99) FILTER (
                WHERE ts >= (t_start_sec + 840) * 1000 AND ts < (t_start_sec + 900) * 1000
                  AND server_ts > 0 AND ts >= server_ts AND (ts - server_ts) < 60000
            ) AS ws_lat_p99_ms
        FROM src
        GROUP BY t_start_sec
        ORDER BY t_start_sec ASC
        """,
        clob_stats_pq,
    )

    # Load deduplicated PolyBolt arrays into memory (~4M rows -> ~90 MB RAM, instant binary search)
    p_tbl = pq.read_table(oracle_price_pq)
    p_ots: List[int] = p_tbl["oracle_ts"].to_pylist()
    p_rts: List[int] = p_tbl["receipt_ts"].to_pylist()
    p_px: List[float] = p_tbl["px"].to_pylist()

    tw_tbl = pq.read_table(oracle_twap_pq)
    tw_ots: List[int] = tw_tbl["oracle_ts"].to_pylist()
    tw_rts: List[int] = tw_tbl["receipt_ts"].to_pylist()
    tw_px: List[float] = tw_tbl["twap_px"].to_pylist()

    cs_tbl = pq.read_table(clob_stats_pq)
    clob_map: Dict[int, dict] = {}
    for row in cs_tbl.to_pylist():
        clob_map[int(row["t_start_sec"])] = row

    def find_boundary_twap(boundary_ms: int) -> Tuple[Optional[float], Optional[int]]:
        idx = bisect.bisect_left(tw_ots, boundary_ms)
        if idx < len(tw_ots) and tw_ots[idx] <= boundary_ms + BOUNDARY_TWAP_MAX_OFFSET_MS:
            return tw_px[idx], tw_ots[idx] - boundary_ms
        return None, None

    records: List[dict] = []
    tau_checkpoints = [60, 45, 30, 20, 15, 10, 5]
    flip_stats = {tau: {"total": 0, "spot_disagrees_with_settlement": 0, "banked_disagrees_with_settlement": 0} for tau in tau_checkpoints}
    twap_formula_abs_diffs: List[float] = []

    for fpath in market_files:
        stem = Path(fpath).stem
        t_start_sec = int(stem.split("-")[-1])
        t_end_sec = t_start_sec + 900
        t_start_ms = t_start_sec * 1000
        t_end_ms = t_end_sec * 1000

        utc_dt = dt.datetime.fromtimestamp(t_start_sec, tz=dt.timezone.utc)
        utc_date = utc_dt.strftime("%Y-%m-%d")
        week_lbl = week_bucket_label(utc_date)
        split = "DISCOVERY" if utc_date <= DISCOVERY_END_DATE else "OOS"

        twap_open, open_off_ms = find_boundary_twap(t_start_ms)
        twap_close, close_off_ms = find_boundary_twap(t_end_ms)

        # Slice oracle 1s prices in (t_end_ms - 60_000, t_end_ms]
        lo60 = bisect.bisect_right(p_ots, t_end_ms - 60_000)
        hi60 = bisect.bisect_right(p_ots, t_end_ms)
        n_ticks_60 = hi60 - lo60
        mean_60 = (sum(p_px[lo60:hi60]) / n_ticks_60) if n_ticks_60 > 0 else float("nan")

        # Causal pre-window 1s realized volatility over [t_start + 600s, t_start + 840s] (4 minutes prior to tau=60s)
        v_lo = bisect.bisect_left(p_ots, (t_start_sec + 600) * 1000)
        v_hi = bisect.bisect_right(p_ots, (t_start_sec + 840) * 1000)
        sq_diffs: List[float] = []
        for k in range(v_lo + 1, v_hi):
            dt_s = (p_ots[k] - p_ots[k - 1]) / 1000.0
            if 0.5 <= dt_s <= 5.0:
                dp = p_px[k] - p_px[k - 1]
                sq_diffs.append((dp * dp) / dt_s)
        sigma_1s_usd = math.sqrt(sum(sq_diffs) / len(sq_diffs)) if len(sq_diffs) >= 20 else float("nan")

        cs = clob_map.get(t_start_sec, {})
        clob_ev_60 = int(cs.get("clob_events_tau60") or 0)
        trade_ev_60 = int(cs.get("trade_events_tau60") or 0)
        has_up_bk = bool(cs.get("has_pre75_up_book"))
        has_dn_bk = bool(cs.get("has_pre75_down_book"))

        is_clean = (
            twap_open is not None
            and twap_close is not None
            and n_ticks_60 >= 50
            and clob_ev_60 >= 20
            and (has_up_bk or has_dn_bk)
            and math.isfinite(sigma_1s_usd)
            and sigma_1s_usd > 0.0
        )

        outcome_up: Optional[bool] = None
        if twap_open is not None and twap_close is not None:
            outcome_up = twap_close >= twap_open
            if n_ticks_60 == 60 and close_off_ms == 0:
                twap_formula_abs_diffs.append(abs(mean_60 - twap_close))

        # Check outcome flip probabilities at checkpoints inside tau <= 60s on clean markets
        if is_clean and outcome_up is not None and twap_open is not None:
            for tau in tau_checkpoints:
                chk_ms = t_end_ms - tau * 1000
                # Latest oracle tick strictly available by receipt_ts <= chk_ms
                r_idx = bisect.bisect_right(p_rts, chk_ms) - 1
                if r_idx >= 0 and p_ots[r_idx] >= chk_ms - 10_000:
                    cur_spot = p_px[r_idx]
                    spot_up = cur_spot >= twap_open
                    # Banked ticks in (t_end_ms - 60_000, p_ots[r_idx]] + constant projection of cur_spot over remaining tau
                    b_lo = bisect.bisect_right(p_ots, t_end_ms - 60_000)
                    b_hi = r_idx + 1
                    banked_n = max(0, b_hi - b_lo)
                    banked_sum = sum(p_px[b_lo:b_hi]) if banked_n > 0 else 0.0
                    rem_n = max(0, 60 - banked_n)
                    proj_twap = (banked_sum + rem_n * cur_spot) / 60.0
                    proj_up = proj_twap >= twap_open

                    flip_stats[tau]["total"] += 1
                    if spot_up != outcome_up:
                        flip_stats[tau]["spot_disagrees_with_settlement"] += 1
                    if proj_up != outcome_up:
                        flip_stats[tau]["banked_disagrees_with_settlement"] += 1

        records.append(
            {
                "t_start_sec": t_start_sec,
                "t_end_sec": t_end_sec,
                "utc_date": utc_date,
                "utc_hour": utc_dt.hour,
                "week_label": week_lbl,
                "split": split,
                "parquet_path": fpath,
                "twap_open": float(twap_open) if twap_open is not None else float("nan"),
                "twap_close": float(twap_close) if twap_close is not None else float("nan"),
                "open_offset_ms": int(open_off_ms) if open_off_ms is not None else -1,
                "close_offset_ms": int(close_off_ms) if close_off_ms is not None else -1,
                "outcome_up": outcome_up,
                "n_oracle_ticks_60s": n_ticks_60,
                "oracle_mean_60s": mean_60,
                "sigma_1s_usd": sigma_1s_usd,
                "clob_events_tau60": clob_ev_60,
                "trade_events_tau60": trade_ev_60,
                "has_pre75_up_book": has_up_bk,
                "has_pre75_down_book": has_dn_bk,
                "ws_lat_p50_ms": float(cs.get("ws_lat_p50_ms") or float("nan")),
                "ws_lat_p95_ms": float(cs.get("ws_lat_p95_ms") or float("nan")),
                "ws_lat_p99_ms": float(cs.get("ws_lat_p99_ms") or float("nan")),
                "is_clean": is_clean,
            }
        )

    out_audit_pq = WORK_DIR / "markets_audit.parquet"
    pq.write_table(pa.Table.from_pylist(records), out_audit_pq, compression="zstd")

    clean_recs = [r for r in records if r["is_clean"]]
    disc_recs = [r for r in clean_recs if r["split"] == "DISCOVERY"]
    oos_recs = [r for r in clean_recs if r["split"] == "OOS"]
    up_wins = sum(1 for r in clean_recs if r["outcome_up"] is True)

    twap_Formula_s = sorted(twap_formula_abs_diffs)
    p50_diff = twap_Formula_s[len(twap_Formula_s) // 2] if twap_Formula_s else float("nan")
    p99_diff = twap_Formula_s[int(0.99 * len(twap_Formula_s))] if twap_Formula_s else float("nan")

    # Oracle receipt latency quantiles
    pb_lags = sorted(
        p_rts[i] - p_ots[i]
        for i in range(len(p_ots))
        if 0 <= (p_rts[i] - p_ots[i]) <= 30_000
    )
    tw_lags = sorted(
        tw_rts[i] - tw_ots[i]
        for i in range(len(tw_ots))
        if 0 <= (tw_rts[i] - tw_ots[i]) <= 30_000
    )

    flip_summary = {}
    for tau, st in flip_stats.items():
        tot = max(1, st["total"])
        flip_summary[str(tau)] = {
            "total_markets": st["total"],
            "spot_flip_rate": round(st["spot_disagrees_with_settlement"] / tot, 5),
            "banked_twap_proj_flip_rate": round(st["banked_disagrees_with_settlement"] / tot, 5),
        }

    summary = {
        "total_market_files": len(records),
        "clean_markets": len(clean_recs),
        "excluded_markets": len(records) - len(clean_recs),
        "discovery_clean_markets": len(disc_recs),
        "oos_clean_markets": len(oos_recs),
        "up_win_rate_clean": round(up_wins / max(1, len(clean_recs)), 5),
        "oracle_60s_arithmetic_mean_vs_twap_close_abs_diff_usd": {
            "n_exact_60_tick_windows": len(twap_Formula_s),
            "p50_usd": round(p50_diff, 6),
            "p99_usd": round(p99_diff, 6),
        },
        "polybolt_price_receipt_lag_ms": {
            "p50": pb_lags[len(pb_lags) // 2] if pb_lags else None,
            "p95": pb_lags[int(0.95 * len(pb_lags))] if pb_lags else None,
            "p99": pb_lags[int(0.99 * len(pb_lags))] if pb_lags else None,
        },
        "polybolt_twap_receipt_lag_ms": {
            "p50": tw_lags[len(tw_lags) // 2] if tw_lags else None,
            "p95": tw_lags[int(0.95 * len(tw_lags))] if tw_lags else None,
            "p99": tw_lags[int(0.99 * len(tw_lags))] if tw_lags else None,
        },
        "flip_rates_by_tau_sec": flip_summary,
    }

    summary_path = WORK_DIR / "audit_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
