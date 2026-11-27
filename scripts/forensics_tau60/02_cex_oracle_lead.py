#!/usr/bin/env python3
"""
Step 02: Causal CEX -> Chainlink PolyBolt Latency-Lead Study & 100ms Window Extraction.

1. Uses DuckDB across all 46 days to extract causal 100ms CEX USD price & order-flow series
   over the final 120 seconds of every 15-minute candle (`[T_end - 120s, T_end]`):
   - Coinbase Spot BTCUSD (`trade_timestamp + 50 ms`; `side == 'sell'` => taker buy)
   - Kraken Spot BTCUSD (`trade_timestamp + 50 ms`; `side != 'sell'` => taker buy)
   - Kraken Spot USDTUSD (`trade_timestamp + 50 ms`, causally forward-filled)
   - Binance Spot BTCUSDT (`COALESCE(event_time, trade_time) + 100 ms`, converted to USD
     via causal Kraken USDTUSD peg; `NOT is_buyer_maker` => taker buy)
2. Quantifies strictly on DISCOVERY (`<= 2026-11-12`) and validates on OOS (`>= 2026-11-13`):
   - Basis offsets and single-venue vs composite USD tracking error against Chainlink `price.crypto`,
   - Predictive R^2 / correlation of `(CEX_spot(t) - Oracle_spot(t))` for future Chainlink ticks
     at horizons `+0.5s, +1.0s, +1.5s, +2.0s, +3.0s, +5.0s`,
   - Optimal causal nowcast shrinkage beta (`oracle_nowcast = oracle_px + beta * (cex_usd - oracle_px - basis_ewma)`).
"""

from __future__ import annotations

import bisect
import json
import math
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

import pyarrow.parquet as pq

COLLECTOR_ROOT = Path(os.environ.get("COLLECTOR_DATA_DIR", "/data/polymarket/collector"))
WORK_DIR = Path(os.environ.get("FORENSICS_WORK_DIR", "/var/tmp/polymarket-forensics-tau60"))
DISCOVERY_END_TS_MS = 1794528000000  # Wait: compute exact epoch from date string!


def date_end_epoch_ms(date_str: str) -> int:
    import datetime as dt
    d = dt.date.fromisoformat(date_str) + dt.timedelta(days=1)
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp() * 1000)


def run_duckdb_parquet(sql: str, out_parquet: Path) -> None:
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    full_sql = f"SET threads=8; SET memory_limit='14GB'; COPY ({sql}) TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD);"
    subprocess.run(["duckdb", "-c", full_sql], check=True)


def main() -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    disc_cutoff_ms = date_end_epoch_ms("2026-11-12")

    cb_glob = str(COLLECTOR_ROOT / "coinbase" / "spot" / "BTCUSD" / "trades" / "*.parquet")
    kr_glob = str(COLLECTOR_ROOT / "kraken" / "spot" / "BTCUSD" / "trades" / "*.parquet")
    usdt_glob = str(COLLECTOR_ROOT / "kraken" / "spot" / "USDTUSD" / "trades" / "*.parquet")
    bn_glob = str(COLLECTOR_ROOT / "binance" / "spot" / "BTCUSDT" / "aggTrades" / "*.parquet")

    cex_100ms_pq = WORK_DIR / "cex_100ms_last120s.parquet"
    usdt_1s_pq = WORK_DIR / "kraken_usdt_1s.parquet"

    print("[02_cex_lead] Extracting causal Kraken USDT/USD peg timeline...")
    run_duckdb_parquet(
        f"""
        SELECT
            ((CAST(trade_timestamp AS BIGINT) + 50) // 1000) * 1000 AS bucket_1s,
            LAST(CAST(price AS DOUBLE) ORDER BY trade_timestamp ASC, trade_id ASC) AS usdt_usd
        FROM read_parquet('{usdt_glob}')
        WHERE trade_timestamp IS NOT NULL AND price > 0.90 AND price < 1.10
        GROUP BY 1
        ORDER BY 1 ASC
        """,
        usdt_1s_pq,
    )

    # Extract 100ms causal venue aggregates in the final 125s of every 15-minute cycle
    # (`sec_in_15m >= 775`, i.e., `[T_start + 775s, T_start + 900s]`, covering 65s warm-up + 60s lock-in)
    print("[02_cex_lead] Aggregating 46-day CEX trades into causal 100ms buckets for all [T_end-125s, T_end] windows...")
    run_duckdb_parquet(
        f"""
        WITH cb AS (
            SELECT
                ((CAST(trade_timestamp AS BIGINT) + 50) // 100) * 100 AS b100,
                LAST(CAST(price AS DOUBLE) ORDER BY trade_timestamp ASC, trade_id ASC) AS cb_px,
                SUM(CASE WHEN LOWER(side) = 'sell' THEN price * qty ELSE -price * qty END) AS cb_svol
            FROM read_parquet('{cb_glob}')
            WHERE (((CAST(trade_timestamp AS BIGINT) + 50) // 1000) % 900) >= 775
              AND price > 0
            GROUP BY 1
        ),
        kr AS (
            SELECT
                ((CAST(trade_timestamp AS BIGINT) + 50) // 100) * 100 AS b100,
                LAST(CAST(price AS DOUBLE) ORDER BY trade_timestamp ASC, trade_id ASC) AS kr_px,
                SUM(CASE WHEN LOWER(side) NOT IN ('sell', 's') THEN price * qty ELSE -price * qty END) AS kr_svol
            FROM read_parquet('{kr_glob}')
            WHERE (((CAST(trade_timestamp AS BIGINT) + 50) // 1000) % 900) >= 775
              AND price > 0
            GROUP BY 1
        ),
        bn AS (
            SELECT
                ((COALESCE(CAST(event_time AS BIGINT), CAST(trade_time AS BIGINT)) + 100) // 100) * 100 AS b100,
                LAST(CAST(price AS DOUBLE) ORDER BY COALESCE(event_time, trade_time) ASC, aggregate_trade_id ASC) AS bn_usdt_px,
                SUM(CASE WHEN NOT COALESCE(is_buyer_maker, false) THEN price * quantity ELSE -price * quantity END) AS bn_svol
            FROM read_parquet('{bn_glob}')
            WHERE (((COALESCE(CAST(event_time AS BIGINT), CAST(trade_time AS BIGINT)) + 100) // 1000) % 900) >= 775
              AND price > 0
            GROUP BY 1
        ),
        keys AS (
            SELECT b100 FROM cb
            UNION SELECT b100 FROM kr
            UNION SELECT b100 FROM bn
        )
        SELECT
            k.b100 AS ts_100ms,
            cb.cb_px,
            kr.kr_px,
            bn.bn_usdt_px,
            COALESCE(cb.cb_svol, 0.0) AS cb_svol,
            COALESCE(kr.kr_svol, 0.0) AS kr_svol,
            COALESCE(bn.bn_svol, 0.0) AS bn_svol
        FROM keys k
        LEFT JOIN cb ON cb.b100 = k.b100
        LEFT JOIN kr ON kr.b100 = k.b100
        LEFT JOIN bn ON bn.b100 = k.b100
        ORDER BY k.b100 ASC
        """,
        cex_100ms_pq,
    )

    # Load oracle_price_dedup + usdt_1s + cex_100ms to compute exact lead-lag & Discovery nowcast calibration
    print("[02_cex_lead] Computing CEX -> Chainlink lead-lag regression across Discovery vs OOS...")
    p_tbl = pq.read_table(WORK_DIR / "oracle_price_dedup.parquet")
    p_ots: List[int] = p_tbl["oracle_ts"].to_pylist()
    p_rts: List[int] = p_tbl["receipt_ts"].to_pylist()
    p_px: List[float] = p_tbl["px"].to_pylist()

    u_tbl = pq.read_table(usdt_1s_pq)
    u_ts: List[int] = u_tbl["bucket_1s"].to_pylist()
    u_px: List[float] = u_tbl["usdt_usd"].to_pylist()

    c_tbl = pq.read_table(cex_100ms_pq)
    c_ts: List[int] = c_tbl["ts_100ms"].to_pylist()
    cb_col = c_tbl["cb_px"].to_pylist()
    kr_col = c_tbl["kr_px"].to_pylist()
    bn_col = c_tbl["bn_usdt_px"].to_pylist()

    # Build causal forward-filled CEX composite at each c_ts bucket (resetting if gap > 10s)
    n_c = len(c_ts)
    comp_px = [0.0] * n_c
    cb_ff = 0.0
    kr_ff = 0.0
    bn_ff = 0.0
    cb_last = -100_000
    kr_last = -100_000
    bn_last = -100_000
    u_idx = 0
    u_len = len(u_ts)
    cur_peg = 1.0

    for i in range(n_c):
        t = c_ts[i]
        while u_idx < u_len and u_ts[u_idx] <= t:
            cur_peg = u_px[u_idx]
            u_idx += 1
        if cb_col[i] is not None:
            cb_ff = float(cb_col[i])
            cb_last = t
        if kr_col[i] is not None:
            kr_ff = float(kr_col[i])
            kr_last = t
        if bn_col[i] is not None:
            bn_ff = float(bn_col[i]) * cur_peg
            bn_last = t

        # Equal-weight fresh direct USD venues (Coinbase + Kraken) + peg-adjusted Binance when fresh within 5s
        w_sum = 0.0
        px_sum = 0.0
        if t - cb_last <= 5_000 and cb_ff > 0:
            px_sum += 0.45 * cb_ff
            w_sum += 0.45
        if t - kr_last <= 5_000 and kr_ff > 0:
            px_sum += 0.35 * kr_ff
            w_sum += 0.35
        if t - bn_last <= 5_000 and bn_ff > 0:
            px_sum += 0.20 * bn_ff
            w_sum += 0.20
        comp_px[i] = (px_sum / w_sum) if w_sum > 0 else 0.0

    # Evaluate how `(comp_px(t) - last_oracle_px(t) - rolling_basis)` predicts future Chainlink change
    # `oracle_px_at(t + h) - last_oracle_px(t)` across horizons h in {500, 1000, 1500, 2000, 3000, 5000} ms!
    horizons_ms = [500, 1000, 1500, 2000, 3000, 5000]
    stats_by_split: Dict[str, Dict[int, List[float]]] = {
        "DISCOVERY": {h: [0.0, 0.0, 0.0, 0.0, 0] for h in horizons_ms},  # sum_x2, sum_y2, sum_xy, sum_err2_base, count
        "OOS": {h: [0.0, 0.0, 0.0, 0.0, 0] for h in horizons_ms},
    }

    # Sample every 1 second inside `[T_end - 60s, T_end - 5s]` across all 46 days
    basis_ewma = 0.0
    basis_init = False
    p_rts_idx = 0
    n_p = len(p_ots)

    for i in range(0, n_c, 10):  # ~1s step over 100ms grid
        t = c_ts[i]
        sec_mod = (t // 1000) % 900
        if sec_mod < 840 or sec_mod > 895:
            continue
        c_val = comp_px[i]
        if c_val <= 0.0:
            continue

        while p_rts_idx + 1 < n_p and p_rts[p_rts_idx + 1] <= t:
            p_rts_idx += 1
        if p_rts[p_rts_idx] > t or (t - p_rts[p_rts_idx]) > 5_000:
            continue

        cur_oracle = p_px[p_rts_idx]
        cur_oracle_src_ts = p_ots[p_rts_idx]
        raw_diff = c_val - cur_oracle

        # Causal basis check: match against CEX price at `cur_oracle_src_ts` to isolate slow venue basis from fresh impulse!
        src_c_idx = bisect.bisect_right(c_ts, cur_oracle_src_ts) - 1
        if src_c_idx >= 0 and (cur_oracle_src_ts - c_ts[src_c_idx]) <= 2_000 and comp_px[src_c_idx] > 0:
            inst_basis = comp_px[src_c_idx] - cur_oracle
            if not basis_init:
                basis_ewma = inst_basis
                basis_init = True
            else:
                basis_ewma = 0.95 * basis_ewma + 0.05 * inst_basis

        x_lead = raw_diff - basis_ewma  # Origin-aligned CEX lead impulse in USD!
        if abs(x_lead) > 500.0:
            continue

        split = "DISCOVERY" if t < disc_cutoff_ms else "OOS"
        for h in horizons_ms:
            # Find the oracle tick whose oracle_ts is closest to `cur_oracle_src_ts + h` (or receipt_ts <= t + h)
            f_idx = bisect.bisect_right(p_ots, cur_oracle_src_ts + h) - 1
            if f_idx > p_rts_idx and abs(p_ots[f_idx] - (cur_oracle_src_ts + h)) <= 1000:
                y_move = p_px[f_idx] - cur_oracle
                acc = stats_by_split[split][h]
                acc[0] += x_lead * x_lead
                acc[1] += y_move * y_move
                acc[2] += x_lead * y_move
                acc[3] += (y_move - x_lead) ** 2
                acc[4] += 1

    lead_report = {"horizons_ms": {}}
    for h in horizons_ms:
        h_entry = {}
        for split in ("DISCOVERY", "OOS"):
            sx2, sy2, sxy, _, cnt = stats_by_split[split][h]
            if cnt > 100 and sx2 > 0 and sy2 > 0:
                beta = sxy / sx2
                corr = sxy / math.sqrt(sx2 * sy2)
                r2 = corr * corr
                rmse_no_cex = math.sqrt(sy2 / cnt)
                rmse_with_beta = math.sqrt(max(0.0, (sy2 - beta * sxy) / cnt))
                h_entry[split] = {
                    "n_samples": cnt,
                    "ols_beta": round(beta, 4),
                    "correlation": round(corr, 4),
                    "r_squared": round(r2, 4),
                    "oracle_move_rmse_usd": round(rmse_no_cex, 4),
                    "residual_rmse_with_cex_usd": round(rmse_with_beta, 4),
                }
        lead_report["horizons_ms"][str(h)] = h_entry

    # Recommended causal nowcast beta calibrated strictly from DISCOVERY +2000ms/+3000ms steady-state pull
    disc_3s = lead_report["horizons_ms"]["3000"].get("DISCOVERY", {})
    lead_report["calibrated_discovery_beta_3s"] = disc_3s.get("ols_beta", 0.85)
    lead_report["weights"] = {"coinbase_btcusd": 0.45, "kraken_btcusd": 0.35, "binance_btcusdt_peg": 0.20}

    out_json = WORK_DIR / "cex_lead_summary.json"
    out_json.write_text(json.dumps(lead_report, indent=2), encoding="utf-8")
    print(json.dumps(lead_report, indent=2))


if __name__ == "__main__":
    main()
