#!/usr/bin/env python3
"""
Step 03: Multi-Core 46-Day Level-2 CLOB + Oracle + CEX Forensic Simulator (tau < 60s).

Replays all clean 15-minute markets from `markets_audit.parquet` across 8 worker processes
(sharing read-only Oracle & CEX arrays via Linux fork COW) over causal checkpoints
`tau_sec in [58, 55, 50, 45, 40, 35, 30, 25, 20, 15, 12, 10, 8, 6, 4, 2]`:

Evaluates side-by-side (strictly One-Side Hold-to-Settlement):
  1. Exact TWAP Lock-In integral (`z_oracle`) vs. CEX-Nowcasted Lock-In (`z_total`),
  2. Archetype 1 — FAK Taker across `220, 350, 500, 1000, 2000, 3000 ms` (both Tight `+1c`
     limit and Wide `+5c` limit) with parabolic fee `0.07 * p * (1 - p)`,
  3. Archetype 2 — Passive Post-Only Maker (`join_bid`, `improve_bid_1c`, `ask_minus_1c`)
     across post latencies `15, 30, 50 ms`, evaluated under BOTH Conservative Lower Bound
     (`Price-Through / Wipe`) and Realistic Bound (`Trade-Correlated FIFO + event_code=3`),
     at both `0%` fee and `+20%` Maker Rebate (`0.014 * p * (1 - p)`),
  4. Archetype 3 — Pre-Positioning Maker + Causal CEX `DELETE /order` Cancel Protection
     across cancel latencies `15, 30, 50 ms` (measuring Cancel Race survival),
  5. Archetype 4 — Hybrid Maker -> FAK Taker fallback after `wait_ms in {1000, 2000, 4000}`,
  6. Multi-size capacity profile across `$10, $25, $50, $100, $250` notional per market.
"""

from __future__ import annotations

import bisect
import json
import math
import multiprocessing as mp
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from sim_core import (
    MAX_TICK,
    TICK_INV,
    BookTimelinePoint,
    build_book_timeline,
    compute_target_whole_shares,
    find_point_at_or_before,
    maker_rebate_usd,
    simulate_passive_maker_buy,
    sweep_point_fak_buy,
)

WORK_DIR = Path(os.environ.get("FORENSICS_WORK_DIR", "/var/tmp/polymarket-forensics-tau60"))

TAU_CHECKPOINTS_SEC = (58, 55, 50, 45, 40, 35, 30, 25, 20, 15, 12, 10, 8, 6, 4, 2)
TAKER_DELAYS_MS = (220, 350, 500, 1000, 2000, 3000)
MAKER_POST_DELAYS_MS = (15, 30, 50)
MAKER_CANCEL_DELAYS_MS = (15, 30, 50)
HYBRID_WAIT_MS = (1000, 2000, 4000)
NOTIONAL_SIZES_USD = (10, 25, 50, 100, 250)

# Shared read-only globals populated in parent before fork()
G_P_OTS: List[int] = []
G_P_RTS: List[int] = []
G_P_PX: List[float] = []

G_C_TS: List[int] = []
G_C_PX: List[float] = []
G_C_SVOL: List[float] = []
G_DISC_BETA: float = 0.85


def _load_shared_streams() -> None:
    """Loads deduplicated Oracle ticks and 100ms CEX composite series into parent memory prior to fork."""
    global G_P_OTS, G_P_RTS, G_P_PX, G_C_TS, G_C_PX, G_C_SVOL, G_DISC_BETA

    lead_json = json.loads((WORK_DIR / "cex_lead_summary.json").read_text(encoding="utf-8"))
    G_DISC_BETA = float(lead_json.get("calibrated_discovery_beta_3s", 0.85))

    p_tbl = pq.read_table(WORK_DIR / "oracle_price_dedup.parquet")
    G_P_OTS = p_tbl["oracle_ts"].to_pylist()
    G_P_RTS = p_tbl["receipt_ts"].to_pylist()
    G_P_PX = p_tbl["px"].to_pylist()

    u_tbl = pq.read_table(WORK_DIR / "kraken_usdt_1s.parquet")
    u_ts = u_tbl["bucket_1s"].to_pylist()
    u_px = u_tbl["usdt_usd"].to_pylist()

    c_tbl = pq.read_table(WORK_DIR / "cex_100ms_last120s.parquet")
    c_ts = c_tbl["ts_100ms"].to_pylist()
    cb_col = c_tbl["cb_px"].to_pylist()
    kr_col = c_tbl["kr_px"].to_pylist()
    bn_col = c_tbl["bn_usdt_px"].to_pylist()
    cb_sv = c_tbl["cb_svol"].to_pylist()
    kr_sv = c_tbl["kr_svol"].to_pylist()
    bn_sv = c_tbl["bn_svol"].to_pylist()

    n_c = len(c_ts)
    comp_px = [0.0] * n_c
    tot_svol = [0.0] * n_c

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
        tot_svol[i] = float(cb_sv[i] or 0.0) + float(kr_sv[i] or 0.0) + float(bn_sv[i] or 0.0) * cur_peg

    G_C_TS = c_ts
    G_C_PX = comp_px
    G_C_SVOL = tot_svol


def _read_clob_window_rows(
    parquet_path: str,
    t_end_ms: int,
) -> List[Tuple[int, int, int, int, Optional[bool], Optional[bool], Optional[int], Optional[float]]]:
    """
    Loads CLOB rows needed to reconstruct exact L2 state over `[t_end_ms - 65_000, t_end_ms]`.
    First attempts fast row-group filtered read over `[t_end_ms - 120_000, t_end_ms]`; if an initial
    `event_code == 1` snapshot for either active token precedes `t_end_ms - 120_000`, falls back
    to full-file scan up to `t_end_ms`.
    """
    cols = ["ts", "server_ts", "frame_id", "event_code", "is_up", "is_bid", "tick", "size"]
    win_start = t_end_ms - 65_000
    fast_floor = t_end_ms - 120_000

    tbl = pq.read_table(
        parquet_path,
        columns=cols,
        filters=[("ts", ">=", fast_floor), ("ts", "<=", t_end_ms), ("event_code", ">", 0)],
    )
    ts_l = tbl["ts"].to_pylist()
    ec_l = tbl["event_code"].to_pylist()
    up_l = tbl["is_up"].to_pylist()

    has_up_snap = any(ec_l[k] == 1 and up_l[k] is True and ts_l[k] <= win_start for k in range(len(ts_l)))
    has_dn_snap = any(ec_l[k] == 1 and up_l[k] is False and ts_l[k] <= win_start for k in range(len(ts_l)))

    if not (has_up_snap and has_dn_snap):
        tbl = pq.read_table(
            parquet_path,
            columns=cols,
            filters=[("ts", "<=", t_end_ms), ("event_code", ">", 0)],
        )
        ts_l = tbl["ts"].to_pylist()
        ec_l = tbl["event_code"].to_pylist()
        up_l = tbl["is_up"].to_pylist()

    # Find latest pre-window snapshot start so we don't replay unnecessary early deltas
    last_up_snap_ts = 0
    last_dn_snap_ts = 0
    for k in range(len(ts_l)):
        t_k = ts_l[k]
        if t_k > win_start:
            break
        if ec_l[k] == 1:
            if up_l[k] is True:
                last_up_snap_ts = t_k
            elif up_l[k] is False:
                last_dn_snap_ts = t_k

    seed_ts = min(last_up_snap_ts, last_dn_snap_ts) if (last_up_snap_ts > 0 and last_dn_snap_ts > 0) else 0
    lo_idx = bisect.bisect_left(ts_l, seed_ts) if seed_ts > 0 else 0

    srv_l = tbl["server_ts"].to_pylist()
    fid_l = tbl["frame_id"].to_pylist()
    bid_l = tbl["is_bid"].to_pylist()
    tck_l = tbl["tick"].to_pylist()
    sz_l = tbl["size"].to_pylist()

    return [
        (
            int(ts_l[k]),
            int(srv_l[k] or 0),
            int(fid_l[k] or 0),
            int(ec_l[k]),
            up_l[k],
            bid_l[k],
            int(tck_l[k]) if tck_l[k] is not None else None,
            float(sz_l[k]) if sz_l[k] is not None else None,
        )
        for k in range(lo_idx, len(ts_l))
    ]


def _compute_causal_state_at(
    t_ms: int,
    t_end_ms: int,
    twap_open: float,
    sigma_1s_usd: float,
    basis_ewma: float,
) -> Optional[Tuple[float, float, float, float, float, float, float, int]]:
    """
    Computes strictly causal TWAP Lock-In + CEX Lead features at `t_ms`:
    Returns `(z_oracle, z_total, proj_diff_oracle_usd, proj_diff_cex_usd, sigma_rem_usd,
              cex_lead_usd, cex_move_500ms_usd, n_banked)`.
    """
    r_idx = bisect.bisect_right(G_P_RTS, t_ms) - 1
    if r_idx < 0 or (t_ms - G_P_RTS[r_idx]) > 8_000:
        return None
    cur_oracle_px = G_P_PX[r_idx]

    # Collect all ALREADY RECEIVED (`receipt_ts <= t_ms`) oracle ticks with `oracle_ts` in `(t_end_ms - 60_000, t_end_ms]`
    # Since `G_P_OTS` is strictly increasing, scan backwards from `r_idx` while `G_P_OTS[k] > t_end_ms - 60_000`
    win_open_ots = t_end_ms - 60_000
    n_banked = 0
    sum_banked_diff = 0.0
    k = r_idx
    while k >= 0 and G_P_OTS[k] > win_open_ots:
        if G_P_RTS[k] <= t_ms and G_P_OTS[k] <= t_end_ms:
            n_banked += 1
            sum_banked_diff += G_P_PX[k] - twap_open
        k -= 1

    n_rem = max(0, 60 - n_banked)
    tau_rem_s = max(0.5, (t_end_ms - t_ms) / 1000.0)
    tau_eff = max(1.0, float(n_rem), tau_rem_s)

    # Remaining 60s TWAP uncertainty scales as tau^{3/2} / (60 * sqrt(3)) + sub-second discretization floor
    diff_var = (sigma_1s_usd * sigma_1s_usd) * (tau_eff ** 3) / (3.0 * 3600.0)
    floor_var = (0.55 * (tau_eff / 60.0)) ** 2
    sigma_rem_usd = math.sqrt(diff_var + floor_var)

    c_idx = bisect.bisect_right(G_C_TS, t_ms) - 1
    cex_px = G_C_PX[c_idx] if (c_idx >= 0 and (t_ms - G_C_TS[c_idx]) <= 3_000 and G_C_PX[c_idx] > 0) else cur_oracle_px
    cex_lead_usd = (cex_px - cur_oracle_px - basis_ewma) if cex_px > 0 else 0.0

    # 500ms CEX impulse
    c_prev_idx = bisect.bisect_right(G_C_TS, t_ms - 500) - 1
    cex_prev_px = G_C_PX[c_prev_idx] if (c_prev_idx >= 0 and (t_ms - 500 - G_C_TS[c_prev_idx]) <= 3_000 and G_C_PX[c_prev_idx] > 0) else cex_px
    cex_move_500 = cex_px - cex_prev_px

    proj_diff_oracle = (sum_banked_diff + n_rem * (cur_oracle_px - twap_open)) / 60.0
    # Exact Discovery impulse-response across remaining ticks (+1s: ~0.39, +2s: ~0.78, >=+3s: ~0.964)
    if n_rem <= 0:
        lead_ticks_sum = 0.0
    elif n_rem == 1:
        lead_ticks_sum = 0.39 * cex_lead_usd
    elif n_rem == 2:
        lead_ticks_sum = (0.39 + 0.78) * cex_lead_usd
    else:
        lead_ticks_sum = (0.39 + 0.78 + (n_rem - 2) * G_DISC_BETA) * cex_lead_usd
    proj_diff_cex = (sum_banked_diff + n_rem * (cur_oracle_px - twap_open) + lead_ticks_sum) / 60.0

    z_oracle = proj_diff_oracle / sigma_rem_usd
    z_total = proj_diff_cex / sigma_rem_usd
    return (
        z_oracle,
        z_total,
        proj_diff_oracle,
        proj_diff_cex,
        sigma_rem_usd,
        cex_lead_usd,
        cex_move_500,
        n_banked,
    )


def _simulate_single_market(mkt: dict) -> List[dict]:
    """Worker routine simulating all checkpoints, archetypes, latencies, and sizes on one clean 15m market."""
    t_start_sec = int(mkt["t_start_sec"])
    t_end_sec = int(mkt["t_end_sec"])
    t_end_ms = t_end_sec * 1000
    twap_open = float(mkt["twap_open"])
    outcome_up = bool(mkt["outcome_up"])
    sigma_1s_usd = max(0.50, float(mkt["sigma_1s_usd"]))

    try:
        clob_rows = _read_clob_window_rows(mkt["parquet_path"], t_end_ms)
    except Exception:
        return []
    if not clob_rows:
        return []

    win_start_ms = t_end_ms - 62_000
    timeline, tr_srv, tr_rec = build_book_timeline(clob_rows, win_start_ms, t_end_ms)
    if len(timeline) < 3:
        return []
    tl_ts = [p.receipt_ts for p in timeline]

    # Estimate causal CEX-to-Oracle basis EWMA over `[t_end_ms - 90_000, t_end_ms - 60_000]` (strictly before tau=60s!)
    basis_vals: List[float] = []
    b_lo = bisect.bisect_left(G_P_OTS, t_end_ms - 90_000)
    b_hi = bisect.bisect_right(G_P_OTS, t_end_ms - 60_000)
    for k in range(b_lo, b_hi):
        ots = G_P_OTS[k]
        ci = bisect.bisect_right(G_C_TS, ots) - 1
        if ci >= 0 and (ots - G_C_TS[ci]) <= 2_000 and G_C_PX[ci] > 0:
            basis_vals.append(G_C_PX[ci] - G_P_PX[k])
    basis_ewma = (sum(basis_vals) / len(basis_vals)) if basis_vals else 0.0

    # Precompute 100ms causal `z_total` trajectory over `[t_end_ms - 60_000, t_end_ms)` so CEX Cancel Race
    # can detect exact 100ms abort timestamps in O(1)!
    grid_100_ts = list(range(t_end_ms - 60_000, t_end_ms, 100))
    grid_100_z: List[float] = []
    grid_100_cex_px: List[float] = []
    for gt in grid_100_ts:
        st = _compute_causal_state_at(gt, t_end_ms, twap_open, sigma_1s_usd, basis_ewma)
        grid_100_z.append(st[1] if st is not None else 0.0)
        ci = bisect.bisect_right(G_C_TS, gt) - 1
        grid_100_cex_px.append(G_C_PX[ci] if (ci >= 0 and G_C_PX[ci] > 0) else 0.0)

    out_rows: List[dict] = []

    for tau_s in TAU_CHECKPOINTS_SEC:
        t_ms = t_end_ms - tau_s * 1000
        pt_now = find_point_at_or_before(timeline, tl_ts, t_ms)
        if pt_now is None:
            continue
        quote_age_ms = t_ms - pt_now.receipt_ts
        if quote_age_ms > 15_000:
            continue

        st = _compute_causal_state_at(t_ms, t_end_ms, twap_open, sigma_1s_usd, basis_ewma)
        if st is None:
            continue
        z_oracle, z_total, proj_ora, proj_cex, sig_rem, cex_lead, cex_mv500, n_banked = st

        # Candidate side is the side favored by causal TWAP + CEX nowcast (`z_total >= 0` => UP, else DOWN)
        cand_up = z_total >= 0.0
        sign = 1.0 if cand_up else -1.0
        won_cand = (outcome_up == cand_up)
        payout_per_share = 1.0 if won_cand else 0.0

        b_bid_t = pt_now.best_bid_up if cand_up else pt_now.best_bid_down
        b_ask_t = pt_now.best_ask_up if cand_up else pt_now.best_ask_down
        if b_bid_t <= 0 or b_ask_t <= 0 or b_ask_t > MAX_TICK or b_bid_t >= b_ask_t:
            continue

        best_bid = b_bid_t * TICK_INV
        best_ask = b_ask_t * TICK_INV
        spread = best_ask - best_bid
        bids_map_now = pt_now.up_bids_map if cand_up else pt_now.down_bids_map
        asks_sorted_now = pt_now.up_asks_sorted if cand_up else pt_now.down_asks_sorted
        bid_q_ahead_now = bids_map_now.get(b_bid_t, 0.0)
        depth_1c_ask = sum(sz for tk, sz in asks_sorted_now if tk <= b_ask_t + 100)

        # Find first causal CEX abort timestamp after `t_ms` where EITHER:
        #   - signed z_total drops below `max(0.20, 0.45 * abs(z_total))` (cushion eroded by >55%), OR
        #   - CEX spot moves adversely by `>= max(12.0, 1.5 * sigma_1s_usd)` USD from `t_ms`!
        g_start = (t_ms - (t_end_ms - 60_000)) // 100
        cex_at_t = grid_100_cex_px[g_start] if 0 <= g_start < len(grid_100_cex_px) else 0.0
        abs_z = abs(z_total)
        z_abort_thresh = max(0.20, 0.45 * abs_z)
        px_abort_drop = max(12.0, 1.5 * sigma_1s_usd)
        cex_abort_ts: Optional[int] = None
        for gi in range(max(0, g_start + 1), len(grid_100_ts)):
            signed_z_g = sign * grid_100_z[gi]
            adverse_px = sign * (cex_at_t - grid_100_cex_px[gi]) if (cex_at_t > 0 and grid_100_cex_px[gi] > 0) else 0.0
            if signed_z_g < z_abort_thresh or adverse_px >= px_abort_drop:
                cex_abort_ts = grid_100_ts[gi]
                break

        row: dict = {
            "t_start_sec": t_start_sec,
            "utc_date": mkt["utc_date"],
            "utc_hour": int(mkt["utc_hour"]),
            "week_label": mkt["week_label"],
            "split": mkt["split"],
            "tau_s": tau_s,
            "cand_up": cand_up,
            "won": won_cand,
            "sigma_1s_usd": round(sigma_1s_usd, 4),
            "sigma_rem_usd": round(sig_rem, 4),
            "n_banked": n_banked,
            "proj_ora_usd": round(sign * proj_ora, 4),
            "proj_cex_usd": round(sign * proj_cex, 4),
            "z_ora_signed": round(sign * z_oracle, 4),
            "z_tot_abs": round(abs_z, 4),
            "cex_lead_favor_usd": round(sign * cex_lead, 4),
            "cex_mv500_favor_usd": round(sign * cex_mv500, 4),
            "best_bid": round(best_bid, 4),
            "best_ask": round(best_ask, 4),
            "spread": round(spread, 4),
            "bid_q_ahead": round(bid_q_ahead_now, 2),
            "depth_1c_ask": round(depth_1c_ask, 2),
            "quote_age_ms": quote_age_ms,
            "ws_lat_ms": max(0, pt_now.receipt_ts - pt_now.server_ts) if pt_now.server_ts > 0 else -1,
            "has_cex_abort": cex_abort_ts is not None,
        }

        # ---------------------------------------------------------------------
        # Evaluate across all 5 target USD sizes ($10, $25, $50, $100, $250)
        # ---------------------------------------------------------------------
        # Pre-compute size-independent unit/fill ratios where possible, or exact per-size sweeps!
        # First, evaluate FAK Taker across all 6 delays (220..3000ms) for reference $25 and all sizes at 220/500/1000/2000ms:
        # Actually, computing exact sweeps for ALL 5 sizes x 6 delays is just 30 tiny tuple loops (< 0.05 ms)!
        tight_lim_tick = min(MAX_TICK - 10, b_ask_t + 100)  # +1c discipline limit
        wide_lim_tick = min(9800, max(b_ask_t + 100, b_ask_t + 500))  # +5c chase limit

        for sz_usd in NOTIONAL_SIZES_USD:
            req_taker_sh = compute_target_whole_shares(float(sz_usd), best_ask)
            row[f"tk_req_sh_{sz_usd}"] = req_taker_sh

            for d_ms in TAKER_DELAYS_MS:
                match_ts = t_ms + int(round(0.89 * d_ms))
                if req_taker_sh <= 0 or match_ts >= t_end_ms:
                    row[f"tk_tight_pnl_{sz_usd}_{d_ms}"] = 0.0
                    row[f"tk_tight_fill_{sz_usd}_{d_ms}"] = 0.0
                    if sz_usd == 25:
                        row[f"tk_wide_pnl_25_{d_ms}"] = 0.0
                        row[f"tk_rej_25_{d_ms}"] = True
                    continue

                pt_m = find_point_at_or_before(timeline, tl_ts, match_ts)
                sw_t = sweep_point_fak_buy(pt_m, cand_up, req_taker_sh, tight_lim_tick)
                pnl_t = sw_t.executed_shares * (payout_per_share - sw_t.avg_price) - sw_t.fee_usd if sw_t.executed_shares > 0 else 0.0
                row[f"tk_tight_pnl_{sz_usd}_{d_ms}"] = round(pnl_t, 4)
                row[f"tk_tight_fill_{sz_usd}_{d_ms}"] = round(sw_t.executed_shares, 2)

                if sz_usd == 25:
                    sw_w = sweep_point_fak_buy(pt_m, cand_up, req_taker_sh, wide_lim_tick)
                    pnl_w = sw_w.executed_shares * (payout_per_share - sw_w.avg_price) - sw_w.fee_usd if sw_w.executed_shares > 0 else 0.0
                    row[f"tk_wide_pnl_25_{d_ms}"] = round(pnl_w, 4)
                    row[f"tk_rej_25_{d_ms}"] = sw_t.rejected_no_match

        # ---------------------------------------------------------------------
        # Archetype 2 & 3: Passive Maker Variants
        # Quote placements:
        #   - `join`: b_bid_t
        #   - `imp1`: b_bid_t + 100 if (b_ask_t - b_bid_t) >= 200 else b_bid_t
        #   - `ask1`: b_ask_t - 100 if (b_ask_t - b_bid_t) >= 200 else b_bid_t
        # ---------------------------------------------------------------------
        spread_ticks = b_ask_t - b_bid_t
        quote_ticks = {
            "join": b_bid_t,
            "imp1": (b_bid_t + 100) if spread_ticks >= 200 else b_bid_t,
            "ask1": (b_ask_t - 100) if spread_ticks >= 200 else b_bid_t,
        }

        # A) Full multi-size ($10..$250) evaluation at canonical baseline post=30ms, cancel=30ms for `join` and `imp1`
        for q_name in ("join", "imp1", "ask1"):
            q_tick = quote_ticks[q_name]
            q_px = q_tick * TICK_INV
            sizes_to_run = NOTIONAL_SIZES_USD if q_name in ("join", "imp1") else (25,)
            for sz_usd in sizes_to_run:
                req_mk_sh = compute_target_whole_shares(float(sz_usd), q_px)
                if req_mk_sh <= 0:
                    for tag in ("nocx", "cx30"):
                        if q_name == "ask1" and tag == "cx30" and sz_usd != 25:
                            continue
                        row[f"mk_{q_name}_{tag}_low_pnl0_{sz_usd}"] = 0.0
                        row[f"mk_{q_name}_{tag}_fifo_pnl0_{sz_usd}"] = 0.0
                        row[f"mk_{q_name}_{tag}_fifo_pnlreb_{sz_usd}"] = 0.0
                        row[f"mk_{q_name}_{tag}_fifo_fill_{sz_usd}"] = 0.0
                        row[f"mk_{q_name}_{tag}_low_fill_{sz_usd}"] = 0.0
                    continue

                # 1. Pure Hold-to-Settlement Maker (no cancel) @ post=30ms
                m_no = simulate_passive_maker_buy(
                    timeline, tl_ts, tr_srv, tr_rec,
                    is_up=cand_up, bid_tick=q_tick, requested_shares=req_mk_sh,
                    submit_ts=t_ms, post_latency_ms=30, end_ts=t_end_ms, cex_abort_ts=None,
                )
                u_net = payout_per_share - q_px
                reb_1 = maker_rebate_usd(1.0, q_px)
                row[f"mk_{q_name}_nocx_low_pnl0_{sz_usd}"] = round(m_no.filled_shares_lower * u_net, 4)
                row[f"mk_{q_name}_nocx_fifo_pnl0_{sz_usd}"] = round(m_no.filled_shares_fifo * u_net, 4)
                row[f"mk_{q_name}_nocx_fifo_pnlreb_{sz_usd}"] = round(m_no.filled_shares_fifo * (u_net + reb_1), 4)
                row[f"mk_{q_name}_nocx_fifo_fill_{sz_usd}"] = round(m_no.filled_shares_fifo, 2)
                row[f"mk_{q_name}_nocx_low_fill_{sz_usd}"] = round(m_no.filled_shares_lower, 2)

                # 2. CEX-Cancel-Protected Maker @ post=30ms, cancel=30ms
                m_cx = simulate_passive_maker_buy(
                    timeline, tl_ts, tr_srv, tr_rec,
                    is_up=cand_up, bid_tick=q_tick, requested_shares=req_mk_sh,
                    submit_ts=t_ms, post_latency_ms=30, end_ts=t_end_ms,
                    cex_abort_ts=cex_abort_ts, cancel_latency_ms=30,
                )
                row[f"mk_{q_name}_cx30_low_pnl0_{sz_usd}"] = round(m_cx.filled_shares_lower * u_net, 4)
                row[f"mk_{q_name}_cx30_fifo_pnl0_{sz_usd}"] = round(m_cx.filled_shares_fifo * u_net, 4)
                row[f"mk_{q_name}_cx30_fifo_pnlreb_{sz_usd}"] = round(m_cx.filled_shares_fifo * (u_net + reb_1), 4)
                row[f"mk_{q_name}_cx30_fifo_fill_{sz_usd}"] = round(m_cx.filled_shares_fifo, 2)
                row[f"mk_{q_name}_cx30_low_fill_{sz_usd}"] = round(m_cx.filled_shares_lower, 2)
                if sz_usd == 25 and q_name == "imp1":
                    row["mk_imp1_post_rej_25"] = (m_cx.status == "POST_ONLY_REJECTED")
                    row["mk_imp1_cx_saved_fifo_25"] = m_cx.cancel_beat_adverse_fill_fifo
                    row["mk_imp1_cx_saved_low_25"] = m_cx.cancel_beat_adverse_fill_lower

        # B) Sensitivity to Maker Post Latency (15, 30, 50 ms) & Cancel Latency (15, 30, 50 ms) at $25 on `imp1`
        q_imp = quote_ticks["imp1"]
        q_imp_px = q_imp * TICK_INV
        req_25 = compute_target_whole_shares(25.0, q_imp_px)
        u_imp = payout_per_share - q_imp_px
        for p_ms in MAKER_POST_DELAYS_MS:
            for c_ms in MAKER_CANCEL_DELAYS_MS:
                if req_25 <= 0:
                    row[f"mk_sens_fifo_pnl0_p{p_ms}_c{c_ms}"] = 0.0
                    continue
                ms = simulate_passive_maker_buy(
                    timeline, tl_ts, tr_srv, tr_rec,
                    is_up=cand_up, bid_tick=q_imp, requested_shares=req_25,
                    submit_ts=t_ms, post_latency_ms=p_ms, end_ts=t_end_ms,
                    cex_abort_ts=cex_abort_ts, cancel_latency_ms=c_ms,
                )
                row[f"mk_sens_fifo_pnl0_p{p_ms}_c{c_ms}"] = round(ms.filled_shares_fifo * u_imp, 4)

        # C) Archetype 4 — Hybrid Maker (`imp1` + CEX cancel) -> FAK Taker (220ms & 500ms) fallback after wait_ms in {1000, 2000, 4000} at $25
        for w_ms in HYBRID_WAIT_MS:
            t_fb = t_ms + w_ms
            if req_25 <= 0 or t_fb >= t_end_ms - 500:
                row[f"hyb_fifo_pnl0_w{w_ms}_tk220"] = row.get("mk_imp1_cx30_fifo_pnl0_25", 0.0)
                row[f"hyb_fifo_pnl0_w{w_ms}_tk500"] = row.get("mk_imp1_cx30_fifo_pnl0_25", 0.0)
                continue

            m_wait = simulate_passive_maker_buy(
                timeline, tl_ts, tr_srv, tr_rec,
                is_up=cand_up, bid_tick=q_imp, requested_shares=req_25,
                submit_ts=t_ms, post_latency_ms=30, end_ts=t_fb,
                cex_abort_ts=cex_abort_ts, cancel_latency_ms=30,
            )
            mk_pnl_part = m_wait.filled_shares_fifo * u_imp
            rem_sh = int(math.floor(max(0.0, req_25 - m_wait.filled_shares_fifo) + 1e-9))

            # Only escalate to FAK Taker at `t_fb` if CEX has NOT aborted and remaining shares >= 5!
            aborted_already = cex_abort_ts is not None and cex_abort_ts <= t_fb
            for tk_d in (220, 500):
                tk_pnl_part = 0.0
                if not aborted_already and rem_sh >= 5:
                    pt_fb = find_point_at_or_before(timeline, tl_ts, t_fb)
                    if pt_fb is not None:
                        fb_ask_t = pt_fb.best_ask_up if cand_up else pt_fb.best_ask_down
                        if 0 < fb_ask_t <= 9200 and rem_sh * (fb_ask_t * TICK_INV) >= 1.0:
                            match_fb = t_fb + int(round(0.89 * tk_d))
                            if match_fb < t_end_ms:
                                pt_mfb = find_point_at_or_before(timeline, tl_ts, match_fb)
                                sw_fb = sweep_point_fak_buy(pt_mfb, cand_up, rem_sh, min(MAX_TICK - 10, fb_ask_t + 100))
                                if sw_fb.executed_shares > 0:
                                    tk_pnl_part = sw_fb.executed_shares * (payout_per_share - sw_fb.avg_price) - sw_fb.fee_usd
                row[f"hyb_fifo_pnl0_w{w_ms}_tk{tk_d}"] = round(mk_pnl_part + tk_pnl_part, 4)

        out_rows.append(row)

    return out_rows


def main() -> None:
    t0 = time.time()
    print("[03_sim] Loading shared Oracle & 100ms CEX series into memory...")
    _load_shared_streams()

    audit_tbl = pq.read_table(WORK_DIR / "markets_audit.parquet")
    clean_markets = [r for r in audit_tbl.to_pylist() if r["is_clean"]]
    print(f"[03_sim] Launching 8-worker L2 + Maker/Taker simulation across {len(clean_markets)} clean 15m markets...")

    ctx = mp.get_context("fork")
    all_rows: List[dict] = []
    done = 0
    with ctx.Pool(processes=min(8, os.cpu_count() or 8)) as pool:
        for batch in pool.imap_unordered(_simulate_single_market, clean_markets, chunksize=24):
            if batch:
                all_rows.extend(batch)
            done += 1
            if done % 500 == 0 or done == len(clean_markets):
                print(f"[03_sim] Processed {done}/{len(clean_markets)} markets ({len(all_rows)} checkpoint rows, {time.time() - t0:.1f}s)")

    # Sort deterministically by (t_start_sec ASC, tau_s DESC) so tau=58 -> tau=2 is strictly chronological!
    all_rows.sort(key=lambda r: (r["t_start_sec"], -r["tau_s"]))
    out_pq = WORK_DIR / "decision_grid.parquet"
    pq.write_table(pa.Table.from_pylist(all_rows), out_pq, compression="zstd")
    print(f"[03_sim] Saved {len(all_rows)} checkpoint rows to {out_pq} in {time.time() - t0:.1f}s.")


if __name__ == "__main__":
    main()
