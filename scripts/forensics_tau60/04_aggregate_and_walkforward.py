#!/usr/bin/env python3
"""
Step 04: Non-Parametric Opportunity Surface Mapping, Adverse Selection Decomposition,
Capacity Profiling ($10..$250), Weekly Alpha Decay (W1..W7), and Walk-Forward OOS Validation.

Reads `/var/tmp/polymarket-forensics-tau60/decision_grid.parquet` and produces:
  - `/var/tmp/polymarket-forensics-tau60/forensics_results.json`: complete structured tables
    powering the final forensic report, enforcing strict `<= 1` trade episode per 15m market
    for all PnL, Sharpe, Drawdown, and Daily Capacity metrics, with Discovery (`<= 2026-11-12`)
    strictly separated from Out-of-Sample (`>= 2026-11-13`).
"""

from __future__ import annotations

import collections
import json
import math
import os
from pathlib import Path
from typing import Callable, Dict, List, Tuple

import pyarrow.parquet as pq

WORK_DIR = Path(os.environ.get("FORENSICS_WORK_DIR", "/var/tmp/polymarket-forensics-tau60"))
TAKER_DELAYS_MS = (220, 350, 500, 1000, 2000, 3000)
NOTIONAL_SIZES_USD = (10, 25, 50, 100, 250)


def quantile(vals: List[float], q: float) -> float:
    if not vals:
        return float("nan")
    s = sorted(vals)
    idx = min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))
    return s[idx]


def compute_daily_risk_metrics(daily_pnl_by_date: Dict[str, float], all_dates_sorted: List[str]) -> dict:
    """Computes total PnL, mean $/day, annualized daily Sharpe, win days %, and max drawdown ($)."""
    series = [daily_pnl_by_date.get(d, 0.0) for d in all_dates_sorted]
    n_days = len(series)
    if n_days == 0:
        return {"total_pnl": 0.0, "pnl_per_day": 0.0, "sharpe_ann": 0.0, "max_dd": 0.0, "pos_days_pct": 0.0}
    tot = sum(series)
    mean_d = tot / n_days
    var_d = sum((x - mean_d) ** 2 for x in series) / max(1, n_days - 1) if n_days > 1 else 0.0
    sd_d = math.sqrt(var_d)
    sharpe = (mean_d / sd_d) * math.sqrt(365.0) if sd_d > 1e-9 else 0.0

    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    pos_days = 0
    for x in series:
        if x > 1e-9:
            pos_days += 1
        cum += x
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd

    return {
        "n_days": n_days,
        "total_pnl": round(tot, 2),
        "pnl_per_day": round(mean_d, 2),
        "sharpe_ann": round(sharpe, 2),
        "max_dd": round(max_dd, 2),
        "pos_days_pct": round(100.0 * pos_days / n_days, 1),
    }


def select_first_episode_per_market(rows: List[dict], predicate: Callable[[dict], bool]) -> List[dict]:
    """
    Iterates chronologically (`tau = 58 -> 2` within each `t_start_sec`) and selects at most
    ONE trigger episode per 15-minute market to prevent intra-market correlation bias.
    """
    picked: List[dict] = []
    seen_markets = set()
    for r in rows:
        m_id = r["t_start_sec"]
        if m_id in seen_markets:
            continue
        if predicate(r):
            seen_markets.add(m_id)
            picked.append(r)
    return picked


def evaluate_pocket_comprehensive(
    episodes: List[dict],
    dates_sorted: List[str],
) -> dict:
    """Evaluates all Taker, Maker (Lower & FIFO, 0% & Rebate, Cancel vs No-Cancel), Hybrid, and Size metrics."""
    n_ep = len(episodes)
    if n_ep == 0:
        return {"n_episodes": 0}

    uncond_win_rate = sum(1 for r in episodes if r["won"]) / n_ep
    avg_ask = sum(r["best_ask"] for r in episodes) / n_ep
    avg_bid = sum(r["best_bid"] for r in episodes) / n_ep
    avg_tau = sum(r["tau_s"] for r in episodes) / n_ep

    # 1. Taker latency curve @ $25 (Tight +1c vs Wide +5c)
    taker_curve = {}
    for d_ms in TAKER_DELAYS_MS:
        daily_t: Dict[str, float] = collections.defaultdict(float)
        fills = 0
        rejs = 0
        wins_filled = 0
        wide_tot = 0.0
        for r in episodes:
            sh = r[f"tk_tight_fill_25_{d_ms}"]
            pnl = r[f"tk_tight_pnl_25_{d_ms}"]
            wide_tot += r[f"tk_wide_pnl_25_{d_ms}"]
            daily_t[r["utc_date"]] += pnl
            if sh > 0:
                fills += 1
                if r["won"]:
                    wins_filled += 1
            if r[f"tk_rej_25_{d_ms}"]:
                rejs += 1
        dm = compute_daily_risk_metrics(daily_t, dates_sorted)
        taker_curve[str(d_ms)] = {
            **dm,
            "fill_rate_pct": round(100.0 * fills / n_ep, 1),
            "reject_rate_pct": round(100.0 * rejs / n_ep, 1),
            "win_rate_filled_pct": round(100.0 * wins_filled / max(1, fills), 2),
            "ev_per_signal_usd": round(dm["total_pnl"] / n_ep, 4),
            "ev_per_fill_usd": round(dm["total_pnl"] / max(1, fills), 4),
            "wide_limit_total_pnl": round(wide_tot, 2),
        }

    # 2. Maker variants @ $25 (post=30ms, cancel=30ms vs nocx, Lower vs FIFO, 0% fee vs Rebate)
    maker_variants = {}
    for q_name in ("join", "imp1", "ask1"):
        for cx_tag in ("nocx", "cx30"):
            key = f"{q_name}_{cx_tag}"
            d_low0: Dict[str, float] = collections.defaultdict(float)
            d_fifo0: Dict[str, float] = collections.defaultdict(float)
            d_fiforeb: Dict[str, float] = collections.defaultdict(float)
            f_low_cnt = 0
            f_fifo_cnt = 0
            w_fifo_cnt = 0
            unfilled_fifo_wins = 0
            unfilled_fifo_cnt = 0
            cx_saved_fifo = 0
            cx_saved_low = 0

            for r in episodes:
                fl = r[f"mk_{q_name}_{cx_tag}_low_fill_25"]
                ff = r[f"mk_{q_name}_{cx_tag}_fifo_fill_25"]
                pl0 = r[f"mk_{q_name}_{cx_tag}_low_pnl0_25"]
                pf0 = r[f"mk_{q_name}_{cx_tag}_fifo_pnl0_25"]
                pfr = r[f"mk_{q_name}_{cx_tag}_fifo_pnlreb_25"]

                d_low0[r["utc_date"]] += pl0
                d_fifo0[r["utc_date"]] += pf0
                d_fiforeb[r["utc_date"]] += pfr

                if fl > 0:
                    f_low_cnt += 1
                if ff > 0:
                    f_fifo_cnt += 1
                    if r["won"]:
                        w_fifo_cnt += 1
                else:
                    unfilled_fifo_cnt += 1
                    if r["won"]:
                        unfilled_fifo_wins += 1

                if q_name == "imp1" and cx_tag == "cx30":
                    if r.get("mk_imp1_cx_saved_fifo_25"):
                        cx_saved_fifo += 1
                    if r.get("mk_imp1_cx_saved_low_25"):
                        cx_saved_low += 1

            m_low = compute_daily_risk_metrics(d_low0, dates_sorted)
            m_fifo0 = compute_daily_risk_metrics(d_fifo0, dates_sorted)
            m_fiforeb = compute_daily_risk_metrics(d_fiforeb, dates_sorted)

            wr_filled = (w_fifo_cnt / f_fifo_cnt) if f_fifo_cnt > 0 else 0.0
            wr_unfilled = (unfilled_fifo_wins / unfilled_fifo_cnt) if unfilled_fifo_cnt > 0 else 0.0

            maker_variants[key] = {
                "lower_bound_0fee": {
                    **m_low,
                    "fill_rate_pct": round(100.0 * f_low_cnt / n_ep, 1),
                    "ev_per_signal_usd": round(m_low["total_pnl"] / n_ep, 4),
                    "ev_per_fill_usd": round(m_low["total_pnl"] / max(1, f_low_cnt), 4),
                },
                "fifo_0fee": {
                    **m_fifo0,
                    "fill_rate_pct": round(100.0 * f_fifo_cnt / n_ep, 1),
                    "win_rate_filled_pct": round(100.0 * wr_filled, 2),
                    "win_rate_unfilled_pct": round(100.0 * wr_unfilled, 2),
                    "adverse_selection_gap_pp": round(100.0 * (wr_unfilled - wr_filled), 2),
                    "ev_per_signal_usd": round(m_fifo0["total_pnl"] / n_ep, 4),
                    "ev_per_fill_usd": round(m_fifo0["total_pnl"] / max(1, f_fifo_cnt), 4),
                },
                "fifo_with_rebate": {
                    **m_fiforeb,
                    "ev_per_signal_usd": round(m_fiforeb["total_pnl"] / n_ep, 4),
                    "ev_per_fill_usd": round(m_fiforeb["total_pnl"] / max(1, f_fifo_cnt), 4),
                },
                "cex_cancel_saved_adverse_fills_fifo": cx_saved_fifo,
                "cex_cancel_saved_adverse_fills_lower": cx_saved_low,
            }

    # 3. Maker Post/Cancel Latency Sensitivity (`imp1`, $25, FIFO 0% fee)
    maker_lat_sens = {}
    for p_ms in (15, 30, 50):
        for c_ms in (15, 30, 50):
            tot_s = sum(r[f"mk_sens_fifo_pnl0_p{p_ms}_c{c_ms}"] for r in episodes)
            maker_lat_sens[f"post{p_ms}_cancel{c_ms}"] = round(tot_s, 2)

    # 4. Hybrid Maker -> FAK Taker Fallback ($25)
    hybrid_variants = {}
    for w_ms in (1000, 2000, 4000):
        for tk_d in (220, 500):
            d_hyb: Dict[str, float] = collections.defaultdict(float)
            for r in episodes:
                d_hyb[r["utc_date"]] += r[f"hyb_fifo_pnl0_w{w_ms}_tk{tk_d}"]
            hm = compute_daily_risk_metrics(d_hyb, dates_sorted)
            hybrid_variants[f"wait{w_ms}_tk{tk_d}"] = {
                **hm,
                "ev_per_signal_usd": round(hm["total_pnl"] / n_ep, 4),
            }

    # 5. Multi-Size Capacity Profile ($10, $25, $50, $100, $250)
    capacity_profile = {}
    for sz in NOTIONAL_SIZES_USD:
        d_tk220: Dict[str, float] = collections.defaultdict(float)
        d_tk500: Dict[str, float] = collections.defaultdict(float)
        d_tk1000: Dict[str, float] = collections.defaultdict(float)
        d_mk_imp_low: Dict[str, float] = collections.defaultdict(float)
        d_mk_imp_fifo0: Dict[str, float] = collections.defaultdict(float)
        d_mk_imp_fiforeb: Dict[str, float] = collections.defaultdict(float)
        mk_req_tot = 0.0
        mk_fill_fifo_tot = 0.0
        tk220_req_tot = 0.0
        tk220_fill_tot = 0.0

        for r in episodes:
            d_tk220[r["utc_date"]] += r[f"tk_tight_pnl_{sz}_220"]
            d_tk500[r["utc_date"]] += r[f"tk_tight_pnl_{sz}_500"]
            d_tk1000[r["utc_date"]] += r[f"tk_tight_pnl_{sz}_1000"]
            d_mk_imp_low[r["utc_date"]] += r[f"mk_imp1_cx30_low_pnl0_{sz}"]
            d_mk_imp_fifo0[r["utc_date"]] += r[f"mk_imp1_cx30_fifo_pnl0_{sz}"]
            d_mk_imp_fiforeb[r["utc_date"]] += r[f"mk_imp1_cx30_fifo_pnlreb_{sz}"]

            tk220_req_tot += r[f"tk_req_sh_{sz}"]
            tk220_fill_tot += r[f"tk_tight_fill_{sz}_220"]
            q_px = min(r["best_ask"] - 0.01, r["best_bid"] + 0.01) if r["spread"] >= 0.02 else r["best_bid"]
            if q_px > 0:
                mk_req_tot += math.floor(sz / q_px)
            mk_fill_fifo_tot += r[f"mk_imp1_cx30_fifo_fill_{sz}"]

        capacity_profile[str(sz)] = {
            "taker_220ms": compute_daily_risk_metrics(d_tk220, dates_sorted),
            "taker_500ms": compute_daily_risk_metrics(d_tk500, dates_sorted),
            "taker_1000ms": compute_daily_risk_metrics(d_tk1000, dates_sorted),
            "maker_imp1_cx30_lower_0fee": compute_daily_risk_metrics(d_mk_imp_low, dates_sorted),
            "maker_imp1_cx30_fifo_0fee": compute_daily_risk_metrics(d_mk_imp_fifo0, dates_sorted),
            "maker_imp1_cx30_fifo_rebate": compute_daily_risk_metrics(d_mk_imp_fiforeb, dates_sorted),
            "taker_220ms_share_fill_pct": round(100.0 * tk220_fill_tot / max(1.0, tk220_req_tot), 1),
            "maker_fifo_share_fill_pct": round(100.0 * mk_fill_fifo_tot / max(1.0, mk_req_tot), 1),
        }

    return {
        "n_episodes": n_ep,
        "episodes_per_day": round(n_ep / max(1, len(dates_sorted)), 2),
        "uncond_win_rate_pct": round(100.0 * uncond_win_rate, 2),
        "avg_entry_ask": round(avg_ask, 4),
        "avg_entry_bid": round(avg_bid, 4),
        "gross_prob_minus_ask_pp": round(100.0 * (uncond_win_rate - avg_ask), 2),
        "gross_prob_minus_bid_pp": round(100.0 * (uncond_win_rate - avg_bid), 2),
        "avg_tau_s": round(avg_tau, 1),
        "taker_latency_curve_25usd": taker_curve,
        "maker_variants_25usd": maker_variants,
        "maker_post_cancel_sensitivity_25usd": maker_lat_sens,
        "hybrid_variants_25usd": hybrid_variants,
        "capacity_profile_by_usd": capacity_profile,
    }


def main() -> None:
    print("[04_agg] Loading decision_grid.parquet...")
    tbl = pq.read_table(WORK_DIR / "decision_grid.parquet")
    rows = tbl.to_pylist()
    print(f"[04_agg] Loaded {len(rows)} market-checkpoint rows.")

    disc_rows = [r for r in rows if r["split"] == "DISCOVERY"]
    oos_rows = [r for r in rows if r["split"] == "OOS"]

    disc_dates = sorted({r["utc_date"] for r in disc_rows})
    oos_dates = sorted({r["utc_date"] for r in oos_rows})
    all_dates = sorted({r["utc_date"] for r in rows})
    weeks_sorted = sorted({r["week_label"] for r in rows}, key=lambda w: int(w[1:]))

    # -------------------------------------------------------------------------
    # 1. Microstructure Profile by Remaining Tau Window
    # -------------------------------------------------------------------------
    tau_bands = [
        ("60-45s", 45, 60),
        ("44-30s", 30, 44),
        ("29-15s", 15, 29),
        ("14-6s", 6, 14),
        ("5-2s", 2, 5),
    ]
    micro_by_tau = {}
    for lbl, t_lo, t_hi in tau_bands:
        sub = [r for r in rows if t_lo <= r["tau_s"] <= t_hi]
        spreads = [r["spread"] for r in sub]
        depths = [r["depth_1c_ask"] for r in sub]
        bid_qs = [r["bid_q_ahead"] for r in sub]
        ws_lats = [r["ws_lat_ms"] for r in sub if r["ws_lat_ms"] >= 0]
        micro_by_tau[lbl] = {
            "n_obs": len(sub),
            "spread_p50": round(quantile(spreads, 0.50), 4),
            "spread_p90": round(quantile(spreads, 0.90), 4),
            "depth1c_ask_p50_sh": round(quantile(depths, 0.50), 1),
            "depth1c_ask_p25_sh": round(quantile(depths, 0.25), 1),
            "bid_queue_p50_sh": round(quantile(bid_qs, 0.50), 1),
            "ws_lat_p50_ms": round(quantile(ws_lats, 0.50), 1),
            "ws_lat_p95_ms": round(quantile(ws_lats, 0.95), 1),
        }

    # -------------------------------------------------------------------------
    # 2. Non-Parametric Empirical Calibration Surface: (tau_band x z_tot_band)
    #    Shows raw `P(won)` vs `avg(best_ask)` and `avg(best_bid)` without ANY model!
    # -------------------------------------------------------------------------
    z_bands = [
        ("0.5-1.0", 0.5, 1.0),
        ("1.0-1.5", 1.0, 1.5),
        ("1.5-2.0", 1.5, 2.0),
        ("2.0-3.0", 2.0, 3.0),
        ("3.0+", 3.0, 1e9),
    ]
    calibration_grid = {}
    for t_lbl, t_lo, t_hi in tau_bands:
        calibration_grid[t_lbl] = {}
        for z_lbl, z_lo, z_hi in z_bands:
            cell = {}
            for sp_name, src_rows in (("DISCOVERY", disc_rows), ("OOS", oos_rows)):
                sub = [
                    r for r in src_rows
                    if t_lo <= r["tau_s"] <= t_hi
                    and z_lo <= r["z_tot_abs"] < z_hi
                    and 0.35 <= r["best_ask"] <= 0.96
                ]
                if len(sub) >= 25:
                    wr = sum(1 for r in sub if r["won"]) / len(sub)
                    a_ask = sum(r["best_ask"] for r in sub) / len(sub)
                    a_bid = sum(r["best_bid"] for r in sub) / len(sub)
                    tk220_ev = sum(r["tk_tight_pnl_25_220"] for r in sub) / len(sub)
                    tk1000_ev = sum(r["tk_tight_pnl_25_1000"] for r in sub) / len(sub)
                    mk_fifo_ev = sum(r["mk_imp1_cx30_fifo_pnl0_25"] for r in sub) / len(sub)
                    mk_low_ev = sum(r["mk_imp1_cx30_low_pnl0_25"] for r in sub) / len(sub)
                    cell[sp_name] = {
                        "n": len(sub),
                        "emp_win_pct": round(100.0 * wr, 2),
                        "avg_ask_pct": round(100.0 * a_ask, 2),
                        "avg_bid_pct": round(100.0 * a_bid, 2),
                        "edge_vs_ask_pp": round(100.0 * (wr - a_ask), 2),
                        "edge_vs_bid_pp": round(100.0 * (wr - a_bid), 2),
                        "tk220_ev25": round(tk220_ev, 3),
                        "tk1000_ev25": round(tk1000_ev, 3),
                        "mk_fifo_ev25": round(mk_fifo_ev, 3),
                        "mk_low_ev25": round(mk_low_ev, 3),
                    }
            calibration_grid[t_lbl][z_lbl] = cell

    # -------------------------------------------------------------------------
    # 2b. Discovery-Only Monotone Empirical Calibration Lookup `P_disc(z_tot_abs)`
    #     Strictly fit on `disc_rows` (`<= 2026-11-12`) with zero OOS leakage!
    # -------------------------------------------------------------------------
    z_cal_edges = [0.0, 0.25, 0.50, 0.75, 1.00, 1.25, 1.50, 1.75, 2.00, 2.50, 3.00, 4.00, 1e9]
    disc_cal_bins: List[Tuple[float, float, float, int]] = []
    running_max_p = 0.50
    for idx_b in range(len(z_cal_edges) - 1):
        zlo, zhi = z_cal_edges[idx_b], z_cal_edges[idx_b + 1]
        sub_c = [r for r in disc_rows if zlo <= r["z_tot_abs"] < zhi]
        raw_p = (sum(1 for r in sub_c if r["won"]) / len(sub_c)) if sub_c else running_max_p
        mono_p = max(running_max_p, raw_p)
        running_max_p = mono_p
        disc_cal_bins.append((zlo, zhi, round(mono_p, 5), len(sub_c)))

    def lookup_disc_prob(z_val: float) -> float:
        for zlo, zhi, p_val, _ in disc_cal_bins:
            if zlo <= z_val < zhi:
                return p_val
        return disc_cal_bins[-1][2]

    for r in rows:
        p_c = lookup_disc_prob(r["z_tot_abs"])
        r["p_disc_cal"] = p_c
        r["edge_vs_ask"] = p_c - r["best_ask"]
        r["edge_vs_bid"] = p_c - r["best_bid"]

    # -------------------------------------------------------------------------
    # 3. Define First-Principles Candidate Opportunity Pockets (Strictly Fixed Before OOS!)
    #    Each pocket enforces `<= 1` trade per 15m market.
    # -------------------------------------------------------------------------
    pockets: Dict[str, Tuple[str, Callable[[dict], bool]]] = {
        "M1_calibrated_mispricing_5c": (
            "Discovery-Calibrated Mispricing (EdgeAsk >= +5c, z >= 0.5, tau in [6..55s], all prices)",
            lambda r: 6 <= r["tau_s"] <= 55 and r["z_tot_abs"] >= 0.5 and r["edge_vs_ask"] >= 0.05,
        ),
        "M2_midprice_sweetspot_5c": (
            "Convex Mid-Price Lock-In Sweet Spot (ask in [0.40..0.82], EdgeAsk >= +4c, z >= 0.5, tau in [6..55s])",
            lambda r: 6 <= r["tau_s"] <= 55 and r["z_tot_abs"] >= 0.5 and 0.40 <= r["best_ask"] <= 0.82 and r["edge_vs_ask"] >= 0.04,
        ),
        "M3_high_prob_82_95_tail_trap": (
            "High-Price Negative-Skew Zone (ask in (0.82..0.95], EdgeBid >= +4c, z >= 1.5, tau in [6..50s] — Tail Risk Test)",
            lambda r: 6 <= r["tau_s"] <= 50 and r["z_tot_abs"] >= 1.5 and 0.82 < r["best_ask"] <= 0.95 and r["edge_vs_bid"] >= 0.04,
        ),
        "P1_broad_lock_in": (
            "Broad TWAP+CEX Lock-In: tau in [8..50s], z_total >= 1.5, ask in [0.50..0.90], spread <= 0.04",
            lambda r: 8 <= r["tau_s"] <= 50 and r["z_tot_abs"] >= 1.5 and 0.50 <= r["best_ask"] <= 0.90 and r["spread"] <= 0.04,
        ),
        "P2_deep_cushion_mispricing": (
            "Deep Lock-In Cushion Mispricing: tau in [6..45s], z_total >= 2.0, ask <= 0.88 (market underpricing >=95% lock-in)",
            lambda r: 6 <= r["tau_s"] <= 45 and r["z_tot_abs"] >= 2.0 and 0.45 <= r["best_ask"] <= 0.88 and r["spread"] <= 0.04,
        ),
        "P3_cex_impulse_lead": (
            "Fresh CEX Latency-Lead Impulse: tau in [10..55s], z_total >= 1.5, cex_lead_favor_usd >= $6.0, ask in [0.45..0.88]",
            lambda r: 10 <= r["tau_s"] <= 55 and r["z_tot_abs"] >= 1.5 and r["cex_lead_favor_usd"] >= 6.0 and 0.45 <= r["best_ask"] <= 0.88,
        ),
        "P4_spot_vs_twap_divergence": (
            "Spot-vs-TWAP Divergence Trap: banked TWAP leads (z_ora_signed >= 1.0 & z_total >= 1.0) while ask <= 0.82 in tau in [4..35s]",
            lambda r: 4 <= r["tau_s"] <= 35 and r["z_ora_signed"] >= 1.0 and r["z_tot_abs"] >= 1.0 and 0.40 <= r["best_ask"] <= 0.82,
        ),
    }

    pocket_results = {}
    weekly_evolution = {}

    for p_key, (p_desc, pred) in pockets.items():
        ep_disc = select_first_episode_per_market(disc_rows, pred)
        ep_oos = select_first_episode_per_market(oos_rows, pred)
        ep_all = select_first_episode_per_market(rows, pred)

        pocket_results[p_key] = {
            "description": p_desc,
            "DISCOVERY": evaluate_pocket_comprehensive(ep_disc, disc_dates),
            "OOS": evaluate_pocket_comprehensive(ep_oos, oos_dates),
            "FULL_46D": evaluate_pocket_comprehensive(ep_all, all_dates),
        }

        # Weekly breakdown W1..W7 ($25 size)
        wk_map = {}
        for w in weeks_sorted:
            w_dates = sorted({r["utc_date"] for r in rows if r["week_label"] == w})
            w_eps = [r for r in ep_all if r["week_label"] == w]
            if not w_eps:
                wk_map[w] = {"n_episodes": 0}
                continue
            tk220_tot = sum(r["tk_tight_pnl_25_220"] for r in w_eps)
            tk1000_tot = sum(r["tk_tight_pnl_25_1000"] for r in w_eps)
            mk_low_tot = sum(r["mk_imp1_cx30_low_pnl0_25"] for r in w_eps)
            mk_fifo0_tot = sum(r["mk_imp1_cx30_fifo_pnl0_25"] for r in w_eps)
            mk_fiforeb_tot = sum(r["mk_imp1_cx30_fifo_pnlreb_25"] for r in w_eps)
            hyb_tot = sum(r["hyb_fifo_pnl0_w2000_tk220"] for r in w_eps)
            wr = sum(1 for r in w_eps if r["won"]) / len(w_eps)
            wk_map[w] = {
                "n_days": len(w_dates),
                "n_episodes": len(w_eps),
                "win_rate_pct": round(100.0 * wr, 1),
                "tk220_pnl": round(tk220_tot, 2),
                "tk1000_pnl": round(tk1000_tot, 2),
                "mk_low_0fee_pnl": round(mk_low_tot, 2),
                "mk_fifo_0fee_pnl": round(mk_fifo0_tot, 2),
                "mk_fifo_reb_pnl": round(mk_fiforeb_tot, 2),
                "hyb_w2s_tk220_pnl": round(hyb_tot, 2),
            }
        weekly_evolution[p_key] = wk_map

    # -------------------------------------------------------------------------
    # 4. Quantify Pure CEX Nowcast Lift vs Oracle-Only (`z_total >= 2.0` vs `z_ora_signed >= 2.0`)
    # -------------------------------------------------------------------------
    ep_ora_only_oos = select_first_episode_per_market(
        oos_rows,
        lambda r: 6 <= r["tau_s"] <= 45 and r["z_ora_signed"] >= 2.0 and 0.45 <= r["best_ask"] <= 0.88 and r["spread"] <= 0.04,
    )
    ep_cex_only_diff_oos = select_first_episode_per_market(
        oos_rows,
        lambda r: 6 <= r["tau_s"] <= 45 and r["z_tot_abs"] >= 2.0 and r["z_ora_signed"] < 2.0 and 0.45 <= r["best_ask"] <= 0.88 and r["spread"] <= 0.04,
    )
    cex_lift_oos = {
        "oracle_only_z2_oos": evaluate_pocket_comprehensive(ep_ora_only_oos, oos_dates),
        "incremental_cex_unlocked_episodes_z_ora_lt_2_z_tot_ge_2_oos": evaluate_pocket_comprehensive(ep_cex_only_diff_oos, oos_dates),
    }

    final_output = {
        "dataset_dates": {
            "discovery_dates": [disc_dates[0], disc_dates[-1], len(disc_dates)],
            "oos_dates": [oos_dates[0], oos_dates[-1], len(oos_dates)],
            "total_days": len(all_dates),
        },
        "discovery_monotone_calibration": [
            {"z_lo": b[0], "z_hi": b[1] if b[1] < 100 else 999.0, "p_win": b[2], "n_disc": b[3]}
            for b in disc_cal_bins
        ],
        "microstructure_by_tau": micro_by_tau,
        "empirical_calibration_grid": calibration_grid,
        "pocket_results": pocket_results,
        "weekly_evolution_25usd": weekly_evolution,
        "cex_lead_incremental_lift_oos": cex_lift_oos,
    }

    out_path = WORK_DIR / "forensics_results.json"
    out_path.write_text(json.dumps(final_output, indent=2), encoding="utf-8")
    print(f"[04_agg] Wrote complete forensic tables to {out_path}.")


if __name__ == "__main__":
    main()
