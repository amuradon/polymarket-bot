"""
Deterministic synthetic unit tests for `sim_core.py` verifying:
1. Binary CTF complementarity (`UP` <-> `1 - DOWN`) without double-counting mirrored levels.
2. Per-side monotonic `server_ts` filtering and non-fabricating `event_code = 4` (`bbo_hint`) pruning.
3. Polymarket order normalization (`>= 5` whole shares, `>= $1.00`), parabolic taker fee, and maker rebate.
4. Two-tier Maker fill inference:
   - Pure cancel (`no event_code = 3`) does NOT advance queue ahead optimistically or trigger fake fills.
   - Trade depletion (`with event_code = 3` within +-15 ms) advances FIFO queue and fills partial/full shares,
     while Conservative Lower Bound requires price-through or full level wipeout.
   - `Post-Only` rejection when `best_ask <= bid_tick` at `t_submit + post_latency_ms`.
   - CEX Cancel Race timing (`cex_abort_ts + cancel_latency_ms` beating vs losing to incoming aggressive trade).
"""

from __future__ import annotations

import math
import unittest

from sim_core import (
    ForensicOrderBook,
    build_book_timeline,
    compute_target_whole_shares,
    maker_rebate_usd,
    simulate_passive_maker_buy,
    sweep_point_fak_buy,
    taker_fee_usd,
)


class ForensicSimCoreTest(unittest.TestCase):

    def test_complementarity_never_double_counts_mirrored_levels(self) -> None:
        book = ForensicOrderBook()
        # Polymarket broadcasts both UP ask @ 0.60 (100 sh) and mirrored DOWN bid @ 0.40 (100 sh)
        book.apply_book_snapshot(True, 1000, 1010, [(True, 5800, 50.0), (False, 6000, 100.0)])
        book.apply_book_snapshot(False, 1000, 1010, [(True, 4000, 100.0), (False, 4200, 50.0)])

        self.assertEqual(book.best_ask_tick(True), 6000)
        self.assertEqual(book.best_bid_tick(False), 4000)
        self.assertAlmostEqual(book.depth_1c_ask(True), 100.0, places=6)

        res = book.simulate_fak_buy(is_up=True, requested_shares=150, limit_tick=6500)
        # Must cap at 100 shares (not 200 double-counted shares!)
        self.assertAlmostEqual(res.executed_shares, 100.0, places=6)
        self.assertAlmostEqual(res.avg_price, 0.60, places=6)

    def test_complementarity_fallback_when_one_side_explicit_book_empty(self) -> None:
        book = ForensicOrderBook()
        # Only DOWN bids/asks populated; UP book derives BBO & sweep via 1 - P_down
        book.apply_book_snapshot(False, 1000, 1010, [(True, 3000, 80.0), (False, 3200, 60.0)])

        self.assertEqual(book.best_ask_tick(True), 7000)
        self.assertEqual(book.best_bid_tick(True), 6800)
        res = book.simulate_fak_buy(is_up=True, requested_shares=50, limit_tick=7000)
        self.assertAlmostEqual(res.executed_shares, 50.0, places=6)
        self.assertAlmostEqual(res.avg_price, 0.70, places=6)

    def test_per_side_monotonic_server_ts_and_non_fabricating_bbo_hint(self) -> None:
        book = ForensicOrderBook()
        book.apply_book_snapshot(True, 2000, 2010, [(True, 5000, 100.0), (False, 5200, 100.0)])
        book.apply_book_snapshot(False, 1500, 2012, [(True, 4800, 100.0), (False, 5000, 100.0)])

        # Stale UP frame (server_ts 1999 < 2000) is rejected, while DOWN frame (server_ts 1600 > 1500) is accepted
        self.assertFalse(book.apply_price_change(True, False, 5100, 50.0, 1999, 2015))
        self.assertTrue(book.apply_price_change(False, True, 4900, 50.0, 1600, 2015))

        # BBO hint reporting UP best_ask = 5400 prunes stale UP ask @ 5200 without fabricating depth @ 5400
        self.assertTrue(book.apply_bbo_hint(True, False, 5400, 2005, 2020))
        self.assertNotIn(5200, book.up_asks)
        self.assertNotIn(5400, book.up_asks)

    def test_fees_rebates_and_order_minimums(self) -> None:
        self.assertEqual(compute_target_whole_shares(0.90, 0.50), 0)  # < $1.00 notional & < 5 shares
        self.assertEqual(compute_target_whole_shares(3.60, 0.90), 0)  # 4 shares < 5 minimum
        self.assertEqual(compute_target_whole_shares(10.0, 0.80), 12)  # floor(10 / 0.8) = 12 shares ($9.60)

        # At p = 0.50, 100 shares -> taker fee = 100 * 0.07 * 0.25 = $1.75; maker rebate = 20% = $0.35
        self.assertAlmostEqual(taker_fee_usd(100.0, 0.50), 1.75, places=8)
        self.assertAlmostEqual(maker_rebate_usd(100.0, 0.50), 0.35, places=8)
        self.assertTrue(math.isclose(taker_fee_usd(100.0, 0.90), 100.0 * 0.07 * 0.09, rel_tol=1e-9))

    def test_maker_fifo_distinguishes_pure_cancel_from_trade_depletion(self) -> None:
        # Build synthetic CLOB stream over [1000, 2000] ms
        rows = [
            # Initial books @ ts=900: UP 0.70 bid (100 sh) / 0.72 ask (100 sh), mirrored DOWN 0.28 bid / 0.30 ask
            (900, 890, 1, 1, True, True, 7000, 100.0),
            (900, 890, 1, 1, True, False, 7200, 100.0),
            (900, 890, 2, 1, False, True, 2800, 100.0),
            (900, 890, 2, 1, False, False, 3000, 100.0),
            # At ts=1200 (server_ts=1190): another maker adds 50 shares at 7000 (depth 100 -> 150), then cancels 60 shares @ ts=1300 (150 -> 90) WITHOUT event_code=3
            (1200, 1190, 3, 2, True, True, 7000, 150.0),
            (1300, 1290, 4, 2, True, True, 7000, 90.0),
            # At ts=1500 (server_ts=1490): real aggressive trade (event_code=3) hits 7000, dropping depth 90 -> 0 -> 15 (Wait: 90 -> 10 = 80 shares traded!)
            (1500, 1488, 5, 3, None, None, None, None),
            (1502, 1490, 6, 2, True, True, 7000, 10.0),
        ]
        timeline, tr_srv, tr_rec = build_book_timeline(rows, window_start_ms=1000, window_end_ms=2000)
        tl_ts = [p.receipt_ts for p in timeline]

        # Submit Maker BUY 30 shares @ 0.70 (tick=7000) at ts=1050, post_latency=30ms -> live at 1080ms.
        # At 1080ms, queue_ahead = 100.
        # At 1300ms (pure cancel 150 -> 90), queue_ahead clamps to 90 (zero fake fill!).
        # At 1502ms (trade 90 -> 10, delta=80 with event_code=3), queue_ahead drops 90 -> 10 => STILL 10 ahead, 0 fill!
        res_unfilled = simulate_passive_maker_buy(
            timeline, tl_ts, tr_srv, tr_rec,
            is_up=True, bid_tick=7000, requested_shares=30,
            submit_ts=1050, post_latency_ms=30, end_ts=2000,
        )
        self.assertEqual(res_unfilled.filled_shares_lower, 0.0)
        self.assertEqual(res_unfilled.filled_shares_fifo, 0.0)

        # Now add a second trade @ ts=1600 dropping depth 10 -> 2 (8 shares traded -> still 2 ahead),
        # and a third trade @ ts=1700 dropping depth 2 -> 0 (wait: if wiped to 0 and best_bid drops, lower bound triggers too;
        # test partial fill first by having maker behind us add 40 shares @ 1550 so depth is 50, then trade of 25 shares @ 1600 -> depth 25:
        # 10 ahead consumed + 15 of our 30 shares filled!)
        rows_partial = rows + [
            (1550, 1540, 7, 2, True, True, 7000, 50.0),
            (1600, 1590, 8, 3, None, None, None, None),
            (1601, 1592, 9, 2, True, True, 7000, 25.0),
        ]
        tl2, tr_s2, tr_r2 = build_book_timeline(rows_partial, 1000, 2000)
        tl2_ts = [p.receipt_ts for p in tl2]
        res_partial = simulate_passive_maker_buy(
            tl2, tl2_ts, tr_s2, tr_r2,
            is_up=True, bid_tick=7000, requested_shares=30,
            submit_ts=1050, post_latency_ms=30, end_ts=2000,
        )
        self.assertEqual(res_partial.filled_shares_lower, 0.0)
        self.assertAlmostEqual(res_partial.filled_shares_fifo, 15.0, places=6)

    def test_post_only_rejection_and_cex_cancel_race(self) -> None:
        rows = [
            (900, 890, 1, 1, True, True, 7000, 20.0),
            (900, 890, 1, 1, True, False, 7200, 50.0),
            (900, 890, 2, 1, False, True, 2800, 50.0),
            (900, 890, 2, 1, False, False, 3000, 20.0),
            # At ts=1500, market crashes through 0.70 (UP ask drops to 0.68)
            (1500, 1490, 3, 3, None, None, None, None),
            (1500, 1490, 4, 2, True, True, 7000, 0.0),
            (1500, 1490, 4, 2, True, True, 6600, 50.0),
            (1500, 1490, 4, 2, True, False, 6800, 50.0),
        ]
        tl, tr_s, tr_r = build_book_timeline(rows, 1000, 2000)
        tl_ts = [p.receipt_ts for p in tl]

        # 1. Post-Only order @ 0.72 crosses current ask 0.72 -> rejected!
        rej = simulate_passive_maker_buy(
            tl, tl_ts, tr_s, tr_r,
            is_up=True, bid_tick=7200, requested_shares=20,
            submit_ts=1050, post_latency_ms=20, end_ts=2000,
        )
        self.assertEqual(rej.status, "POST_ONLY_REJECTED")
        self.assertEqual(rej.filled_shares_fifo, 0.0)

        # 2. CEX detects crash at ts=1450; with cancel_latency=30ms -> cancelled at 1480ms BEFORE 1500ms crash!
        saved = simulate_passive_maker_buy(
            tl, tl_ts, tr_s, tr_r,
            is_up=True, bid_tick=7000, requested_shares=20,
            submit_ts=1050, post_latency_ms=20, end_ts=2000,
            cex_abort_ts=1450, cancel_latency_ms=30,
        )
        self.assertEqual(saved.status, "CANCELLED")
        self.assertEqual(saved.filled_shares_lower, 0.0)
        self.assertEqual(saved.filled_shares_fifo, 0.0)
        self.assertTrue(saved.cancel_beat_adverse_fill_lower)
        self.assertTrue(saved.cancel_beat_adverse_fill_fifo)

        # 3. Late CEX abort at ts=1485 + 30ms = 1515ms -> LOSES cancel race to 1500ms dump!
        caught = simulate_passive_maker_buy(
            tl, tl_ts, tr_s, tr_r,
            is_up=True, bid_tick=7000, requested_shares=20,
            submit_ts=1050, post_latency_ms=20, end_ts=2000,
            cex_abort_ts=1485, cancel_latency_ms=30,
        )
        self.assertEqual(caught.status, "FILLED")
        self.assertEqual(caught.filled_shares_lower, 20.0)
        self.assertEqual(caught.filled_shares_fifo, 20.0)

        # Verify sweep helper on timeline point at ts=1500
        fak = sweep_point_fak_buy(tl[-1], is_up=True, requested_shares=25, limit_tick=7000)
        self.assertAlmostEqual(fak.executed_shares, 25.0)
        self.assertAlmostEqual(fak.avg_price, 0.68)

        # 4. Large-size capacity conservation: a 50-share ask cross @ 0.68 cannot fill 300 shares!
        large = simulate_passive_maker_buy(
            tl, tl_ts, tr_s, tr_r,
            is_up=True, bid_tick=7000, requested_shares=300,
            submit_ts=1050, post_latency_ms=20, end_ts=2000,
        )
        self.assertAlmostEqual(large.filled_shares_lower, 50.0, places=6)
        self.assertAlmostEqual(large.filled_shares_fifo, 50.0, places=6)
        self.assertLessEqual(large.filled_shares_lower, large.filled_shares_fifo)


if __name__ == "__main__":
    unittest.main()
