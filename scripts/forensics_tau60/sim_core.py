"""
Core Level-2 orderbook reconstructor and Taker/Maker execution simulator for
Polymarket BTC Up/Down 15m forensic opportunity analysis (tau < 60s).

Implements exact Polymarket CTF binary complementarity (P_up + P_down = 1.0)
without double-counting mirrored levels, per-side monotonic server_ts filtering,
non-fabricating BBO hint pruning (event_code = 4), parabolic taker fees,
maker rebates, and two-tier passive Maker fill inference (strict Price-Through
lower bound + event_code=3 trade-correlated FIFO queue model).
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

MAX_TICK = 10_000
TICK_INV = 1e-4
MIN_CLOB_SHARES = 5
MIN_CLOB_NOTIONAL_USD = 1.0
DEFAULT_TAKER_FEE_RATE = 0.07
MAKER_REBATE_SHARE_OF_TAKER_FEE = 0.20
TRADE_CORRELATION_WINDOW_MS = 15
EPS = 1e-9


def price_to_tick(price: float) -> int:
    """Converts a decimal contract price in [0.0, 1.0] to a 1-bps integer tick in [0, 10_000]."""
    if not math.isfinite(price) or price <= 0.0:
        return 0
    if price >= 1.0:
        return MAX_TICK
    return int(round(price * MAX_TICK))


def taker_fee_usd(shares: float, avg_price: float, fee_rate: float = DEFAULT_TAKER_FEE_RATE) -> float:
    """Computes Polymarket parabolic crypto taker fee: shares * rate * p * (1 - p)."""
    if shares <= 0.0 or avg_price <= 0.0 or avg_price >= 1.0:
        return 0.0
    return shares * fee_rate * avg_price * (1.0 - avg_price)


def maker_rebate_usd(
    shares: float,
    avg_price: float,
    fee_rate: float = DEFAULT_TAKER_FEE_RATE,
    rebate_share: float = MAKER_REBATE_SHARE_OF_TAKER_FEE,
) -> float:
    """Computes estimated Polymarket crypto Maker Rebate (20% of symmetric taker fee)."""
    if shares <= 0.0 or avg_price <= 0.0 or avg_price >= 1.0:
        return 0.0
    return shares * fee_rate * rebate_share * avg_price * (1.0 - avg_price)


def compute_target_whole_shares(target_notional_usd: float, ref_price: float) -> int:
    """
    Computes normalized integer share size for target USD notional at ref_price,
    enforcing Polymarket CLOB minimums (>= 5 whole shares and >= $1.00 notional).
    Returns 0 if constraints cannot be met.
    """
    if not math.isfinite(target_notional_usd) or target_notional_usd <= 0.0:
        return 0
    if not math.isfinite(ref_price) or ref_price <= 0.0 or ref_price >= 1.0:
        return 0
    whole_shares = int(math.floor((target_notional_usd + 1e-9) / ref_price))
    if whole_shares < MIN_CLOB_SHARES:
        return 0
    if whole_shares * ref_price + 1e-9 < MIN_CLOB_NOTIONAL_USD:
        return 0
    return whole_shares


@dataclass(frozen=True)
class FakSweepResult:
    """Outcome of a simulated marketable FAK BUY sweep against the L2 book."""

    executed_shares: float
    avg_price: float
    fee_usd: float
    rejected_no_match: bool


@dataclass(frozen=True)
class MakerSimResult:
    """Outcome of a simulated passive Post-Only Maker BUY order."""

    status: str  # "POST_ONLY_REJECTED", "RESTED_UNFILLED", "FILLED", "CANCELLED"
    bid_price: float
    requested_shares: int
    filled_shares_lower: float
    filled_shares_fifo: float
    cancelled_by_cex: bool
    cancel_beat_adverse_fill_lower: bool
    cancel_beat_adverse_fill_fifo: bool


class ForensicOrderBook:
    """
    Canonical sparse-dict Level-2 orderbook matching PolymarketOrderBook rules:
    - Separate explicit maps for UP bids/asks and DOWN bids/asks on ticks 0..10_000.
    - Per-side monotonic server_ts filtering.
    - Non-fabricating BBO hint pruning (event_code = 4).
    - Symmetric binary complementarity fallback when one token side's explicit book is empty,
      never summing mirrored levels across UP and DOWN simultaneously.
    """

    def __init__(self) -> None:
        self.up_bids: Dict[int, float] = {}
        self.up_asks: Dict[int, float] = {}
        self.down_bids: Dict[int, float] = {}
        self.down_asks: Dict[int, float] = {}
        self.last_up_server_ts: int = -1
        self.last_down_server_ts: int = -1
        self.last_update_ts: int = 0

    def clear(self) -> None:
        """Resets all book levels and watermarks."""
        self.up_bids.clear()
        self.up_asks.clear()
        self.down_bids.clear()
        self.down_asks.clear()
        self.last_up_server_ts = -1
        self.last_down_server_ts = -1
        self.last_update_ts = 0

    def try_accept_server_ts(self, is_up: bool, server_ts: int) -> bool:
        """Enforces per-token monotonic server_ts filtering."""
        if server_ts <= 0:
            return True
        if is_up:
            if server_ts < self.last_up_server_ts:
                return False
            self.last_up_server_ts = server_ts
            return True
        if server_ts < self.last_down_server_ts:
            return False
        self.last_down_server_ts = server_ts
        return True

    def apply_book_snapshot(
        self,
        is_up: bool,
        server_ts: int,
        receipt_ts: int,
        levels: List[Tuple[Optional[bool], Optional[int], float]],
    ) -> bool:
        """Replaces one token's raw bids and asks from a grouped event_code=1 frame."""
        if not self.try_accept_server_ts(is_up, server_ts):
            return False
        if receipt_ts > 0:
            self.last_update_ts = receipt_ts
        bids = self.up_bids if is_up else self.down_bids
        asks = self.up_asks if is_up else self.down_asks
        bids.clear()
        asks.clear()
        for is_bid, tick, size in levels:
            if is_bid is None or tick is None:
                continue
            if 0 <= tick <= MAX_TICK and size > EPS:
                if is_bid:
                    bids[tick] = size
                else:
                    asks[tick] = size
        return True

    def apply_price_change(
        self,
        is_up: bool,
        is_bid: bool,
        tick: int,
        size: float,
        server_ts: int,
        receipt_ts: int,
    ) -> bool:
        """Applies an incremental L2 level update (event_code=2) and prunes crossed raw levels."""
        if not self.try_accept_server_ts(is_up, server_ts):
            return False
        if tick < 0 or tick > MAX_TICK:
            return False
        if receipt_ts > 0:
            self.last_update_ts = receipt_ts
        bids = self.up_bids if is_up else self.down_bids
        asks = self.up_asks if is_up else self.down_asks
        if is_bid:
            if size > EPS:
                bids[tick] = size
                crossed = [a for a in asks if a <= tick]
                for a in crossed:
                    del asks[a]
            else:
                bids.pop(tick, None)
        else:
            if size > EPS:
                asks[tick] = size
                crossed = [b for b in bids if b >= tick]
                for b in crossed:
                    del bids[b]
            else:
                asks.pop(tick, None)
        return True

    def apply_bbo_hint(
        self,
        is_up: bool,
        is_bid_hint: bool,
        hint_tick: int,
        server_ts: int,
        receipt_ts: int,
    ) -> bool:
        """Prunes stale raw levels violating exchange-reported BBO hint (event_code=4) without fabricating depth."""
        if not self.try_accept_server_ts(is_up, server_ts):
            return False
        if hint_tick < 0 or hint_tick > MAX_TICK:
            return False
        if receipt_ts > 0:
            self.last_update_ts = receipt_ts
        bids = self.up_bids if is_up else self.down_bids
        asks = self.up_asks if is_up else self.down_asks
        if is_bid_hint:
            for b in [k for k in bids if k > hint_tick]:
                del bids[b]
            for a in [k for k in asks if k <= hint_tick]:
                del asks[a]
        else:
            for a in [k for k in asks if k < hint_tick]:
                del asks[a]
            for b in [k for k in bids if k >= hint_tick]:
                del bids[b]
        return True

    def best_bid_tick(self, is_up: bool) -> int:
        """Returns effective best bid tick for UP or DOWN (or -1 if empty)."""
        if is_up:
            if self.up_bids:
                return max(self.up_bids)
            if self.down_asks:
                return max(1, MAX_TICK - min(self.down_asks))
            return -1
        if self.down_bids:
            return max(self.down_bids)
        if self.up_asks:
            return max(1, MAX_TICK - min(self.up_asks))
        return -1

    def best_ask_tick(self, is_up: bool) -> int:
        """Returns effective best ask tick for UP or DOWN (or MAX_TICK + 1 if empty)."""
        if is_up:
            if self.up_asks:
                return min(self.up_asks)
            if self.down_bids:
                return max(1, MAX_TICK - max(self.down_bids))
            return MAX_TICK + 1
        if self.down_asks:
            return min(self.down_asks)
        if self.up_bids:
            return max(1, MAX_TICK - max(self.up_bids))
        return MAX_TICK + 1

    def has_valid_two_sided_quote(self) -> bool:
        """Returns True when both UP and DOWN sides have valid positive bid and ask ticks."""
        return (
            0 < self.best_bid_tick(True) <= MAX_TICK
            and 0 < self.best_ask_tick(True) <= MAX_TICK
            and 0 < self.best_bid_tick(False) <= MAX_TICK
            and 0 < self.best_ask_tick(False) <= MAX_TICK
        )

    def effective_bid_depth_at_tick(self, is_up: bool, tick: int) -> float:
        """
        Returns effective resting bid size at `tick` for `is_up` without double-counting
        Polymarket's mirrored opposite-token ask level.
        """
        if tick <= 0 or tick >= MAX_TICK:
            return 0.0
        primary = self.up_bids if is_up else self.down_bids
        comp_asks = self.down_asks if is_up else self.up_asks
        if primary:
            return primary.get(tick, 0.0)
        return comp_asks.get(MAX_TICK - tick, 0.0)

    def depth_1c_ask(self, is_up: bool) -> float:
        """Returns cumulative ask depth within 1 cent (100 bps) of effective best ask."""
        b_ask = self.best_ask_tick(is_up)
        if b_ask > MAX_TICK:
            return 0.0
        max_t = min(MAX_TICK, b_ask + 100)
        primary_asks = self.up_asks if is_up else self.down_asks
        if primary_asks:
            return sum(sz for t, sz in primary_asks.items() if b_ask <= t <= max_t)
        comp_bids = self.down_bids if is_up else self.up_bids
        min_comp = max(0, MAX_TICK - max_t)
        max_comp = MAX_TICK - b_ask
        return sum(sz for t, sz in comp_bids.items() if min_comp <= t <= max_comp)

    def simulate_fak_buy(
        self,
        is_up: bool,
        requested_shares: int,
        limit_tick: int,
        fee_rate: float = DEFAULT_TAKER_FEE_RATE,
    ) -> FakSweepResult:
        """
        Evaluates an atomic FAK BUY sweep up to `limit_tick` without mutating book state
        (suitable for evaluating independent candidate delays at a single historical point).
        """
        if requested_shares <= 0 or limit_tick <= 0:
            return FakSweepResult(0.0, 0.0, 0.0, True)
        max_t = min(MAX_TICK, limit_tick)
        rem = float(requested_shares)
        cost = 0.0
        filled = 0.0

        primary_asks = self.up_asks if is_up else self.down_asks
        if primary_asks:
            for t in sorted(primary_asks):
                if t > max_t:
                    break
                avail = primary_asks[t]
                if avail <= EPS:
                    continue
                take = min(rem, avail)
                cost += take * (t * TICK_INV)
                filled += take
                rem -= take
                if rem <= EPS:
                    break
        else:
            comp_bids = self.down_bids if is_up else self.up_bids
            for comp_t in sorted(comp_bids, reverse=True):
                eff_ask_t = max(1, MAX_TICK - comp_t)
                if eff_ask_t > max_t:
                    break
                avail = comp_bids[comp_t]
                if avail <= EPS:
                    continue
                take = min(rem, avail)
                cost += take * (eff_ask_t * TICK_INV)
                filled += take
                rem -= take
                if rem <= EPS:
                    break

        if filled <= EPS:
            return FakSweepResult(0.0, 0.0, 0.0, True)
        avg_p = cost / filled
        fee = taker_fee_usd(filled, avg_p, fee_rate)
        return FakSweepResult(filled, avg_p, fee, False)


@dataclass(frozen=True)
class BookTimelinePoint:
    """Compact snapshot of L2 state after applying a distinct timestamp batch in tau <= 75s."""

    receipt_ts: int
    server_ts: int
    best_bid_up: int
    best_ask_up: int
    best_bid_down: int
    best_ask_down: int
    up_asks_sorted: Tuple[Tuple[int, float], ...]
    down_asks_sorted: Tuple[Tuple[int, float], ...]
    up_bids_map: Dict[int, float]
    down_bids_map: Dict[int, float]


def build_book_timeline(
    clob_rows: List[Tuple[int, int, int, int, Optional[bool], Optional[bool], Optional[int], Optional[float]]],
    window_start_ms: int,
    window_end_ms: int,
) -> Tuple[List[BookTimelinePoint], List[int], List[int]]:
    """
    Replays sorted CLOB Parquet rows `(ts, server_ts, frame_id, event_code, is_up, is_bid, tick, size)`
    from pre-window snapshot seed through `window_end_ms`, emitting:
      1. `timeline`: chronological `BookTimelinePoint` states inside `[window_start_ms, window_end_ms]`
         (seeded with initial state at `window_start_ms` if available),
      2. `trade_server_ts_sorted`: sorted `server_ts` values of all `event_code == 3` (`last_trade_price`),
      3. `trade_receipt_ts_sorted`: sorted `ts` values of all `event_code == 3` (`last_trade_price`).
    """
    book = ForensicOrderBook()
    trade_server_ts: List[int] = []
    trade_receipt_ts: List[int] = []
    timeline: List[BookTimelinePoint] = []
    seeded_emitted = False

    n = len(clob_rows)
    i = 0
    while i < n:
        ts, server_ts, frame_id, ev_code, is_up, is_bid, tick, size = clob_rows[i]
        if ts > window_end_ms:
            break

        if ts >= window_start_ms and not seeded_emitted:
            if book.has_valid_two_sided_quote():
                timeline.append(_snapshot_point(book, window_start_ms, max(book.last_up_server_ts, book.last_down_server_ts)))
            seeded_emitted = True

        # Group all rows belonging to the same (ts, frame_id, event_code)
        j = i + 1
        while j < n and clob_rows[j][0] == ts and clob_rows[j][2] == frame_id and clob_rows[j][3] == ev_code:
            j += 1

        mutated = False
        if ev_code == 1:
            # Book snapshot may contain UP or DOWN levels (split per is_up just in case)
            up_lvls: List[Tuple[Optional[bool], Optional[int], float]] = []
            dn_lvls: List[Tuple[Optional[bool], Optional[int], float]] = []
            has_up = False
            has_dn = False
            for k in range(i, j):
                _, _, _, _, r_up, r_bid, r_tick, r_size = clob_rows[k]
                if r_up is True:
                    has_up = True
                    up_lvls.append((r_bid, r_tick, float(r_size or 0.0)))
                elif r_up is False:
                    has_dn = True
                    dn_lvls.append((r_bid, r_tick, float(r_size or 0.0)))
            if has_up and book.apply_book_snapshot(True, server_ts, ts, up_lvls):
                mutated = True
            if has_dn and book.apply_book_snapshot(False, server_ts, ts, dn_lvls):
                mutated = True
        elif ev_code == 2:
            for k in range(i, j):
                _, r_srv, _, _, r_up, r_bid, r_tick, r_size = clob_rows[k]
                if r_up is not None and r_bid is not None and r_tick is not None:
                    if book.apply_price_change(r_up, r_bid, int(r_tick), float(r_size or 0.0), r_srv, ts):
                        mutated = True
        elif ev_code == 4:
            for k in range(i, j):
                _, r_srv, _, _, r_up, r_bid, r_tick, _ = clob_rows[k]
                if r_up is not None and r_bid is not None and r_tick is not None:
                    if book.apply_bbo_hint(r_up, r_bid, int(r_tick), r_srv, ts):
                        mutated = True
        elif ev_code == 3:
            if server_ts > 0:
                trade_server_ts.append(server_ts)
            trade_receipt_ts.append(ts)

        if ts >= window_start_ms and mutated and book.has_valid_two_sided_quote():
            if timeline and timeline[-1].receipt_ts == ts:
                timeline[-1] = _snapshot_point(book, ts, server_ts)
            else:
                timeline.append(_snapshot_point(book, ts, server_ts))

        i = j

    trade_server_ts.sort()
    trade_receipt_ts.sort()
    return timeline, trade_server_ts, trade_receipt_ts


def _snapshot_point(book: ForensicOrderBook, receipt_ts: int, server_ts: int) -> BookTimelinePoint:
    """Creates an immutable timeline point from the current book state without double-counting."""
    b_bid_u = book.best_bid_tick(True)
    b_ask_u = book.best_ask_tick(True)
    b_bid_d = book.best_bid_tick(False)
    b_ask_d = book.best_ask_tick(False)

    if book.up_asks:
        up_asks_s = tuple(sorted(book.up_asks.items()))
    else:
        up_asks_s = tuple(
            (max(1, MAX_TICK - ct), sz)
            for ct, sz in sorted(book.down_bids.items(), reverse=True)
        )

    if book.down_asks:
        dn_asks_s = tuple(sorted(book.down_asks.items()))
    else:
        dn_asks_s = tuple(
            (max(1, MAX_TICK - ct), sz)
            for ct, sz in sorted(book.up_bids.items(), reverse=True)
        )

    if book.up_bids:
        up_bids_m = dict(book.up_bids)
    else:
        up_bids_m = {max(1, MAX_TICK - ct): sz for ct, sz in book.down_asks.items()}

    if book.down_bids:
        dn_bids_m = dict(book.down_bids)
    else:
        dn_bids_m = {max(1, MAX_TICK - ct): sz for ct, sz in book.up_asks.items()}

    return BookTimelinePoint(
        receipt_ts=receipt_ts,
        server_ts=server_ts,
        best_bid_up=b_bid_u,
        best_ask_up=b_ask_u,
        best_bid_down=b_bid_d,
        best_ask_down=b_ask_d,
        up_asks_sorted=up_asks_s,
        down_asks_sorted=dn_asks_s,
        up_bids_map=up_bids_m,
        down_bids_map=dn_bids_m,
    )


def find_point_at_or_before(
    timeline: List[BookTimelinePoint],
    timeline_ts: List[int],
    target_ts: int,
) -> Optional[BookTimelinePoint]:
    """Returns the latest BookTimelinePoint with `receipt_ts <= target_ts`, or None."""
    idx = bisect.bisect_right(timeline_ts, target_ts) - 1
    if idx < 0:
        return None
    return timeline[idx]


def sweep_point_fak_buy(
    pt: Optional[BookTimelinePoint],
    is_up: bool,
    requested_shares: int,
    limit_tick: int,
    fee_rate: float = DEFAULT_TAKER_FEE_RATE,
) -> FakSweepResult:
    """Evaluates an atomic FAK BUY sweep against a precomputed BookTimelinePoint."""
    if pt is None or requested_shares <= 0 or limit_tick <= 0:
        return FakSweepResult(0.0, 0.0, 0.0, True)
    asks = pt.up_asks_sorted if is_up else pt.down_asks_sorted
    max_t = min(MAX_TICK, limit_tick)
    rem = float(requested_shares)
    cost = 0.0
    filled = 0.0
    for t, avail in asks:
        if t > max_t:
            break
        if avail <= EPS:
            continue
        take = min(rem, avail)
        cost += take * (t * TICK_INV)
        filled += take
        rem -= take
        if rem <= EPS:
            break
    if filled <= EPS:
        return FakSweepResult(0.0, 0.0, 0.0, True)
    avg_p = cost / filled
    return FakSweepResult(filled, avg_p, taker_fee_usd(filled, avg_p, fee_rate), False)


def _has_nearby_trade(
    trade_server_ts: List[int],
    trade_receipt_ts: List[int],
    event_server_ts: int,
    event_receipt_ts: int,
    window_ms: int = TRADE_CORRELATION_WINDOW_MS,
) -> bool:
    """Checks whether an `event_code == 3` (`last_trade_price`) occurred within `+-window_ms`."""
    if event_server_ts > 0 and trade_server_ts:
        lo = bisect.bisect_left(trade_server_ts, event_server_ts - window_ms)
        if lo < len(trade_server_ts) and trade_server_ts[lo] <= event_server_ts + window_ms:
            return True
    if trade_receipt_ts:
        lo2 = bisect.bisect_left(trade_receipt_ts, event_receipt_ts - (window_ms + 5))
        if lo2 < len(trade_receipt_ts) and trade_receipt_ts[lo2] <= event_receipt_ts + (window_ms + 5):
            return True
    return False


def simulate_passive_maker_buy(
    timeline: List[BookTimelinePoint],
    timeline_ts: List[int],
    trade_server_ts: List[int],
    trade_receipt_ts: List[int],
    is_up: bool,
    bid_tick: int,
    requested_shares: int,
    submit_ts: int,
    post_latency_ms: int,
    end_ts: int,
    cex_abort_ts: Optional[int] = None,
    cancel_latency_ms: int = 30,
) -> MakerSimResult:
    """
    Simulates a passive Post-Only Maker BUY order on `timeline` under BOTH:
      1. Conservative Lower Bound (`filled_shares_lower`): fills strictly when effective
         `best_ask <= bid_tick` OR the entire resting level at `bid_tick` is wiped to 0
         alongside a nearby `event_code=3` trade.
      2. Realistic Trade-Correlated FIFO Bound (`filled_shares_fifo`): records `queue_ahead`
         at `live_ts = submit_ts + post_latency_ms`; consumes queue and fills our order on
         depth drops at `bid_tick` (when at/above previous best bid) correlated with
         `event_code=3`, while clamping `queue_ahead = min(queue_ahead, new_depth)` on pure cancels.

    Also tracks CEX Cancel Race when `cex_abort_ts` is supplied (`cancel_effective_ts = cex_abort_ts + cancel_latency_ms`).
    """
    bid_price = bid_tick * TICK_INV
    if requested_shares <= 0 or bid_tick <= 0 or bid_tick >= MAX_TICK:
        return MakerSimResult("POST_ONLY_REJECTED", bid_price, requested_shares, 0.0, 0.0, False, False, False)

    live_ts = submit_ts + max(0, post_latency_ms)
    if live_ts >= end_ts:
        return MakerSimResult("RESTED_UNFILLED", bid_price, requested_shares, 0.0, 0.0, False, False, False)

    pt_live = find_point_at_or_before(timeline, timeline_ts, live_ts)
    if pt_live is None:
        return MakerSimResult("POST_ONLY_REJECTED", bid_price, requested_shares, 0.0, 0.0, False, False, False)

    cur_ask = pt_live.best_ask_up if is_up else pt_live.best_ask_down
    if cur_ask <= bid_tick:
        return MakerSimResult("POST_ONLY_REJECTED", bid_price, requested_shares, 0.0, 0.0, False, False, False)

    bids_live = pt_live.up_bids_map if is_up else pt_live.down_bids_map
    asks_live = pt_live.up_asks_sorted if is_up else pt_live.down_asks_sorted
    prev_bids_map = bids_live
    prev_asks_map = dict(asks_live)
    prev_depth = bids_live.get(bid_tick, 0.0)
    prev_best_bid = pt_live.best_bid_up if is_up else pt_live.best_bid_down
    queue_ahead = prev_depth

    cancel_eff_ts: Optional[int] = None
    if cex_abort_ts is not None:
        cancel_eff_ts = max(live_ts, cex_abort_ts + max(0, cancel_latency_ms))

    active_stop_ts = min(end_ts, cancel_eff_ts) if cancel_eff_ts is not None else end_ts

    filled_lower = 0.0
    filled_fifo = 0.0
    req_f = float(requested_shares)

    start_idx = bisect.bisect_right(timeline_ts, live_ts)
    n = len(timeline)

    for idx in range(start_idx, n):
        pt = timeline[idx]
        if pt.receipt_ts > active_stop_ts:
            break

        b_ask = pt.best_ask_up if is_up else pt.best_ask_down
        b_bid = pt.best_bid_up if is_up else pt.best_bid_down
        bids_map = pt.up_bids_map if is_up else pt.down_bids_map
        asks_sorted = pt.up_asks_sorted if is_up else pt.down_asks_sorted
        asks_map = dict(asks_sorted)
        new_depth = bids_map.get(bid_tick, 0.0)
        has_trade = _has_nearby_trade(trade_server_ts, trade_receipt_ts, pt.server_ts, pt.receipt_ts)

        # 1. Process depletion AT `bid_tick` against `queue_ahead` FIRST (before zeroing queue on cross!)
        at_tick_my_fill = 0.0
        if new_depth < prev_depth - EPS:
            delta = prev_depth - new_depth
            was_exposed_at_top = (bid_tick >= prev_best_bid) or (b_ask <= bid_tick)
            if (has_trade or b_ask <= bid_tick) and was_exposed_at_top:
                consume_q = min(queue_ahead, delta)
                queue_ahead -= consume_q
                at_tick_my_fill = max(0.0, delta - consume_q)
            else:
                if new_depth < queue_ahead:
                    queue_ahead = new_depth

        # 2. Exact Price-Time Priority Volume Conservation strictly below / across `bid_tick`:
        #    No seller can trade against bids `< bid_tick` or rest an ask `<= bid_tick`
        #    without first exhausting `queue_ahead` and matching our resting bid at `bid_tick`.
        below_bid_swept = 0.0
        if b_bid < bid_tick and has_trade:
            for tk, old_sz in prev_bids_map.items():
                if tk < bid_tick:
                    cur_sz = bids_map.get(tk, 0.0)
                    if tk >= b_bid and cur_sz < old_sz - EPS:
                        below_bid_swept += (old_sz - cur_sz)
                    elif tk > b_bid and cur_sz <= EPS:
                        below_bid_swept += old_sz

        newly_crossed_ask = 0.0
        if b_ask <= bid_tick:
            queue_ahead = 0.0
            for tk, cur_asz in asks_sorted:
                if tk > bid_tick:
                    break
                prev_asz = prev_asks_map.get(tk, 0.0)
                if cur_asz > prev_asz + EPS:
                    newly_crossed_ask += (cur_asz - prev_asz)

        provable_through_vol = below_bid_swept + newly_crossed_ask
        if provable_through_vol > EPS:
            filled_lower = min(req_f, filled_lower + provable_through_vol)

        fifo_step = at_tick_my_fill + provable_through_vol
        if fifo_step > EPS:
            filled_fifo = min(req_f, filled_fifo + fifo_step)
        if filled_fifo < filled_lower:
            filled_fifo = filled_lower

        prev_bids_map = bids_map
        prev_asks_map = asks_map
        prev_depth = new_depth
        prev_best_bid = b_bid
        if filled_lower >= req_f - EPS and filled_fifo >= req_f - EPS:
            break

    cancelled_by_cex = cancel_eff_ts is not None and cancel_eff_ts < end_ts
    beat_lower = False
    beat_fifo = False

    # Check counterfactual: if CEX cancelled our order before full fill, would we have been
    # filled between `active_stop_ts` and `end_ts` had we NOT cancelled?
    if cancelled_by_cex and (filled_lower < req_f - EPS or filled_fifo < req_f - EPS):
        cf_res = simulate_passive_maker_buy(
            timeline=timeline,
            timeline_ts=timeline_ts,
            trade_server_ts=trade_server_ts,
            trade_receipt_ts=trade_receipt_ts,
            is_up=is_up,
            bid_tick=bid_tick,
            requested_shares=requested_shares,
            submit_ts=submit_ts,
            post_latency_ms=post_latency_ms,
            end_ts=end_ts,
            cex_abort_ts=None,
        )
        if cf_res.filled_shares_lower > filled_lower + EPS:
            beat_lower = True
        if cf_res.filled_shares_fifo > filled_fifo + EPS:
            beat_fifo = True

    if filled_fifo > EPS or filled_lower > EPS:
        status = "FILLED"
    elif cancelled_by_cex:
        status = "CANCELLED"
    else:
        status = "RESTED_UNFILLED"

    return MakerSimResult(
        status=status,
        bid_price=bid_price,
        requested_shares=requested_shares,
        filled_shares_lower=filled_lower,
        filled_shares_fifo=filled_fifo,
        cancelled_by_cex=cancelled_by_cex,
        cancel_beat_adverse_fill_lower=beat_lower,
        cancel_beat_adverse_fill_fifo=beat_fifo,
    )
