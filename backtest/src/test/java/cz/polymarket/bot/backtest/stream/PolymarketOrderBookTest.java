package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.OrderBookQuote;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class PolymarketOrderBookTest {

    @Test
    @DisplayName("Should apply typed snapshot levels and derive complementary DOWN prices when only UP side is populated")
    void shouldApplySnapshotAndDeriveDown() {
        PolymarketOrderBook book = new PolymarketOrderBook();

        book.clearSideForSnapshot(true);
        book.setSnapshotLevel(true, true, 5100, 200.0);
        book.setSnapshotLevel(true, true, 5000, 100.0);
        book.setSnapshotLevel(true, false, 5300, 150.0);
        book.setSnapshotLevel(true, false, 5400, 250.0);
        book.finishBookSnapshot(true);

        assertThat(book.getBestBidUp()).isCloseTo(0.51, within(0.0001));
        assertThat(book.getBestAskUp()).isCloseTo(0.53, within(0.0001));

        // Down derivation via complementarity when only UP book is populated
        assertThat(book.getBestBidDown()).isCloseTo(0.47, within(0.001));
        assertThat(book.getBestAskDown()).isCloseTo(0.49, within(0.001));

        OrderBookQuote quote = book.toOrderBookQuote(1000L);
        assertThat(quote.bestAskUp()).isCloseTo(0.53, within(0.0001));
        assertThat(quote.bestBidUp()).isCloseTo(0.51, within(0.0001));
        assertThat(quote.bestAskDown()).isCloseTo(0.49, within(0.001));
        assertThat(quote.bestBidDown()).isCloseTo(0.47, within(0.001));
        assertThat(quote.timestampMs()).isEqualTo(1000L);
    }

    @Test
    @DisplayName("Should apply typed CLOB frames (book, price_change, last_trade_price) and track observed one-way latency")
    void shouldApplyTypedClobFramesAndTrackObservedLatency() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.setMarketTokenIds("95285", "18701");

        // Frame 1: UP book snapshot (receipt 1775939386058 - server 1775939386039 = 19ms latency)
        book.applyClobFrame(new BacktestEvent.PolymarketClobFrameEvent(
                1775939386058L,
                1775939386039L,
                1L,
                BacktestEvent.PolymarketClobFrameEvent.EVENT_BOOK,
                true,
                2,
                new boolean[]{true, true},
                new boolean[]{true, false},
                new int[]{4900, 5100},
                new double[]{200.0, 250.0}
        ));
        // Frame 2: DOWN book snapshot
        book.applyClobFrame(new BacktestEvent.PolymarketClobFrameEvent(
                1775939386058L,
                1775939386039L,
                2L,
                BacktestEvent.PolymarketClobFrameEvent.EVENT_BOOK,
                false,
                2,
                new boolean[]{false, false},
                new boolean[]{true, false},
                new int[]{4900, 5100},
                new double[]{250.0, 200.0}
        ));

        assertThat(book.getLastObservedOneWayLatencyMs()).isEqualTo(19L);
        assertThat(book.getBestBidUp()).isCloseTo(0.49, within(0.0001));
        assertThat(book.getBestAskUp()).isCloseTo(0.51, within(0.0001));
        assertThat(book.getBestBidDown()).isCloseTo(0.49, within(0.0001));
        assertThat(book.getBestAskDown()).isCloseTo(0.51, within(0.0001));

        // Frame 3: multi-item price_change batch (receipt 1775939406647 - server 1775939406635 = 12ms latency)
        book.applyClobFrame(new BacktestEvent.PolymarketClobFrameEvent(
                1775939406647L,
                1775939406635L,
                3L,
                BacktestEvent.PolymarketClobFrameEvent.EVENT_PRICE_CHANGE,
                false,
                3,
                new boolean[]{true, true, false},
                new boolean[]{false, false, true},
                new int[]{5200, 5100, 4800},
                new double[]{346.0, 0.0, 346.0}
        ));

        assertThat(book.getLastObservedOneWayLatencyMs()).isEqualTo(12L);
        assertThat(book.getBestAskUp()).isCloseTo(0.52, within(0.0001));

        // Frame 4: last_trade_price updates observed latency without altering book levels
        book.applyClobFrame(new BacktestEvent.PolymarketClobFrameEvent(
                1775939407015L,
                1775939407000L,
                4L,
                BacktestEvent.PolymarketClobFrameEvent.EVENT_LAST_TRADE,
                false,
                0,
                new boolean[0],
                new boolean[0],
                new int[0],
                new double[0]
        ));
        assertThat(book.getLastObservedOneWayLatencyMs()).isEqualTo(15L);
        assertThat(book.getBestAskUp()).isCloseTo(0.52, within(0.0001));

        // Frame 5: empty book snapshot on DOWN side clears DOWN bids and asks cleanly
        book.applyClobFrame(new BacktestEvent.PolymarketClobFrameEvent(
                1775939408010L,
                1775939408000L,
                5L,
                BacktestEvent.PolymarketClobFrameEvent.EVENT_BOOK,
                false,
                0,
                new boolean[0],
                new boolean[0],
                new int[0],
                new double[0]
        ));
        // DOWN falls back to UP complementarity (1 - 0.52 = 0.48 bid, 1 - 0.49 = 0.51 ask)
        assertThat(book.getBestBidDown()).isCloseTo(0.48, within(0.0001));
        assertThat(book.getBestAskDown()).isCloseTo(0.51, within(0.0001));
    }

    @Test
    @DisplayName("Should handle boundary ticks 0 and 10000 and sparse jumps across 64-bit bitset words")
    void shouldHandleBoundaryTicksAndSparseBitsetJumps() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.applyTickDelta(true, true, 0, 50.0, 1000L);
        book.applyTickDelta(true, true, 63, 75.0, 1000L);
        book.applyTickDelta(true, true, 64, 80.0, 1000L);
        book.applyTickDelta(true, false, 9999, 60.0, 1000L);
        book.applyTickDelta(true, false, 10_000, 90.0, 1000L);

        assertThat(book.getBestBidUp()).isCloseTo(0.0064, within(0.00001));
        assertThat(book.getBestAskUp()).isCloseTo(0.9999, within(0.00001));

        book.applyTickDelta(true, true, 64, 0.0, 1010L);
        assertThat(book.getBestBidUp()).isCloseTo(0.0063, within(0.00001));

        book.applyTickDelta(true, true, 63, 0.0, 1020L);
        assertThat(book.getBestBidUp()).isCloseTo(0.0, within(0.00001));

        book.applyTickDelta(true, true, 0, 0.0, 1030L);
        assertThat(book.getBestBidUp()).isNaN();

        book.applyTickDelta(true, false, 9999, 0.0, 1040L);
        assertThat(book.getBestAskUp()).isCloseTo(1.0, within(0.00001));

        book.applyTickDelta(true, false, 10_000, 0.0, 1050L);
        assertThat(book.getBestAskUp()).isNaN();
    }

    @Test
    @DisplayName("Should simulate market buy fill and compute crypto taker fee")
    void shouldSimulateFillWithFee() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.clearSideForSnapshot(true);
        book.setSnapshotLevel(true, false, 5000, 50.0);
        book.setSnapshotLevel(true, false, 5200, 50.0);
        book.finishBookSnapshot(true);

        // Buy 100 UP shares: 50 @ 0.50 + 50 @ 0.52 -> avg price 0.51
        PolymarketOrderBook.FillResult fill = book.simulateFill(true, true, 100.0, 1.0);

        assertThat(fill.executedShares()).isEqualTo(100.0);
        assertThat(fill.avgPrice()).isCloseTo(0.51, within(0.001));

        // Fee = shares * 0.07 * P * (1 - P) = 100 * 0.07 * 0.51 * 0.49 ~= 1.7489
        assertThat(fill.fee()).isCloseTo(1.7489, within(0.01));
    }

    @Test
    @DisplayName("Should not synthesize liquidity when order book is empty or has partial depth")
    void shouldNotSynthesizeLiquidityWhenBookInsufficient() {
        PolymarketOrderBook book = new PolymarketOrderBook();

        PolymarketOrderBook.FillResult emptyFill = book.simulateFill(true, true, 100.0, 1.0);
        assertThat(emptyFill.executedShares()).isEqualTo(0.0);
        assertThat(emptyFill.avgPrice()).isEqualTo(0.0);
        assertThat(emptyFill.fee()).isEqualTo(0.0);

        book.clearSideForSnapshot(true);
        book.setSnapshotLevel(true, false, 5000, 40.0);
        book.finishBookSnapshot(true);

        PolymarketOrderBook.FillResult partialFill = book.simulateFill(true, true, 100.0, 1.0);
        assertThat(partialFill.executedShares()).isEqualTo(40.0);
        assertThat(partialFill.avgPrice()).isCloseTo(0.50, within(0.0001));
    }

    @Test
    @DisplayName("Should deplete liquidity across 200ms batch buys, clamp on partial reductions, and reset when level disappears")
    void shouldDepleteLiquidityAcrossBatchPurchasesAndResetWhenLevelDisappears() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.clearSideForSnapshot(true);
        book.setSnapshotLevel(true, false, 5000, 100.0);
        book.setSnapshotLevel(true, false, 5200, 100.0);
        book.finishBookSnapshot(true);

        // Batch 1 at t=1000ms: buy 40 shares -> takes 40 @ 0.50, 60 remaining at 0.50
        PolymarketOrderBook.FillResult batch1 = book.executeFill(true, true, 40.0, 0.55);
        assertThat(batch1.executedShares()).isEqualTo(40.0);
        assertThat(batch1.avgPrice()).isCloseTo(0.50, within(0.0001));
        assertThat(book.getBestAskUp()).isCloseTo(0.50, within(0.0001));
        assertThat(book.getDepth1cUp()).isCloseTo(60.0, within(0.0001));

        // Batch 2 at t=1200ms: buy 40 shares -> takes 40 @ 0.50, 20 remaining at 0.50
        PolymarketOrderBook.FillResult batch2 = book.executeFill(true, true, 40.0, 0.55);
        assertThat(batch2.executedShares()).isEqualTo(40.0);
        assertThat(batch2.avgPrice()).isCloseTo(0.50, within(0.0001));
        assertThat(book.getDepth1cUp()).isCloseTo(20.0, within(0.0001));

        // Batch 3 at t=1400ms: buy 40 shares -> takes remaining 20 @ 0.50 + 20 @ 0.52 (avg 0.51)
        PolymarketOrderBook.FillResult batch3 = book.executeFill(true, true, 40.0, 0.55);
        assertThat(batch3.executedShares()).isEqualTo(40.0);
        assertThat(batch3.avgPrice()).isCloseTo(0.51, within(0.0001));
        assertThat(book.getBestAskUp()).isCloseTo(0.52, within(0.0001));
        assertThat(book.getDepth1cUp()).isCloseTo(80.0, within(0.0001));

        // External delta updates 0.50 to size 90 (below our 100 consumed): consumed clamped to 90 -> effective still 0
        book.applyTickDelta(true, false, 5000, 90.0, 1500L);
        assertThat(book.getBestAskUp()).isCloseTo(0.52, within(0.0001));

        // External maker adds 25 shares at 0.50 (size becomes 115): 25 new shares become available at 0.50
        book.applyTickDelta(true, false, 5000, 115.0, 1600L);
        assertThat(book.getBestAskUp()).isCloseTo(0.50, within(0.0001));
        assertThat(book.getDepth1cUp()).isCloseTo(25.0, within(0.0001));

        // Level 0.50 completely disappears from the order book (size = 0) -> resets consumed memory
        book.applyTickDelta(true, false, 5000, 0.0, 1700L);
        assertThat(book.getBestAskUp()).isCloseTo(0.52, within(0.0001));

        // Level 0.50 reappears with size 50 -> full 50 shares available because consumed memory was reset
        book.applyTickDelta(true, false, 5000, 50.0, 1800L);
        assertThat(book.getBestAskUp()).isCloseTo(0.50, within(0.0001));
        assertThat(book.getDepth1cUp()).isCloseTo(50.0, within(0.0001));
    }

    @Test
    @DisplayName("Dirty-flag cached toOrderBookQuote must produce identical quotes to uncached recomputation across deep and BBO mutations")
    void shouldProduceBitIdenticalQuotesWithDirtyFlagCaching() {
        PolymarketOrderBook cachedBook = new PolymarketOrderBook();

        cachedBook.clearSideForSnapshot(true);
        cachedBook.setSnapshotLevel(true, true, 4900, 120.0);
        cachedBook.setSnapshotLevel(true, true, 4000, 500.0);
        cachedBook.setSnapshotLevel(true, false, 5100, 60.0);
        cachedBook.setSnapshotLevel(true, false, 5200, 80.0);
        cachedBook.setSnapshotLevel(true, false, 7500, 400.0);
        cachedBook.finishBookSnapshot(true);

        cachedBook.clearSideForSnapshot(false);
        cachedBook.setSnapshotLevel(false, true, 4800, 150.0);
        cachedBook.setSnapshotLevel(false, false, 5000, 70.0);
        cachedBook.setSnapshotLevel(false, false, 5150, 90.0);
        cachedBook.setSnapshotLevel(false, false, 8000, 300.0);
        cachedBook.finishBookSnapshot(false);

        OrderBookQuote q1 = cachedBook.toOrderBookQuote(1000L);
        assertThat(q1.bestAskUp()).isCloseTo(0.51, within(1e-6));
        assertThat(q1.estimatedFillPriceUp()).isCloseTo((60.0 * 0.51 + 40.0 * 0.52) / 100.0, within(1e-6));
        assertThat(q1.estimatedFillPriceDown()).isCloseTo((70.0 * 0.50 + 30.0 * 0.515) / 100.0, within(1e-6));

        // 1. Deep ask & deep bid updates far outside BBO / 1c depth / 100-share slippage range
        cachedBook.applyTickDelta(true, false, 7500, 900.0, 1010L);
        cachedBook.applyTickDelta(true, true, 4000, 250.0, 1010L);
        cachedBook.applyTickDelta(false, false, 8000, 600.0, 1010L);
        OrderBookQuote q2 = cachedBook.toOrderBookQuote(1010L);
        assertThat(q2.bestAskUp()).isEqualTo(q1.bestAskUp());
        assertThat(q2.bestBidUp()).isEqualTo(q1.bestBidUp());
        assertThat(q2.depth1cUp()).isEqualTo(q1.depth1cUp());
        assertThat(q2.depth1cDown()).isEqualTo(q1.depth1cDown());
        assertThat(q2.estimatedFillPriceUp()).isEqualTo(q1.estimatedFillPriceUp());
        assertThat(q2.estimatedFillPriceDown()).isEqualTo(q1.estimatedFillPriceDown());
        assertThat(q2.timestampMs()).isEqualTo(1010L);

        // 2. Better ask inserted inside active range -> must invalidate and recompute immediately
        cachedBook.applyTickDelta(true, false, 5050, 100.0, 1020L);
        OrderBookQuote q3 = cachedBook.toOrderBookQuote(1020L);
        assertThat(q3.bestAskUp()).isCloseTo(0.505, within(1e-6));
        assertThat(q3.estimatedFillPriceUp()).isCloseTo(0.505, within(1e-6));
        assertThat(q3.depth1cUp()).isCloseTo(160.0, within(1e-6));

        // 3. Execute fill consuming 50 shares -> must invalidate and update effective depth & slippage
        cachedBook.executeFill(true, true, 50.0, 0.55);
        OrderBookQuote q4 = cachedBook.toOrderBookQuote(1030L);
        assertThat(q4.depth1cUp()).isCloseTo(110.0, within(1e-6));
        assertThat(q4.estimatedFillPriceUp()).isCloseTo((50.0 * 0.505 + 50.0 * 0.51) / 100.0, within(1e-6));
    }
}
