package cz.polymarket.bot.orderbook;

import cz.polymarket.bot.domain.OrderBookQuote;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Deterministic synthetic unit tests for {@link PolymarketOrderBook} and {@link OrderBookQuote}.
 */
class PolymarketOrderBookTest {

    @Test
    @DisplayName("Should derive symmetric complementary quotes when only UP or only DOWN book is populated")
    void shouldDeriveSymmetricComplementarityForBothSides() {
        PolymarketOrderBook upOnly = new PolymarketOrderBook();
        upOnly.clearSideForSnapshot(true);
        upOnly.setSnapshotLevel(true, true, 5100, 200.0);
        upOnly.setSnapshotLevel(true, true, 5000, 100.0);
        upOnly.setSnapshotLevel(true, false, 5300, 150.0);
        upOnly.setSnapshotLevel(true, false, 5400, 250.0);
        upOnly.finishBookSnapshot(true, 1_000L);

        OrderBookQuote qFromUp = upOnly.toOrderBookQuote(1_000L);
        assertThat(qFromUp.bestBidUp()).isCloseTo(0.51, within(1e-6));
        assertThat(qFromUp.bestAskUp()).isCloseTo(0.53, within(1e-6));
        assertThat(qFromUp.bestBidDown()).isCloseTo(0.47, within(1e-6));
        assertThat(qFromUp.bestAskDown()).isCloseTo(0.49, within(1e-6));
        assertThat(qFromUp.depth1cUp()).isCloseTo(400.0, within(1e-6));
        assertThat(qFromUp.depth1cDown()).isCloseTo(300.0, within(1e-6));

        // Symmetric test: ONLY DOWN side populated -> UP must derive cleanly without Double.NaN
        PolymarketOrderBook downOnly = new PolymarketOrderBook();
        downOnly.clearSideForSnapshot(false);
        downOnly.setSnapshotLevel(false, true, 4700, 150.0);
        downOnly.setSnapshotLevel(false, true, 4600, 250.0);
        downOnly.setSnapshotLevel(false, false, 4900, 200.0);
        downOnly.setSnapshotLevel(false, false, 5000, 100.0);
        downOnly.finishBookSnapshot(false, 1_000L);

        OrderBookQuote qFromDown = downOnly.toOrderBookQuote(1_000L);
        assertThat(qFromDown.bestAskUp()).isCloseTo(0.53, within(1e-6));
        assertThat(qFromDown.bestBidUp()).isCloseTo(0.51, within(1e-6));
        assertThat(qFromDown.bestAskDown()).isCloseTo(0.49, within(1e-6));
        assertThat(qFromDown.bestBidDown()).isCloseTo(0.47, within(1e-6));
        assertThat(qFromDown.depth1cUp()).isCloseTo(400.0, within(1e-6));
        assertThat(qFromDown.depth1cDown()).isCloseTo(300.0, within(1e-6));
    }

    @Test
    @DisplayName("Should reject Double.NaN and Infinity in OrderBookQuote validation")
    void shouldRejectNaNAndInfinityInOrderBookQuote() {
        assertThatThrownBy(() -> new OrderBookQuote(Double.NaN, 0.49, 0.51, 0.49, 100.0, 100.0, 1_000L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderBookQuote(0.51, 0.49, Double.POSITIVE_INFINITY, 0.49, 100.0, 100.0, 1_000L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderBookQuote(0.51, 0.49, 0.51, 0.49, Double.NaN, 100.0, 1_000L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderBookQuote(0.51, 0.49, 0.51, 0.49, -1.0, 100.0, 1_000L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should prune crossed stale levels on applyBboHint without fabricating synthetic 100-share liquidity")
    void shouldPruneCrossedLevelsOnBboHintWithoutFabricatingLiquidity() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.applyTickDelta(true, true, 4900, 100.0, 1_000L);
        book.applyTickDelta(true, false, 5000, 40.0, 1_000L);
        book.applyTickDelta(true, false, 5300, 80.0, 1_000L);

        // Exchange BBO hint reports bestAsk jumped to 0.54 while 0.54 delta hasn't arrived yet:
        // stale asks at 0.50 and 0.53 must be pruned, and 0.54 must NOT be fabricated with 100 shares!
        book.applyBboHint(true, 4900, 5400, 1_010L);

        assertThat(book.getBestAskUp()).isNaN();
        assertThat(book.estimateBuyFillableShares(true, 0.60)).isEqualTo(0.0);
        assertThat(book.estimateBuyFillVwap(true, 20.0, 0.60)).isNaN();

        // Once real delta arrives at 0.54 with 25 shares, exact 25 shares become available
        book.applyTickDelta(true, false, 5400, 25.0, 1_015L);
        assertThat(book.getBestAskUp()).isCloseTo(0.54, within(1e-6));
        assertThat(book.getDepth1cUp()).isCloseTo(25.0, within(1e-6));
    }

    @Test
    @DisplayName("Should track server timestamps independently per UP and DOWN token side")
    void shouldEnforceMonotonicServerTimestampsPerTokenSide() {
        PolymarketOrderBook book = new PolymarketOrderBook();

        assertThat(book.tryAcceptServerTimestamp(true, 1_005L)).isTrue();
        // DOWN frame arriving with server_ts = 1_002L (< UP's 1_005L) MUST be accepted!
        assertThat(book.tryAcceptServerTimestamp(false, 1_002L)).isTrue();
        // Stale UP frame with 1_004L (< 1_005L) MUST be rejected
        assertThat(book.tryAcceptServerTimestamp(true, 1_004L)).isFalse();
        // Stale DOWN frame with 1_001L (< 1_002L) MUST be rejected
        assertThat(book.tryAcceptServerTimestamp(false, 1_001L)).isFalse();
        // Equal or newer timestamps on each side MUST be accepted
        assertThat(book.tryAcceptServerTimestamp(true, 1_005L)).isTrue();
        assertThat(book.tryAcceptServerTimestamp(false, 1_006L)).isTrue();
    }

    @Test
    @DisplayName("Should compute exact order-size and limit-price bounded VWAP without 100-share distortion")
    void shouldComputeExactOrderSizeAndLimitBoundedVwap() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.clearSideForSnapshot(true);
        book.setSnapshotLevel(true, true, 5400, 100.0);
        book.setSnapshotLevel(true, false, 5500, 30.0);
        book.setSnapshotLevel(true, false, 5600, 20.0);
        book.setSnapshotLevel(true, false, 6400, 70.0);
        book.finishBookSnapshot(true, 1_000L);

        // 1. Small 20-share order with limit 0.56 fills 100% at 0.55 (NOT distorted by 0.64 level!)
        assertThat(book.estimateBuyFillableShares(true, 0.56)).isCloseTo(50.0, within(1e-6));
        assertThat(book.estimateBuyFillVwap(true, 20.0, 0.56)).isCloseTo(0.55, within(1e-6));

        // 2. 50-share order with limit 0.56 sweeps 30 @ 0.55 + 20 @ 0.56 = 0.554 (ignores 0.64 > limitPrice)
        assertThat(book.estimateBuyFillVwap(true, 50.0, 0.56)).isCloseTo(0.554, within(1e-6));
        assertThat(book.estimateBuyFillVwap(true, 100.0, 0.56)).isCloseTo(0.554, within(1e-6));

        // 3. Wide limit 0.65 for 100 shares sweeps 30 @ 0.55 + 20 @ 0.56 + 50 @ 0.64 = 0.597
        assertThat(book.estimateBuyFillVwap(true, 100.0, 0.65)).isCloseTo(0.597, within(1e-6));
    }

    @Test
    @DisplayName("Should deplete liquidity across consecutive fills and reconcile snapshots cleanly")
    void shouldDepleteLiquidityAndReconcileSnapshots() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.clearSideForSnapshot(true);
        book.setSnapshotLevel(true, true, 4800, 100.0);
        book.setSnapshotLevel(true, false, 5000, 100.0);
        book.setSnapshotLevel(true, false, 5200, 100.0);
        book.finishBookSnapshot(true, 1_000L);

        PolymarketOrderBook.FillResult f1 = book.executeFill(true, true, 60.0, 0.55);
        assertThat(f1.executedShares()).isEqualTo(60.0);
        assertThat(f1.avgPrice()).isCloseTo(0.50, within(1e-6));
        assertThat(book.getDepth1cUp()).isCloseTo(40.0, within(1e-6));

        PolymarketOrderBook.FillResult f2 = book.executeFill(true, true, 60.0, 0.55);
        assertThat(f2.executedShares()).isEqualTo(60.0);
        assertThat(f2.avgPrice()).isCloseTo((40.0 * 0.50 + 20.0 * 0.52) / 60.0, within(1e-6));
        assertThat(book.getBestAskUp()).isCloseTo(0.52, within(1e-6));
        assertThat(book.getDepth1cUp()).isCloseTo(80.0, within(1e-6));
    }
}
