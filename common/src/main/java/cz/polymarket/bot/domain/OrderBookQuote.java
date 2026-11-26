package cz.polymarket.bot.domain;

/**
 * Immutable top-of-book and 1-cent liquidity depth quote for Polymarket UP/DOWN binary contracts.
 *
 * @param bestAskUp   lowest executable ask price for the UP token in {@code (0.0, 1.0]}
 * @param bestBidUp   highest executable bid price for the UP token in {@code (0.0, 1.0]}
 * @param bestAskDown lowest executable ask price for the DOWN token in {@code (0.0, 1.0]}
 * @param bestBidDown highest executable bid price for the DOWN token in {@code (0.0, 1.0]}
 * @param depth1cUp   cumulative effective ask share depth within 1 cent of {@code bestAskUp}
 * @param depth1cDown cumulative effective ask share depth within 1 cent of {@code bestAskDown}
 * @param timestampMs quote receipt timestamp in epoch milliseconds UTC
 */
public record OrderBookQuote(
        double bestAskUp,
        double bestBidUp,
        double bestAskDown,
        double bestBidDown,
        double depth1cUp,
        double depth1cDown,
        long timestampMs
) {

    /**
     * Validates that all BBO prices are finite positive numbers and 1-cent depths are finite non-negative numbers.
     */
    public OrderBookQuote {
        if (!Double.isFinite(bestAskUp) || bestAskUp <= 0.0
                || !Double.isFinite(bestBidUp) || bestBidUp <= 0.0
                || !Double.isFinite(bestAskDown) || bestAskDown <= 0.0
                || !Double.isFinite(bestBidDown) || bestBidDown <= 0.0) {
            throw new IllegalArgumentException("Order book BBO prices must be finite and positive");
        }
        if (!Double.isFinite(depth1cUp) || depth1cUp < 0.0
                || !Double.isFinite(depth1cDown) || depth1cDown < 0.0) {
            throw new IllegalArgumentException("Order book 1-cent depths must be finite and non-negative");
        }
    }

    /**
     * Checks whether this quote carries the same executable BBO prices and 1-cent ask depths as {@code other}
     * within floating-point tick tolerances, allowing identical quote deduplication across LIVE, PAPER, and BACKTEST.
     *
     * @param other previous quote snapshot (may be {@code null})
     * @return {@code true} if both BBO prices and 1-cent depths are unchanged
     */
    public boolean hasSameExecutableState(OrderBookQuote other) {
        if (other == null) {
            return false;
        }
        return Math.abs(this.bestAskUp - other.bestAskUp) <= 1e-6
                && Math.abs(this.bestBidUp - other.bestBidUp) <= 1e-6
                && Math.abs(this.bestAskDown - other.bestAskDown) <= 1e-6
                && Math.abs(this.bestBidDown - other.bestBidDown) <= 1e-6
                && Math.abs(this.depth1cUp - other.depth1cUp) <= 1e-4
                && Math.abs(this.depth1cDown - other.depth1cDown) <= 1e-4;
    }
}
