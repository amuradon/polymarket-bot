package cz.polymarket.bot.engine;

import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.TradeDirection;

/**
 * Pluggable order execution transport backing {@link CoreTradingEngine}.
 * <p>
 * Implemented by {@link SimulatedClobExecutionEngine} in both {@code PAPER} and {@code BACKTEST}
 * modes, and by the Polymarket EIP-712 + REST CLOB client adapter in {@code LIVE} mode.
 */
public interface OrderExecutionGateway {

    /**
     * Submits a normalized, balance-validated order command to the execution venue.
     * Implementations must asynchronously or synchronously deliver a terminal
     * {@link cz.polymarket.bot.strategy.ExecutionReport} back to {@link CoreTradingEngine}.
     *
     * @param command           original strategy order command
     * @param side              resolved binary token direction ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
     * @param isBuy             {@code true} for {@code BUY}, {@code false} for {@code SELL}
     * @param limitPriceMc      clamped limit price in millicents ({@code 1..999})
     * @param normalizedShares  validated share quantity ({@code wholeShares} for {@code BUY}, open-capped shares for {@code SELL})
     * @param submitTimestampMs order submission timestamp in epoch milliseconds UTC
     */
    void executeOrder(
            OrderCommand command,
            TradeDirection side,
            boolean isBuy,
            int limitPriceMc,
            double normalizedShares,
            long submitTimestampMs);

    /**
     * Requests cancellation of an in-flight or resting order if supported by the execution venue.
     *
     * @param clientOrderId client order identifier
     * @param timestampMs   cancellation request timestamp in epoch milliseconds UTC
     */
    default void cancelOrder(String clientOrderId, long timestampMs) {
    }
}
