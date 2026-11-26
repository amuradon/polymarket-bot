package cz.polymarket.bot.strategy;

/**
 * Execution context provided to a {@link TradingStrategy} instance across {@code LIVE}, {@code PAPER},
 * and {@code BACKTEST} runtimes.
 * <p>
 * Provides access to order routing, account collateral balance, and zero-allocation on-demand L2 orderbook
 * sweep queries for exact order-size and limit-price slippage evaluation.
 */
public interface StrategyContext {

    /**
     * Returns the execution router for order submission and cancellation.
     *
     * @return execution router
     */
    ExecutionRouter getExecutionRouter();

    /**
     * Returns the currently available account collateral balance in USD.
     *
     * @return available balance in USD
     */
    default double getAccountBalanceUsd() {
        return 0.0;
    }

    /**
     * Estimates the total share quantity fillable at or below {@code limitPrice} for a marketable {@code BUY}
     * order on the given outcome side against current effective L2 orderbook depth.
     *
     * @param side       directional outcome ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
     * @param limitPrice maximum acceptable execution price in {@code (0.0, 1.0]}
     * @return fillable share quantity ({@code >= 0.0}), or {@link Double#POSITIVE_INFINITY} when unbacked by an L2 book
     */
    default double estimateBuyFillableShares(TradeDirection side, double limitPrice) {
        return Double.POSITIVE_INFINITY;
    }

    /**
     * Estimates the volume-weighted average fill price (VWAP) for a marketable {@code BUY} order of
     * {@code requestedShares} capped at {@code limitPrice} against current effective L2 orderbook depth.
     * <p>
     * Returns {@link Double#NaN} when an attached L2 orderbook has zero fillable shares at or below {@code limitPrice},
     * or {@code 0.0} in lightweight synthetic unit-test contexts without an attached L2 book (indicating top-of-book fallback).
     *
     * @param side            directional outcome ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
     * @param requestedShares desired share quantity
     * @param limitPrice      maximum acceptable execution price in {@code (0.0, 1.0]}
     * @return estimated VWAP fill price in {@code (0.0, 1.0]}, {@link Double#NaN} when unfillable, or {@code 0.0} if unbacked
     */
    default double estimateBuyFillVwap(TradeDirection side, double requestedShares, double limitPrice) {
        return 0.0;
    }

    /**
     * Requests an asynchronous refresh of the account collateral balance from Polymarket in live trading
     * (no-op in paper trading and backtesting).
     */
    default void refreshAccountBalance() {
    }
}
