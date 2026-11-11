package cz.polymarket.bot.live.engine;

import cz.polymarket.bot.strategy.TradeDirection;

/**
 * Immutable representation of an open live trading position on Polymarket.
 *
 * @param clientOrderId    internal strategy client order ID
 * @param clobOrderId      Polymarket CLOB order ID ({@code 0x...})
 * @param side             directional outcome ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
 * @param entryPrice       actual execution fill price in {@code (0.0, 1.0)}
 * @param shares           filled conditional token share quantity
 * @param sizeUsd          total USDC notional cost ({@code entryPrice * shares})
 * @param entryFee         estimated taker fee in USD
 * @param entryTimestampMs fill timestamp in epoch milliseconds
 * @param intervalStartSec 15m market cycle start timestamp in epoch seconds
 * @param token            strategy token label (e.g. {@code "BTC-UP"} or {@code "BTC-DOWN"})
 * @param clobTokenId      256-bit decimal Polymarket conditional token ID
 * @param modelProbability strategy model probability at entry
 * @param marketPrice      market ask price at entry
 * @param edge             estimated edge at entry
 * @param maxObservedPrice highest observed bid price since entry
 * @param trailingStopArmed whether trailing stop has been armed
 */
public record LivePosition(
        String clientOrderId,
        String clobOrderId,
        TradeDirection side,
        double entryPrice,
        double shares,
        double sizeUsd,
        double entryFee,
        long entryTimestampMs,
        long intervalStartSec,
        String token,
        String clobTokenId,
        double modelProbability,
        double marketPrice,
        double edge,
        double maxObservedPrice,
        boolean trailingStopArmed
) {

    /**
     * Validates position invariants.
     */
    public LivePosition {
        if (clientOrderId == null || clientOrderId.isBlank()) {
            throw new IllegalArgumentException("clientOrderId cannot be null or blank");
        }
        if (side == null || side == TradeDirection.NO_TRADE) {
            throw new IllegalArgumentException("side must be UP or DOWN");
        }
        if (entryPrice <= 0.0 || shares <= 0.0 || sizeUsd <= 0.0) {
            throw new IllegalArgumentException("entryPrice, shares, and sizeUsd must be positive");
        }
    }

    /**
     * Returns an updated position copy recording a new observed bid price and trailing stop state.
     *
     * @param currentPrice latest observed bid price for this outcome token
     * @param armed        whether trailing stop is armed
     * @return updated {@link LivePosition}
     */
    public LivePosition withPriceObservation(double currentPrice, boolean armed) {
        double newMax = Math.max(this.maxObservedPrice, currentPrice);
        return new LivePosition(
                clientOrderId,
                clobOrderId,
                side,
                entryPrice,
                shares,
                sizeUsd,
                entryFee,
                entryTimestampMs,
                intervalStartSec,
                token,
                clobTokenId,
                modelProbability,
                marketPrice,
                edge,
                newMax,
                armed || trailingStopArmed
        );
    }
}
