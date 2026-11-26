package cz.polymarket.bot.engine;

import cz.polymarket.bot.strategy.TradeDirection;

/**
 * Immutable representation of an open conditional-token trading position, shared identically across
 * {@code LIVE}, {@code PAPER}, and {@code BACKTEST} execution runtimes.
 *
 * @param clientOrderId     internal strategy client order ID
 * @param clobOrderId       Polymarket CLOB order ID ({@code 0x...} in {@code LIVE}, or virtual ID in simulation)
 * @param side              directional outcome ({@link TradeDirection#UP} or {@link TradeDirection#DOWN})
 * @param entryPrice        execution fill price in {@code (0.0, 1.0]}
 * @param shares            remaining open conditional token share quantity
 * @param sizeUsd           remaining USDC notional cost ({@code entryPrice * shares})
 * @param entryFee          remaining allocated taker entry fee in USD
 * @param entryTimestampMs  fill timestamp in epoch milliseconds UTC
 * @param intervalStartSec  market cycle start timestamp in epoch seconds UTC
 * @param token             strategy token label (e.g. {@code "BTC-UP"} or {@code "BTC-DOWN"})
 * @param clobTokenId       256-bit decimal Polymarket conditional token ID (or {@code ""} in simulation)
 * @param modelProbability  strategy directional model probability at entry
 * @param marketPrice       market best ask price at order submission
 * @param edge              estimated probability edge over entry price
 * @param maxObservedPrice  highest observed bid price since entry
 * @param trailingStopArmed whether trailing stop has been armed
 */
public record Position(
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
     * Validates position invariants and normalizes optional string identifiers.
     */
    public Position {
        if (clientOrderId == null || clientOrderId.isBlank()) {
            throw new IllegalArgumentException("clientOrderId cannot be null or blank");
        }
        if (side == null || side == TradeDirection.NO_TRADE) {
            throw new IllegalArgumentException("side must be UP or DOWN");
        }
        if (!Double.isFinite(entryPrice) || entryPrice <= 0.0
                || !Double.isFinite(shares) || shares <= 0.0
                || !Double.isFinite(sizeUsd) || sizeUsd <= 0.0) {
            throw new IllegalArgumentException("entryPrice, shares, and sizeUsd must be finite and positive");
        }
        clobOrderId = clobOrderId != null ? clobOrderId : "";
        token = token != null ? token : "";
        clobTokenId = clobTokenId != null ? clobTokenId : "";
    }

    /**
     * Returns an updated position copy recording a new observed bid price and trailing stop state.
     *
     * @param currentPrice latest observed bid price for this outcome token
     * @param armed        whether trailing stop is armed
     * @return updated {@link Position}
     */
    public Position withPriceObservation(double currentPrice, boolean armed) {
        double newMax = Math.max(this.maxObservedPrice, currentPrice);
        return new Position(
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

    /**
     * Returns a reduced position copy after a partial FIFO {@code SELL} exit.
     *
     * @param remainingShares remaining open share count ({@code > 0})
     * @param remainingFee    remaining unallocated entry fee in USD
     * @return reduced {@link Position}
     */
    public Position withRemainingShares(double remainingShares, double remainingFee) {
        return new Position(
                clientOrderId,
                clobOrderId,
                side,
                entryPrice,
                remainingShares,
                remainingShares * entryPrice,
                Math.max(0.0, remainingFee),
                entryTimestampMs,
                intervalStartSec,
                token,
                clobTokenId,
                modelProbability,
                marketPrice,
                edge,
                maxObservedPrice,
                trailingStopArmed
        );
    }
}
