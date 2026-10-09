package cz.polymarket.bot.backtest.cache;

/**
 * Compact immutable record representing a raw market interval and Polymarket quote state,
 * stored efficiently in memory-mapped binary cache.
 */
public record CachedMarketRow(
        long tStart,
        long tEnd,
        double sOpen,
        double sHigh,
        double sLow,
        double sClose,
        double sVolBtc,
        double sVolUsd,
        double sDeltaBtc,
        double fOpen,
        double fHigh,
        double fLow,
        double fClose,
        double fVolBtc,
        double fVolUsd,
        double fDeltaBtc,
        boolean actualOutcomeUp,
        double pmAsk0,
        double pmBid0,
        double pmAsk60,
        double pmBid60,
        double pmAsk180,
        double pmBid180,
        double pmAsk300,
        double pmBid300,
        double pmFill100Up,
        double pmFee100Up,
        double pmFill100Down,
        double pmFee100Down,
        double pmMaxPrice,
        double pmMinPrice,
        double pmDepth1cUp,
        double pmDepth1cDown
) {
}
