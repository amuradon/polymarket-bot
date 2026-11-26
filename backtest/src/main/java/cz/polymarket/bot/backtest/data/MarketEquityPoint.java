package cz.polymarket.bot.backtest.data;

/**
 * Per-market equity and outcome point recorded for every evaluated 15-minute Polymarket market
 * (including markets with zero trades so multi-run comparison shares an aligned timeline).
 *
 * @param tStart candle interval start timestamp in epoch seconds UTC
 * @param dateUtc UTC date string ({@code yyyy-MM-dd})
 * @param tradesCount count of executed trades in this market
 * @param winningTrades count of winning trades in this market
 * @param netPnl aggregated net PnL in USD for this market
 * @param feesUsd aggregated taker fees in USD for this market
 * @param brierDiffSq squared probability calibration error for this market
 * @param evaluated whether the market had valid metadata and was evaluated
 * @param balanceAfter cumulative account balance in USD immediately after this market
 */
public record MarketEquityPoint(
        long tStart,
        String dateUtc,
        int tradesCount,
        int winningTrades,
        double netPnl,
        double feesUsd,
        double brierDiffSq,
        boolean evaluated,
        double balanceAfter
) {
}
