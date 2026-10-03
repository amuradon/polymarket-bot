package cz.polymarket.bot.calculator;

/**
 * Quantitative trading performance metrics.
 * Continuously computed across Backtesting, Paper Trading, and Live Trading.
 */
public record PerformanceMetrics(
        int totalTrades,
        int winningTrades,
        int losingTrades,
        double winRatePct,
        double totalNetPnl,
        double totalFees,
        double grossPnl,
        double grossProfit,
        double grossLoss,
        double profitFactor,
        double maxDrawdownUsd,
        double maxDrawdownPct,
        double expectedValuePerTrade,
        double sharpeRatio,
        double sortinoRatio,
        double brierScore,
        double initialCapital,
        double finalBalance
) {
    public static PerformanceMetrics empty(double initialCapital) {
        return new PerformanceMetrics(
                0, 0, 0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.25,
                initialCapital, initialCapital
        );
    }
}
