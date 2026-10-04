package cz.polymarket.bot.calculator;

import cz.polymarket.bot.domain.TradeRecord;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

/**
 * Calculates quantitative performance metrics from trade records and model predictions.
 * Reusable across backtesting, paper trading, and live trading to ensure zero metric discrepancy.
 */
@ApplicationScoped
public class PerformanceMetricsCalculator {

    public PerformanceMetrics calculate(List<TradeRecord> trades, double initialCapital) {
        return calculate(trades, initialCapital, 0.0, 55.0);
    }

    public PerformanceMetrics calculate(List<TradeRecord> trades, double initialCapital, double brierScore) {
        return calculate(trades, initialCapital, brierScore, 55.0);
    }

    public PerformanceMetrics calculate(List<TradeRecord> trades, double initialCapital, double brierScore, double durationDays) {
        if (trades == null || trades.isEmpty()) {
            return PerformanceMetrics.empty(initialCapital);
        }

        int totalTrades = trades.size();
        int winningTrades = 0;
        double totalNetPnl = 0.0;
        double totalFees = 0.0;
        double grossProfit = 0.0;
        double grossLoss = 0.0;

        double currentBalance = initialCapital;
        double peakBalance = initialCapital;
        double maxDrawdownUsd = 0.0;
        double maxDrawdownPct = 0.0;

        double[] pnlReturns = new double[totalTrades];

        for (int i = 0; i < totalTrades; i++) {
            TradeRecord t = trades.get(i);
            if (t.isWin()) {
                winningTrades++;
                grossProfit += t.netPnl();
            } else {
                grossLoss += Math.abs(t.netPnl());
            }

            totalNetPnl += t.netPnl();
            totalFees += t.feeUsd();

            currentBalance += t.netPnl();
            if (currentBalance > peakBalance) {
                peakBalance = currentBalance;
            }
            double ddUsd = peakBalance - currentBalance;
            double ddPct = peakBalance > 0.0 ? (ddUsd / peakBalance * 100.0) : 0.0;
            if (ddUsd > maxDrawdownUsd) {
                maxDrawdownUsd = ddUsd;
            }
            if (ddPct > maxDrawdownPct) {
                maxDrawdownPct = ddPct;
            }

            double safeSize = Math.max(t.sizeUsd(), 1.0);
            pnlReturns[i] = t.netPnl() / safeSize;
        }

        int losingTrades = totalTrades - winningTrades;
        double winRatePct = totalTrades > 0 ? ((double) winningTrades / totalTrades * 100.0) : 0.0;
        double grossPnl = totalNetPnl + totalFees;

        double profitFactor;
        if (grossLoss > 0.0) {
            profitFactor = grossProfit / grossLoss;
        } else if (grossProfit > 0.0) {
            profitFactor = 999.99;
        } else {
            profitFactor = 0.0;
        }

        double evPerTrade = totalTrades > 0 ? (totalNetPnl / totalTrades) : 0.0;

        double sharpe = 0.0;
        double sortino = 0.0;
        if (totalTrades > 1) {
            double sumRet = 0.0;
            for (double r : pnlReturns) {
                sumRet += r;
            }
            double meanRet = sumRet / totalTrades;

            double varRet = 0.0;
            for (double r : pnlReturns) {
                varRet += (r - meanRet) * (r - meanRet);
            }
            double stdRet = Math.sqrt(varRet / (totalTrades - 1));

            int downsideCount = 0;
            for (double r : pnlReturns) {
                if (r < 0.0) {
                    downsideCount++;
                }
            }

            double downsideStd = stdRet;
            if (downsideCount > 1) {
                double downsideMean = 0.0;
                for (double r : pnlReturns) {
                    if (r < 0.0) downsideMean += r;
                }
                downsideMean /= downsideCount;
                double dVar = 0.0;
                for (double r : pnlReturns) {
                    if (r < 0.0) dVar += (r - downsideMean) * (r - downsideMean);
                }
                downsideStd = Math.sqrt(dVar / (downsideCount - 1));
            }

            double days = durationDays > 0.0 ? durationDays : 55.0;
            double tradesPerYear = totalTrades * (365.25 / days);
            double annualizationFactor = Math.sqrt(tradesPerYear);

            if (stdRet > 0.0) {
                sharpe = (meanRet / stdRet) * annualizationFactor;
            }
            if (downsideStd > 0.0) {
                sortino = (meanRet / downsideStd) * annualizationFactor;
            }
        }

        return new PerformanceMetrics(
                totalTrades,
                winningTrades,
                losingTrades,
                sanitizeDouble(winRatePct, 0.0),
                sanitizeDouble(totalNetPnl, 0.0),
                sanitizeDouble(totalFees, 0.0),
                sanitizeDouble(grossPnl, 0.0),
                sanitizeDouble(grossProfit, 0.0),
                sanitizeDouble(grossLoss, 0.0),
                sanitizeDouble(profitFactor, 0.0),
                sanitizeDouble(maxDrawdownUsd, 0.0),
                sanitizeDouble(maxDrawdownPct, 0.0),
                sanitizeDouble(evPerTrade, 0.0),
                sanitizeDouble(sharpe, 0.0),
                sanitizeDouble(sortino, 0.0),
                sanitizeDouble(brierScore, 0.25),
                sanitizeDouble(initialCapital, 10000.0),
                sanitizeDouble(currentBalance, initialCapital)
        );
    }

    private static double sanitizeDouble(double val, double defaultValue) {
        if (Double.isNaN(val) || Double.isInfinite(val)) {
            return defaultValue;
        }
        return val;
    }
}
