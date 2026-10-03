package cz.polymarket.bot.backtest.engine;

import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExecutionRouter;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;

import java.math.BigDecimal;
import java.time.Instant;

import cz.polymarket.bot.cache.HourlyPriceCache;

/**
 * Strategy execution context simulating order fills, fees, and settlements during backtests.
 */
public class BacktestSimulationContext implements StrategyContext, ExecutionRouter {

    private final HourlyPriceCache priceCache = new HourlyPriceCache(3600L);
    private TradingStrategy strategy;
    private BacktestMarketRow currentRow;

    private boolean hasActiveTrade = false;
    private TradeDirection side = TradeDirection.NO_TRADE;
    private double entryPrice = 0.0;
    private double sizeUsd = 0.0;
    private double shares = 0.0;
    private double entryFee = 0.0;
    private double modelProbability = 0.5;
    private double marketPrice = 0.5;
    private double edge = 0.0;

    private boolean exitExecuted = false;
    private double exitPrice = 0.0;
    private String exitReason = null;
    private double exitFee = 0.0;

    public void resetForCandle(BacktestMarketRow row, TradingStrategy strategy) {
        this.currentRow = row;
        this.strategy = strategy;
        this.hasActiveTrade = false;
        this.side = TradeDirection.NO_TRADE;
        this.entryPrice = 0.0;
        this.sizeUsd = 0.0;
        this.shares = 0.0;
        this.entryFee = 0.0;
        this.modelProbability = 0.5;
        this.marketPrice = 0.5;
        this.edge = 0.0;
        this.exitExecuted = false;
        this.exitPrice = 0.0;
        this.exitReason = null;
        this.exitFee = 0.0;
    }

    @Override
    public ExecutionRouter getExecutionRouter() {
        return this;
    }

    @Override
    public HourlyPriceCache getPriceCache() {
        return priceCache;
    }

    @Override
    public void cancelOrder(String clientOrderId) {
        // No-op in backtest simulation
    }

    @Override
    public void submitOrder(OrderCommand command) {
        if (command == null || strategy == null) {
            return;
        }

        if ("BUY".equalsIgnoreCase(command.side())) {
            this.hasActiveTrade = true;
            this.entryPrice = command.price().doubleValue();
            this.shares = command.size().doubleValue();
            this.sizeUsd = this.entryPrice * this.shares;
            this.side = command.token().toLowerCase().contains("down")
                    ? TradeDirection.DOWN
                    : TradeDirection.UP;

            this.entryFee = this.shares * 0.07 * this.entryPrice * (1.0 - this.entryPrice);
            this.marketPrice = this.entryPrice;

            // Approximate model probability and edge
            double distTwap = currentRow.sClose() - currentRow.twapOpen();
            if (Math.abs(distTwap) > 80.0) {
                this.modelProbability = 0.94;
            } else {
                this.modelProbability = Math.min(0.95, this.entryPrice + 0.10);
            }
            this.edge = Math.max(0.0, this.modelProbability - this.marketPrice);

            ExecutionReport report = new ExecutionReport(
                    command.clientOrderId(),
                    "sim-fill-" + command.clientOrderId(),
                    "FILLED",
                    command.price(),
                    command.size(),
                    (currentRow.tStart() + 60) * 1000L,
                    "Simulated fill"
            );
            strategy.onExecutionReport(report);

        } else if ("SELL".equalsIgnoreCase(command.side())) {
            this.exitExecuted = true;
            this.exitPrice = command.price().doubleValue();
            this.exitFee = (this.exitPrice > 0.0 && this.exitPrice < 1.0)
                    ? (this.shares * 0.07 * this.exitPrice * (1.0 - this.exitPrice))
                    : 0.0;

            if (this.exitPrice >= 0.70) {
                this.exitReason = "Take Profit (0.70)";
            } else {
                this.exitReason = "Trailing Stop (+0.05)";
            }

            ExecutionReport report = new ExecutionReport(
                    command.clientOrderId(),
                    "sim-exit-" + command.clientOrderId(),
                    "FILLED",
                    command.price(),
                    command.size(),
                    (currentRow.tStart() + 300) * 1000L,
                    "Simulated exit fill"
            );
            strategy.onExecutionReport(report);
        }
    }


    public TradeRecord finalizeTrade(BacktestMarketRow row, TradeDirection actualOutcome, double currentBalance) {
        if (!hasActiveTrade) {
            return null;
        }

        double finalExitPrice;
        String finalExitReason;
        double finalExitFee;

        if (exitExecuted) {
            finalExitPrice = this.exitPrice;
            finalExitReason = this.exitReason;
            finalExitFee = this.exitFee;
        } else {
            finalExitPrice = (this.side == actualOutcome) ? 1.00 : 0.00;
            finalExitReason = "Resolution (TWAP 60s)";
            finalExitFee = 0.0;
        }

        double payout = this.shares * finalExitPrice;
        double totalFee = this.entryFee + finalExitFee;
        double netPnl = payout - this.sizeUsd - totalFee;
        double newBalance = currentBalance + netPnl;
        boolean isWin = netPnl > 0.0;

        return new TradeRecord(
                row.datetimeUtc(),
                row.tStart(),
                this.side,
                this.entryPrice,
                finalExitPrice,
                this.sizeUsd,
                this.shares,
                totalFee,
                netPnl,
                finalExitReason,
                this.modelProbability,
                this.marketPrice,
                this.edge,
                isWin,
                newBalance
        );
    }

    public boolean hasActiveTrade() {
        return hasActiveTrade;
    }

    public double getEstimatedModelProbUp(BacktestMarketRow row) {
        double distTwap = row.sClose() - row.twapOpen();
        if (distTwap > 80.0) {
            return 0.94;
        } else if (distTwap < -80.0) {
            return 0.06;
        }
        return 0.50;
    }
}
