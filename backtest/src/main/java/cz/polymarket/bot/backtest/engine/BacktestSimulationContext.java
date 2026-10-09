package cz.polymarket.bot.backtest.engine;

import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import cz.polymarket.bot.backtest.stream.DynamicIndicatorEngine;
import cz.polymarket.bot.backtest.stream.SimulatedMatchingEngine;
import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.ExecutionRouter;
import cz.polymarket.bot.strategy.IndicatorType;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;

import java.util.Map;

/**
 * Strategy execution context simulating order fills, fees, and settlements during backtests,
 * strictly without looking ahead into future candle data.
 */
public class BacktestSimulationContext implements StrategyContext, ExecutionRouter {

    private final HourlyPriceCache priceCache = new HourlyPriceCache(3600L);
    private final SimulatedMatchingEngine matchingEngine;
    private final DynamicIndicatorEngine indicatorEngine;

    private TradingStrategy strategy;
    private BacktestMarketRow currentRow;
    private long currentTimestampMs;

    public BacktestSimulationContext(SimulatedMatchingEngine matchingEngine, DynamicIndicatorEngine indicatorEngine) {
        this.matchingEngine = matchingEngine;
        this.indicatorEngine = indicatorEngine;
    }

    public void resetForCandle(BacktestMarketRow row, TradingStrategy strategy) {
        this.currentRow = row;
        this.strategy = strategy;
        if (matchingEngine != null) {
            matchingEngine.resetForCandle();
        }
    }

    public void setCurrentTimestampMs(long currentTimestampMs) {
        this.currentTimestampMs = currentTimestampMs;
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
    public double getIndicatorValue(IndicatorType type) {
        return (indicatorEngine != null) ? indicatorEngine.getIndicatorValue(type) : Double.NaN;
    }

    @Override
    public Map<IndicatorType, Double> getIndicators() {
        return (indicatorEngine != null) ? indicatorEngine.getAllIndicators() : Map.of();
    }

    @Override
    public void cancelOrder(String clientOrderId) {
        // No-op in backtest simulation
    }

    @Override
    public void submitOrder(OrderCommand command) {
        if (command == null || matchingEngine == null) {
            return;
        }
        matchingEngine.submitOrder(command, currentTimestampMs);
    }

    public TradeRecord finalizeTrade(BacktestMarketRow row, TradeDirection actualOutcome, double currentBalance) {
        if (matchingEngine == null) {
            return null;
        }
        return matchingEngine.finalizeTrade(row.tStart(), row.datetimeUtc(), actualOutcome, currentBalance);
    }

    public boolean hasActiveTrade() {
        return matchingEngine != null && matchingEngine.hasActiveTrade();
    }

    public double getEstimatedModelProbUp(BacktestMarketRow row) {
        double spotPrice = (indicatorEngine != null && indicatorEngine.getCurrentSpotPrice() > 0.0)
                ? indicatorEngine.getCurrentSpotPrice()
                : row.sOpen();
        double distTwap = spotPrice - row.twapOpen();
        if (distTwap > 80.0) {
            return 0.94;
        } else if (distTwap < -80.0) {
            return 0.06;
        }
        return 0.50;
    }
}
