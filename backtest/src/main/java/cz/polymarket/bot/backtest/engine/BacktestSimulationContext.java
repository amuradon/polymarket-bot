package cz.polymarket.bot.backtest.engine;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.stream.DynamicIndicatorEngine;
import cz.polymarket.bot.backtest.stream.SimulatedMatchingEngine;
import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.ExecutionRouter;
import cz.polymarket.bot.strategy.IndicatorType;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.TradeDirection;

import java.util.Map;

/**
 * Strategy execution context simulating order fills, fees, and settlements during backtests,
 * strictly without looking ahead into future candle data.
 */
public class BacktestSimulationContext implements StrategyContext, ExecutionRouter {

    private final HourlyPriceCache priceCache = new HourlyPriceCache(3600L);
    private final SimulatedMatchingEngine matchingEngine;
    private final DynamicIndicatorEngine indicatorEngine;

    private long currentTimestampMs;

    /**
     * Constructs the simulation context with matching engine and dynamic indicators.
     *
     * @param matchingEngine simulated latency and book fill matching engine
     * @param indicatorEngine dynamic real-time indicator calculation engine
     */
    public BacktestSimulationContext(SimulatedMatchingEngine matchingEngine, DynamicIndicatorEngine indicatorEngine) {
        this.matchingEngine = matchingEngine;
        this.indicatorEngine = indicatorEngine;
    }

    /**
     * Resets the execution state for a newly starting 15-minute candle.
     */
    public void resetForCandle() {
        if (matchingEngine != null) {
            matchingEngine.resetForCandle();
        }
    }

    /**
     * Updates the simulated high-precision clock timestamp in milliseconds UTC.
     *
     * @param currentTimestampMs current event timestamp in milliseconds UTC
     */
    public void setCurrentTimestampMs(long currentTimestampMs) {
        this.currentTimestampMs = currentTimestampMs;
    }

    /**
     * Returns the execution router for order submission and cancellation.
     */
    @Override
    public ExecutionRouter getExecutionRouter() {
        return this;
    }

    /**
     * Returns the shared rolling price cache.
     */
    @Override
    public HourlyPriceCache getPriceCache() {
        return priceCache;
    }

    /**
     * Returns the on-the-fly calculated indicator value.
     *
     * @param type indicator type
     * @return current indicator value, or {@link Double#NaN} if not available
     */
    @Override
    public double getIndicatorValue(IndicatorType type) {
        return (indicatorEngine != null) ? indicatorEngine.getIndicatorValue(type) : Double.NaN;
    }

    /**
     * Returns a snapshot of all currently calculated indicators.
     *
     * @return map of indicator types to current values
     */
    @Override
    public Map<IndicatorType, Double> getIndicators() {
        return (indicatorEngine != null) ? indicatorEngine.getAllIndicators() : Map.of();
    }

    /**
     * Requests cancellation of an order (no-op in backtest simulation).
     *
     * @param clientOrderId client-assigned order ID
     */
    @Override
    public void cancelOrder(String clientOrderId) {
        // No-op in backtest simulation
    }

    /**
     * Submits an order command to the simulated matching engine with current simulated timestamp.
     *
     * @param command order command specifying token, direction, price, and size
     */
    @Override
    public void submitOrder(OrderCommand command) {
        if (command == null || matchingEngine == null) {
            return;
        }
        matchingEngine.submitOrder(command, currentTimestampMs);
    }

    /**
     * Finalizes and records any active trade at candle close / settlement.
     *
     * @param metadata completed candle market metadata
     * @param actualOutcome settled binary outcome direction
     * @param currentBalance account balance before settlement
     * @return generated TradeRecord, or null if no trade was active
     */
    public TradeRecord finalizeTrade(CollectorMarketMetadata metadata, TradeDirection actualOutcome, double currentBalance) {
        if (matchingEngine == null || metadata == null) {
            return null;
        }
        return matchingEngine.finalizeTrade(metadata.tStart(), metadata.datetimeUtc(), actualOutcome, currentBalance);
    }

    /**
     * Checks whether there is currently an active position in the matching engine.
     *
     * @return true if an active trade exists
     */
    public boolean hasActiveTrade() {
        return matchingEngine != null && matchingEngine.hasActiveTrade();
    }

    /**
     * Computes the estimated directional probability for UP used in Brier score calibration.
     *
     * @param metadata current market metadata
     * @return estimated model probability for UP
     */
    public double getEstimatedModelProbUp(CollectorMarketMetadata metadata) {
        double openRef = (metadata != null && !Double.isNaN(metadata.twapOpen()) && metadata.twapOpen() > 0.0)
                ? metadata.twapOpen()
                : (indicatorEngine != null ? indicatorEngine.getTwapOpenPrice() : 0.0);
        double spotPrice = (indicatorEngine != null && indicatorEngine.getCurrentSpotPrice() > 0.0)
                ? indicatorEngine.getCurrentSpotPrice()
                : openRef;
        if (openRef <= 0.0 || spotPrice <= 0.0) {
            return 0.50;
        }
        double distTwap = spotPrice - openRef;
        if (distTwap > 80.0) {
            return 0.94;
        } else if (distTwap < -80.0) {
            return 0.06;
        }
        return 0.50;
    }
}
