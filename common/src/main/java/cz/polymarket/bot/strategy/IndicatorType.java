package cz.polymarket.bot.strategy;

/**
 * Enumeration of technical indicators available for dynamic calculation and caching.
 * Strategies declare their required indicators via {@link TradingStrategy#getRequiredIndicators()}.
 */
public enum IndicatorType {
    /**
     * Cumulative Volume Delta measuring net aggressive buying vs selling volume.
     */
    CVD
}
