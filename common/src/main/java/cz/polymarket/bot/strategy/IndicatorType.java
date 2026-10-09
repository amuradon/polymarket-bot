package cz.polymarket.bot.strategy;

/**
 * Enumeration of technical indicators available for dynamic calculation and caching.
 * Strategies declare their required indicators via {@link TradingStrategy#getRequiredIndicators()}.
 */
public enum IndicatorType {
    TWAP,
    BASIS,
    VWAP,
    VWAP_ZSCORE,
    CVD,
    VOLATILITY_4H,
    ORDER_BOOK_IMBALANCE,
    MICRO_PRICE
}
