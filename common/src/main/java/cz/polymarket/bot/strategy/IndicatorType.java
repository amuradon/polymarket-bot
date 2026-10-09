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
    REALIZED_VOLATILITY,
    VOLATILITY_4H,
    BINANCE_OBI,
    ORDER_BOOK_IMBALANCE,
    MICRO_PRICE
}
