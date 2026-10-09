package cz.polymarket.bot.strategy;

/**
 * Enumeration of technical indicators available for dynamic calculation and caching.
 * Strategies declare their required indicators via {@link TradingStrategy#getRequiredIndicators()}.
 */
public enum IndicatorType {
    /**
     * Time-Weighted Average Price of the reference spot asset over the candle window.
     */
    TWAP,

    /**
     * Basis spread between Binance Futures and Spot in basis points ((Futures - Spot) / Spot * 10000).
     */
    BASIS,

    /**
     * Volume-Weighted Average Price computed across recent candles or trades.
     */
    VWAP,

    /**
     * Number of standard deviations the current price deviates from the rolling VWAP.
     */
    VWAP_ZSCORE,

    /**
     * Cumulative Volume Delta measuring net aggressive buying vs selling volume.
     */
    CVD,

    /**
     * 4-hour annualized / realized volatility used for market regime filtering.
     */
    VOLATILITY_4H,

    /**
     * Order Book Imbalance ratio between bid and ask depth in the top book levels.
     */
    ORDER_BOOK_IMBALANCE,

    /**
     * Depth-weighted micro price reflecting fair near-term equilibrium price.
     */
    MICRO_PRICE
}
