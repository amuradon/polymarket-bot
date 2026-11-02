package cz.polymarket.bot.strategy;

/**
 * Enumeration of external (non-Polymarket) market data streams captured by the data collector.
 * <p>
 * Strategies declare which external streams they require via
 * {@link TradingStrategy#getRequiredExternalStreams()}. During backtesting, only the
 * declared external streams (and indicators derived from them) are loaded from Parquet
 * archives and processed, avoiding unnecessary I/O and CPU overhead for strategies that
 * operate exclusively on Polymarket streams.
 */
public enum ExternalStreamType {

    /**
     * Binance Spot aggregated trade stream ({@code binance/spot/<SYMBOL>/aggTrades}).
     */
    BINANCE_SPOT_AGG_TRADES,

    /**
     * Binance USD-M Futures aggregated trade stream ({@code binance/futures/<SYMBOL>/aggTrades}).
     */
    BINANCE_FUTURES_AGG_TRADES,

    /**
     * Binance USD-M Futures top-of-book partial depth stream ({@code binance/futures/<SYMBOL>/depthPartial}).
     */
    BINANCE_FUTURES_DEPTH_PARTIAL,

    /**
     * Binance USD-M Futures incremental diff depth stream ({@code binance/futures/<SYMBOL>/depthDiff}).
     */
    BINANCE_FUTURES_DEPTH_DIFF,

    /**
     * Binance USD-M Futures liquidation force-order stream ({@code binance/futures/<SYMBOL>/forceOrder}).
     */
    BINANCE_FUTURES_FORCE_ORDER,

    /**
     * Deribit Perpetual Futures trade stream ({@code deribit/futures/<INSTRUMENT>/trades}).
     */
    DERIBIT_FUTURES_TRADES,

    /**
     * Multi-exchange (Binance, Coinbase, Kraken) aggregated TWAP engine stream ({@code TwapEngine}).
     */
    TWAP_ENGINE
}
