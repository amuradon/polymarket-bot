package cz.polymarket.bot.connector.binance;

/**
 * Enumeration of Binance Spot and USD-M Futures WebSocket streams collected for backtesting and live execution.
 */
public enum BinanceStreamType {

    SPOT_AGG_TRADES("spot", "aggTrades", "", "@aggTrade", true),
    FUTURES_AGG_TRADES("futures", "aggTrades", "/market/ws", "@aggTrade", false),
    FUTURES_DIFF_DEPTH_100MS("futures", "depthDiff", "/public/ws", "@depth@100ms", false),
    FUTURES_PARTIAL_DEPTH_100MS("futures", "depthPartial", "/public/ws", "@depth20@100ms", false),
    FUTURES_FORCE_ORDER("futures", "forceOrder", "/market/ws", "@forceOrder", false);

    private final String marketCategory;
    private final String streamFolder;
    private final String wsRoutePrefix;
    private final String streamSuffix;
    private final boolean spot;

    BinanceStreamType(String marketCategory, String streamFolder, String wsRoutePrefix, String streamSuffix, boolean spot) {
        this.marketCategory = marketCategory;
        this.streamFolder = streamFolder;
        this.wsRoutePrefix = wsRoutePrefix;
        this.streamSuffix = streamSuffix;
        this.spot = spot;
    }

    /**
     * Returns the market category folder name ("spot" or "futures").
     *
     * @return market category
     */
    public String getMarketCategory() {
        return marketCategory;
    }

    /**
     * Returns the stream subfolder and file descriptor ("aggTrades", "depthDiff", "depthPartial", "forceOrder").
     *
     * @return stream folder name
     */
    public String getStreamFolder() {
        return streamFolder;
    }

    /**
     * Returns the Binance WebSocket stream suffix (e.g. "@aggTrade", "@depth@100ms").
     *
     * @return stream suffix
     */
    public String getStreamSuffix() {
        return streamSuffix;
    }

    /**
     * Indicates whether this stream belongs to Binance Spot (true) or USD-M Futures (false).
     *
     * @return true if Spot stream, false if Futures stream
     */
    public boolean isSpot() {
        return spot;
    }

    /**
     * Builds the full WebSocket URL for the given trading pair symbol, applying the routed
     * {@code /market/ws} or {@code /public/ws} path for Binance USD-M Futures streams.
     *
     * @param spotWsBaseUrl    base WebSocket URL for Binance Spot (e.g. wss://stream.binance.com:9443/ws)
     * @param futuresWsBaseUrl base WebSocket URL for Binance Futures (e.g. wss://fstream.binance.com)
     * @param symbol           trading pair symbol (e.g. BTCUSDT)
     * @return full WebSocket endpoint URL for this stream
     */
    public String buildWsUrl(String spotWsBaseUrl, String futuresWsBaseUrl, String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("Symbol cannot be null or blank");
        }
        String normalizedSymbol = symbol.trim().toLowerCase();
        if (spot) {
            return stripTrailingSlash(spotWsBaseUrl) + "/" + normalizedSymbol + streamSuffix;
        }
        String futuresRoot = stripFuturesTrailingWs(stripTrailingSlash(futuresWsBaseUrl));
        return futuresRoot + wsRoutePrefix + "/" + normalizedSymbol + streamSuffix;
    }

    private static String stripTrailingSlash(String url) {
        if (url == null) {
            return "";
        }
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static String stripFuturesTrailingWs(String url) {
        if (url.endsWith("/ws")) {
            return url.substring(0, url.length() - 3);
        }
        return url;
    }
}
