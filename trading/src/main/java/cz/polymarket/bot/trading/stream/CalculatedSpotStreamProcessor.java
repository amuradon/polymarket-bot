package cz.polymarket.bot.trading.stream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.calculator.CalculatedSpotTwapEngine;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.polymarket.stream.PolymarketStreamParser;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Shared zero-allocation streaming processor that parses live Binance Spot ({@code aggTrades}),
 * Kraken Spot ({@code trade} updates for {@code USDT/USD} and {@code <ASSET>/USD}), and
 * Coinbase Spot ({@code matches} for {@code <ASSET>-USD}) WebSocket frames into
 * {@link CalculatedSpotTwapEngine} and forwards the resulting USD spot and 60-second TWAP
 * to the active {@link TradingStrategy}.
 */
public final class CalculatedSpotStreamProcessor {

    private final ObjectMapper objectMapper;
    private final CalculatedSpotTwapEngine calculatedSpotTwapEngine;
    private final String cryptoSymbol;

    private double[] scratchKrakenPrices = new double[32];
    private double[] scratchKrakenQtys = new double[32];
    private boolean[] scratchKrakenIsUsdt = new boolean[32];

    /**
     * Constructs the multi-exchange spot stream processor.
     *
     * @param objectMapper             shared Jackson {@link ObjectMapper} used to create streaming {@link JsonParser} instances
     * @param calculatedSpotTwapEngine calculator aggregating Binance, Coinbase, and Kraken spot trades
     * @param symbol                   configured trading asset symbol (e.g. {@code "BTCUSDT"}, {@code "ETHUSDT"})
     */
    public CalculatedSpotStreamProcessor(
            ObjectMapper objectMapper,
            CalculatedSpotTwapEngine calculatedSpotTwapEngine,
            String symbol) {
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper cannot be null");
        }
        if (calculatedSpotTwapEngine == null) {
            throw new IllegalArgumentException("calculatedSpotTwapEngine cannot be null");
        }
        this.objectMapper = objectMapper;
        this.calculatedSpotTwapEngine = calculatedSpotTwapEngine;
        this.cryptoSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(symbol);
    }

    /**
     * Resets the underlying {@link CalculatedSpotTwapEngine}.
     */
    public void reset() {
        calculatedSpotTwapEngine.reset();
    }

    /**
     * Returns the normalized lowercase USD asset symbol delivered to {@link TradingStrategy#onCalculatedSpotTwap}
     * (e.g. {@code "btcusd"}).
     *
     * @return normalized lowercase USD symbol
     */
    public String getCryptoSymbol() {
        return cryptoSymbol;
    }

    /**
     * Returns the underlying {@link CalculatedSpotTwapEngine}.
     *
     * @return calculated spot and 60s TWAP engine
     */
    public CalculatedSpotTwapEngine getCalculatedSpotTwapEngine() {
        return calculatedSpotTwapEngine;
    }

    /**
     * Returns the most recently calculated Chainlink-emulated USD spot price.
     *
     * @return latest USD spot price, or {@link Double#NaN} if no asset trade has been processed
     */
    public double getLastSpotPriceUsd() {
        return calculatedSpotTwapEngine.getLastSpotPriceUsd();
    }

    /**
     * Returns the most recently calculated 60-second time-weighted average USD price.
     *
     * @return latest 60s TWAP in USD, or {@link Double#NaN} if no asset trade has been processed
     */
    public double getLastTwap60sPriceUsd() {
        return calculatedSpotTwapEngine.getLastTwap60sPriceUsd();
    }

    /**
     * Returns the current 5-second time-weighted EMA of the {@code USDT/USD} peg.
     *
     * @return current {@code USDT/USD} EMA
     */
    public double getUsdtUsdEma() {
        return calculatedSpotTwapEngine.getUsdtUsdEma();
    }

    /**
     * Parses and processes a Binance WebSocket text frame if it belongs to {@link BinanceStreamType#SPOT_AGG_TRADES}.
     *
     * @param streamType      Binance stream type
     * @param rawJson         raw JSON text frame
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap} and {@code onBinanceSpotTrade}
     * @return {@code true} if a valid Spot {@code aggTrade} was processed
     */
    public boolean processBinanceFrame(
            BinanceStreamType streamType,
            String rawJson,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (streamType != BinanceStreamType.SPOT_AGG_TRADES || rawJson == null || rawJson.isBlank()) {
            return false;
        }
        return processBinanceFrameBytes(
                streamType,
                rawJson.getBytes(StandardCharsets.UTF_8),
                recvTimestampMs,
                strategy
        );
    }

    /**
     * Parses and processes a raw UTF-8 {@code byte[]} Binance WebSocket frame if it belongs to
     * {@link BinanceStreamType#SPOT_AGG_TRADES}.
     *
     * @param streamType      Binance stream type
     * @param rawUtf8Bytes    raw UTF-8 JSON bytes
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap} and {@code onBinanceSpotTrade}
     * @return {@code true} if a valid Spot {@code aggTrade} was processed
     */
    public boolean processBinanceFrameBytes(
            BinanceStreamType streamType,
            byte[] rawUtf8Bytes,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (streamType != BinanceStreamType.SPOT_AGG_TRADES
                || rawUtf8Bytes == null
                || rawUtf8Bytes.length == 0
                || recvTimestampMs <= 0L) {
            return false;
        }
        try (JsonParser parser = objectMapper.createParser(rawUtf8Bytes)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return false;
            }
            double price = 0.0;
            double quantity = 1.0;
            boolean isBuyerMaker = false;

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.currentName();
                JsonToken token = parser.nextToken();
                if (field == null || token == null) {
                    break;
                }
                switch (field) {
                    case "p", "price" -> price = PolymarketStreamParser.parseCurrentNumberOrText(parser);
                    case "q", "qty", "quantity" -> {
                        double q = PolymarketStreamParser.parseCurrentNumberOrText(parser);
                        if (q > 0.0) {
                            quantity = q;
                        }
                    }
                    case "m", "isBuyerMaker" -> {
                        if (token == JsonToken.VALUE_TRUE) {
                            isBuyerMaker = true;
                        } else if (token == JsonToken.VALUE_FALSE) {
                            isBuyerMaker = false;
                        }
                    }
                    default -> parser.skipChildren();
                }
            }

            if (price <= 0.0 || Double.isNaN(price)) {
                return false;
            }
            onBinanceSpotTrade(recvTimestampMs, price, quantity, isBuyerMaker, strategy);
            return true;
        } catch (Exception e) {
            Log.warnf("Failed to parse Binance SPOT_AGG_TRADES frame: %s", e.getMessage());
            return false;
        }
    }

    /**
     * Processes a parsed Binance Spot {@code <ASSET>USDT} trade, updating {@link CalculatedSpotTwapEngine}
     * and notifying {@link TradingStrategy#onCalculatedSpotTwap} and {@link TradingStrategy#onBinanceSpotTrade}.
     *
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param priceUsdt       Binance Spot trade price in USDT
     * @param quantity        executed trade quantity in base asset units
     * @param isBuyerMaker    whether the buyer was the maker
     * @param strategy        active trading strategy
     */
    public void onBinanceSpotTrade(
            long recvTimestampMs,
            double priceUsdt,
            double quantity,
            boolean isBuyerMaker,
            TradingStrategy strategy) {
        if (priceUsdt <= 0.0 || Double.isNaN(priceUsdt) || recvTimestampMs <= 0L) {
            return;
        }
        double twap60sUsd = calculatedSpotTwapEngine.onBinanceUsdtTrade(recvTimestampMs, priceUsdt, quantity);
        double spotUsd = calculatedSpotTwapEngine.getLastSpotPriceUsd();
        long effectiveTsMs = resolveEffectiveTimestampMs(recvTimestampMs);
        if (strategy != null) {
            if (spotUsd > 0.0 && !Double.isNaN(spotUsd)) {
                strategy.onCalculatedSpotTwap(effectiveTsMs, cryptoSymbol, spotUsd, twap60sUsd);
            }
            strategy.onBinanceSpotTrade(recvTimestampMs, priceUsdt, quantity, isBuyerMaker);
        }
    }

    /**
     * Parses and processes a Kraken Spot v2 {@code trade} update JSON text frame.
     *
     * @param streamSymbol    normalized Kraken stream symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawJson         raw JSON text frame
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap} for asset trades
     * @return {@code true} if at least one live trade update was processed
     */
    public boolean processKrakenFrame(
            String streamSymbol,
            String rawJson,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (rawJson == null || rawJson.isBlank()) {
            return false;
        }
        return processKrakenFrameBytes(
                streamSymbol,
                rawJson.getBytes(StandardCharsets.UTF_8),
                recvTimestampMs,
                strategy
        );
    }

    /**
     * Parses and processes a raw UTF-8 {@code byte[]} Kraken Spot v2 {@code trade} update frame.
     *
     * @param streamSymbol    normalized Kraken stream symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawUtf8Bytes    raw UTF-8 JSON bytes
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap} for asset trades
     * @return {@code true} if at least one live trade update was processed
     */
    public boolean processKrakenFrameBytes(
            String streamSymbol,
            byte[] rawUtf8Bytes,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (rawUtf8Bytes == null || rawUtf8Bytes.length == 0 || recvTimestampMs <= 0L) {
            return false;
        }
        boolean defaultUsdtUsd = isUsdtUsdSymbol(streamSymbol);

        try (JsonParser parser = objectMapper.createParser(rawUtf8Bytes)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return false;
            }
            boolean isTradeChannel = false;
            boolean isUpdateType = false;
            int tradeCount = 0;

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.currentName();
                JsonToken token = parser.nextToken();
                if (field == null || token == null) {
                    break;
                }
                switch (field) {
                    case "channel" -> {
                        String ch = parser.getValueAsString("");
                        if (!"trade".equals(ch)) {
                            return false;
                        }
                        isTradeChannel = true;
                    }
                    case "type" -> {
                        String t = parser.getValueAsString("");
                        if (!"update".equals(t)) {
                            return false;
                        }
                        isUpdateType = true;
                    }
                    case "data" -> {
                        if (token == JsonToken.START_ARRAY) {
                            tradeCount = streamKrakenDataArray(parser, defaultUsdtUsd);
                        } else {
                            parser.skipChildren();
                        }
                    }
                    default -> parser.skipChildren();
                }
            }

            if (!isTradeChannel || !isUpdateType || tradeCount <= 0) {
                return false;
            }

            boolean updatedAssetSpot = false;
            double latestTwap60sUsd = Double.NaN;
            for (int i = 0; i < tradeCount; i++) {
                double price = scratchKrakenPrices[i];
                double qty = scratchKrakenQtys[i];
                if (scratchKrakenIsUsdt[i]) {
                    calculatedSpotTwapEngine.onUsdtUsdTrade(recvTimestampMs, price);
                } else {
                    latestTwap60sUsd = calculatedSpotTwapEngine.onKrakenUsdTrade(recvTimestampMs, price, qty);
                    updatedAssetSpot = true;
                }
            }

            if (updatedAssetSpot && strategy != null) {
                double spotUsd = calculatedSpotTwapEngine.getLastSpotPriceUsd();
                long effectiveTsMs = resolveEffectiveTimestampMs(recvTimestampMs);
                if (spotUsd > 0.0 && !Double.isNaN(spotUsd)) {
                    strategy.onCalculatedSpotTwap(effectiveTsMs, cryptoSymbol, spotUsd, latestTwap60sUsd);
                }
            }
            return true;
        } catch (Exception e) {
            Log.warnf("Failed to parse Kraken Spot trade frame: %s", e.getMessage());
            return false;
        }
    }

    private int streamKrakenDataArray(JsonParser parser, boolean defaultUsdtUsd) throws IOException {
        int count = 0;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() == JsonToken.START_OBJECT) {
                double price = 0.0;
                double qty = 1.0;
                boolean isUsdt = defaultUsdtUsd;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String field = parser.currentName();
                    parser.nextToken();
                    if (field == null) {
                        break;
                    }
                    switch (field) {
                        case "price" -> price = PolymarketStreamParser.parseCurrentNumberOrText(parser);
                        case "qty", "size", "quantity" -> {
                            double q = PolymarketStreamParser.parseCurrentNumberOrText(parser);
                            if (q > 0.0) {
                                qty = q;
                            }
                        }
                        case "symbol" -> {
                            String sym = parser.getValueAsString("");
                            if (!sym.isEmpty()) {
                                isUsdt = isUsdtUsdSymbol(sym);
                            }
                        }
                        default -> parser.skipChildren();
                    }
                }
                if (price > 0.0 && !Double.isNaN(price)) {
                    ensureKrakenScratchCapacity(count + 1);
                    scratchKrakenPrices[count] = price;
                    scratchKrakenQtys[count] = qty;
                    scratchKrakenIsUsdt[count] = isUsdt;
                    count++;
                }
            } else {
                parser.skipChildren();
            }
        }
        return count;
    }

    private void ensureKrakenScratchCapacity(int required) {
        if (required <= scratchKrakenPrices.length) {
            return;
        }
        int newCap = Math.max(required, scratchKrakenPrices.length << 1);
        scratchKrakenPrices = Arrays.copyOf(scratchKrakenPrices, newCap);
        scratchKrakenQtys = Arrays.copyOf(scratchKrakenQtys, newCap);
        scratchKrakenIsUsdt = Arrays.copyOf(scratchKrakenIsUsdt, newCap);
    }

    private static boolean isUsdtUsdSymbol(String symbol) {
        if (symbol == null || symbol.isEmpty()) {
            return false;
        }
        return KrakenStreamConnector.USDTUSD_SYMBOL.equalsIgnoreCase(symbol)
                || "USDT/USD".equalsIgnoreCase(symbol)
                || "USDT-USD".equalsIgnoreCase(symbol);
    }

    /**
     * Updates the 5-second time-weighted {@code USDT/USD} EMA in {@link CalculatedSpotTwapEngine}
     * from a Kraken {@code USDT/USD} spot trade.
     *
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param usdtUsdPrice    Kraken {@code USDT/USD} trade price
     */
    public void onKrakenUsdtUsdTrade(long recvTimestampMs, double usdtUsdPrice) {
        calculatedSpotTwapEngine.onUsdtUsdTrade(recvTimestampMs, usdtUsdPrice);
    }

    /**
     * Processes a parsed Kraken Spot {@code <ASSET>USD} trade, updating {@link CalculatedSpotTwapEngine}
     * and notifying {@link TradingStrategy#onCalculatedSpotTwap}.
     *
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param priceUsd        Kraken Spot trade price in USD
     * @param quantity        executed trade quantity in base asset units
     * @param strategy        active trading strategy
     */
    public void onKrakenSpotTrade(
            long recvTimestampMs,
            double priceUsd,
            double quantity,
            TradingStrategy strategy) {
        if (priceUsd <= 0.0 || Double.isNaN(priceUsd) || recvTimestampMs <= 0L) {
            return;
        }
        double twap60sUsd = calculatedSpotTwapEngine.onKrakenUsdTrade(recvTimestampMs, priceUsd, quantity);
        double spotUsd = calculatedSpotTwapEngine.getLastSpotPriceUsd();
        long effectiveTsMs = resolveEffectiveTimestampMs(recvTimestampMs);
        if (strategy != null && spotUsd > 0.0 && !Double.isNaN(spotUsd)) {
            strategy.onCalculatedSpotTwap(effectiveTsMs, cryptoSymbol, spotUsd, twap60sUsd);
        }
    }

    /**
     * Parses and processes a Coinbase Spot {@code matches} JSON text frame.
     *
     * @param streamSymbol    normalized Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawJson         raw JSON text frame
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap}
     * @return {@code true} if a valid live {@code match} trade was processed
     */
    public boolean processCoinbaseFrame(
            String streamSymbol,
            String rawJson,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (rawJson == null || rawJson.isBlank()) {
            return false;
        }
        return processCoinbaseFrameBytes(
                streamSymbol,
                rawJson.getBytes(StandardCharsets.UTF_8),
                recvTimestampMs,
                strategy
        );
    }

    /**
     * Parses and processes a raw UTF-8 {@code byte[]} Coinbase Spot {@code matches} frame.
     *
     * @param streamSymbol    normalized Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawUtf8Bytes    raw UTF-8 JSON bytes
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap}
     * @return {@code true} if a valid live {@code match} trade was processed
     */
    public boolean processCoinbaseFrameBytes(
            String streamSymbol,
            byte[] rawUtf8Bytes,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (rawUtf8Bytes == null || rawUtf8Bytes.length == 0 || recvTimestampMs <= 0L) {
            return false;
        }
        try (JsonParser parser = objectMapper.createParser(rawUtf8Bytes)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return false;
            }
            boolean isMatchType = false;
            double price = 0.0;
            double size = 1.0;

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.currentName();
                JsonToken token = parser.nextToken();
                if (field == null || token == null) {
                    break;
                }
                switch (field) {
                    case "type" -> {
                        String t = parser.getValueAsString("");
                        if (!"match".equals(t)) {
                            return false;
                        }
                        isMatchType = true;
                    }
                    case "price" -> price = PolymarketStreamParser.parseCurrentNumberOrText(parser);
                    case "size", "qty", "quantity" -> {
                        double s = PolymarketStreamParser.parseCurrentNumberOrText(parser);
                        if (s > 0.0) {
                            size = s;
                        }
                    }
                    default -> parser.skipChildren();
                }
            }

            if (!isMatchType || price <= 0.0 || Double.isNaN(price)) {
                return false;
            }
            onCoinbaseSpotTrade(recvTimestampMs, price, size, strategy);
            return true;
        } catch (Exception e) {
            Log.warnf("Failed to parse Coinbase Spot match frame: %s", e.getMessage());
            return false;
        }
    }

    /**
     * Processes a parsed Coinbase Spot {@code <ASSET>USD} trade, updating {@link CalculatedSpotTwapEngine}
     * and notifying {@link TradingStrategy#onCalculatedSpotTwap}.
     *
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param priceUsd        Coinbase Spot trade price in USD
     * @param quantity        executed trade quantity in base asset units
     * @param strategy        active trading strategy
     */
    public void onCoinbaseSpotTrade(
            long recvTimestampMs,
            double priceUsd,
            double quantity,
            TradingStrategy strategy) {
        if (priceUsd <= 0.0 || Double.isNaN(priceUsd) || recvTimestampMs <= 0L) {
            return;
        }
        double twap60sUsd = calculatedSpotTwapEngine.onCoinbaseUsdTrade(recvTimestampMs, priceUsd, quantity);
        double spotUsd = calculatedSpotTwapEngine.getLastSpotPriceUsd();
        long effectiveTsMs = resolveEffectiveTimestampMs(recvTimestampMs);
        if (strategy != null && spotUsd > 0.0 && !Double.isNaN(spotUsd)) {
            strategy.onCalculatedSpotTwap(effectiveTsMs, cryptoSymbol, spotUsd, twap60sUsd);
        }
    }

    private long resolveEffectiveTimestampMs(long recvTimestampMs) {
        long engineTs = calculatedSpotTwapEngine.getLastAssetTimestampMs();
        return engineTs > 0L ? Math.max(recvTimestampMs, engineTs) : recvTimestampMs;
    }
}
