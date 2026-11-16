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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Shared zero-allocation CDI bean that streams live Binance Spot ({@code aggTrades}),
 * Kraken Spot ({@code trade} updates for {@code USDT/USD} and {@code <ASSET>/USD}), and
 * Coinbase Spot ({@code matches} for {@code <ASSET>-USD}) WebSocket frames into
 * {@link CalculatedSpotTwapEngine} and forwards the resulting USD spot and 60-second TWAP
 * to the active {@link TradingStrategy}.
 */
@ApplicationScoped
public class CalculatedSpotStreamProcessor {

    private final ObjectMapper objectMapper;
    private final CalculatedSpotTwapEngine calculatedSpotTwapEngine;
    private final String cryptoSymbol;

    private double[] scratchKrakenPrices = new double[32];
    private double[] scratchKrakenQtys = new double[32];
    private boolean[] scratchKrakenIsUsdt = new boolean[32];

    /**
     * Constructs the multi-exchange spot stream processor CDI bean.
     *
     * @param objectMapper             shared Jackson {@link ObjectMapper} used to create streaming {@link JsonParser} instances
     * @param calculatedSpotTwapEngine calculator aggregating Binance, Coinbase, and Kraken spot trades
     * @param symbol                   configured trading asset symbol (e.g. {@code "BTCUSDT"}, {@code "ETHUSDT"})
     */
    @Inject
    public CalculatedSpotStreamProcessor(
            ObjectMapper objectMapper,
            CalculatedSpotTwapEngine calculatedSpotTwapEngine,
            @ConfigProperty(name = "polymarket.live.symbol", defaultValue = "BTCUSDT")
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
     * Parses and processes a Binance WebSocket text frame if it belongs to {@link BinanceStreamType#SPOT_AGG_TRADES}.
     *
     * @param streamType      Binance stream type
     * @param rawJson         raw JSON text frame
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap}
     */
    public void processBinanceFrame(
            BinanceStreamType streamType,
            String rawJson,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (streamType != BinanceStreamType.SPOT_AGG_TRADES || rawJson == null || rawJson.isBlank()) {
            return;
        }
        processBinanceFrameBytes(
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
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap}
     */
    public void processBinanceFrameBytes(
            BinanceStreamType streamType,
            byte[] rawUtf8Bytes,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (streamType != BinanceStreamType.SPOT_AGG_TRADES
                || rawUtf8Bytes == null
                || rawUtf8Bytes.length == 0
                || recvTimestampMs <= 0L) {
            return;
        }
        try (JsonParser parser = objectMapper.createParser(rawUtf8Bytes)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return;
            }
            double price = 0.0;
            double quantity = 1.0;

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
                    default -> parser.skipChildren();
                }
            }

            if (price <= 0.0 || Double.isNaN(price)) {
                return;
            }
            double twap60sUsd = calculatedSpotTwapEngine.onBinanceUsdtTrade(recvTimestampMs, price, quantity);
            double spotUsd = calculatedSpotTwapEngine.getLastSpotPriceUsd();
            long effectiveTsMs = resolveEffectiveTimestampMs(recvTimestampMs);
            if (strategy != null && spotUsd > 0.0 && !Double.isNaN(spotUsd)) {
                strategy.onCalculatedSpotTwap(effectiveTsMs, cryptoSymbol, spotUsd, twap60sUsd);
            }
        } catch (Exception e) {
            Log.warnf("Failed to parse Binance SPOT_AGG_TRADES frame: %s", e.getMessage());
        }
    }

    /**
     * Parses and processes a Kraken Spot v2 {@code trade} update JSON text frame.
     *
     * @param streamSymbol    normalized Kraken stream symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawJson         raw JSON text frame
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap} for asset trades
     */
    public void processKrakenFrame(
            String streamSymbol,
            String rawJson,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (rawJson == null || rawJson.isBlank()) {
            return;
        }
        processKrakenFrameBytes(
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
     */
    public void processKrakenFrameBytes(
            String streamSymbol,
            byte[] rawUtf8Bytes,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (rawUtf8Bytes == null || rawUtf8Bytes.length == 0 || recvTimestampMs <= 0L) {
            return;
        }
        boolean defaultUsdtUsd = isUsdtUsdSymbol(streamSymbol);

        try (JsonParser parser = objectMapper.createParser(rawUtf8Bytes)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return;
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
                            return;
                        }
                        isTradeChannel = true;
                    }
                    case "type" -> {
                        String t = parser.getValueAsString("");
                        if (!"update".equals(t)) {
                            return;
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
                return;
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
        } catch (Exception e) {
            Log.warnf("Failed to parse Kraken Spot trade frame: %s", e.getMessage());
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
     * Parses and processes a Coinbase Spot {@code matches} JSON text frame.
     *
     * @param streamSymbol    normalized Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawJson         raw JSON text frame
     * @param recvTimestampMs local receipt timestamp in milliseconds UTC
     * @param strategy        active trading strategy receiving {@code onCalculatedSpotTwap}
     */
    public void processCoinbaseFrame(
            String streamSymbol,
            String rawJson,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (rawJson == null || rawJson.isBlank()) {
            return;
        }
        processCoinbaseFrameBytes(
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
     */
    public void processCoinbaseFrameBytes(
            String streamSymbol,
            byte[] rawUtf8Bytes,
            long recvTimestampMs,
            TradingStrategy strategy) {
        if (rawUtf8Bytes == null || rawUtf8Bytes.length == 0 || recvTimestampMs <= 0L) {
            return;
        }
        try (JsonParser parser = objectMapper.createParser(rawUtf8Bytes)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return;
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
                            return;
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
                return;
            }
            double twap60sUsd = calculatedSpotTwapEngine.onCoinbaseUsdTrade(recvTimestampMs, price, size);
            double spotUsd = calculatedSpotTwapEngine.getLastSpotPriceUsd();
            long effectiveTsMs = resolveEffectiveTimestampMs(recvTimestampMs);
            if (strategy != null && spotUsd > 0.0 && !Double.isNaN(spotUsd)) {
                strategy.onCalculatedSpotTwap(effectiveTsMs, cryptoSymbol, spotUsd, twap60sUsd);
            }
        } catch (Exception e) {
            Log.warnf("Failed to parse Coinbase Spot match frame: %s", e.getMessage());
        }
    }

    private long resolveEffectiveTimestampMs(long recvTimestampMs) {
        long engineTs = calculatedSpotTwapEngine.getLastAssetTimestampMs();
        return engineTs > 0L ? Math.max(recvTimestampMs, engineTs) : recvTimestampMs;
    }
}
