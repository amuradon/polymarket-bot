package cz.polymarket.bot.connector.polymarket.stream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Low-latency JSON frame parser for Polymarket PolyBolt v2 ({@code price.crypto} and {@code price.crypto.twap})
 * and Polymarket CLOB Market WebSocket streams ({@code book}, {@code price_change}, {@code best_bid_ask}).
 */
@ApplicationScoped
public class PolymarketStreamParser {

    private final ObjectMapper objectMapper;

    /**
     * Constructs the Polymarket stream parser.
     *
     * @param objectMapper shared Jackson mapper
     */
    @Inject
    public PolymarketStreamParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Parses a raw PolyBolt v2 WebSocket text frame into zero, one, or more {@link PolyBoltTick} updates.
     * <ul>
     *   <li>Control frames ({@code "op": "authed"}, {@code "op": "subscribed"}, {@code "PING"}/{@code "PONG"}) return an empty list.</li>
     *   <li>Initial snapshot frames ({@code payload.data: [...]}) return historical ticks ordered chronologically so the
     *       strategy's 60s TWAP and 1s volatility windows warm up immediately upon connection.</li>
     *   <li>Incremental live frames use {@code recvTimestampMs} as {@code timestampMs} and {@code payload.timestamp}
     *       (or {@code envelope.ts}) as {@code oracleTimestampMs}.</li>
     * </ul>
     *
     * @param rawJson         raw WebSocket text frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @return list of parsed {@link PolyBoltTick} records
     */
    public List<PolyBoltTick> parsePolyBoltFrame(String rawJson, long recvTimestampMs) {
        if (rawJson == null || rawJson.isBlank()) {
            return Collections.emptyList();
        }
        String trimmed = rawJson.trim();
        if ("PING".equalsIgnoreCase(trimmed) || "PONG".equalsIgnoreCase(trimmed)) {
            return Collections.emptyList();
        }

        try {
            JsonNode root = objectMapper.readTree(trimmed);
            if (root == null || !root.isObject() || root.hasNonNull("op")) {
                return Collections.emptyList();
            }

            String channel = root.path("channel").asText("");
            boolean isTwap;
            if ("price.crypto.twap".equals(channel)) {
                isTwap = true;
            } else if ("price.crypto".equals(channel)) {
                isTwap = false;
            } else {
                return Collections.emptyList();
            }

            JsonNode payload = root.get("payload");
            if (payload == null || !payload.isObject()) {
                return Collections.emptyList();
            }

            String symbol = payload.path("symbol").asText("btcusd");
            String source = payload.path("source").asText("chainlink");
            int windowSeconds = payload.path("window_seconds").asInt(isTwap ? 60 : 0);

            JsonNode dataArray = payload.get("data");
            if (dataArray != null && dataArray.isArray()) {
                List<PolyBoltTick> ticks = new ArrayList<>(dataArray.size());
                for (JsonNode item : dataArray) {
                    double price = extractPrice(item);
                    long rawTs = item.path("timestamp").asLong(recvTimestampMs);
                    long itemTsMs = normalizeTimestampMs(rawTs, recvTimestampMs);
                    if (price > 0.0) {
                        ticks.add(new PolyBoltTick(
                                itemTsMs,
                                itemTsMs,
                                symbol,
                                source,
                                price,
                                isTwap,
                                windowSeconds
                        ));
                    }
                }
                return ticks;
            }

            double price = extractPrice(payload);
            if (price <= 0.0) {
                return Collections.emptyList();
            }
            long rawOracleTs = payload.hasNonNull("timestamp")
                    ? payload.path("timestamp").asLong(recvTimestampMs)
                    : root.path("ts").asLong(recvTimestampMs);
            long oracleTimestampMs = normalizeTimestampMs(rawOracleTs, recvTimestampMs);

            return List.of(new PolyBoltTick(
                    recvTimestampMs,
                    oracleTimestampMs,
                    symbol,
                    source,
                    price,
                    isTwap,
                    windowSeconds
            ));
        } catch (Exception e) {
            Log.warnf("Failed to parse PolyBolt frame: %s", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Applies a raw Polymarket CLOB Market WebSocket frame ({@code book}, {@code price_change}, or {@code best_bid_ask})
     * to {@code localBook} and returns the updated {@link PolymarketBboQuote} if a valid BBO is present.
     *
     * @param rawJson         raw CLOB WebSocket frame (JSON object or JSON array of objects)
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param localBook       stateful local orderbook for the active market
     * @return updated {@link PolymarketBboQuote} if available
     */
    public Optional<PolymarketBboQuote> applyClobFrame(
            String rawJson,
            long recvTimestampMs,
            PolymarketLocalBook localBook) {
        if (rawJson == null || rawJson.isBlank() || localBook == null) {
            return Optional.empty();
        }
        if (rawJson.length() <= 16) {
            String trimmed = rawJson.trim();
            if ("PING".equalsIgnoreCase(trimmed) || "PONG".equalsIgnoreCase(trimmed)) {
                return Optional.empty();
            }
        }
        if (!containsRelevantClobEvent(rawJson)) {
            return Optional.empty();
        }

        try (JsonParser parser = objectMapper.createParser(rawJson)) {
            JsonToken firstToken = parser.nextToken();
            if (firstToken == null) {
                return Optional.empty();
            }
            boolean updated = false;
            if (firstToken == JsonToken.START_ARRAY) {
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() == JsonToken.START_OBJECT) {
                        updated |= streamSingleClobEvent(parser, localBook);
                    } else {
                        parser.skipChildren();
                    }
                }
            } else if (firstToken == JsonToken.START_OBJECT) {
                updated = streamSingleClobEvent(parser, localBook);
            }
            return updated ? localBook.snapshotQuote(recvTimestampMs) : Optional.empty();
        } catch (Exception e) {
            Log.warnf("Failed to parse CLOB WS frame: %s", e.getMessage());
            return Optional.empty();
        }
    }

    private static boolean containsRelevantClobEvent(String rawJson) {
        return rawJson.contains("price_change")
                || rawJson.contains("best_bid_ask")
                || rawJson.contains("\"book\"")
                || rawJson.contains("\"bids\"")
                || rawJson.contains("\"asks\"");
    }

    private boolean streamSingleClobEvent(JsonParser parser, PolymarketLocalBook localBook) throws IOException {
        String eventType = "";
        String assetId = "";
        double bestBid = 0.0;
        double bestAsk = 0.0;
        boolean bookCleared = false;
        boolean hasBookLevels = false;
        boolean priceChanged = false;
        List< double[] > deferredBids = null;
        List< double[] > deferredAsks = null;

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            JsonToken valueToken = parser.nextToken();
            if (fieldName == null || valueToken == null) {
                break;
            }
            switch (fieldName) {
                case "event_type" -> eventType = parser.getValueAsString("");
                case "asset_id" -> {
                    assetId = parser.getValueAsString("");
                    if (!assetId.isEmpty() && (deferredBids != null || deferredAsks != null)) {
                        localBook.clearAssetBook(assetId);
                        bookCleared = true;
                        if (deferredBids != null) {
                            for (double[] lvl : deferredBids) {
                                localBook.updateLevel(assetId, true, lvl[0], lvl[1]);
                            }
                            deferredBids = null;
                        }
                        if (deferredAsks != null) {
                            for (double[] lvl : deferredAsks) {
                                localBook.updateLevel(assetId, false, lvl[0], lvl[1]);
                            }
                            deferredAsks = null;
                        }
                    }
                }
                case "bids" -> {
                    if (valueToken == JsonToken.START_ARRAY) {
                        hasBookLevels = true;
                        if (!assetId.isEmpty()) {
                            if (!bookCleared) {
                                localBook.clearAssetBook(assetId);
                                bookCleared = true;
                            }
                            streamBookLevels(parser, assetId, true, localBook);
                        } else {
                            deferredBids = readDeferredBookLevels(parser);
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
                case "asks" -> {
                    if (valueToken == JsonToken.START_ARRAY) {
                        hasBookLevels = true;
                        if (!assetId.isEmpty()) {
                            if (!bookCleared) {
                                localBook.clearAssetBook(assetId);
                                bookCleared = true;
                            }
                            streamBookLevels(parser, assetId, false, localBook);
                        } else {
                            deferredAsks = readDeferredBookLevels(parser);
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
                case "price_changes" -> {
                    if (valueToken == JsonToken.START_ARRAY) {
                        priceChanged |= streamPriceChangesArray(parser, assetId, localBook);
                    } else {
                        parser.skipChildren();
                    }
                }
                case "best_bid" -> bestBid = parseCurrentNumberOrText(parser);
                case "best_ask" -> bestAsk = parseCurrentNumberOrText(parser);
                default -> parser.skipChildren();
            }
        }

        if (hasBookLevels && (deferredBids != null || deferredAsks != null)) {
            localBook.clearAssetBook(assetId);
            if (deferredBids != null) {
                for (double[] lvl : deferredBids) {
                    localBook.updateLevel(assetId, true, lvl[0], lvl[1]);
                }
            }
            if (deferredAsks != null) {
                for (double[] lvl : deferredAsks) {
                    localBook.updateLevel(assetId, false, lvl[0], lvl[1]);
                }
            }
        }

        if ("book".equals(eventType) || (eventType.isEmpty() && hasBookLevels)) {
            if (!bookCleared && !hasBookLevels) {
                localBook.clearAssetBook(assetId);
            }
            return true;
        }

        if ("price_change".equals(eventType)) {
            return priceChanged;
        }

        if ("best_bid_ask".equals(eventType) && bestBid > 0.0 && bestAsk > 0.0) {
            localBook.applyBestBidAsk(assetId, bestBid, bestAsk);
            return true;
        }
        return false;
    }

    private static void streamBookLevels(
            JsonParser parser,
            String assetId,
            boolean isBid,
            PolymarketLocalBook localBook) throws IOException {
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() == JsonToken.START_OBJECT) {
                double price = 0.0;
                double size = 0.0;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String field = parser.currentName();
                    parser.nextToken();
                    if ("price".equals(field)) {
                        price = parseCurrentNumberOrText(parser);
                    } else if ("size".equals(field)) {
                        size = parseCurrentNumberOrText(parser);
                    } else {
                        parser.skipChildren();
                    }
                }
                if (price > 0.0 && size > 0.0) {
                    localBook.updateLevel(assetId, isBid, price, size);
                }
            } else {
                parser.skipChildren();
            }
        }
    }

    private static List<double[]> readDeferredBookLevels(JsonParser parser) throws IOException {
        List<double[]> levels = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() == JsonToken.START_OBJECT) {
                double price = 0.0;
                double size = 0.0;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String field = parser.currentName();
                    parser.nextToken();
                    if ("price".equals(field)) {
                        price = parseCurrentNumberOrText(parser);
                    } else if ("size".equals(field)) {
                        size = parseCurrentNumberOrText(parser);
                    } else {
                        parser.skipChildren();
                    }
                }
                if (price > 0.0 && size > 0.0) {
                    levels.add(new double[]{price, size});
                }
            } else {
                parser.skipChildren();
            }
        }
        return levels;
    }

    private static boolean streamPriceChangesArray(
            JsonParser parser,
            String fallbackAssetId,
            PolymarketLocalBook localBook) throws IOException {
        boolean changed = false;
        String lastAsset1 = null;
        double lastBestBid1 = 0.0;
        double lastBestAsk1 = 0.0;
        String lastAsset2 = null;
        double lastBestBid2 = 0.0;
        double lastBestAsk2 = 0.0;

        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() == JsonToken.START_OBJECT) {
                String itemAsset = fallbackAssetId;
                double price = 0.0;
                double size = 0.0;
                boolean isBid = false;
                double bestBid = 0.0;
                double bestAsk = 0.0;

                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String field = parser.currentName();
                    parser.nextToken();
                    switch (field) {
                        case "asset_id" -> itemAsset = parser.getValueAsString(fallbackAssetId);
                        case "price" -> price = parseCurrentNumberOrText(parser);
                        case "size" -> size = parseCurrentNumberOrText(parser);
                        case "side" -> {
                            String side = parser.getValueAsString("");
                            isBid = "BUY".equalsIgnoreCase(side) || "BID".equalsIgnoreCase(side);
                        }
                        case "best_bid" -> bestBid = parseCurrentNumberOrText(parser);
                        case "best_ask" -> bestAsk = parseCurrentNumberOrText(parser);
                        default -> parser.skipChildren();
                    }
                }
                if (price > 0.0) {
                    localBook.updateLevel(itemAsset, isBid, price, size);
                    changed = true;
                }
                if (bestBid > 0.0 && bestAsk > 0.0) {
                    if (lastAsset1 == null || lastAsset1.equals(itemAsset)) {
                        lastAsset1 = itemAsset;
                        lastBestBid1 = bestBid;
                        lastBestAsk1 = bestAsk;
                    } else {
                        lastAsset2 = itemAsset;
                        lastBestBid2 = bestBid;
                        lastBestAsk2 = bestAsk;
                    }
                    changed = true;
                }
            } else {
                parser.skipChildren();
            }
        }
        if (lastAsset1 != null && lastBestBid1 > 0.0 && lastBestAsk1 > 0.0) {
            localBook.applyBestBidAsk(lastAsset1, lastBestBid1, lastBestAsk1);
        }
        if (lastAsset2 != null && lastBestBid2 > 0.0 && lastBestAsk2 > 0.0) {
            localBook.applyBestBidAsk(lastAsset2, lastBestBid2, lastBestAsk2);
        }
        return changed;
    }

    private static double parseCurrentNumberOrText(JsonParser parser) throws IOException {
        JsonToken token = parser.currentToken();
        if (token == JsonToken.VALUE_NUMBER_FLOAT || token == JsonToken.VALUE_NUMBER_INT) {
            return parser.getDoubleValue();
        }
        if (token == JsonToken.VALUE_STRING) {
            String text = parser.getText();
            if (text != null && !text.isEmpty()) {
                try {
                    return Double.parseDouble(text.trim());
                } catch (NumberFormatException ignored) {
                    return 0.0;
                }
            }
        }
        return 0.0;
    }

    private static double extractPrice(JsonNode node) {
        double fullAcc = parseDoubleSafe(node.get("full_accuracy_value"));
        if (fullAcc > 0.0) {
            return fullAcc;
        }
        return parseDoubleSafe(node.get("value"));
    }

    private static double parseDoubleSafe(JsonNode node) {
        if (node == null || node.isNull()) {
            return 0.0;
        }
        if (node.isNumber()) {
            return node.asDouble();
        }
        if (node.isTextual()) {
            try {
                return Double.parseDouble(node.asText().trim());
            } catch (NumberFormatException ignored) {
                return 0.0;
            }
        }
        return 0.0;
    }

    private static long normalizeTimestampMs(long rawTs, long fallbackMs) {
        if (rawTs <= 0L) {
            return fallbackMs;
        }
        return rawTs < 10_000_000_000L ? rawTs * 1_000L : rawTs;
    }
}
