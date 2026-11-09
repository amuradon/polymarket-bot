package cz.polymarket.bot.connector.polymarket.stream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Low-latency JSON frame parser for Polymarket PolyBolt v2 ({@code price.crypto} and {@code price.crypto.twap})
 * and Polymarket CLOB Market WebSocket streams ({@code book}, {@code price_change}, {@code best_bid_ask}).
 * Uses streaming Jackson {@link JsonParser} on both PolyBolt and CLOB hot paths to eliminate DOM tree allocations.
 */
@ApplicationScoped
public class PolymarketStreamParser {

    private final ObjectMapper objectMapper;
    private final String defaultSymbol;

    /**
     * Constructs the Polymarket stream parser.
     *
     * @param objectMapper shared Jackson mapper
     * @param symbol       configured trading asset symbol (e.g. {@code BTCUSDT}, {@code ETHUSDT}, {@code SOLUSDT})
     */
    @Inject
    public PolymarketStreamParser(
            ObjectMapper objectMapper,
            @ConfigProperty(name = "polymarket.live.symbol", defaultValue = "BTCUSDT")
            String symbol) {
        this.objectMapper = objectMapper;
        this.defaultSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(symbol);
    }

    /**
     * Parses a raw PolyBolt v2 WebSocket text frame into zero, one, or more {@link PolyBoltTick} updates
     * using streaming {@link JsonParser} without building a DOM tree.
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
        if (rawJson == null || rawJson.isBlank() || !rawJson.contains("price.crypto")) {
            return Collections.emptyList();
        }

        try (JsonParser parser = objectMapper.createParser(rawJson)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return Collections.emptyList();
            }

            String channel = "";
            long rootTs = recvTimestampMs;
            String symbol = this.defaultSymbol;
            String source = "chainlink";
            int windowSeconds = -1;
            double payloadPrice = 0.0;
            long payloadTs = -1L;
            List<double[]> rawDataItems = null;

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.currentName();
                JsonToken token = parser.nextToken();
                if (field == null || token == null) {
                    break;
                }
                switch (field) {
                    case "op" -> {
                        if (token != JsonToken.VALUE_NULL) {
                            return Collections.emptyList();
                        }
                    }
                    case "channel" -> channel = parser.getValueAsString("");
                    case "key" -> symbol = parser.getValueAsString(symbol);
                    case "ts", "timestamp" -> rootTs = parser.getValueAsLong(recvTimestampMs);
                    case "payload" -> {
                        if (token == JsonToken.START_OBJECT) {
                            while (parser.nextToken() != JsonToken.END_OBJECT) {
                                String pField = parser.currentName();
                                JsonToken pToken = parser.nextToken();
                                if (pField == null || pToken == null) {
                                    break;
                                }
                                switch (pField) {
                                    case "symbol" -> symbol = parser.getValueAsString(symbol);
                                    case "source" -> source = parser.getValueAsString("chainlink");
                                    case "window_seconds" -> windowSeconds = parser.getValueAsInt(-1);
                                    case "timestamp" -> payloadTs = parser.getValueAsLong(-1L);
                                    case "full_accuracy_value" -> {
                                        double v = parseCurrentNumberOrText(parser);
                                        if (v > 0.0) {
                                            payloadPrice = v;
                                        }
                                    }
                                    case "value", "price", "twap" -> {
                                        double v = parseCurrentNumberOrText(parser);
                                        if (v > 0.0 && payloadPrice <= 0.0) {
                                            payloadPrice = v;
                                        }
                                    }
                                    case "data" -> {
                                        if (pToken == JsonToken.START_ARRAY) {
                                            rawDataItems = streamPolyBoltDataArray(parser, recvTimestampMs);
                                        } else {
                                            parser.skipChildren();
                                        }
                                    }
                                    default -> parser.skipChildren();
                                }
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                    default -> parser.skipChildren();
                }
            }

            boolean isTwap;
            if ("price.crypto.twap".equals(channel)) {
                isTwap = true;
            } else if ("price.crypto".equals(channel)) {
                isTwap = false;
            } else {
                return Collections.emptyList();
            }

            int resolvedWindow = windowSeconds >= 0 ? windowSeconds : (isTwap ? 60 : 0);

            if (rawDataItems != null) {
                List<PolyBoltTick> ticks = new ArrayList<>(rawDataItems.size());
                for (double[] item : rawDataItems) {
                    double itemPrice = item[0];
                    long itemTsMs = normalizeTimestampMs((long) item[1], recvTimestampMs);
                    if (itemPrice > 0.0) {
                        ticks.add(new PolyBoltTick(
                                itemTsMs,
                                itemTsMs,
                                symbol,
                                source,
                                itemPrice,
                                isTwap,
                                resolvedWindow
                        ));
                    }
                }
                return ticks;
            }

            if (payloadPrice <= 0.0) {
                return Collections.emptyList();
            }
            long rawOracleTs = payloadTs > 0L ? payloadTs : rootTs;
            long oracleTimestampMs = normalizeTimestampMs(rawOracleTs, recvTimestampMs);

            return List.of(new PolyBoltTick(
                    recvTimestampMs,
                    oracleTimestampMs,
                    symbol,
                    source,
                    payloadPrice,
                    isTwap,
                    resolvedWindow
            ));
        } catch (Exception e) {
            Log.warnf("Failed to parse PolyBolt frame: %s", e.getMessage());
            return Collections.emptyList();
        }
    }

    private static List<double[]> streamPolyBoltDataArray(JsonParser parser, long defaultTsMs) throws IOException {
        List<double[]> items = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() == JsonToken.START_OBJECT) {
                double itemPrice = 0.0;
                long itemTs = defaultTsMs;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String dField = parser.currentName();
                    parser.nextToken();
                    if (dField == null) {
                        break;
                    }
                    switch (dField) {
                        case "full_accuracy_value" -> {
                            double v = parseCurrentNumberOrText(parser);
                            if (v > 0.0) {
                                itemPrice = v;
                            }
                        }
                        case "value", "price", "twap" -> {
                            double v = parseCurrentNumberOrText(parser);
                            if (v > 0.0 && itemPrice <= 0.0) {
                                itemPrice = v;
                            }
                        }
                        case "timestamp" -> itemTs = parser.getValueAsLong(defaultTsMs);
                        default -> parser.skipChildren();
                    }
                }
                if (itemPrice > 0.0) {
                    items.add(new double[]{itemPrice, (double) itemTs});
                }
            } else {
                parser.skipChildren();
            }
        }
        return items;
    }

    public Optional<PolymarketBboQuote> applyClobFrame(
            String rawJson,
            long recvTimestampMs,
            PolymarketLocalBook localBook) {
        if (!applyClobFrameToBookOnly(rawJson, localBook)) {
            return Optional.empty();
        }
        return localBook.snapshotQuote(recvTimestampMs);
    }

    /**
     * Result of applying a batch of CLOB WebSocket frames to a {@link PolymarketLocalBook}.
     *
     * @param quote                    final {@link PolymarketBboQuote} snapshot at the end of the batch, if valid
     * @param appliedFramesCount       number of frames in the batch that updated the orderbook
     * @param fastForwardedFramesCount number of older frames skipped without JSON parsing due to a newer full book snapshot
     */
    public record ClobBatchResult(
            Optional<PolymarketBboQuote> quote,
            int appliedFramesCount,
            int fastForwardedFramesCount
    ) {
    }

    /**
     * Applies a chronological batch of raw Polymarket CLOB WebSocket frames to {@code localBook} in a single pass.
     * <p>
     * If a newer frame in the batch is a full dual-asset {@code book} snapshot, all earlier frames in the batch
     * are fast-forwarded (skipped without JSON parsing). Intermediate frames mutate {@code localBook} in place,
     * and {@link PolymarketLocalBook#snapshotQuote(long)} is invoked only once at the end of the batch.
     *
     * @param frames    chronological batch of raw CLOB frames
     * @param localBook stateful local orderbook owned by the CLOB book worker thread
     * @return {@link ClobBatchResult} containing the final BBO quote and batch statistics
     */
    public ClobBatchResult applyClobFrameBatch(
            List<TimestampedFrame> frames,
            PolymarketLocalBook localBook) {
        if (frames == null || frames.isEmpty() || localBook == null) {
            return new ClobBatchResult(Optional.empty(), 0, 0);
        }
        int size = frames.size();
        int startIndex = 0;
        String yesToken = localBook.getYesTokenId();
        String noToken = localBook.getNoTokenId();
        if (size > 1 && !yesToken.isEmpty() && !noToken.isEmpty()) {
            for (int i = size - 1; i > 0; i--) {
                String raw = frames.get(i).rawText();
                if (isFullDualBookSnapshot(raw, yesToken, noToken)) {
                    startIndex = i;
                    break;
                }
            }
        }

        int fastForwarded = startIndex;
        int appliedCount = 0;
        long lastUpdatedRecvTs = 0L;
        for (int i = startIndex; i < size; i++) {
            TimestampedFrame frame = frames.get(i);
            if (applyClobFrameToBookOnly(frame.rawText(), localBook)) {
                appliedCount++;
                lastUpdatedRecvTs = frame.recvTs();
            }
        }

        if (appliedCount == 0) {
            return new ClobBatchResult(Optional.empty(), 0, fastForwarded);
        }
        return new ClobBatchResult(localBook.snapshotQuote(lastUpdatedRecvTs), appliedCount, fastForwarded);
    }

    private boolean applyClobFrameToBookOnly(String rawJson, PolymarketLocalBook localBook) {
        if (rawJson == null || rawJson.isBlank() || localBook == null) {
            return false;
        }
        if (rawJson.length() <= 16) {
            String trimmed = rawJson.trim();
            if ("PING".equalsIgnoreCase(trimmed) || "PONG".equalsIgnoreCase(trimmed)) {
                return false;
            }
        }
        if (!containsRelevantClobEvent(rawJson)) {
            return false;
        }
        long eventTimestampMs = extractEventTimestampMs(rawJson);
        if (eventTimestampMs > 0L && eventTimestampMs < localBook.getLastServerTimestampMs()) {
            return false;
        }

        try (JsonParser parser = objectMapper.createParser(rawJson)) {
            JsonToken firstToken = parser.nextToken();
            if (firstToken == null) {
                return false;
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
            if (updated && eventTimestampMs > 0L) {
                localBook.updateServerTimestampMs(eventTimestampMs);
            }
            return updated;
        } catch (Exception e) {
            Log.warnf("Failed to parse CLOB WS frame: %s", e.getMessage());
            return false;
        }
    }

    static long extractEventTimestampMs(String rawJson) {
        int idx = rawJson.lastIndexOf("\"timestamp\"");
        if (idx < 0) {
            return 0L;
        }
        int pos = idx + 11;
        int len = rawJson.length();
        while (pos < len) {
            char c = rawJson.charAt(pos);
            if (c == ':' || c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '"') {
                pos++;
            } else {
                break;
            }
        }
        long value = 0L;
        int digits = 0;
        while (pos < len) {
            char c = rawJson.charAt(pos);
            if (c >= '0' && c <= '9') {
                value = value * 10L + (c - '0');
                digits++;
                pos++;
            } else {
                break;
            }
        }
        return digits > 0 ? value : 0L;
    }

    private static boolean isFullDualBookSnapshot(String rawJson, String yesTokenId, String noTokenId) {
        if (rawJson == null || rawJson.length() < 64) {
            return false;
        }
        return rawJson.contains("\"bids\"")
                && rawJson.contains("\"asks\"")
                && rawJson.contains(yesTokenId)
                && rawJson.contains(noTokenId);
    }

    private static boolean containsRelevantClobEvent(String rawJson) {
        int len = rawJson.length();
        int tailStart = Math.max(0, len - 64);
        if (rawJson.indexOf("last_trade_price", tailStart) >= 0
                || rawJson.indexOf("tick_size_change", tailStart) >= 0) {
            return false;
        }
        return rawJson.contains("\"price_changes\"")
                || rawJson.contains("\"bids\"")
                || rawJson.contains("\"asks\"")
                || rawJson.contains("best_bid_ask")
                || rawJson.contains("price_change")
                || rawJson.contains("\"book\"");
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

    /**
     * Parses a raw JSON frame from the authenticated Polymarket CLOB User WebSocket channel ({@code /ws/user})
     * into zero, one, or more {@link PolymarketUserTradeEvent} records using streaming {@link JsonParser}.
     *
     * @param rawJson raw User channel WebSocket text frame (single object or array of objects)
     * @return list of parsed trade events (empty for {@code PONG}, {@code order} events, or malformed frames)
     */
    public List<PolymarketUserTradeEvent> parseUserTradeEvents(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return Collections.emptyList();
        }
        if (!rawJson.contains("trade") && !rawJson.contains("TRADE")) {
            return Collections.emptyList();
        }

        try (JsonParser parser = objectMapper.createParser(rawJson)) {
            JsonToken firstToken = parser.nextToken();
            if (firstToken == null) {
                return Collections.emptyList();
            }
            if (firstToken == JsonToken.START_OBJECT) {
                PolymarketUserTradeEvent single = streamSingleUserEvent(parser);
                return single != null ? List.of(single) : Collections.emptyList();
            }
            if (firstToken == JsonToken.START_ARRAY) {
                List<PolymarketUserTradeEvent> events = null;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() == JsonToken.START_OBJECT) {
                        PolymarketUserTradeEvent item = streamSingleUserEvent(parser);
                        if (item != null) {
                            if (events == null) {
                                events = new ArrayList<>(2);
                            }
                            events.add(item);
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
                return events != null ? events : Collections.emptyList();
            }
            return Collections.emptyList();
        } catch (Exception e) {
            Log.warnf("Failed to parse CLOB User WS frame: %s", e.getMessage());
            return Collections.emptyList();
        }
    }

    private static PolymarketUserTradeEvent streamSingleUserEvent(JsonParser parser) throws IOException {
        String eventType = "";
        String tradeId = "";
        String status = "";
        String side = "";
        String traderSide = "";
        double size = 0.0;
        double price = 0.0;
        int feeRateBps = -1;
        String assetId = "";
        String orderId = "";

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String field = parser.currentName();
            JsonToken token = parser.nextToken();
            if (field == null || token == null) {
                break;
            }
            switch (field) {
                case "event_type", "type" -> eventType = parser.getValueAsString("");
                case "id" -> tradeId = parser.getValueAsString("");
                case "status" -> status = parser.getValueAsString("");
                case "side" -> side = parser.getValueAsString("");
                case "trader_side" -> traderSide = parser.getValueAsString("");
                case "size", "matched_amount" -> size = parseCurrentNumberOrText(parser);
                case "price" -> price = parseCurrentNumberOrText(parser);
                case "fee_rate_bps" -> feeRateBps = (int) Math.round(parseCurrentNumberOrText(parser));
                case "asset_id" -> assetId = parser.getValueAsString("");
                case "taker_order_id", "order_id" -> orderId = parser.getValueAsString("");
                default -> parser.skipChildren();
            }
        }

        if (!"trade".equalsIgnoreCase(eventType)
                && !"trades".equalsIgnoreCase(eventType)
                && !"user_trades".equalsIgnoreCase(eventType)) {
            return null;
        }
        if (side.isBlank() && ("BUY".equalsIgnoreCase(traderSide) || "SELL".equalsIgnoreCase(traderSide))) {
            side = traderSide;
        }
        return new PolymarketUserTradeEvent(
                tradeId,
                status,
                side,
                size,
                price,
                feeRateBps,
                assetId,
                orderId
        );
    }

    private static long normalizeTimestampMs(long rawTs, long fallbackMs) {
        if (rawTs <= 0L) {
            return fallbackMs;
        }
        return rawTs < 10_000_000_000L ? rawTs * 1_000L : rawTs;
    }
}
