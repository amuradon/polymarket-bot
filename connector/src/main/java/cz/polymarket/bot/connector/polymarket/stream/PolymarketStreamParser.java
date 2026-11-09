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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Low-latency JSON frame parser for Polymarket PolyBolt v2 ({@code price.crypto} and {@code price.crypto.twap})
 * and Polymarket CLOB Market WebSocket streams ({@code book}, {@code price_change}, {@code best_bid_ask}).
 * Uses streaming Jackson {@link JsonParser} directly over UTF-8 {@code byte[]} buffers and zero-allocation
 * {@code char[]} number parsing on hot paths to eliminate DOM tree and per-level {@link String} allocations.
 */
@ApplicationScoped
public class PolymarketStreamParser {

    private static final byte[] BYTES_BIDS = "\"bids\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_ASKS = "\"asks\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_PRICE_CHANGES = "\"price_changes\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_BEST_BID_ASK = "best_bid_ask".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_PRICE_CHANGE = "price_change".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_BOOK = "\"book\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_LAST_TRADE_PRICE = "last_trade_price".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_TICK_SIZE_CHANGE = "tick_size_change".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_TIMESTAMP = "\"timestamp\"".getBytes(StandardCharsets.UTF_8);

    private static final double[] POW10 = {
            1.0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
            1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
    };

    private static final ThreadLocal<DeferredBookBuffers> DEFERRED_BUFFERS =
            ThreadLocal.withInitial(DeferredBookBuffers::new);

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
                if (ticks.size() > 1) {
                    ticks.sort(Comparator.comparingLong(PolyBoltTick::oracleTimestampMs));
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

    /**
     * Applies a single raw CLOB WebSocket JSON text frame to {@code localBook} and returns the updated BBO snapshot.
     *
     * @param rawJson         raw CLOB JSON text frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param localBook       stateful local orderbook
     * @return updated {@link PolymarketBboQuote} if the frame modified the book
     */
    public Optional<PolymarketBboQuote> applyClobFrame(
            String rawJson,
            long recvTimestampMs,
            PolymarketLocalBook localBook) {
        if (rawJson == null || rawJson.isEmpty()) {
            return Optional.empty();
        }
        return applyClobFrame(rawJson.getBytes(StandardCharsets.UTF_8), recvTimestampMs, localBook);
    }

    /**
     * Applies a single raw UTF-8 CLOB WebSocket frame directly from {@code rawUtf8Bytes} to {@code localBook}
     * without allocating a {@link String} or {@link java.io.StringReader}.
     *
     * @param rawUtf8Bytes    raw UTF-8 bytes of the CLOB WebSocket frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param localBook       stateful local orderbook
     * @return updated {@link PolymarketBboQuote} if the frame modified the book
     */
    public Optional<PolymarketBboQuote> applyClobFrame(
            byte[] rawUtf8Bytes,
            long recvTimestampMs,
            PolymarketLocalBook localBook) {
        if (!applyClobFrameToBookOnly(rawUtf8Bytes, localBook)) {
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
     * Operates directly on {@link TimestampedFrame#rawUtf8Bytes()} without allocating intermediate {@link String}
     * instances. If a newer frame in the batch is a full dual-asset {@code book} snapshot, all earlier frames in
     * the batch are fast-forwarded (skipped without JSON parsing). Intermediate frames mutate {@code localBook}
     * in place, and {@link PolymarketLocalBook#snapshotQuote(long)} is invoked only once at the end of the batch.
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
        byte[] yesTokenBytes = localBook.getYesTokenBytes();
        byte[] noTokenBytes = localBook.getNoTokenBytes();
        if (size > 1 && yesTokenBytes.length > 0 && noTokenBytes.length > 0) {
            for (int i = size - 1; i > 0; i--) {
                byte[] rawBytes = frames.get(i).rawUtf8Bytes();
                if (isFullDualBookSnapshot(rawBytes, yesTokenBytes, noTokenBytes)) {
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
            if (applyClobFrameToBookOnly(frame.rawUtf8Bytes(), localBook)) {
                appliedCount++;
                lastUpdatedRecvTs = frame.recvTs();
            }
        }

        if (appliedCount == 0) {
            return new ClobBatchResult(Optional.empty(), 0, fastForwarded);
        }
        return new ClobBatchResult(localBook.snapshotQuote(lastUpdatedRecvTs), appliedCount, fastForwarded);
    }

    private boolean applyClobFrameToBookOnly(byte[] rawBytes, PolymarketLocalBook localBook) {
        if (rawBytes == null || rawBytes.length == 0 || localBook == null) {
            return false;
        }
        if (rawBytes.length <= 16 && isPingOrPongOrBlank(rawBytes)) {
            return false;
        }
        if (!containsRelevantClobEvent(rawBytes)) {
            return false;
        }
        long eventTimestampMs = extractEventTimestampMs(rawBytes);
        if (eventTimestampMs > 0L && eventTimestampMs < localBook.getLastServerTimestampMs()) {
            return false;
        }

        try (JsonParser parser = objectMapper.createParser(rawBytes)) {
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

    private static boolean isPingOrPongOrBlank(byte[] rawBytes) {
        int start = 0;
        int end = rawBytes.length;
        while (start < end && rawBytes[start] <= ' ') {
            start++;
        }
        while (end > start && rawBytes[end - 1] <= ' ') {
            end--;
        }
        int len = end - start;
        if (len == 0) {
            return true;
        }
        if (len == 4) {
            int b0 = rawBytes[start] | 0x20;
            int b1 = rawBytes[start + 1] | 0x20;
            int b2 = rawBytes[start + 2] | 0x20;
            int b3 = rawBytes[start + 3] | 0x20;
            return b0 == 'p' && (b1 == 'i' || b1 == 'o') && b2 == 'n' && b3 == 'g';
        }
        return false;
    }

    static long extractEventTimestampMs(byte[] rawBytes) {
        int idx = lastIndexOfBytes(rawBytes, BYTES_TIMESTAMP);
        if (idx < 0) {
            return 0L;
        }
        int pos = idx + BYTES_TIMESTAMP.length;
        int len = rawBytes.length;
        while (pos < len) {
            byte b = rawBytes[pos];
            if (b == ':' || b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '"') {
                pos++;
            } else {
                break;
            }
        }
        long value = 0L;
        int digits = 0;
        while (pos < len) {
            byte b = rawBytes[pos];
            if (b >= '0' && b <= '9') {
                value = value * 10L + (b - '0');
                digits++;
                pos++;
            } else {
                break;
            }
        }
        return digits > 0 ? value : 0L;
    }

    private static boolean isFullDualBookSnapshot(byte[] rawBytes, byte[] yesTokenBytes, byte[] noTokenBytes) {
        if (rawBytes == null || rawBytes.length < 64) {
            return false;
        }
        return indexOfBytes(rawBytes, BYTES_BIDS, 0) >= 0
                && indexOfBytes(rawBytes, BYTES_ASKS, 0) >= 0
                && indexOfBytes(rawBytes, yesTokenBytes, 0) >= 0
                && indexOfBytes(rawBytes, noTokenBytes, 0) >= 0;
    }

    private static boolean containsRelevantClobEvent(byte[] rawBytes) {
        int len = rawBytes.length;
        int tailStart = Math.max(0, len - 64);
        if (indexOfBytes(rawBytes, BYTES_LAST_TRADE_PRICE, tailStart) >= 0
                || indexOfBytes(rawBytes, BYTES_TICK_SIZE_CHANGE, tailStart) >= 0) {
            return false;
        }
        return indexOfBytes(rawBytes, BYTES_PRICE_CHANGES, 0) >= 0
                || indexOfBytes(rawBytes, BYTES_BIDS, 0) >= 0
                || indexOfBytes(rawBytes, BYTES_ASKS, 0) >= 0
                || indexOfBytes(rawBytes, BYTES_BEST_BID_ASK, 0) >= 0
                || indexOfBytes(rawBytes, BYTES_PRICE_CHANGE, 0) >= 0
                || indexOfBytes(rawBytes, BYTES_BOOK, 0) >= 0;
    }

    private static int indexOfBytes(byte[] haystack, byte[] needle, int fromIndex) {
        int nLen = needle.length;
        if (nLen == 0) {
            return fromIndex;
        }
        int max = haystack.length - nLen;
        byte first = needle[0];
        for (int i = Math.max(0, fromIndex); i <= max; i++) {
            if (haystack[i] != first) {
                while (++i <= max && haystack[i] != first) {
                    // fast scan to first matching byte
                }
                if (i > max) {
                    break;
                }
            }
            int j = 1;
            while (j < nLen && haystack[i + j] == needle[j]) {
                j++;
            }
            if (j == nLen) {
                return i;
            }
        }
        return -1;
    }

    private static int lastIndexOfBytes(byte[] haystack, byte[] needle) {
        int nLen = needle.length;
        int max = haystack.length - nLen;
        if (nLen == 0 || max < 0) {
            return -1;
        }
        byte first = needle[0];
        for (int i = max; i >= 0; i--) {
            if (haystack[i] == first) {
                int j = 1;
                while (j < nLen && haystack[i + j] == needle[j]) {
                    j++;
                }
                if (j == nLen) {
                    return i;
                }
            }
        }
        return -1;
    }

    private boolean streamSingleClobEvent(JsonParser parser, PolymarketLocalBook localBook) throws IOException {
        String eventType = "";
        String assetId = "";
        double bestBid = 0.0;
        double bestAsk = 0.0;
        boolean bookCleared = false;
        boolean hasBookLevels = false;
        boolean priceChanged = false;
        DeferredBookBuffers deferred = null;

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
                    if (!assetId.isEmpty() && deferred != null && (deferred.bidsLen > 0 || deferred.asksLen > 0)) {
                        localBook.clearAssetBook(assetId);
                        bookCleared = true;
                        deferred.flushTo(assetId, localBook);
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
                            if (deferred == null) {
                                deferred = DEFERRED_BUFFERS.get();
                                deferred.reset();
                            }
                            readDeferredBookLevels(parser, deferred, true);
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
                            if (deferred == null) {
                                deferred = DEFERRED_BUFFERS.get();
                                deferred.reset();
                            }
                            readDeferredBookLevels(parser, deferred, false);
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

        if (hasBookLevels && deferred != null && (deferred.bidsLen > 0 || deferred.asksLen > 0)) {
            localBook.clearAssetBook(assetId);
            deferred.flushTo(assetId, localBook);
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

    private static void readDeferredBookLevels(
            JsonParser parser,
            DeferredBookBuffers buffers,
            boolean isBid) throws IOException {
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
                    buffers.addLevel(isBid, price, size);
                }
            } else {
                parser.skipChildren();
            }
        }
    }

    private static final class DeferredBookBuffers {
        private double[] bids = new double[512];
        private double[] asks = new double[512];
        private int bidsLen = 0;
        private int asksLen = 0;

        void reset() {
            bidsLen = 0;
            asksLen = 0;
        }

        void addLevel(boolean isBid, double price, double size) {
            if (isBid) {
                if (bidsLen + 2 > bids.length) {
                    bids = Arrays.copyOf(bids, bids.length << 1);
                }
                bids[bidsLen++] = price;
                bids[bidsLen++] = size;
            } else {
                if (asksLen + 2 > asks.length) {
                    asks = Arrays.copyOf(asks, asks.length << 1);
                }
                asks[asksLen++] = price;
                asks[asksLen++] = size;
            }
        }

        void flushTo(String assetId, PolymarketLocalBook localBook) {
            for (int i = 0; i < bidsLen; i += 2) {
                localBook.updateLevel(assetId, true, bids[i], bids[i + 1]);
            }
            for (int i = 0; i < asksLen; i += 2) {
                localBook.updateLevel(assetId, false, asks[i], asks[i + 1]);
            }
            bidsLen = 0;
            asksLen = 0;
        }
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
            char[] buf = parser.getTextCharacters();
            int offset = parser.getTextOffset();
            int len = parser.getTextLength();
            return parseFastDouble(buf, offset, len);
        }
        return 0.0;
    }

    static double parseFastDouble(char[] buf, int offset, int len) {
        if (buf == null || len <= 0) {
            return 0.0;
        }
        int start = offset;
        int end = offset + len;
        while (start < end && buf[start] <= ' ') {
            start++;
        }
        while (end > start && buf[end - 1] <= ' ') {
            end--;
        }
        if (start >= end) {
            return 0.0;
        }

        int pos = start;
        boolean negative = false;
        char first = buf[pos];
        if (first == '-') {
            negative = true;
            pos++;
        } else if (first == '+') {
            pos++;
        }
        if (pos >= end) {
            return 0.0;
        }

        long mantissa = 0L;
        int fracScale = 0;
        boolean seenDot = false;
        int digits = 0;
        while (pos < end) {
            char c = buf[pos];
            if (c >= '0' && c <= '9') {
                int d = c - '0';
                if (digits < 18) {
                    mantissa = mantissa * 10L + d;
                    if (seenDot) {
                        fracScale++;
                    }
                    digits++;
                } else {
                    return parseDoubleFallback(buf, start, end - start);
                }
            } else if (c == '.' && !seenDot) {
                seenDot = true;
            } else {
                return parseDoubleFallback(buf, start, end - start);
            }
            pos++;
        }

        if (digits == 0) {
            return 0.0;
        }
        double value = fracScale > 0 ? (mantissa / POW10[fracScale]) : (double) mantissa;
        return negative ? -value : value;
    }

    private static double parseDoubleFallback(char[] buf, int start, int len) {
        try {
            return Double.parseDouble(new String(buf, start, len));
        } catch (NumberFormatException ignored) {
            return 0.0;
        }
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
