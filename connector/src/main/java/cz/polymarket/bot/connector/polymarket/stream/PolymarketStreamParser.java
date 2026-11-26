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

/**
 * Low-latency JSON frame parser for Polymarket PolyBolt v2 ({@code price.crypto} and {@code price.crypto.twap})
 * and Polymarket CLOB Market WebSocket streams ({@code book}, {@code price_change}, {@code best_bid_ask}).
 * <p>
 * Uses streaming Jackson {@link JsonParser} directly over UTF-8 {@code byte[]} buffers and reusable primitive
 * thread-local scratch arrays on hot paths to achieve zero per-level heap allocations, 1-bps ({@code 0..10_000})
 * tick resolution, per-token monotonic server timestamp filtering, and non-fabricating BBO hint emission.
 */
@ApplicationScoped
public class PolymarketStreamParser {

    /** Maximum 1-bps tick value ({@code 10_000 = $1.0000}). */
    public static final int MAX_TICK_1BPS = 10_000;

    private static final double TICK_SCALE_1BPS = 10_000.0;

    private static final byte[] BYTES_BIDS = "\"bids\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_ASKS = "\"asks\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_PRICE_CHANGES = "\"price_changes\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_BEST_BID_ASK = "best_bid_ask".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_PRICE_CHANGE = "price_change".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_BOOK = "\"book\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_LAST_TRADE_PRICE = "last_trade_price".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BYTES_TICK_SIZE_CHANGE = "tick_size_change".getBytes(StandardCharsets.UTF_8);

    private static final double[] POW10 = {
            1.0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
            1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
    };

    private static final ThreadLocal<ScratchClobEvent> SCRATCH_EVENT =
            ThreadLocal.withInitial(ScratchClobEvent::new);

    private final ObjectMapper objectMapper;
    private final String defaultSymbol;

    /**
     * Primitive batch execution summary avoiding {@code Optional} boxing on the real-time hot path.
     *
     * @param updated                  {@code true} if at least one frame mutated the orderbook
     * @param lastUpdatedRecvTs        local receipt timestamp of the last applied frame in epoch milliseconds
     * @param appliedFramesCount       number of frames in the batch that mutated the orderbook
     * @param fastForwardedFramesCount number of older frames skipped without JSON parsing due to a newer dual snapshot
     */
    public record ClobBatchResult(
            boolean updated,
            long lastUpdatedRecvTs,
            int appliedFramesCount,
            int fastForwardedFramesCount
    ) {
        /** Shared constant representing a no-op batch. */
        public static final ClobBatchResult EMPTY = new ClobBatchResult(false, 0L, 0, 0);
    }

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
                    case "ts", "timestamp" -> rootTs = parseCurrentLongOrText(parser, recvTimestampMs);
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
                                    case "timestamp" -> payloadTs = parseCurrentLongOrText(parser, -1L);
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
                        case "timestamp" -> itemTs = parseCurrentLongOrText(parser, defaultTsMs);
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
     * Applies a single raw CLOB WebSocket JSON text frame to {@code sink}.
     *
     * @param rawJson         raw CLOB JSON text frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param sink            target primitive book sink
     * @return {@code true} if the frame mutated the orderbook
     */
    public boolean applyClobFrame(String rawJson, long recvTimestampMs, ClobBookSink sink) {
        if (rawJson == null || rawJson.isEmpty() || sink == null) {
            return false;
        }
        return applyClobFrame(rawJson.getBytes(StandardCharsets.UTF_8), recvTimestampMs, sink);
    }

    /**
     * Applies a single raw UTF-8 CLOB WebSocket frame directly from {@code rawUtf8Bytes} to {@code sink}
     * without allocating intermediate {@link String} or DOM instances.
     *
     * @param rawUtf8Bytes    raw UTF-8 bytes of the CLOB WebSocket frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param sink            target primitive book sink
     * @return {@code true} if the frame mutated the orderbook
     */
    public boolean applyClobFrame(byte[] rawUtf8Bytes, long recvTimestampMs, ClobBookSink sink) {
        if (rawUtf8Bytes == null || rawUtf8Bytes.length == 0 || sink == null) {
            return false;
        }
        if (rawUtf8Bytes.length <= 16 && isPingOrPongOrBlank(rawUtf8Bytes)) {
            return false;
        }
        if (!containsRelevantClobEvent(rawUtf8Bytes)) {
            return false;
        }

        ScratchClobEvent scratch = SCRATCH_EVENT.get();
        try (JsonParser parser = objectMapper.createParser(rawUtf8Bytes)) {
            JsonToken firstToken = parser.nextToken();
            if (firstToken == null) {
                return false;
            }
            boolean updated = false;
            if (firstToken == JsonToken.START_ARRAY) {
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() == JsonToken.START_OBJECT) {
                        updated |= parseAndDispatchSingleClobObject(parser, recvTimestampMs, sink, scratch);
                    } else {
                        parser.skipChildren();
                    }
                }
            } else if (firstToken == JsonToken.START_OBJECT) {
                updated = parseAndDispatchSingleClobObject(parser, recvTimestampMs, sink, scratch);
            }
            return updated;
        } catch (Exception e) {
            Log.warnf("Failed to parse CLOB WS frame: %s", e.getMessage());
            return false;
        }
    }

    /**
     * Applies a chronological batch of raw Polymarket CLOB WebSocket frames to {@code sink} in a single pass.
     * <p>
     * If a newer frame in the batch is a full dual-asset {@code book} snapshot, all earlier frames in the batch
     * are fast-forwarded (skipped without JSON parsing).
     *
     * @param frames chronological batch of raw CLOB frames
     * @param sink   target primitive book sink
     * @return {@link ClobBatchResult} summarizing applied and fast-forwarded frames
     */
    public ClobBatchResult applyClobFrameBatch(List<TimestampedFrame> frames, ClobBookSink sink) {
        if (frames == null || frames.isEmpty() || sink == null) {
            return ClobBatchResult.EMPTY;
        }
        int size = frames.size();
        int startIndex = 0;
        byte[] upTokenBytes = sink.getUpTokenBytes();
        byte[] downTokenBytes = sink.getDownTokenBytes();
        if (size > 1 && upTokenBytes != null && upTokenBytes.length > 0
                && downTokenBytes != null && downTokenBytes.length > 0) {
            for (int i = size - 1; i > 0; i--) {
                byte[] rawBytes = frames.get(i).rawUtf8Bytes();
                if (isFullDualBookSnapshot(rawBytes, upTokenBytes, downTokenBytes)) {
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
            if (applyClobFrame(frame.rawUtf8Bytes(), frame.recvTs(), sink)) {
                appliedCount++;
                lastUpdatedRecvTs = frame.recvTs();
            }
        }

        if (appliedCount == 0) {
            return new ClobBatchResult(false, 0L, 0, fastForwarded);
        }
        return new ClobBatchResult(true, lastUpdatedRecvTs, appliedCount, fastForwarded);
    }

    /**
     * Converts a decimal contract price in {@code [0.0, 1.0]} into a 1-bps integer tick in {@code [0, 10_000]},
     * or {@code -1} if non-positive or invalid.
     *
     * @param price parsed decimal price
     * @return 1-bps tick in {@code [0, 10_000]} or {@code -1}
     */
    public static int priceToValidTick(double price) {
        if (!Double.isFinite(price) || price <= 0.0 || price > 1.0) {
            return -1;
        }
        return Math.clamp((int) Math.round(price * TICK_SCALE_1BPS), 1, MAX_TICK_1BPS);
    }

    private static boolean parseAndDispatchSingleClobObject(
            JsonParser parser,
            long recvTimestampMs,
            ClobBookSink sink,
            ScratchClobEvent scratch) throws IOException {
        scratch.reset();
        String eventType = "";
        String assetId = "";
        long serverTimestampMs = 0L;
        int bestBidTick = -1;
        int bestAskTick = -1;
        boolean hasBookArrays = false;
        boolean hasPriceChanges = false;

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            JsonToken valueToken = parser.nextToken();
            if (fieldName == null || valueToken == null) {
                break;
            }
            switch (fieldName) {
                case "event_type" -> eventType = parser.getValueAsString("");
                case "asset_id" -> assetId = parser.getValueAsString("");
                case "timestamp" -> serverTimestampMs = parseCurrentLongOrText(parser, 0L);
                case "bids" -> {
                    if (valueToken == JsonToken.START_ARRAY) {
                        hasBookArrays = true;
                        readBookSideIntoScratch(parser, scratch, true);
                    } else {
                        parser.skipChildren();
                    }
                }
                case "asks" -> {
                    if (valueToken == JsonToken.START_ARRAY) {
                        hasBookArrays = true;
                        readBookSideIntoScratch(parser, scratch, false);
                    } else {
                        parser.skipChildren();
                    }
                }
                case "price_changes" -> {
                    if (valueToken == JsonToken.START_ARRAY) {
                        hasPriceChanges = true;
                        readPriceChangesIntoScratch(parser, assetId, sink, scratch);
                    } else {
                        parser.skipChildren();
                    }
                }
                case "best_bid" -> bestBidTick = priceToValidTick(parseCurrentNumberOrText(parser));
                case "best_ask" -> bestAskTick = priceToValidTick(parseCurrentNumberOrText(parser));
                default -> parser.skipChildren();
            }
        }

        if (serverTimestampMs > 0L) {
            sink.updateObservedLatency(recvTimestampMs, serverTimestampMs);
        }

        if ("book".equals(eventType) || (eventType.isEmpty() && hasBookArrays)) {
            int side = sink.resolveTokenSide(assetId);
            if (side < 0) {
                return false;
            }
            boolean isUp = (side == 1);
            if (!sink.tryAcceptServerTimestamp(isUp, serverTimestampMs)) {
                return false;
            }
            sink.onBookSnapshotStart(isUp);
            for (int i = 0; i < scratch.bookBidsCount; i++) {
                sink.onBookSnapshotLevel(isUp, true, scratch.bookBidTicks[i], scratch.bookBidSizes[i]);
            }
            for (int i = 0; i < scratch.bookAsksCount; i++) {
                sink.onBookSnapshotLevel(isUp, false, scratch.bookAskTicks[i], scratch.bookAskSizes[i]);
            }
            sink.onBookSnapshotEnd(isUp, recvTimestampMs);
            return true;
        }

        if ("price_change".equals(eventType) && hasPriceChanges) {
            int fallbackSide = assetId.isEmpty() ? -1 : sink.resolveTokenSide(assetId);
            boolean touchesUp = false;
            boolean touchesDown = false;
            for (int i = 0; i < scratch.pcCount; i++) {
                int s = scratch.pcSides[i] >= 0 ? scratch.pcSides[i] : fallbackSide;
                scratch.pcSides[i] = s;
                if (s == 1) {
                    touchesUp = true;
                } else if (s == 0) {
                    touchesDown = true;
                }
            }
            boolean acceptUp = touchesUp && sink.tryAcceptServerTimestamp(true, serverTimestampMs);
            boolean acceptDown = touchesDown && sink.tryAcceptServerTimestamp(false, serverTimestampMs);
            if (!acceptUp && !acceptDown) {
                return false;
            }

            boolean mutated = false;
            int finalUpBidHint = -1;
            int finalUpAskHint = -1;
            int finalDownBidHint = -1;
            int finalDownAskHint = -1;

            for (int i = 0; i < scratch.pcCount; i++) {
                int s = scratch.pcSides[i];
                if (s == 1 && !acceptUp) {
                    continue;
                }
                if (s == 0 && !acceptDown) {
                    continue;
                }
                if (s < 0) {
                    continue;
                }
                boolean isUp = (s == 1);
                int tick = scratch.pcTicks[i];
                if (tick >= 0) {
                    sink.onPriceChangeLevel(isUp, scratch.pcIsBid[i], tick, scratch.pcSizes[i], recvTimestampMs);
                    mutated = true;
                }
                int bBid = scratch.pcBestBidTicks[i];
                int bAsk = scratch.pcBestAskTicks[i];
                if (bBid >= 0 || bAsk >= 0) {
                    if (isUp) {
                        if (bBid >= 0) finalUpBidHint = bBid;
                        if (bAsk >= 0) finalUpAskHint = bAsk;
                    } else {
                        if (bBid >= 0) finalDownBidHint = bBid;
                        if (bAsk >= 0) finalDownAskHint = bAsk;
                    }
                    mutated = true;
                }
            }

            if (acceptUp && (finalUpBidHint >= 0 || finalUpAskHint >= 0)) {
                sink.onBboHint(true, finalUpBidHint, finalUpAskHint, recvTimestampMs);
            }
            if (acceptDown && (finalDownBidHint >= 0 || finalDownAskHint >= 0)) {
                sink.onBboHint(false, finalDownBidHint, finalDownAskHint, recvTimestampMs);
            }
            return mutated;
        }

        if ("best_bid_ask".equals(eventType) && (bestBidTick >= 0 || bestAskTick >= 0)) {
            int side = sink.resolveTokenSide(assetId);
            if (side < 0) {
                return false;
            }
            boolean isUp = (side == 1);
            if (!sink.tryAcceptServerTimestamp(isUp, serverTimestampMs)) {
                return false;
            }
            sink.onBboHint(isUp, bestBidTick, bestAskTick, recvTimestampMs);
            return true;
        }

        return false;
    }

    private static void readBookSideIntoScratch(
            JsonParser parser,
            ScratchClobEvent scratch,
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
                int tick = priceToValidTick(price);
                if (tick >= 0 && size > 0.0) {
                    scratch.addBookLevel(isBid, tick, size);
                }
            } else {
                parser.skipChildren();
            }
        }
    }

    private static void readPriceChangesIntoScratch(
            JsonParser parser,
            String fallbackAssetId,
            ClobBookSink sink,
            ScratchClobEvent scratch) throws IOException {
        int fallbackSide = fallbackAssetId.isEmpty() ? -1 : sink.resolveTokenSide(fallbackAssetId);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if (parser.currentToken() == JsonToken.START_OBJECT) {
                int itemSide = fallbackSide;
                double price = 0.0;
                double size = 0.0;
                boolean isBid = false;
                int bestBidTick = -1;
                int bestAskTick = -1;

                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String field = parser.currentName();
                    parser.nextToken();
                    switch (field) {
                        case "asset_id" -> itemSide = sink.resolveTokenSide(parser.getValueAsString(""));
                        case "price" -> price = parseCurrentNumberOrText(parser);
                        case "size" -> size = parseCurrentNumberOrText(parser);
                        case "side" -> {
                            String s = parser.getValueAsString("");
                            isBid = "BUY".equalsIgnoreCase(s) || "BID".equalsIgnoreCase(s);
                        }
                        case "best_bid" -> bestBidTick = priceToValidTick(parseCurrentNumberOrText(parser));
                        case "best_ask" -> bestAskTick = priceToValidTick(parseCurrentNumberOrText(parser));
                        default -> parser.skipChildren();
                    }
                }
                int tick = priceToValidTick(price);
                if (tick >= 0 || bestBidTick >= 0 || bestAskTick >= 0) {
                    scratch.addPriceChange(itemSide, isBid, tick, Math.max(0.0, size), bestBidTick, bestAskTick);
                }
            } else {
                parser.skipChildren();
            }
        }
    }

    private static final class ScratchClobEvent {
        int[] bookBidTicks = new int[256];
        double[] bookBidSizes = new double[256];
        int bookBidsCount = 0;

        int[] bookAskTicks = new int[256];
        double[] bookAskSizes = new double[256];
        int bookAsksCount = 0;

        int[] pcSides = new int[64];
        boolean[] pcIsBid = new boolean[64];
        int[] pcTicks = new int[64];
        double[] pcSizes = new double[64];
        int[] pcBestBidTicks = new int[64];
        int[] pcBestAskTicks = new int[64];
        int pcCount = 0;

        void reset() {
            bookBidsCount = 0;
            bookAsksCount = 0;
            pcCount = 0;
        }

        void addBookLevel(boolean isBid, int tick, double size) {
            if (isBid) {
                if (bookBidsCount == bookBidTicks.length) {
                    int next = bookBidTicks.length << 1;
                    bookBidTicks = Arrays.copyOf(bookBidTicks, next);
                    bookBidSizes = Arrays.copyOf(bookBidSizes, next);
                }
                bookBidTicks[bookBidsCount] = tick;
                bookBidSizes[bookBidsCount] = size;
                bookBidsCount++;
            } else {
                if (bookAsksCount == bookAskTicks.length) {
                    int next = bookAskTicks.length << 1;
                    bookAskTicks = Arrays.copyOf(bookAskTicks, next);
                    bookAskSizes = Arrays.copyOf(bookAskSizes, next);
                }
                bookAskTicks[bookAsksCount] = tick;
                bookAskSizes[bookAsksCount] = size;
                bookAsksCount++;
            }
        }

        void addPriceChange(int side, boolean isBid, int tick, double size, int bestBidTick, int bestAskTick) {
            if (pcCount == pcTicks.length) {
                int next = pcTicks.length << 1;
                pcSides = Arrays.copyOf(pcSides, next);
                pcIsBid = Arrays.copyOf(pcIsBid, next);
                pcTicks = Arrays.copyOf(pcTicks, next);
                pcSizes = Arrays.copyOf(pcSizes, next);
                pcBestBidTicks = Arrays.copyOf(pcBestBidTicks, next);
                pcBestAskTicks = Arrays.copyOf(pcBestAskTicks, next);
            }
            pcSides[pcCount] = side;
            pcIsBid[pcCount] = isBid;
            pcTicks[pcCount] = tick;
            pcSizes[pcCount] = size;
            pcBestBidTicks[pcCount] = bestBidTick;
            pcBestAskTicks[pcCount] = bestAskTick;
            pcCount++;
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

    private static boolean isFullDualBookSnapshot(byte[] rawBytes, byte[] upTokenBytes, byte[] downTokenBytes) {
        if (rawBytes == null || rawBytes.length < 64) {
            return false;
        }
        return indexOfBytes(rawBytes, BYTES_BIDS, 0) >= 0
                && indexOfBytes(rawBytes, BYTES_ASKS, 0) >= 0
                && indexOfBytes(rawBytes, upTokenBytes, 0) >= 0
                && indexOfBytes(rawBytes, downTokenBytes, 0) >= 0;
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

    /**
     * Parses the current Jackson token as a {@code long}, supporting both JSON integer tokens and
     * string-encoded integer timestamps without allocating a {@link String}.
     *
     * @param parser       active Jackson {@link JsonParser} positioned at a value token
     * @param defaultValue fallback value if unparseable
     * @return parsed {@code long} value
     * @throws IOException if token reading fails
     */
    public static long parseCurrentLongOrText(JsonParser parser, long defaultValue) throws IOException {
        JsonToken token = parser.currentToken();
        if (token == JsonToken.VALUE_NUMBER_INT) {
            return parser.getLongValue();
        }
        if (token == JsonToken.VALUE_STRING) {
            char[] buf = parser.getTextCharacters();
            int start = parser.getTextOffset();
            int end = start + parser.getTextLength();
            while (start < end && buf[start] <= ' ') {
                start++;
            }
            while (end > start && buf[end - 1] <= ' ') {
                end--;
            }
            if (start >= end) {
                return defaultValue;
            }
            long val = 0L;
            int digits = 0;
            for (int i = start; i < end; i++) {
                char c = buf[i];
                if (c >= '0' && c <= '9') {
                    val = val * 10L + (c - '0');
                    digits++;
                } else {
                    return defaultValue;
                }
            }
            return digits > 0 ? val : defaultValue;
        }
        return defaultValue;
    }

    /**
     * Parses the current Jackson token as a {@code double}, supporting both numeric JSON tokens and
     * string-encoded decimal numbers via zero-allocation {@code char[]} parsing.
     *
     * @param parser active Jackson {@link JsonParser} positioned at a value token
     * @return parsed {@code double} value, or {@code 0.0} if not a valid number
     * @throws IOException if token reading fails
     */
    public static double parseCurrentNumberOrText(JsonParser parser) throws IOException {
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
                case "size", "matched_amount" -> size = parseCurrentNumberOrText(parser);
                case "price" -> price = parseCurrentNumberOrText(parser);
                case "fee_rate_bps" -> feeRateBps = (int) Math.round(parseCurrentNumberOrText(parser));
                case "asset_id" -> assetId = parser.getValueAsString("");
                case "taker_order_id" -> {
                    String val = parser.getValueAsString("");
                    if (!val.isBlank()) {
                        orderId = val;
                    }
                }
                case "maker_order_id", "order_id" -> {
                    String val = parser.getValueAsString("");
                    if (orderId.isBlank() && !val.isBlank()) {
                        orderId = val;
                    }
                }
                default -> parser.skipChildren();
            }
        }

        if (!"trade".equalsIgnoreCase(eventType)) {
            return null;
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
        return rawTs < 10_000_000_000L ? rawTs * 1000L : rawTs;
    }
}
