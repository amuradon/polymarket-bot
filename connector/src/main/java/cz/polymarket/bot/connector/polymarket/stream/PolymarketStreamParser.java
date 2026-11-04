package cz.polymarket.bot.connector.polymarket.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

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

    private static final Logger LOG = Logger.getLogger(PolymarketStreamParser.class);

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
            LOG.debugf("Failed to parse PolyBolt frame: %s", e.getMessage());
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
        String trimmed = rawJson.trim();
        if ("PING".equalsIgnoreCase(trimmed) || "PONG".equalsIgnoreCase(trimmed)) {
            return Optional.empty();
        }

        try {
            JsonNode root = objectMapper.readTree(trimmed);
            boolean updated = false;
            if (root.isArray()) {
                for (JsonNode event : root) {
                    updated |= applySingleClobEvent(event, localBook);
                }
            } else if (root.isObject()) {
                updated = applySingleClobEvent(root, localBook);
            }
            return updated ? localBook.snapshotQuote(recvTimestampMs) : Optional.empty();
        } catch (Exception e) {
            LOG.debugf("Failed to parse CLOB WS frame: %s", e.getMessage());
            return Optional.empty();
        }
    }

    private boolean applySingleClobEvent(JsonNode event, PolymarketLocalBook localBook) {
        if (event == null || !event.isObject()) {
            return false;
        }
        String eventType = event.path("event_type").asText("");
        String assetId = event.path("asset_id").asText("");

        if ("book".equals(eventType) || (eventType.isEmpty() && (event.has("bids") || event.has("asks")))) {
            localBook.clearAssetBook(assetId);
            applyBookLevels(event.get("bids"), assetId, true, localBook);
            applyBookLevels(event.get("asks"), assetId, false, localBook);
            return true;
        }

        if ("price_change".equals(eventType)) {
            boolean changed = false;
            JsonNode changes = event.get("price_changes");
            if (changes != null && changes.isArray()) {
                for (JsonNode change : changes) {
                    String itemAsset = change.path("asset_id").asText(assetId);
                    double price = parseDoubleSafe(change.get("price"));
                    double size = parseDoubleSafe(change.get("size"));
                    String side = change.path("side").asText("");
                    boolean isBid = "BUY".equalsIgnoreCase(side) || "BID".equalsIgnoreCase(side);
                    if (price > 0.0) {
                        localBook.updateLevel(itemAsset, isBid, price, size);
                        changed = true;
                    }
                    double bestBid = parseDoubleSafe(change.get("best_bid"));
                    double bestAsk = parseDoubleSafe(change.get("best_ask"));
                    if (bestBid > 0.0 && bestAsk > 0.0) {
                        localBook.applyBestBidAsk(itemAsset, bestBid, bestAsk);
                        changed = true;
                    }
                }
            }
            return changed;
        }

        if ("best_bid_ask".equals(eventType)) {
            double bestBid = parseDoubleSafe(event.get("best_bid"));
            double bestAsk = parseDoubleSafe(event.get("best_ask"));
            if (bestBid > 0.0 && bestAsk > 0.0) {
                localBook.applyBestBidAsk(assetId, bestBid, bestAsk);
                return true;
            }
        }
        return false;
    }

    private static void applyBookLevels(
            JsonNode levels,
            String assetId,
            boolean isBid,
            PolymarketLocalBook localBook) {
        if (levels == null || !levels.isArray()) {
            return;
        }
        for (JsonNode level : levels) {
            double price = parseDoubleSafe(level.get("price"));
            double size = parseDoubleSafe(level.get("size"));
            if (price > 0.0 && size > 0.0) {
                localBook.updateLevel(assetId, isBid, price, size);
            }
        }
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
