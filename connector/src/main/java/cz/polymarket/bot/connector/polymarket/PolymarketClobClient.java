package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Resilient WebSocket client for Polymarket CLOB ({@code wss://ws-subscriptions-clob.polymarket.com/ws/market}).
 * Manages per-market WebSocket connections so each market's raw order book and trade frames are forwarded
 * directly to disk without JSON parsing overhead, and cleanly disconnects when a market resolves.
 */
@ApplicationScoped
public class PolymarketClobClient {

    private final Vertx vertx;
    private final String wsUrl;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private final Map<String, ReconnectingWebSocketStream> marketStreams = new ConcurrentHashMap<>();

    /**
     * Constructs the Polymarket CLOB WebSocket client.
     *
     * @param vertx            Vert.x runtime instance
     * @param wsUrl            Polymarket CLOB WebSocket URL
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    @Inject
    public PolymarketClobClient(
            Vertx vertx,
            @ConfigProperty(name = "polymarket.connector.clob.ws-url", defaultValue = "wss://ws-subscriptions-clob.polymarket.com/ws/market")
            String wsUrl,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "10000")
            long pingIntervalMs) {
        this.vertx = vertx;
        this.wsUrl = wsUrl;
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    /**
     * Opens a dedicated CLOB WebSocket connection for a single market subscribed to its token IDs,
     * forwarding all raw frames directly to the listener without parsing.
     *
     * @param marketSlug Polymarket market slug
     * @param tokenIds   CLOB asset token IDs belonging to the market
     * @param listener   listener receiving raw JSON frames and receipt timestamps
     */
    public synchronized void openMarketStream(String marketSlug, List<String> tokenIds, RawWebSocketListener listener) {
        if (marketSlug == null || marketSlug.isBlank() || tokenIds == null || tokenIds.isEmpty()) {
            return;
        }
        if (marketStreams.containsKey(marketSlug)) {
            return;
        }
        String subPayload = buildSubscriptionPayload(tokenIds);
        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "POLYMARKET-CLOB-" + marketSlug,
                wsUrl,
                () -> List.of(subPayload),
                (rawJson, recvTs) -> {
                    if (listener != null && rawJson != null && !rawJson.isBlank()) {
                        listener.onMessage(rawJson, recvTs);
                    }
                },
                reconnectDelayMs,
                pingIntervalMs
        );
        marketStreams.put(marketSlug, stream);
        stream.start();
    }

    /**
     * Disconnects and removes the CLOB WebSocket stream for a market that has finished and resolved.
     *
     * @param marketSlug Polymarket market slug to disconnect
     */
    public synchronized void closeMarketStream(String marketSlug) {
        if (marketSlug == null) {
            return;
        }
        ReconnectingWebSocketStream stream = marketStreams.remove(marketSlug);
        if (stream != null) {
            stream.stop();
        }
    }

    /**
     * Disconnects all active market CLOB WebSocket streams.
     */
    public synchronized void stopAll() {
        for (String slug : List.copyOf(marketStreams.keySet())) {
            closeMarketStream(slug);
        }
    }

    /**
     * Returns an unmodifiable snapshot of the connection states of all active market CLOB streams.
     *
     * @return map of market slug to {@link ConnectionState}
     */
    public Map<String, ConnectionState> getConnectionStates() {
        Map<String, ConnectionState> states = new LinkedHashMap<>();
        marketStreams.forEach((slug, stream) -> states.put(slug, stream.getState()));
        return Collections.unmodifiableMap(states);
    }

    private String buildSubscriptionPayload(List<String> tokenIds) {
        String quotedTokens = tokenIds.stream()
                .filter(id -> id != null && !id.isBlank())
                .map(id -> "\"" + id.trim() + "\"")
                .collect(Collectors.joining(","));
        return "{\"type\":\"market\",\"assets_ids\":[" + quotedTokens + "]}";
    }
}
