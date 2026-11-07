package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.polymarket.auth.PolymarketCredentials;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import io.quarkus.logging.Log;
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
 * Resilient WebSocket client for Polymarket CLOB ({@code wss://ws-subscriptions-clob.polymarket.com/ws/market}
 * and {@code wss://ws-subscriptions-clob.polymarket.com/ws/user}).
 * Manages per-market WebSocket connections and the authenticated User Channel stream,
 * cleanly disconnecting when a market resolves or on shutdown.
 */
@ApplicationScoped
public class PolymarketClobClient {

    /**
     * Connection state map key for the authenticated Polymarket CLOB User Channel WebSocket stream.
     */
    public static final String USER_STREAM_KEY = "USER";

    private final Vertx vertx;
    private final String wsUrl;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private final Map<String, ReconnectingWebSocketStream> marketStreams = new ConcurrentHashMap<>();
    private volatile ReconnectingWebSocketStream userStream;

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
                pingIntervalMs,
                "PING"
        );
        marketStreams.put(marketSlug, stream);
        Log.infof("Opening Polymarket CLOB WebSocket stream for market %s (tokens=%d)", marketSlug, tokenIds.size());
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
            Log.infof("Closing Polymarket CLOB WebSocket stream for market %s", marketSlug);
            stream.stop();
        }
    }

    /**
     * Opens an authenticated CLOB User Channel WebSocket stream ({@code /ws/user}) receiving real-time
     * account order and trade fill notifications.
     *
     * @param credentials Polymarket Level 2 API credentials
     * @param listener    listener receiving raw JSON frames and receipt timestamps
     */
    public synchronized void openUserStream(PolymarketCredentials credentials, RawWebSocketListener listener) {
        if (credentials == null || !credentials.hasL2Credentials()) {
            throw new IllegalArgumentException("Valid Polymarket L2 credentials are required for the CLOB user stream");
        }
        closeUserStream();
        String userWsUrl = resolveUserWsUrl(this.wsUrl);
        String subPayload = buildUserSubscriptionPayload(credentials);
        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                vertx,
                "POLYMARKET-CLOB-USER",
                userWsUrl,
                () -> List.of(subPayload),
                (rawJson, recvTs) -> {
                    if (listener != null && rawJson != null && !rawJson.isBlank()) {
                        listener.onMessage(rawJson, recvTs);
                    }
                },
                reconnectDelayMs,
                pingIntervalMs,
                "PING"
        );
        this.userStream = stream;
        Log.infof("Opening authenticated Polymarket CLOB User WebSocket stream (%s)", userWsUrl);
        stream.start();
    }

    /**
     * Disconnects and removes the authenticated CLOB User Channel WebSocket stream.
     */
    public synchronized void closeUserStream() {
        ReconnectingWebSocketStream current = this.userStream;
        this.userStream = null;
        if (current != null) {
            Log.info("Closing Polymarket CLOB User WebSocket stream");
            current.stop();
        }
    }

    /**
     * Disconnects all active market and user CLOB WebSocket streams.
     */
    public synchronized void stopAll() {
        for (String slug : List.copyOf(marketStreams.keySet())) {
            closeMarketStream(slug);
        }
        closeUserStream();
    }

    /**
     * Returns an unmodifiable snapshot of the connection states of all active market and user CLOB streams.
     *
     * @return map of stream key to {@link ConnectionState}
     */
    public Map<String, ConnectionState> getConnectionStates() {
        Map<String, ConnectionState> states = new LinkedHashMap<>();
        marketStreams.forEach((slug, stream) -> states.put(slug, stream.getState()));
        ReconnectingWebSocketStream currentUser = this.userStream;
        if (currentUser != null) {
            states.put(USER_STREAM_KEY, currentUser.getState());
        }
        return Collections.unmodifiableMap(states);
    }

    private String buildSubscriptionPayload(List<String> tokenIds) {
        String quotedTokens = tokenIds.stream()
                .filter(id -> id != null && !id.isBlank())
                .map(id -> "\"" + id.trim() + "\"")
                .collect(Collectors.joining(","));
        return "{\"assets_ids\":[" + quotedTokens + "],\"type\":\"market\",\"custom_feature_enabled\":true}";
    }

    private static String buildUserSubscriptionPayload(PolymarketCredentials credentials) {
        return "{\"auth\":{\"apiKey\":\"" + credentials.apiKey().trim()
                + "\",\"secret\":\"" + credentials.secret().trim()
                + "\",\"passphrase\":\"" + credentials.passphrase().trim()
                + "\"},\"markets\":[],\"type\":\"user\"}";
    }

    private static String resolveUserWsUrl(String marketWsUrl) {
        String trimmed = marketWsUrl != null ? marketWsUrl.trim() : "";
        if (trimmed.endsWith("/market")) {
            return trimmed.substring(0, trimmed.length() - "/market".length()) + "/user";
        }
        if (trimmed.endsWith("/clob")) {
            return trimmed.substring(0, trimmed.length() - "/clob".length()) + "/user";
        }
        return trimmed.endsWith("/") ? (trimmed + "user") : (trimmed + "/user");
    }
}
