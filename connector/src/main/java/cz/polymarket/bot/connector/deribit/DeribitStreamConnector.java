package cz.polymarket.bot.connector.deribit;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Resilient WebSocket client for Deribit v2 API ({@code wss://www.deribit.com/ws/api/v2}).
 * Supports authenticated raw trade channels (e.g. {@code trades.BTC-PERPETUAL.raw}) via
 * JSON-RPC {@code public/auth} client credentials authentication, as well as public interval channels.
 * Automatically handles keep-alive heartbeats, reconnection, and forwards raw trade notification payloads
 * to the listener with local receipt timestamps.
 */
@ApplicationScoped
public class DeribitStreamConnector {

    private static final Logger LOG = Logger.getLogger(DeribitStreamConnector.class);
    private static final int AUTH_REQUEST_ID = 1;
    private static final int SUBSCRIBE_REQUEST_ID = 2;
    private static final int HEARTBEAT_REPLY_ID = 999;

    private final Vertx vertx;
    private final String wsUrl;
    private final String interval;
    private final String clientId;
    private final String clientSecret;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private volatile ReconnectingWebSocketStream stream;
    private volatile String activeInstrument;

    /**
     * Constructs the Deribit WebSocket connector with full configuration and optional credentials.
     *
     * @param vertx            Vert.x instance
     * @param wsUrl            Deribit WebSocket v2 endpoint URL
     * @param interval         trade aggregation interval (e.g. raw or 100ms)
     * @param clientId         optional Deribit API Client ID
     * @param clientSecret     optional Deribit API Client Secret
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    @Inject
    public DeribitStreamConnector(
            Vertx vertx,
            @ConfigProperty(name = "polymarket.connector.deribit.ws-url", defaultValue = "wss://www.deribit.com/ws/api/v2")
            String wsUrl,
            @ConfigProperty(name = "polymarket.connector.deribit.interval", defaultValue = "raw")
            String interval,
            @ConfigProperty(name = "polymarket.connector.deribit.client-id", defaultValue = "")
            Optional<String> clientId,
            @ConfigProperty(name = "polymarket.connector.deribit.client-secret", defaultValue = "")
            Optional<String> clientSecret,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "15000")
            long pingIntervalMs) {
        this.vertx = vertx;
        this.wsUrl = wsUrl;
        this.interval = (interval == null || interval.isBlank()) ? "raw" : interval.trim();
        this.clientId = clientId.filter(s -> !s.isBlank()).orElse(null);
        this.clientSecret = clientSecret.filter(s -> !s.isBlank()).orElse(null);
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    /**
     * Constructs the Deribit WebSocket connector with explicit credentials for testing or programmatic setup.
     *
     * @param vertx            Vert.x instance
     * @param wsUrl            Deribit WebSocket v2 endpoint URL
     * @param interval         trade aggregation interval (e.g. raw or 100ms)
     * @param clientId         Deribit API Client ID (nullable)
     * @param clientSecret     Deribit API Client Secret (nullable)
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    public DeribitStreamConnector(
            Vertx vertx,
            String wsUrl,
            String interval,
            String clientId,
            String clientSecret,
            long reconnectDelayMs,
            long pingIntervalMs) {
        this(vertx, wsUrl, interval, Optional.ofNullable(clientId), Optional.ofNullable(clientSecret), reconnectDelayMs, pingIntervalMs);
    }

    /**
     * Constructs the Deribit WebSocket connector with custom interval and without credentials.
     *
     * @param vertx            Vert.x instance
     * @param wsUrl            Deribit WebSocket v2 endpoint URL
     * @param interval         trade aggregation interval (e.g. raw or 100ms)
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    public DeribitStreamConnector(
            Vertx vertx,
            String wsUrl,
            String interval,
            long reconnectDelayMs,
            long pingIntervalMs) {
        this(vertx, wsUrl, interval, Optional.empty(), Optional.empty(), reconnectDelayMs, pingIntervalMs);
    }

    /**
     * Constructs the Deribit WebSocket connector without credentials and default raw interval.
     *
     * @param vertx            Vert.x instance
     * @param wsUrl            Deribit WebSocket v2 endpoint URL
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    public DeribitStreamConnector(
            Vertx vertx,
            String wsUrl,
            long reconnectDelayMs,
            long pingIntervalMs) {
        this(vertx, wsUrl, "raw", Optional.empty(), Optional.empty(), reconnectDelayMs, pingIntervalMs);
    }

    /**
     * Checks whether Deribit API authentication credentials are provided.
     *
     * @return true if both client ID and client secret are configured
     */
    public boolean isAuthenticated() {
        return clientId != null && clientSecret != null;
    }

    /**
     * Starts the Deribit WebSocket stream for the given symbol/instrument and routes raw trade payloads to the listener.
     *
     * @param symbol   trading symbol or instrument name (e.g. BTCUSDT, BTC, or BTC-PERPETUAL)
     * @param listener raw WebSocket listener receiving unmodified JSON payloads and local receipt timestamps
     */
    public synchronized void start(String symbol, RawWebSocketListener listener) {
        stop();
        String instrument = normalizeInstrument(symbol);
        this.activeInstrument = instrument;
        String streamName = "DERIBIT-FUTURES-TRADES-" + instrument;

        Supplier<List<String>> initMessagesSupplier = () -> {
            if (isAuthenticated()) {
                LOG.infof("[%s] Authenticating Deribit session with clientId=%s...", streamName, maskKey(clientId));
                return List.of(buildAuthMessage());
            } else {
                if ("raw".equalsIgnoreCase(interval)) {
                    LOG.warnf("[%s] Deribit stream interval is set to 'raw' without API credentials. Deribit requires authentication for raw trade channels.", streamName);
                }
                LOG.infof("[%s] Subscribing without authentication to channel trades.%s.%s", streamName, instrument, interval);
                return List.of(buildSubscribeMessage(instrument));
            }
        };

        this.stream = new ReconnectingWebSocketStream(
                vertx,
                streamName,
                wsUrl,
                initMessagesSupplier,
                (rawJson, recvTs) -> {
                    if (rawJson == null || rawJson.isBlank()) {
                        return;
                    }
                    // Handle Deribit heartbeat test request by replying immediately
                    if (isTestRequest(rawJson)) {
                        ReconnectingWebSocketStream current = this.stream;
                        if (current != null) {
                            current.sendText("{\"jsonrpc\":\"2.0\",\"id\":" + HEARTBEAT_REPLY_ID + ",\"method\":\"public/test\"}");
                        }
                        return;
                    }
                    // Handle authentication response
                    if (isAuthResponse(rawJson)) {
                        if (isErrorResponse(rawJson)) {
                            LOG.errorf("[%s] Deribit authentication failed: %s", streamName, rawJson);
                        } else {
                            LOG.infof("[%s] Deribit session authenticated successfully. Subscribing to channel trades.%s.%s...", streamName, instrument, interval);
                            ReconnectingWebSocketStream current = this.stream;
                            if (current != null) {
                                current.sendText(buildSubscribeMessage(instrument));
                            }
                        }
                        return;
                    }
                    // Handle subscription response
                    if (isSubscriptionResponse(rawJson)) {
                        if (isErrorResponse(rawJson)) {
                            LOG.errorf("[%s] Deribit subscription failed: %s", streamName, rawJson);
                        } else {
                            LOG.infof("[%s] Subscribed successfully to channel: trades.%s.%s", streamName, instrument, interval);
                        }
                        return;
                    }
                    // Handle general error response
                    if (isErrorResponse(rawJson)) {
                        LOG.errorf("[%s] Deribit returned error: %s", streamName, rawJson);
                        return;
                    }
                    // Forward real trade notification events ({"jsonrpc":"2.0","method":"subscription",...})
                    if (isTradeNotification(rawJson)) {
                        if (listener != null) {
                            listener.onMessage(rawJson, recvTs);
                        }
                    }
                },
                reconnectDelayMs,
                pingIntervalMs
        );
        this.stream.start();
    }

    /**
     * Stops and closes the Deribit WebSocket stream.
     */
    public synchronized void stop() {
        ReconnectingWebSocketStream current = this.stream;
        this.stream = null;
        if (current != null) {
            current.stop();
        }
    }

    /**
     * Returns the current connection state of the Deribit stream.
     *
     * @return current {@link ConnectionState}
     */
    public ConnectionState getState() {
        ReconnectingWebSocketStream current = this.stream;
        return current != null ? current.getState() : ConnectionState.DISCONNECTED;
    }

    /**
     * Checks whether the Deribit stream is currently connected.
     *
     * @return true if connected
     */
    public boolean isConnected() {
        return getState() == ConnectionState.CONNECTED;
    }

    /**
     * Returns the currently active instrument name (e.g. BTC-PERPETUAL).
     *
     * @return instrument name, or null if not started
     */
    public String getActiveInstrument() {
        return activeInstrument;
    }

    /**
     * Normalizes a symbol (such as BTC, BTCUSDT, or BTC-PERPETUAL) into a standard Deribit perpetual instrument name.
     *
     * @param symbol input symbol
     * @return normalized Deribit instrument name (e.g. BTC-PERPETUAL)
     */
    public static String normalizeInstrument(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "BTC-PERPETUAL";
        }
        String clean = symbol.trim().toUpperCase();
        if (clean.contains("-")) {
            return clean;
        }
        if (clean.endsWith("USDT")) {
            clean = clean.substring(0, clean.length() - 4);
        } else if (clean.endsWith("USDC")) {
            clean = clean.substring(0, clean.length() - 4);
        } else if (clean.endsWith("USD")) {
            clean = clean.substring(0, clean.length() - 3);
        }
        return clean + "-PERPETUAL";
    }

    private String buildAuthMessage() {
        return "{\"jsonrpc\":\"2.0\",\"id\":" + AUTH_REQUEST_ID + ",\"method\":\"public/auth\",\"params\":{\"grant_type\":\"client_credentials\",\"client_id\":\""
                + escapeJson(clientId) + "\",\"client_secret\":\"" + escapeJson(clientSecret) + "\"}}";
    }

    private String buildSubscribeMessage(String instrument) {
        return "{\"jsonrpc\":\"2.0\",\"id\":" + SUBSCRIBE_REQUEST_ID + ",\"method\":\"public/subscribe\",\"params\":{\"channels\":[\"trades."
                + instrument + "." + interval + "\"]}}";
    }

    private static boolean isTestRequest(String rawJson) {
        return rawJson.contains("\"test_request\"");
    }

    private static boolean isAuthResponse(String rawJson) {
        return (rawJson.contains("\"id\":" + AUTH_REQUEST_ID) || rawJson.contains("\"id\": " + AUTH_REQUEST_ID))
                && (rawJson.contains("\"result\"") || rawJson.contains("\"error\""));
    }

    private static boolean isSubscriptionResponse(String rawJson) {
        return (rawJson.contains("\"id\":" + SUBSCRIBE_REQUEST_ID) || rawJson.contains("\"id\": " + SUBSCRIBE_REQUEST_ID))
                && (rawJson.contains("\"result\"") || rawJson.contains("\"error\""));
    }

    private static boolean isTradeNotification(String rawJson) {
        return rawJson.contains("\"method\":\"subscription\"") || rawJson.contains("\"method\": \"subscription\"");
    }

    private static boolean isErrorResponse(String rawJson) {
        return rawJson.contains("\"error\"");
    }

    private static String escapeJson(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String maskKey(String key) {
        if (key == null || key.length() <= 4) {
            return "****";
        }
        return key.substring(0, 2) + "****" + key.substring(key.length() - 2);
    }
}
