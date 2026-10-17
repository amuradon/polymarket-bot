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

/**
 * Resilient WebSocket client for Deribit v2 API ({@code wss://www.deribit.com/ws/api/v2}).
 * Connects to public trade channels (e.g. {@code trades.BTC-PERPETUAL.100ms}), sends the JSON-RPC
 * subscription request, handles keep-alive heartbeats, and forwards raw trade notification payloads
 * to the listener with local receipt timestamps.
 */
@ApplicationScoped
public class DeribitStreamConnector {

    private static final Logger LOG = Logger.getLogger(DeribitStreamConnector.class);

    private final Vertx vertx;
    private final String wsUrl;
    private final String interval;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private volatile ReconnectingWebSocketStream stream;
    private volatile String activeInstrument;

    /**
     * Constructs the Deribit WebSocket connector with full configuration.
     *
     * @param vertx            Vert.x instance
     * @param wsUrl            Deribit WebSocket v2 endpoint URL
     * @param interval         trade aggregation interval (e.g. 100ms or raw)
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    @Inject
    public DeribitStreamConnector(
            Vertx vertx,
            @ConfigProperty(name = "polymarket.connector.deribit.ws-url", defaultValue = "wss://www.deribit.com/ws/api/v2")
            String wsUrl,
            @ConfigProperty(name = "polymarket.connector.deribit.interval", defaultValue = "100ms")
            String interval,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "15000")
            long pingIntervalMs) {
        this.vertx = vertx;
        this.wsUrl = wsUrl;
        this.interval = (interval == null || interval.isBlank()) ? "100ms" : interval.trim();
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    /**
     * Constructs the Deribit WebSocket connector with default 100ms interval.
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
        this(vertx, wsUrl, "100ms", reconnectDelayMs, pingIntervalMs);
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
        String subscribeMsg = buildSubscribeMessage(instrument);

        this.stream = new ReconnectingWebSocketStream(
                vertx,
                streamName,
                wsUrl,
                () -> List.of(subscribeMsg),
                (rawJson, recvTs) -> {
                    if (rawJson == null || rawJson.isBlank()) {
                        return;
                    }
                    // Handle Deribit heartbeat test request by replying immediately
                    if (isTestRequest(rawJson)) {
                        ReconnectingWebSocketStream current = this.stream;
                        if (current != null) {
                            current.sendText("{\"jsonrpc\":\"2.0\",\"id\":999,\"method\":\"public/test\"}");
                        }
                        return;
                    }
                    // Handle Deribit error response
                    if (isErrorResponse(rawJson)) {
                        LOG.errorf("[%s] Deribit returned error: %s", streamName, rawJson);
                        return;
                    }
                    // Filter out subscription confirmation response (e.g. {"jsonrpc":"2.0","id":1,"result":[...]})
                    if (isSubscriptionConfirmation(rawJson)) {
                        LOG.infof("[%s] Subscribed successfully to channel: trades.%s.%s", streamName, instrument, interval);
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

    private String buildSubscribeMessage(String instrument) {
        return "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"public/subscribe\",\"params\":{\"channels\":[\"trades."
                + instrument + "." + interval + "\"]}}";
    }

    private static boolean isTestRequest(String rawJson) {
        return rawJson.contains("\"test_request\"");
    }

    private static boolean isSubscriptionConfirmation(String rawJson) {
        return rawJson.contains("\"result\"") && rawJson.contains("\"id\":1");
    }

    private static boolean isTradeNotification(String rawJson) {
        return rawJson.contains("\"method\":\"subscription\"") || rawJson.contains("\"method\": \"subscription\"");
    }

    private static boolean isErrorResponse(String rawJson) {
        return rawJson.contains("\"error\"");
    }
}
