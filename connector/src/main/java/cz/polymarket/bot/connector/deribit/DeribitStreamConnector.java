package cz.polymarket.bot.connector.deribit;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketDisconnectListener;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import io.netty.handler.ssl.OpenSsl;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Quarkus;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.dns.AddressResolverOptions;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Resilient WebSocket client for Deribit v2 API ({@code wss://www.deribit.com/ws/api/v2}).
 * Exclusively connects to authenticated raw trade channels ({@code trades.{instrument}.raw}) via
 * JSON-RPC {@code public/auth} client credentials on an isolated 1-thread Netty EventLoop
 * ({@code deribit-ws-io-reader}) with native Linux Epoll transport and BoringSSL.
 * If credentials are missing or authentication fails, the connector aborts and terminates the application with an error.
 */
@ApplicationScoped
public class DeribitStreamConnector {

    /**
     * Dedicated thread name for the isolated Netty EventLoop servicing the Deribit WebSocket stream.
     */
    public static final String DEDICATED_DERIBIT_IO_THREAD_NAME = "deribit-ws-io-reader";

    private static final int AUTH_REQUEST_ID = 1;
    private static final int SUBSCRIBE_REQUEST_ID = 2;
    private static final int HEARTBEAT_REPLY_ID = 999;
    private static final String HEARTBEAT_REPLY_JSON =
            "{\"jsonrpc\":\"2.0\",\"id\":" + HEARTBEAT_REPLY_ID + ",\"method\":\"public/test\"}";
    private static final byte[] SUBSCRIPTION_PATTERN_COMPACT =
            "\"method\":\"subscription\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SUBSCRIPTION_PATTERN_SPACED =
            "\"method\": \"subscription\"".getBytes(StandardCharsets.UTF_8);

    private final Vertx deribitVertx;
    private final String wsUrl;
    private final String clientId;
    private final String clientSecret;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private volatile ReconnectingWebSocketStream stream;
    private volatile String activeInstrument;
    private volatile Consumer<String> fatalErrorHandler;
    private volatile WebSocketDisconnectListener disconnectListener;

    /**
     * Constructs the Deribit WebSocket connector with configuration, credentials, and a dedicated 1-thread Vert.x instance.
     *
     * @param vertx            shared Vert.x instance
     * @param wsUrl            Deribit WebSocket v2 endpoint URL
     * @param clientId         Deribit API Client ID (required for raw trades)
     * @param clientSecret     Deribit API Client Secret (required for raw trades)
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    @Inject
    public DeribitStreamConnector(
            Vertx vertx,
            @ConfigProperty(name = "polymarket.connector.deribit.ws-url", defaultValue = "wss://www.deribit.com/ws/api/v2")
            String wsUrl,
            @ConfigProperty(name = "polymarket.connector.deribit.client-id", defaultValue = "")
            Optional<String> clientId,
            @ConfigProperty(name = "polymarket.connector.deribit.client-secret", defaultValue = "")
            Optional<String> clientSecret,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "15000")
            long pingIntervalMs) {
        this.deribitVertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(1)
                .setPreferNativeTransport(true)
                .setAddressResolverOptions(new AddressResolverOptions()
                        .setRoundRobinInetAddress(true)
                        .setRotateServers(true)
                        .setCacheMaxTimeToLive(60)));
        if (!this.deribitVertx.isNativeTransportEnabled()) {
            Log.warnf("Native Linux Epoll transport is unavailable on deribitVertx (os=%s); falling back to JDK NIO EventLoop",
                    System.getProperty("os.name", "unknown"));
        } else {
            Log.infof("Initialized dedicated deribitVertx with native Linux Epoll transport (openSslAvailable=%b)",
                    OpenSsl.isAvailable());
        }
        this.wsUrl = wsUrl;
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
     * @param clientId         Deribit API Client ID
     * @param clientSecret     Deribit API Client Secret
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    public DeribitStreamConnector(
            Vertx vertx,
            String wsUrl,
            String clientId,
            String clientSecret,
            long reconnectDelayMs,
            long pingIntervalMs) {
        this(vertx, wsUrl, Optional.ofNullable(clientId), Optional.ofNullable(clientSecret), reconnectDelayMs, pingIntervalMs);
    }

    @PreDestroy
    public void destroy() {
        stop();
        try {
            deribitVertx.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Returns whether native Linux Epoll transport is active on the dedicated Deribit Vert.x instance.
     *
     * @return {@code true} if native transport is enabled
     */
    public boolean isNativeTransportEnabled() {
        return deribitVertx.isNativeTransportEnabled();
    }

    /**
     * Returns whether native OpenSSL/BoringSSL ({@code netty-tcnative}) is available on the runtime classpath.
     *
     * @return {@code true} if native OpenSSL/BoringSSL is available
     */
    public boolean isOpenSslAvailable() {
        return OpenSsl.isAvailable();
    }

    /**
     * Registers a listener notified whenever the Deribit stream disconnects unexpectedly.
     *
     * @param disconnectListener listener invoked with the stream telemetry snapshot at the moment of disconnect
     */
    public synchronized void setDisconnectListener(WebSocketDisconnectListener disconnectListener) {
        this.disconnectListener = disconnectListener;
        ReconnectingWebSocketStream current = this.stream;
        if (current != null) {
            current.setDisconnectListener(disconnectListener);
        }
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
     * Sets a custom handler for fatal errors (e.g. auth failure).
     * If not set, {@link io.quarkus.runtime.Quarkus#asyncExit(int)} is called with exit code 1.
     *
     * @param fatalErrorHandler consumer receiving the raw error JSON payload
     */
    public void setFatalErrorHandler(Consumer<String> fatalErrorHandler) {
        this.fatalErrorHandler = fatalErrorHandler;
    }

    /**
     * Starts the Deribit WebSocket stream for the given symbol/instrument and routes raw trade payloads to the listener.
     * Enforces that credentials are present; otherwise aborts startup with an {@link IllegalStateException}.
     *
     * @param symbol   trading symbol or instrument name (e.g. BTCUSDT, BTC, or BTC-PERPETUAL)
     * @param listener raw WebSocket listener receiving unmodified JSON payloads and local receipt timestamps
     * @throws IllegalStateException if required Deribit API credentials are not configured
     */
    public synchronized void start(String symbol, RawWebSocketListener listener) {
        start(symbol, listener, null);
    }

    /**
     * Starts the Deribit WebSocket stream for the given symbol/instrument bound to an optional external worker thread.
     *
     * @param symbol         trading symbol or instrument name (e.g. BTCUSDT, BTC, or BTC-PERPETUAL)
     * @param listener       raw WebSocket listener receiving unmodified JSON payloads and local receipt timestamps
     * @param externalWorker optional dedicated platform worker thread responsible for draining this stream
     * @throws IllegalStateException if required Deribit API credentials are not configured
     */
    public synchronized void start(String symbol, RawWebSocketListener listener, Thread externalWorker) {
        stop();
        String instrument = normalizeInstrument(symbol);
        this.activeInstrument = instrument;
        String streamName = "DERIBIT-FUTURES-TRADES-" + instrument;

        if (!isAuthenticated()) {
            String errorMsg = String.format(
                    "[%s] Missing required Deribit API credentials (DERIBIT_CLIENT_ID, DERIBIT_CLIENT_SECRET). Deribit raw trade streams require authentication; aborting startup.",
                    streamName
            );
            Log.fatal(errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        Log.infof("[%s] Initializing authenticated Deribit raw stream (trades.%s.raw) with clientId=%s...",
                streamName, instrument, maskKey(clientId));

        String authMessage = buildAuthMessage();
        String subscribeMessage = buildSubscribeMessage(instrument);

        RawWebSocketListener internalListener = new RawWebSocketListener() {
            @Override
            public void onMessage(String rawJson, long recvTs) {
                handleControlFrame(streamName, instrument, subscribeMessage, rawJson);
            }

            @Override
            public void onMessageBytes(TimestampedFrame frame) {
                if (frame == null) {
                    return;
                }
                byte[] rawBytes = frame.rawUtf8Bytes();
                if (rawBytes == null || rawBytes.length == 0) {
                    return;
                }
                // Fast-path: >99.9% of frames are subscription trade notifications (evaluated directly on byte[] without String allocation)
                if (isTradeNotificationBytes(rawBytes)) {
                    if (listener != null) {
                        listener.onMessageBytes(frame);
                    }
                    return;
                }
                String rawJson = frame.rawText();
                if (rawJson != null && !rawJson.isBlank()) {
                    handleControlFrame(streamName, instrument, subscribeMessage, rawJson);
                }
            }
        };

        ReconnectingWebSocketStream newStream = new ReconnectingWebSocketStream(
                deribitVertx,
                streamName,
                wsUrl,
                () -> List.of(authMessage),
                internalListener,
                reconnectDelayMs,
                pingIntervalMs
        );
        newStream.setDedicatedIoThreadName(DEDICATED_DERIBIT_IO_THREAD_NAME);
        newStream.setDisconnectListener(this.disconnectListener);
        if (externalWorker != null) {
            newStream.bindExternalWorker(externalWorker);
        }
        this.stream = newStream;
        newStream.start();
    }

    private void handleControlFrame(String streamName, String instrument, String subscribeMessage, String rawJson) {
        // Handle Deribit heartbeat test request by replying immediately
        if (isTestRequest(rawJson)) {
            ReconnectingWebSocketStream current = this.stream;
            if (current != null) {
                current.sendText(HEARTBEAT_REPLY_JSON);
            }
            return;
        }
        // Handle authentication response
        if (isAuthResponse(rawJson)) {
            if (isErrorResponse(rawJson)) {
                Log.fatalf("[%s] Deribit authentication failed: %s. Terminating application.", streamName, rawJson);
                triggerFatalError(rawJson);
            } else {
                Log.infof("[%s] Deribit session authenticated successfully. Subscribing to channel trades.%s.raw...", streamName, instrument);
                ReconnectingWebSocketStream current = this.stream;
                if (current != null) {
                    current.sendText(subscribeMessage);
                }
            }
            return;
        }
        // Handle subscription response
        if (isSubscriptionResponse(rawJson)) {
            if (isErrorResponse(rawJson)) {
                Log.fatalf("[%s] Deribit raw subscription rejected: %s. Terminating application.", streamName, rawJson);
                triggerFatalError(rawJson);
            } else {
                Log.infof("[%s] Subscribed successfully to channel: trades.%s.raw", streamName, instrument);
            }
            return;
        }
        // Handle any unexpected error response
        if (isErrorResponse(rawJson)) {
            Log.errorf("[%s] Deribit returned error: %s", streamName, rawJson);
        }
    }

    /**
     * Drains all currently queued frames from the Deribit stream into {@code scratchBatch}.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return number of frames drained
     */
    public int drainStream(List<TimestampedFrame> scratchBatch) {
        ReconnectingWebSocketStream current = this.stream;
        return current != null ? current.drainAvailableBatch(scratchBatch) : 0;
    }

    /**
     * Returns whether the Deribit stream currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if the Deribit queue is non-empty
     */
    public boolean hasQueuedFrames() {
        ReconnectingWebSocketStream current = this.stream;
        return current != null && current.hasQueuedFrames();
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
     * Returns telemetry snapshots for the active Deribit WebSocket stream.
     *
     * @return list of {@link WebSocketStreamTelemetry} snapshots
     */
    public List<WebSocketStreamTelemetry> getStreamTelemetries() {
        ReconnectingWebSocketStream current = this.stream;
        return current != null ? List.of(current.getTelemetrySnapshot()) : List.of();
    }

    /**
     * Returns the dedicated Netty I/O reader thread observed by the active Deribit stream, or {@code null}.
     *
     * @return Netty I/O reader thread
     */
    public Thread getIoReaderThread() {
        ReconnectingWebSocketStream current = this.stream;
        return current != null ? current.getIoReaderThread() : null;
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

    private void triggerFatalError(String rawErrorJson) {
        stop();
        Consumer<String> handler = this.fatalErrorHandler;
        if (handler != null) {
            handler.accept(rawErrorJson);
        } else {
            try {
                Quarkus.asyncExit(1);
            } catch (Throwable t) {
                Log.errorf("Error triggering Quarkus exit: %s", t.getMessage());
            }
        }
    }

    private String buildAuthMessage() {
        return "{\"jsonrpc\":\"2.0\",\"id\":" + AUTH_REQUEST_ID + ",\"method\":\"public/auth\",\"params\":{\"grant_type\":\"client_credentials\",\"client_id\":\""
                + escapeJson(clientId) + "\",\"client_secret\":\"" + escapeJson(clientSecret) + "\"}}";
    }

    private String buildSubscribeMessage(String instrument) {
        return "{\"jsonrpc\":\"2.0\",\"id\":" + SUBSCRIBE_REQUEST_ID + ",\"method\":\"public/subscribe\",\"params\":{\"channels\":[\"trades."
                + instrument + ".raw\"]}}";
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

    static boolean isTradeNotificationBytes(byte[] rawBytes) {
        // Deribit subscription header is always within the first 96 bytes of the JSON-RPC envelope
        int maxSearchLen = Math.min(rawBytes.length, 96);
        return containsAsciiPattern(rawBytes, maxSearchLen, SUBSCRIPTION_PATTERN_COMPACT)
                || containsAsciiPattern(rawBytes, maxSearchLen, SUBSCRIPTION_PATTERN_SPACED);
    }

    private static boolean containsAsciiPattern(byte[] haystack, int searchLimit, byte[] needle) {
        int nLen = needle.length;
        int lastStart = searchLimit - nLen;
        if (lastStart < 0) {
            return false;
        }
        byte first = needle[0];
        for (int i = 0; i <= lastStart; i++) {
            if (haystack[i] == first) {
                boolean match = true;
                for (int j = 1; j < nLen; j++) {
                    if (haystack[i + j] != needle[j]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    return true;
                }
            }
        }
        return false;
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

