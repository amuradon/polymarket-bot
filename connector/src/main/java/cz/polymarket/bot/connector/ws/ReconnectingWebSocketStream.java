package cz.polymarket.bot.connector.ws;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import org.jboss.logging.Logger;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Resilient, non-blocking Vert.x WebSocket stream client with connection state monitoring,
 * periodic ping frames, automatic reconnection on disconnect or failure, and immediate local timestamping.
 */
public class ReconnectingWebSocketStream {

    private static final Logger LOG = Logger.getLogger(ReconnectingWebSocketStream.class);
    private static final int MAX_FRAME_SIZE_BYTES = 4 * 1024 * 1024;

    private final Vertx vertx;
    private final String streamName;
    private final String wsUrl;
    private final Supplier<List<String>> subscriptionMessagesSupplier;
    private final RawWebSocketListener messageListener;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;
    private final HttpClient httpClient;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.DISCONNECTED);
    private final AtomicLong lastMessageTimestampMs = new AtomicLong(0L);
    private final AtomicLong reconnectCount = new AtomicLong(0L);
    private volatile WebSocket currentWebSocket;
    private volatile long pingTimerId = -1L;

    /**
     * Constructs a managed, auto-reconnecting WebSocket stream.
     *
     * @param vertx                        Vert.x runtime instance
     * @param streamName                   human-readable stream identifier for logging and diagnostics
     * @param wsUrl                        target WebSocket URL (ws:// or wss://)
     * @param subscriptionMessagesSupplier supplier returning initial subscription messages to send upon each connection
     * @param messageListener              listener invoked with raw JSON and local receipt timestamp
     * @param reconnectDelayMs             delay in milliseconds before attempting reconnection after disconnect
     * @param pingIntervalMs               interval in milliseconds for sending WebSocket ping frames (0 to disable)
     */
    public ReconnectingWebSocketStream(
            Vertx vertx,
            String streamName,
            String wsUrl,
            Supplier<List<String>> subscriptionMessagesSupplier,
            RawWebSocketListener messageListener,
            long reconnectDelayMs,
            long pingIntervalMs) {
        this.vertx = vertx;
        this.streamName = streamName;
        this.wsUrl = wsUrl;
        this.subscriptionMessagesSupplier = subscriptionMessagesSupplier;
        this.messageListener = messageListener;
        this.reconnectDelayMs = Math.max(50L, reconnectDelayMs);
        this.pingIntervalMs = pingIntervalMs;

        HttpClientOptions options = new HttpClientOptions()
                .setSsl(wsUrl.toLowerCase().startsWith("wss"))
                .setTcpNoDelay(true)
                .setKeepAlive(true)
                .setMaxWebSocketFrameSize(MAX_FRAME_SIZE_BYTES)
                .setMaxWebSocketMessageSize(MAX_FRAME_SIZE_BYTES * 4);
        this.httpClient = vertx.createHttpClient(options);
    }

    /**
     * Starts the WebSocket connection and automatic reconnection loop.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            state.set(ConnectionState.CONNECTING);
            connect();
        }
    }

    /**
     * Stops the WebSocket stream, cancels timers, and closes the underlying connection and HTTP client.
     */
    public void stop() {
        running.set(false);
        cancelPingTimer();
        state.set(ConnectionState.DISCONNECTED);
        WebSocket ws = this.currentWebSocket;
        this.currentWebSocket = null;
        if (ws != null) {
            try {
                ws.close();
            } catch (Exception ignored) {
            }
        }
        httpClient.close();
    }

    /**
     * Sends a text frame over the active WebSocket connection if currently connected.
     *
     * @param textPayload JSON text message to send
     * @return true if the connection was active and the message was dispatched
     */
    public boolean sendText(String textPayload) {
        WebSocket ws = this.currentWebSocket;
        if (ws != null && !ws.isClosed() && state.get() == ConnectionState.CONNECTED) {
            ws.writeTextMessage(textPayload);
            return true;
        }
        return false;
    }

    /**
     * Returns the current connection state.
     *
     * @return current {@link ConnectionState}
     */
    public ConnectionState getState() {
        return state.get();
    }

    /**
     * Returns the epoch millisecond timestamp of the last received message, or 0 if none received yet.
     *
     * @return last received message timestamp in ms
     */
    public long getLastMessageTimestampMs() {
        return lastMessageTimestampMs.get();
    }

    /**
     * Returns the total number of reconnection attempts triggered since startup.
     *
     * @return reconnect attempt counter
     */
    public long getReconnectCount() {
        return reconnectCount.get();
    }

    private void connect() {
        if (!running.get()) {
            return;
        }

        try {
            URI uri = URI.create(wsUrl);
            boolean isSsl = "wss".equalsIgnoreCase(uri.getScheme());
            int port = uri.getPort() == -1 ? (isSsl ? 443 : 80) : uri.getPort();
            String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
            if (uri.getRawQuery() != null) {
                path += "?" + uri.getRawQuery();
            }

            WebSocketConnectOptions connectOptions = new WebSocketConnectOptions()
                    .setHost(uri.getHost())
                    .setPort(port)
                    .setSsl(isSsl)
                    .setURI(path);

            httpClient.webSocket(connectOptions)
                    .onSuccess(ws -> {
                        if (!running.get()) {
                            ws.close();
                            return;
                        }
                        this.currentWebSocket = ws;
                        this.state.set(ConnectionState.CONNECTED);
                        LOG.infof("[%s] WebSocket connected to %s", streamName, wsUrl);

                        ws.textMessageHandler(rawText -> {
                            long recvTs = System.currentTimeMillis();
                            lastMessageTimestampMs.set(recvTs);
                            if (messageListener != null) {
                                messageListener.onMessage(rawText, recvTs);
                            }
                        });

                        ws.pongHandler(buf -> lastMessageTimestampMs.set(System.currentTimeMillis()));

                        ws.closeHandler(v -> {
                            LOG.warnf("[%s] WebSocket connection closed", streamName);
                            onConnectionClosed(ws);
                        });

                        ws.exceptionHandler(err -> {
                            LOG.warnf("[%s] WebSocket error: %s", streamName, err.getMessage());
                            onConnectionClosed(ws);
                        });

                        if (subscriptionMessagesSupplier != null) {
                            List<String> subs = subscriptionMessagesSupplier.get();
                            if (subs != null) {
                                for (String sub : subs) {
                                    if (sub != null && !sub.isBlank()) {
                                        ws.writeTextMessage(sub);
                                    }
                                }
                            }
                        }

                        startPingTimer();
                    })
                    .onFailure(err -> {
                        LOG.warnf("[%s] WebSocket connect failed (%s): %s", streamName, wsUrl, err.getMessage());
                        scheduleReconnect();
                    });
        } catch (Exception e) {
            LOG.warnf("[%s] Error initiating WebSocket connection (%s): %s", streamName, wsUrl, e.getMessage());
            scheduleReconnect();
        }
    }

    private void startPingTimer() {
        cancelPingTimer();
        if (pingIntervalMs > 0L && running.get()) {
            pingTimerId = vertx.setPeriodic(pingIntervalMs, id -> {
                WebSocket ws = this.currentWebSocket;
                if (ws != null && !ws.isClosed() && running.get()) {
                    ws.writePing(Buffer.buffer("ping"));
                }
            });
        }
    }

    private void cancelPingTimer() {
        long timerId = this.pingTimerId;
        this.pingTimerId = -1L;
        if (timerId != -1L) {
            vertx.cancelTimer(timerId);
        }
    }

    private void onConnectionClosed(WebSocket closedWs) {
        if (this.currentWebSocket == closedWs) {
            this.currentWebSocket = null;
            cancelPingTimer();
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (!running.get()) {
            state.set(ConnectionState.DISCONNECTED);
            return;
        }
        state.set(ConnectionState.RECONNECTING);
        reconnectCount.incrementAndGet();
        vertx.setTimer(reconnectDelayMs, timerId -> {
            if (running.get()) {
                connect();
            }
        });
    }
}
