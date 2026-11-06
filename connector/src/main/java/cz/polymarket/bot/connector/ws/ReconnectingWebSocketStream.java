package cz.polymarket.bot.connector.ws;

import io.quarkus.logging.Log;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * Resilient, non-blocking Vert.x WebSocket stream client with connection state monitoring,
 * periodic ping frames, automatic reconnection on disconnect or failure, immediate local timestamping,
 * and lock-free asynchronous message draining on a single persistent worker thread off the Netty I/O event loop.
 */
public class ReconnectingWebSocketStream {

    private static final int MAX_FRAME_SIZE_BYTES = 4 * 1024 * 1024;
    private static final int MAX_HEARTBEAT_FRAME_LENGTH = 64;
    private static final int SPIN_ITERATIONS_BEFORE_PARK = 64;
    private static final long DEBUG_MESSAGE_LOG_INTERVAL = 500L;

    private record QueuedFrame(String rawText, long recvTs) {
    }

    private final Vertx vertx;
    private final String streamName;
    private final String wsUrl;
    private final Supplier<List<String>> subscriptionMessagesSupplier;
    private final RawWebSocketListener messageListener;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;
    private final String textPingPayload;
    private final HttpClient httpClient;

    private final ConcurrentLinkedQueue<QueuedFrame> inboundQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean workerParked = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.DISCONNECTED);
    private final AtomicLong lastMessageTimestampMs = new AtomicLong(0L);
    private final AtomicLong reconnectCount = new AtomicLong(0L);
    private final AtomicLong debugReceivedFrameCounter = new AtomicLong(0L);
    private volatile Thread drainThread;
    private volatile WebSocket currentWebSocket;
    private volatile long pingTimerId = -1L;

    /**
     * Constructs a managed, auto-reconnecting WebSocket stream with protocol-level ping frames.
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
        this(vertx, streamName, wsUrl, subscriptionMessagesSupplier, messageListener, reconnectDelayMs, pingIntervalMs, null);
    }

    /**
     * Constructs a managed, auto-reconnecting WebSocket stream with optional application-level text ping.
     *
     * @param vertx                        Vert.x runtime instance
     * @param streamName                   human-readable stream identifier for logging and diagnostics
     * @param wsUrl                        target WebSocket URL (ws:// or wss://)
     * @param subscriptionMessagesSupplier supplier returning initial subscription messages to send upon each connection
     * @param messageListener              listener invoked with raw JSON and local receipt timestamp
     * @param reconnectDelayMs             delay in milliseconds before attempting reconnection after disconnect
     * @param pingIntervalMs               interval in milliseconds for sending WebSocket ping frames (0 to disable)
     * @param textPingPayload              optional application-level text ping payload (e.g. {@code "PING"}), or null if disabled
     */
    public ReconnectingWebSocketStream(
            Vertx vertx,
            String streamName,
            String wsUrl,
            Supplier<List<String>> subscriptionMessagesSupplier,
            RawWebSocketListener messageListener,
            long reconnectDelayMs,
            long pingIntervalMs,
            String textPingPayload) {
        this.vertx = vertx;
        this.streamName = streamName;
        this.wsUrl = wsUrl;
        this.subscriptionMessagesSupplier = subscriptionMessagesSupplier;
        this.messageListener = messageListener;
        this.reconnectDelayMs = Math.max(50L, reconnectDelayMs);
        this.pingIntervalMs = pingIntervalMs;
        this.textPingPayload = textPingPayload;

        HttpClientOptions options = new HttpClientOptions()
                .setSsl(wsUrl.toLowerCase().startsWith("wss"))
                .setTcpNoDelay(true)
                .setKeepAlive(true)
                .setTryUsePerMessageWebSocketCompression(true)
                .setMaxWebSocketFrameSize(MAX_FRAME_SIZE_BYTES)
                .setMaxWebSocketMessageSize(MAX_FRAME_SIZE_BYTES * 4);
        this.httpClient = vertx.createHttpClient(options);
    }

    /**
     * Starts the persistent drain worker thread, WebSocket connection, and automatic reconnection loop.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            state.set(ConnectionState.CONNECTING);
            this.drainThread = Thread.ofVirtual()
                    .name("ws-drain-" + streamName)
                    .start(this::drainLoop);
            connect();
        }
    }

    /**
     * Stops the WebSocket stream, wakes and terminates the drain worker, cancels timers,
     * and closes the underlying connection and HTTP client.
     */
    public void stop() {
        running.set(false);
        inboundQueue.clear();
        Thread worker = this.drainThread;
        this.drainThread = null;
        if (worker != null) {
            workerParked.set(false);
            LockSupport.unpark(worker);
        }
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
            if (Log.isTraceEnabled()) {
                Log.tracef("[%s] Sending WebSocket text frame: %s", streamName, textPayload);
            }
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
                        if (!running.get() || ws.isClosed()) {
                            if (!ws.isClosed()) {
                                ws.close();
                            }
                            return;
                        }
                        this.currentWebSocket = ws;
                        this.state.set(ConnectionState.CONNECTED);
                        Log.infof("[%s] WebSocket connected to %s (reconnectCount=%d)",
                                streamName, wsUrl, reconnectCount.get());

                        ws.textMessageHandler(rawText -> {
                            long recvTs = System.currentTimeMillis();
                            lastMessageTimestampMs.set(recvTs);
                            if (isHeartbeatFrame(rawText)) {
                                if (Log.isTraceEnabled()) {
                                    Log.tracef("[%s] Received WebSocket text heartbeat: %s", streamName, rawText.trim());
                                }
                                return;
                            }
                            if (Log.isTraceEnabled()) {
                                Log.tracef("[%s] Received WebSocket text frame at %d ms: %s", streamName, recvTs, rawText);
                            }
                            if (Log.isDebugEnabled()) {
                                long count = debugReceivedFrameCounter.incrementAndGet();
                                if (count % DEBUG_MESSAGE_LOG_INTERVAL == 0L) {
                                    Log.debugf("[%s] Received %d WebSocket frames (lastMessageTimestampMs=%d)",
                                            streamName, count, recvTs);
                                }
                            }
                            if (messageListener != null && rawText != null) {
                                inboundQueue.offer(new QueuedFrame(rawText, recvTs));
                                if (workerParked.get() && workerParked.compareAndSet(true, false)) {
                                    LockSupport.unpark(this.drainThread);
                                }
                            }
                        });

                        ws.pongHandler(buf -> {
                            long nowMs = System.currentTimeMillis();
                            lastMessageTimestampMs.set(nowMs);
                            if (Log.isTraceEnabled()) {
                                Log.tracef("[%s] Received WebSocket PONG at %d ms", streamName, nowMs);
                            }
                        });

                        ws.closeHandler(v -> {
                            short closeCode = ws.closeStatusCode() != null ? ws.closeStatusCode() : -1;
                            String closeReason = ws.closeReason() != null ? ws.closeReason() : "none";
                            if (running.get()) {
                                Log.errorf("[%s] WebSocket connection closed unexpectedly (code=%d, reason=%s); scheduling reconnect in %d ms",
                                        streamName, closeCode, closeReason, reconnectDelayMs);
                            } else if (Log.isDebugEnabled()) {
                                Log.debugf("[%s] WebSocket connection closed cleanly on stop (code=%d)", streamName, closeCode);
                            }
                            onConnectionClosed(ws);
                        });

                        ws.exceptionHandler(err -> {
                            Log.errorf("[%s] WebSocket error: %s", streamName, err.getMessage());
                            onConnectionClosed(ws);
                        });

                        if (subscriptionMessagesSupplier != null) {
                            List<String> subs = subscriptionMessagesSupplier.get();
                            if (subs != null) {
                                for (String sub : subs) {
                                    if (sub != null && !sub.isBlank()) {
                                        if (Log.isTraceEnabled()) {
                                            Log.tracef("[%s] Sending initial WebSocket subscription/auth message", streamName);
                                        }
                                        ws.writeTextMessage(sub);
                                    }
                                }
                            }
                        }

                        startPingTimer();
                    })
                    .onFailure(err -> {
                        Log.errorf("[%s] WebSocket connect failed (%s): %s", streamName, wsUrl, err.getMessage());
                        scheduleReconnect();
                    });
        } catch (Exception e) {
            Log.errorf("[%s] Error initiating WebSocket connection (%s): %s", streamName, wsUrl, e.getMessage());
            scheduleReconnect();
        }
    }

    private void startPingTimer() {
        cancelPingTimer();
        if (pingIntervalMs > 0L && running.get()) {
            pingTimerId = vertx.setPeriodic(pingIntervalMs, id -> {
                WebSocket ws = this.currentWebSocket;
                if (ws != null && !ws.isClosed() && running.get()) {
                    if (textPingPayload != null && !textPingPayload.isBlank()) {
                        if (Log.isTraceEnabled()) {
                            Log.tracef("[%s] Sending WebSocket text ping: %s", streamName, textPingPayload);
                        }
                        ws.writeTextMessage(textPingPayload);
                    }
                    if (Log.isTraceEnabled()) {
                        Log.tracef("[%s] Sending WebSocket protocol PING frame", streamName);
                    }
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

    private void drainLoop() {
        while (running.get()) {
            QueuedFrame frame = inboundQueue.poll();
            if (frame != null) {
                try {
                    messageListener.onMessage(frame.rawText(), frame.recvTs());
                } catch (Exception e) {
                    Log.errorf("[%s] Unhandled exception in WebSocket message listener: %s",
                            streamName, e.getMessage());
                }
                continue;
            }
            for (int spin = 0; spin < SPIN_ITERATIONS_BEFORE_PARK && running.get() && inboundQueue.isEmpty(); spin++) {
                Thread.onSpinWait();
            }
            if (!running.get() || !inboundQueue.isEmpty()) {
                continue;
            }
            workerParked.set(true);
            if (running.get() && inboundQueue.isEmpty()) {
                LockSupport.park(this);
            }
            workerParked.set(false);
        }
        inboundQueue.clear();
    }

    static boolean isHeartbeatFrame(String rawText) {
        if (rawText == null || rawText.length() > MAX_HEARTBEAT_FRAME_LENGTH) {
            return false;
        }
        String trimmed = rawText.trim();
        return "PONG".equalsIgnoreCase(trimmed)
                || "PING".equalsIgnoreCase(trimmed)
                || trimmed.contains("\"op\":\"pong\"")
                || trimmed.contains("\"op\": \"pong\"");
    }
}
