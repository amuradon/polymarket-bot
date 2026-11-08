package cz.polymarket.bot.connector.ws;

import io.quarkus.logging.Log;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * Resilient, non-blocking Vert.x WebSocket stream client with connection state monitoring,
 * periodic ping frames, automatic reconnection on disconnect or failure, immediate local timestamping
 * on the network I/O reader thread, zero-decode raw {@code byte[]} frame handoff, and lock-free batch draining
 * with real-time queue and latency telemetry.
 */
public class ReconnectingWebSocketStream {

    private static final int MAX_FRAME_SIZE_BYTES = 4 * 1024 * 1024;
    private static final int SOCKET_RECEIVE_BUFFER_BYTES = 16 * 1024 * 1024;
    private static final int MAX_HEARTBEAT_FRAME_LENGTH = 64;
    private static final int SPIN_ITERATIONS_BEFORE_PARK = 64;
    private static final int DEFAULT_BATCH_LIMIT = 512;
    private static final long DEBUG_MESSAGE_LOG_INTERVAL = 500L;

    private final Vertx vertx;
    private final String streamName;
    private final String wsUrl;
    private final Supplier<List<String>> subscriptionMessagesSupplier;
    private final RawWebSocketListener messageListener;
    private final RawWebSocketBatchListener batchListener;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;
    private final String textPingPayload;
    private final HttpClient httpClient;

    private final ConcurrentLinkedQueue<TimestampedFrame> inboundQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean workerParked = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.DISCONNECTED);
    private final AtomicLong lastMessageTimestampMs = new AtomicLong(0L);
    private final AtomicLong reconnectCount = new AtomicLong(0L);
    private final AtomicLong debugReceivedFrameCounter = new AtomicLong(0L);

    // Real-time stream & queue telemetry counters
    private final AtomicInteger queueDepth = new AtomicInteger(0);
    private final AtomicInteger maxQueueDepth = new AtomicInteger(0);
    private final AtomicLong lastQueueLagMs = new AtomicLong(0L);
    private final AtomicLong maxQueueLagMs = new AtomicLong(0L);
    private final AtomicLong lastProcessTimeUs = new AtomicLong(0L);
    private final AtomicLong maxProcessTimeUs = new AtomicLong(0L);
    private final AtomicLong totalFramesReceived = new AtomicLong(0L);
    private final AtomicLong coalescedFramesCount = new AtomicLong(0L);
    private final AtomicInteger lastCloseCode = new AtomicInteger(0);
    private final AtomicReference<String> lastCloseReason = new AtomicReference<>("");
    private final AtomicLong totalBusyTimeNs = new AtomicLong(0L);

    private volatile String dedicatedIoThreadName;
    private volatile Thread ioReaderThread;
    private volatile Thread drainThread;
    private volatile Thread externalWorkerThread;
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
        this(vertx, streamName, wsUrl, subscriptionMessagesSupplier, messageListener, null, reconnectDelayMs, pingIntervalMs, null);
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
        this(vertx, streamName, wsUrl, subscriptionMessagesSupplier, messageListener, null, reconnectDelayMs, pingIntervalMs, textPingPayload);
    }

    /**
     * Constructs a managed, auto-reconnecting WebSocket stream with batch listener support for burst coalescing.
     *
     * @param vertx                        Vert.x runtime instance
     * @param streamName                   human-readable stream identifier for logging and diagnostics
     * @param wsUrl                        target WebSocket URL (ws:// or wss://)
     * @param subscriptionMessagesSupplier supplier returning initial subscription messages to send upon each connection
     * @param messageListener              optional per-frame listener (used when {@code batchListener} is null)
     * @param batchListener                optional batch listener invoked with all frames drained in a single pass
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
            RawWebSocketBatchListener batchListener,
            long reconnectDelayMs,
            long pingIntervalMs,
            String textPingPayload) {
        this.vertx = vertx;
        this.streamName = streamName;
        this.wsUrl = wsUrl;
        this.subscriptionMessagesSupplier = subscriptionMessagesSupplier;
        this.messageListener = messageListener;
        this.batchListener = batchListener;
        this.reconnectDelayMs = Math.max(50L, reconnectDelayMs);
        this.pingIntervalMs = pingIntervalMs;
        this.textPingPayload = textPingPayload;

        HttpClientOptions options = new HttpClientOptions()
                .setSsl(wsUrl.toLowerCase().startsWith("wss"))
                .setTcpNoDelay(true)
                .setKeepAlive(true)
                .setReceiveBufferSize(SOCKET_RECEIVE_BUFFER_BYTES)
                .setTryUsePerMessageWebSocketCompression(false)
                .setMaxWebSocketFrameSize(MAX_FRAME_SIZE_BYTES)
                .setMaxWebSocketMessageSize(MAX_FRAME_SIZE_BYTES * 4);
        this.httpClient = vertx.createHttpClient(options);
    }

    /**
     * Configures an explicit thread name and {@link Thread#MAX_PRIORITY} for the isolated Netty EventLoop
     * thread servicing this stream (e.g. {@code "clob-ws-io-reader"}).
     *
     * @param dedicatedIoThreadName thread name to assign to the isolated EventLoop thread
     */
    public void setDedicatedIoThreadName(String dedicatedIoThreadName) {
        this.dedicatedIoThreadName = dedicatedIoThreadName;
    }

    /**
     * Seeds historical high-water mark and reconnect telemetry counters carried over from a previously
     * rotated market stream so 15-minute market rotations do not wipe out historical diagnostics.
     *
     * @param priorReconnectCount cumulative reconnect count from prior market streams
     * @param priorCloseCode      last observed close status code from prior market streams
     * @param priorCloseReason    last observed close reason from prior market streams
     * @param priorMaxQueueDepth  historical peak inbound queue depth
     * @param priorMaxQueueLagMs  historical peak queue wait latency in milliseconds
     * @param priorMaxProcessUs   historical peak batch processing time in microseconds
     */
    public void seedHistoricalTelemetry(
            long priorReconnectCount,
            int priorCloseCode,
            String priorCloseReason,
            int priorMaxQueueDepth,
            long priorMaxQueueLagMs,
            long priorMaxProcessUs) {
        if (priorReconnectCount > 0L) {
            reconnectCount.addAndGet(priorReconnectCount);
        }
        if (priorCloseCode != 0 && lastCloseCode.get() == 0) {
            lastCloseCode.set(priorCloseCode);
        }
        if (priorCloseReason != null && !priorCloseReason.isEmpty() && lastCloseReason.get().isEmpty()) {
            lastCloseReason.set(priorCloseReason);
        }
        if (priorMaxQueueDepth > 0) {
            updateMaxInt(maxQueueDepth, priorMaxQueueDepth);
        }
        if (priorMaxQueueLagMs > 0L) {
            updateMaxLong(maxQueueLagMs, priorMaxQueueLagMs);
        }
        if (priorMaxProcessUs > 0L) {
            updateMaxLong(maxProcessTimeUs, priorMaxProcessUs);
        }
    }

    /**
     * Binds this stream's lock-free {@code inboundQueue} wakeups to an externally managed dedicated platform thread
     * (such as {@code clob-book-worker}, {@code strategy-calculator-worker}, or {@code order-execution-io-worker}).
     * When bound, {@link #start()} will not spawn an internal drain thread; instead, the bound worker thread
     * is unparked upon frame arrival and drains batches via {@link #drainAvailableBatch(List)}.
     *
     * @param workerThread dedicated platform thread responsible for draining this stream
     */
    public void bindExternalWorker(Thread workerThread) {
        this.externalWorkerThread = workerThread;
        Thread internal = this.drainThread;
        this.drainThread = null;
        if (internal != null) {
            workerParked.set(false);
            LockSupport.unpark(internal);
        }
    }

    /**
     * Starts the WebSocket connection, automatic reconnection loop, and (unless bound to an external worker via
     * {@link #bindExternalWorker(Thread)}) a dedicated high-priority platform drain thread.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            state.set(ConnectionState.CONNECTING);
            if (externalWorkerThread == null) {
                this.drainThread = Thread.ofPlatform()
                        .daemon(true)
                        .priority(Thread.MAX_PRIORITY)
                        .name("ws-drain-" + streamName)
                        .start(this::drainLoop);
            }
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
        queueDepth.set(0);
        Thread worker = this.drainThread;
        this.drainThread = null;
        if (worker != null) {
            workerParked.set(false);
            LockSupport.unpark(worker);
        }
        Thread ext = this.externalWorkerThread;
        if (ext != null) {
            LockSupport.unpark(ext);
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
     * Drains all currently queued frames (up to {@code DEFAULT_BATCH_LIMIT}) from the lock-free {@code inboundQueue}
     * into {@code scratchBatch}, updates queue depth, wait lag, and processing time telemetry, and dispatches the
     * frames to the configured {@link RawWebSocketBatchListener} or {@link RawWebSocketListener}.
     *
     * @param scratchBatch reusable pre-allocated list owned by the calling worker thread
     * @return number of frames drained and processed in this pass
     */
    public int drainAvailableBatch(List<TimestampedFrame> scratchBatch) {
        scratchBatch.clear();
        TimestampedFrame first = inboundQueue.poll();
        if (first == null) {
            return 0;
        }
        scratchBatch.add(first);
        while (scratchBatch.size() < DEFAULT_BATCH_LIMIT) {
            TimestampedFrame next = inboundQueue.poll();
            if (next == null) {
                break;
            }
            scratchBatch.add(next);
        }

        int drainedCount = scratchBatch.size();
        int remaining = queueDepth.addAndGet(-drainedCount);
        if (remaining < 0) {
            queueDepth.set(0);
        }

        long drainStartMs = System.currentTimeMillis();
        long oldestLagMs = Math.max(0L, drainStartMs - first.recvTs());
        lastQueueLagMs.set(oldestLagMs);
        updateMaxLong(maxQueueLagMs, oldestLagMs);

        long procStartNs = System.nanoTime();
        try {
            if (batchListener != null) {
                if (drainedCount > 1) {
                    coalescedFramesCount.addAndGet(drainedCount - 1L);
                }
                batchListener.onMessageBatch(scratchBatch);
            } else if (messageListener != null) {
                for (int i = 0; i < drainedCount; i++) {
                    TimestampedFrame frame = scratchBatch.get(i);
                    messageListener.onMessage(frame.rawText(), frame.recvTs());
                }
            }
        } catch (Exception e) {
            Log.errorf("[%s] Unhandled exception in WebSocket message listener: %s",
                    streamName, e.getMessage());
        } finally {
            long elapsedNs = Math.max(0L, System.nanoTime() - procStartNs);
            totalBusyTimeNs.addAndGet(elapsedNs);
            long elapsedUs = elapsedNs / 1_000L;
            lastProcessTimeUs.set(elapsedUs);
            updateMaxLong(maxProcessTimeUs, elapsedUs);
            scratchBatch.clear();
        }
        return drainedCount;
    }

    /**
     * Returns whether any frames are currently waiting in {@code inboundQueue}.
     *
     * @return {@code true} if {@code inboundQueue} is non-empty
     */
    public boolean hasQueuedFrames() {
        return !inboundQueue.isEmpty();
    }

    /**
     * Increments the coalesced/fast-forwarded frames counter by {@code delta}.
     *
     * @param delta number of additional coalesced frames
     */
    public void addCoalescedFrames(long delta) {
        if (delta > 0L) {
            coalescedFramesCount.addAndGet(delta);
        }
    }

    /**
     * Returns the human-readable stream identifier.
     *
     * @return stream name
     */
    public String getStreamName() {
        return streamName;
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

    /**
     * Returns the Netty I/O event-loop thread that most recently delivered frames for this stream, or {@code null}.
     *
     * @return Netty I/O reader thread
     */
    public Thread getIoReaderThread() {
        return ioReaderThread;
    }

    /**
     * Returns the internal drain worker thread (or externally bound worker thread) for this stream.
     *
     * @return worker thread
     */
    public Thread getWorkerThread() {
        Thread ext = this.externalWorkerThread;
        return ext != null ? ext : this.drainThread;
    }

    /**
     * Returns the cumulative nanosecond busy time spent processing frames from this stream.
     *
     * @return cumulative busy time in nanoseconds
     */
    public long getTotalBusyTimeNs() {
        return totalBusyTimeNs.get();
    }

    /**
     * Returns an immutable point-in-time snapshot of this stream's queue depth, latency, and connection telemetry.
     *
     * @return {@link WebSocketStreamTelemetry} snapshot
     */
    public WebSocketStreamTelemetry getTelemetrySnapshot() {
        return new WebSocketStreamTelemetry(
                streamName,
                state.get(),
                Math.max(0, queueDepth.get()),
                maxQueueDepth.get(),
                lastQueueLagMs.get(),
                maxQueueLagMs.get(),
                lastProcessTimeUs.get(),
                maxProcessTimeUs.get(),
                totalFramesReceived.get(),
                coalescedFramesCount.get(),
                reconnectCount.get(),
                lastMessageTimestampMs.get(),
                lastCloseCode.get(),
                lastCloseReason.get()
        );
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
                        Thread currentThread = Thread.currentThread();
                        applyDedicatedIoThreadTuning(currentThread);
                        this.ioReaderThread = currentThread;
                        this.currentWebSocket = ws;
                        this.state.set(ConnectionState.CONNECTED);
                        Log.infof("[%s] WebSocket connected to %s on thread %s (reconnectCount=%d)",
                                streamName, wsUrl, currentThread.getName(), reconnectCount.get());

                        final Buffer[] continuationBuffer = new Buffer[1];

                        ws.frameHandler(frame -> {
                            this.ioReaderThread = Thread.currentThread();
                            long recvTs = System.currentTimeMillis();
                            lastMessageTimestampMs.set(recvTs);

                            if (frame.isText() || frame.isBinary()) {
                                if (frame.isFinal()) {
                                    continuationBuffer[0] = null;
                                    handleInboundPayloadBuffer(frame.binaryData(), recvTs);
                                } else {
                                    Buffer partial = frame.binaryData();
                                    Buffer acc = Buffer.buffer(Math.max(4096, partial != null ? partial.length() * 2 : 4096));
                                    if (partial != null) {
                                        acc.appendBuffer(partial);
                                    }
                                    continuationBuffer[0] = acc;
                                }
                            } else if (frame.isContinuation()) {
                                Buffer acc = continuationBuffer[0];
                                if (acc != null) {
                                    Buffer partial = frame.binaryData();
                                    if (partial != null) {
                                        acc.appendBuffer(partial);
                                    }
                                    if (frame.isFinal()) {
                                        continuationBuffer[0] = null;
                                        handleInboundPayloadBuffer(acc, recvTs);
                                    }
                                }
                            }
                        });

                        ws.pongHandler(buf -> {
                            this.ioReaderThread = Thread.currentThread();
                            long nowMs = System.currentTimeMillis();
                            lastMessageTimestampMs.set(nowMs);
                            if (Log.isTraceEnabled()) {
                                Log.tracef("[%s] Received WebSocket PONG at %d ms", streamName, nowMs);
                            }
                        });

                        ws.closeHandler(v -> {
                            short closeCode = ws.closeStatusCode() != null ? ws.closeStatusCode() : -1;
                            String closeReason = ws.closeReason() != null ? ws.closeReason() : "none";
                            lastCloseCode.set(closeCode);
                            lastCloseReason.set(closeReason);
                            if (running.get()) {
                                Log.errorf("[%s] WebSocket connection closed unexpectedly (code=%d, reason=%s); scheduling reconnect in %d ms",
                                        streamName, closeCode, closeReason, reconnectDelayMs);
                            } else if (Log.isDebugEnabled()) {
                                Log.debugf("[%s] WebSocket connection closed cleanly on stop (code=%d)", streamName, closeCode);
                            }
                            onConnectionClosed(ws);
                        });

                        ws.exceptionHandler(err -> {
                            lastCloseReason.set(err.getMessage() != null ? err.getMessage() : "exception");
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
                        lastCloseReason.set(err.getMessage() != null ? err.getMessage() : "connect_failed");
                        Log.errorf("[%s] WebSocket connect failed (%s): %s", streamName, wsUrl, err.getMessage());
                        scheduleReconnect();
                    });
        } catch (Exception e) {
            Log.errorf("[%s] Error initiating WebSocket connection (%s): %s", streamName, wsUrl, e.getMessage());
            scheduleReconnect();
        }
    }

    private void applyDedicatedIoThreadTuning(Thread thread) {
        String customName = this.dedicatedIoThreadName;
        if (customName != null && !customName.isBlank() && thread != null) {
            try {
                if (!customName.equals(thread.getName())) {
                    thread.setName(customName);
                }
                if (thread.getPriority() != Thread.MAX_PRIORITY) {
                    thread.setPriority(Thread.MAX_PRIORITY);
                }
            } catch (SecurityException ignored) {
            }
        }
    }

    private void handleInboundPayloadBuffer(Buffer payloadBuffer, long recvTs) {
        if (payloadBuffer == null || payloadBuffer.length() == 0) {
            return;
        }
        if (isHeartbeatBuffer(payloadBuffer)) {
            if (Log.isTraceEnabled()) {
                Log.tracef("[%s] Received WebSocket text heartbeat: %s",
                        streamName, payloadBuffer.toString(StandardCharsets.UTF_8).trim());
            }
            return;
        }
        if (Log.isDebugEnabled()) {
            long count = debugReceivedFrameCounter.incrementAndGet();
            if (count % DEBUG_MESSAGE_LOG_INTERVAL == 0L) {
                Log.debugf("[%s] Received %d WebSocket frames (lastMessageTimestampMs=%d)",
                        streamName, count, recvTs);
            }
        }
        if (messageListener != null || batchListener != null) {
            byte[] rawUtf8Bytes = payloadBuffer.getBytes();
            if (Log.isTraceEnabled()) {
                Log.tracef("[%s] Received WebSocket frame at %d ms (%d bytes)", streamName, recvTs, rawUtf8Bytes.length);
            }
            inboundQueue.offer(new TimestampedFrame(rawUtf8Bytes, recvTs));
            int depth = queueDepth.incrementAndGet();
            updateMaxInt(maxQueueDepth, depth);
            totalFramesReceived.incrementAndGet();

            Thread extWorker = this.externalWorkerThread;
            if (extWorker != null) {
                LockSupport.unpark(extWorker);
            } else if (workerParked.get() && workerParked.compareAndSet(true, false)) {
                LockSupport.unpark(this.drainThread);
            }
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
        List<TimestampedFrame> scratchBatch = new ArrayList<>(64);
        while (running.get() && externalWorkerThread == null) {
            int drained = drainAvailableBatch(scratchBatch);
            if (drained > 0) {
                continue;
            }
            for (int spin = 0; spin < SPIN_ITERATIONS_BEFORE_PARK && running.get() && externalWorkerThread == null && inboundQueue.isEmpty(); spin++) {
                Thread.onSpinWait();
            }
            if (!running.get() || externalWorkerThread != null || !inboundQueue.isEmpty()) {
                continue;
            }
            workerParked.set(true);
            if (running.get() && externalWorkerThread == null && inboundQueue.isEmpty()) {
                LockSupport.park(this);
            }
            workerParked.set(false);
        }
    }

    private static void updateMaxInt(AtomicInteger maxHolder, int candidate) {
        int prev = maxHolder.get();
        while (candidate > prev) {
            if (maxHolder.compareAndSet(prev, candidate)) {
                return;
            }
            prev = maxHolder.get();
        }
    }

    private static void updateMaxLong(AtomicLong maxHolder, long candidate) {
        long prev = maxHolder.get();
        while (candidate > prev) {
            if (maxHolder.compareAndSet(prev, candidate)) {
                return;
            }
            prev = maxHolder.get();
        }
    }

    static boolean isHeartbeatBuffer(Buffer buf) {
        if (buf == null) {
            return false;
        }
        int len = buf.length();
        if (len == 0 || len > MAX_HEARTBEAT_FRAME_LENGTH) {
            return false;
        }
        if (len == 4) {
            byte b0 = buf.getByte(0);
            byte b1 = buf.getByte(1);
            byte b2 = buf.getByte(2);
            byte b3 = buf.getByte(3);
            if ((b0 == 'P' || b0 == 'p')
                    && (b1 == 'O' || b1 == 'o' || b1 == 'I' || b1 == 'i')
                    && (b2 == 'N' || b2 == 'n')
                    && (b3 == 'G' || b3 == 'g')) {
                return true;
            }
        }
        return isHeartbeatFrame(buf.toString(StandardCharsets.UTF_8));
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
