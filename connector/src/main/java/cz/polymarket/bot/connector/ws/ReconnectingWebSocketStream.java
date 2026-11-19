package cz.polymarket.bot.connector.ws;

import io.netty.handler.ssl.OpenSsl;
import io.netty.resolver.AbstractAddressResolver;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketClientOptions;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.http.impl.WebSocketClientImpl;
import io.vertx.core.impl.CloseFuture;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.SocketAddress;
import org.jctools.queues.MpscUnboundedArrayQueue;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Resilient, non-blocking Vert.x WebSocket stream client with connection state monitoring,
 * periodic ping frames, read-idle/pong watchdog detection, automatic reconnection with optional
 * edge IPv4 slot diversity, immediate local timestamping on the network I/O reader thread,
 * zero-decode raw {@code byte[]} frame handoff, and lock-free batch draining with adaptive
 * user-space micro-spinning to eliminate {@code futex} context-switch storms during bursts.
 */
public class ReconnectingWebSocketStream {

    /**
     * User-space {@link Thread#onSpinWait()} duration in nanoseconds ({@code 300 us}) after active work
     * before entering kernel {@code futex} park, absorbing burst inter-arrival gaps with zero context switches.
     */
    public static final long HOT_SPIN_WINDOW_NS = 300_000L;

    /**
     * Fallback park timeout in nanoseconds ({@code 500 ms}) when idle, minimizing background timer wakeups.
     */
    public static final long IDLE_PARK_TIMEOUT_NS = 500_000_000L;

    private static final int SPIN_CLOCK_CHECK_STRIDE = 64;
    private static final int MAX_FRAME_SIZE_BYTES = 4 * 1024 * 1024;
    private static final int INITIAL_QUEUE_CHUNK_SIZE = 1024;
    private static final int MAX_HEARTBEAT_FRAME_LENGTH = 64;
    private static final int DEFAULT_BATCH_LIMIT = 512;
    private static final long DEBUG_MESSAGE_LOG_INTERVAL = 500L;
    private static final long MIN_READ_IDLE_TIMEOUT_MS = 150L;
    private static final Buffer PROTOCOL_PING_BUFFER = Buffer.buffer("ping");
    static final long INITIAL_FAST_RECONNECT_DELAY_MS = 50L;

    private final Vertx vertx;
    private final String streamName;
    private final String wsUrl;
    private final Supplier<List<String>> subscriptionMessagesSupplier;
    private final RawWebSocketListener messageListener;
    private final RawWebSocketBatchListener batchListener;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;
    private final String textPingPayload;
    private final HttpClientOptions httpClientOptions;
    private final WebSocketClient webSocketClient;

    // Cache-line-padded lock-free queue (eliminates Node allocations and producer/consumer CAS contention)
    private final MpscUnboundedArrayQueue<TimestampedFrame> inboundQueue =
            new MpscUnboundedArrayQueue<>(INITIAL_QUEUE_CHUNK_SIZE);
    private final AtomicBoolean workerParked = new AtomicBoolean(true);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.DISCONNECTED);
    private final AtomicLong lastMessageTimestampMs = new AtomicLong(0L);
    private final AtomicLong lastInboundActivityMs = new AtomicLong(0L);
    private final AtomicLong reconnectCount = new AtomicLong(0L);
    private final AtomicInteger consecutiveReconnectAttempts = new AtomicInteger(0);
    private final AtomicLong debugReceivedFrameCounter = new AtomicLong(0L);

    // Real-time stream & queue telemetry counters
    private final AtomicInteger maxQueueDepth = new AtomicInteger(0);
    private final AtomicLong lastQueueLagMs = new AtomicLong(0L);
    private final AtomicLong maxQueueLagMs = new AtomicLong(0L);
    private final AtomicLong lastProcessTimeUs = new AtomicLong(0L);
    private final AtomicLong maxProcessTimeUs = new AtomicLong(0L);
    private final AtomicLong totalFramesReceived = new AtomicLong(0L);
    private final AtomicLong coalescedFramesCount = new AtomicLong(0L);
    private final AtomicInteger lastCloseCode = new AtomicInteger(0);
    private final AtomicReference<String> lastCloseReason = new AtomicReference<>("");
    private final AtomicReference<String> lastRemoteAddress = new AtomicReference<>("");
    private final AtomicReference<String> pendingRemoteIp = new AtomicReference<>("");
    private final AtomicReference<String> lastFailedIp = new AtomicReference<>("");
    private final AtomicLong totalBusyTimeNs = new AtomicLong(0L);
    private final AtomicBoolean inboundQueueEnabled = new AtomicBoolean(true);
    private final AtomicBoolean normalCloseTriggered = new AtomicBoolean(false);
    private final AtomicBoolean terminalCloseCompleted = new AtomicBoolean(false);

    private volatile String dedicatedIoThreadName;
    private volatile MarketWebSocketRuntime addressSlotRuntime;
    private volatile int preferredAddressSlot = -1;
    private volatile Supplier<String> excludedPeerIpSupplier;
    private volatile Consumer<TimestampedFrame> collectorTap;
    private volatile Thread ioReaderThread;
    private volatile Thread drainThread;
    private volatile Thread externalWorkerThread;
    private volatile WebSocket currentWebSocket;
    private volatile long pingTimerId = -1L;
    private volatile WebSocketDisconnectListener disconnectListener;
    private volatile Consumer<WebSocketStreamTelemetry> normalCloseListener;
    private volatile BooleanSupplier shouldReconnectSupplier = () -> true;

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
        this.reconnectDelayMs = Math.max(10L, reconnectDelayMs);
        this.pingIntervalMs = pingIntervalMs;
        this.textPingPayload = textPingPayload;

        boolean isSsl = wsUrl != null && wsUrl.toLowerCase().startsWith("wss");
        HttpClientOptions options = new HttpClientOptions()
                .setSsl(isSsl)
                .setTcpNoDelay(true)
                .setKeepAlive(true)
                .setTryUsePerMessageWebSocketCompression(false)
                .setMaxWebSocketFrameSize(MAX_FRAME_SIZE_BYTES)
                .setMaxWebSocketMessageSize(MAX_FRAME_SIZE_BYTES * 4);
        configureSslEngineOptions(options, isSsl, OpenSsl.isAvailable(), OpenSsl.unavailabilityCause(), streamName);
        this.httpClientOptions = options;
        this.webSocketClient = createSlotAwareWebSocketClient(vertx, options);
    }

    private WebSocketClient createSlotAwareWebSocketClient(Vertx baseVertx, HttpClientOptions options) {
        if (baseVertx instanceof VertxInternal vertxInternal) {
            AddressResolverGroup<InetSocketAddress> slotResolverGroup =
                    createSlotAwareResolverGroup(vertxInternal.nettyAddressResolverGroup());
            VertxInternal[] ownerHolder = new VertxInternal[1];
            VertxInternal slotAwareVertx = (VertxInternal) Proxy.newProxyInstance(
                    VertxInternal.class.getClassLoader(),
                    new Class<?>[]{VertxInternal.class},
                    (proxy, method, args) -> {
                        String name = method.getName();
                        if ("nettyAddressResolverGroup".equals(name) && (args == null || args.length == 0)) {
                            return slotResolverGroup;
                        }
                        Object res = invokeUnwrapping(vertxInternal, method, args);
                        if (res instanceof ContextInternal ctx) {
                            return wrapSlotAwareContext(ctx, ownerHolder[0]);
                        }
                        if (res instanceof PromiseInternal<?> promise) {
                            return wrapSlotAwarePromise(promise, ownerHolder[0]);
                        }
                        return res;
                    }
            );
            ownerHolder[0] = slotAwareVertx;
            CloseFuture closeFuture = new CloseFuture();
            WebSocketClientImpl client = new WebSocketClientImpl(slotAwareVertx, options, closeFuture);
            closeFuture.add(client);
            return client;
        }
        WebSocketClientOptions wsOptions = new WebSocketClientOptions()
                .setSsl(options.isSsl())
                .setTcpNoDelay(options.isTcpNoDelay())
                .setTcpKeepAlive(options.isTcpKeepAlive())
                .setTryUsePerMessageCompression(options.getTryUsePerMessageWebSocketCompression())
                .setMaxFrameSize(options.getMaxWebSocketFrameSize())
                .setMaxMessageSize(options.getMaxWebSocketMessageSize());
        if (options.getSslEngineOptions() != null) {
            wsOptions.setSslEngineOptions(options.getSslEngineOptions());
        }
        return baseVertx.createWebSocketClient(wsOptions);
    }

    private static ContextInternal wrapSlotAwareContext(ContextInternal delegate, VertxInternal ownerProxy) {
        if (Proxy.isProxyClass(delegate.getClass())) {
            return delegate;
        }
        return (ContextInternal) Proxy.newProxyInstance(
                ContextInternal.class.getClassLoader(),
                new Class<?>[]{ContextInternal.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("owner".equals(name) && (args == null || args.length == 0)) {
                        return ownerProxy;
                    }
                    if ("equals".equals(name) && args != null && args.length == 1) {
                        return proxy == args[0] || delegate.equals(args[0]);
                    }
                    if ("hashCode".equals(name) && (args == null || args.length == 0)) {
                        return delegate.hashCode();
                    }
                    Object res = invokeUnwrapping(delegate, method, args);
                    if (res instanceof ContextInternal ctx) {
                        return wrapSlotAwareContext(ctx, ownerProxy);
                    }
                    if (res instanceof PromiseInternal<?> promise) {
                        return wrapSlotAwarePromise(promise, ownerProxy);
                    }
                    return res;
                }
        );
    }

    private static PromiseInternal<?> wrapSlotAwarePromise(PromiseInternal<?> delegate, VertxInternal ownerProxy) {
        if (Proxy.isProxyClass(delegate.getClass())) {
            return delegate;
        }
        return (PromiseInternal<?>) Proxy.newProxyInstance(
                PromiseInternal.class.getClassLoader(),
                new Class<?>[]{PromiseInternal.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("context".equals(name) && (args == null || args.length == 0)) {
                        ContextInternal ctx = delegate.context();
                        return ctx != null ? wrapSlotAwareContext(ctx, ownerProxy) : null;
                    }
                    if ("equals".equals(name) && args != null && args.length == 1) {
                        return proxy == args[0] || delegate.equals(args[0]);
                    }
                    if ("hashCode".equals(name) && (args == null || args.length == 0)) {
                        return delegate.hashCode();
                    }
                    return invokeUnwrapping(delegate, method, args);
                }
        );
    }

    private static Object invokeUnwrapping(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (java.lang.reflect.InvocationTargetException ite) {
            throw ite.getTargetException();
        }
    }

    private AddressResolverGroup<InetSocketAddress> createSlotAwareResolverGroup(
            AddressResolverGroup<InetSocketAddress> fallbackGroup) {
        return new AddressResolverGroup<>() {
            @Override
            protected AddressResolver<InetSocketAddress> newResolver(EventExecutor executor) {
                AddressResolver<InetSocketAddress> fallback = fallbackGroup.getResolver(executor);
                return new AbstractAddressResolver<>(executor, InetSocketAddress.class) {
                    @Override
                    protected boolean doIsResolved(InetSocketAddress address) {
                        return !address.isUnresolved();
                    }

                    @Override
                    protected void doResolve(InetSocketAddress unresolvedAddress, Promise<InetSocketAddress> promise) {
                        String pinnedIp = pendingRemoteIp.get();
                        if (pinnedIp != null && !pinnedIp.isBlank()) {
                            try {
                                promise.setSuccess(new InetSocketAddress(
                                        InetAddress.getByName(pinnedIp), unresolvedAddress.getPort()));
                                return;
                            } catch (Exception e) {
                                promise.setFailure(e);
                                return;
                            }
                        }
                        fallback.resolve(unresolvedAddress, promise);
                    }

                    @Override
                    protected void doResolveAll(InetSocketAddress unresolvedAddress, Promise<List<InetSocketAddress>> promise) {
                        String pinnedIp = pendingRemoteIp.get();
                        if (pinnedIp != null && !pinnedIp.isBlank()) {
                            try {
                                promise.setSuccess(List.of(new InetSocketAddress(
                                        InetAddress.getByName(pinnedIp), unresolvedAddress.getPort())));
                                return;
                            } catch (Exception e) {
                                promise.setFailure(e);
                                return;
                            }
                        }
                        fallback.resolveAll(unresolvedAddress, promise);
                    }
                };
            }
        };
    }

    /**
     * Configures native BoringSSL ({@link OpenSSLEngineOptions}) when available for {@code wss://} streams,
     * logging a {@code WARN} message if falling back to the default JDK JSSE {@code SSLEngine}.
     *
     * @param options             target {@link HttpClientOptions}
     * @param isSsl               whether the WebSocket URL uses TLS ({@code wss://})
     * @param openSslAvailable    whether native OpenSSL/BoringSSL is available
     * @param unavailabilityCause optional cause when native OpenSSL is unavailable
     * @param streamName          stream identifier for warning logs
     * @return {@code true} if native OpenSSL was enabled, {@code false} otherwise
     */
    static boolean configureSslEngineOptions(
            HttpClientOptions options,
            boolean isSsl,
            boolean openSslAvailable,
            Throwable unavailabilityCause,
            String streamName) {
        if (!isSsl) {
            return false;
        }
        if (openSslAvailable) {
            options.setSslEngineOptions(new OpenSSLEngineOptions().setSessionCacheEnabled(true));
            return true;
        }
        String reason = unavailabilityCause != null && unavailabilityCause.getMessage() != null
                ? unavailabilityCause.getMessage()
                : "netty-tcnative native library not loaded";
        Log.warnf("[%s] Native OpenSSL/BoringSSL is unavailable (%s); falling back to default JDK JSSE SSLEngine",
                streamName, reason);
        return false;
    }

    /**
     * Returns the {@link HttpClientOptions} used by this stream.
     *
     * @return configured {@link HttpClientOptions}
     */
    HttpClientOptions getHttpClientOptions() {
        return httpClientOptions;
    }

    /**
     * Configures deterministic sibling-aware edge IPv4 address slot selection ({@code 0} for {@code PRIMARY} and
     * {@code 1} for {@code BACKUP}) via {@link MarketWebSocketRuntime} so redundant legs connect to distinct
     * upstream CDN/exchange edge servers while preserving TLS SNI, X.509 hostname verification, and HTTP {@code Host} headers.
     *
     * @param runtime                shared market WebSocket runtime providing cached IPv4 pool resolution
     * @param slot                   0-based address slot index ({@code >= 0})
     * @param excludedPeerIpSupplier optional supplier returning the IPv4 address currently active or connecting on the sibling leg
     */
    public void setPreferredAddressSlot(
            MarketWebSocketRuntime runtime,
            int slot,
            Supplier<String> excludedPeerIpSupplier) {
        this.addressSlotRuntime = runtime;
        this.preferredAddressSlot = slot;
        this.excludedPeerIpSupplier = excludedPeerIpSupplier;
    }

    /**
     * Returns the IPv4/host address currently connected or actively connecting on this stream, or {@code ""}
     * when disconnected or waiting in a reconnect backoff timer.
     *
     * @return active or pending remote IPv4/host string, or {@code ""}
     */
    public String getActiveOrPendingRemoteIp() {
        ConnectionState currentState = state.get();
        if (currentState == ConnectionState.CONNECTED) {
            String activeIp = extractHostPart(lastRemoteAddress.get());
            if (!activeIp.isEmpty()) {
                return activeIp;
            }
        }
        if (currentState == ConnectionState.CONNECTING) {
            return pendingRemoteIp.get();
        }
        return "";
    }

    /**
     * Registers an optional zero-copy listener invoked on the I/O reader thread with each decoded
     * {@link TimestampedFrame} (used to fan out shared market frames to the Data Collector queue on {@code CPU #0}
     * without duplicating {@code byte[]} allocations or touching Strategy's {@code CPU #2} queue).
     *
     * @param collectorTap consumer receiving each non-heartbeat {@link TimestampedFrame}, or {@code null} to detach
     */
    public void setCollectorTap(Consumer<TimestampedFrame> collectorTap) {
        this.collectorTap = collectorTap;
    }

    /**
     * Enables or disables enqueuing inbound frames into this stream's {@code inboundQueue}.
     * When disabled (e.g. a collector-only lookahead stream or when Strategy is {@code OFF}),
     * {@code inboundQueue} is kept empty and no internal {@code ws-drain-*} thread is spawned.
     *
     * @param enabled {@code true} to enqueue frames into {@code inboundQueue}, {@code false} to bypass and clear it
     */
    public void setInboundQueueEnabled(boolean enabled) {
        this.inboundQueueEnabled.set(enabled);
        if (!enabled) {
            inboundQueue.clear();
        } else if (running.get() && externalWorkerThread == null && drainThread == null) {
            startInternalDrainWorkerIfNeeded();
        }
    }

    /**
     * Unbinds any external worker thread and disables and clears {@code inboundQueue} while keeping
     * the underlying WebSocket connection and {@code collectorTap} running uninterrupted.
     */
    public void unbindExternalWorkerAndDisableQueue() {
        this.inboundQueueEnabled.set(false);
        this.externalWorkerThread = null;
        this.workerParked.set(false);
        inboundQueue.clear();
    }

    /**
     * Registers an optional listener invoked whenever this WebSocket stream closes unexpectedly while running.
     *
     * @param disconnectListener listener receiving the stream telemetry snapshot at the moment of disconnect
     */
    public void setDisconnectListener(WebSocketDisconnectListener disconnectListener) {
        this.disconnectListener = disconnectListener;
    }

    /**
     * Registers an optional listener invoked when the remote WebSocket server cleanly terminates
     * the stream with {@code closeCode == 1000} (Normal Closure) or when the stream disconnects
     * while {@link #setShouldReconnectSupplier(BooleanSupplier)} evaluates to {@code false}.
     *
     * @param normalCloseListener listener receiving the final stream telemetry snapshot upon terminal closure
     */
    public void setNormalCloseListener(Consumer<WebSocketStreamTelemetry> normalCloseListener) {
        this.normalCloseListener = normalCloseListener;
    }

    /**
     * Registers an optional predicate evaluated upon WebSocket disconnection or reconnect timer expiration
     * to determine whether the stream should attempt to reconnect.
     *
     * @param shouldReconnectSupplier supplier returning {@code true} if reconnection is allowed, or {@code false} to finalize
     */
    public void setShouldReconnectSupplier(BooleanSupplier shouldReconnectSupplier) {
        this.shouldReconnectSupplier = shouldReconnectSupplier != null ? shouldReconnectSupplier : () -> true;
    }

    /**
     * Configures an explicit thread name and {@link Thread#MAX_PRIORITY} for the EventLoop thread servicing this stream.
     *
     * @param dedicatedIoThreadName thread name to assign to the EventLoop thread
     */
    public void setDedicatedIoThreadName(String dedicatedIoThreadName) {
        this.dedicatedIoThreadName = dedicatedIoThreadName;
    }

    /**
     * Returns the current consecutive reconnect attempt counter (reset to {@code 0} upon receiving a valid frame).
     *
     * @return consecutive reconnect attempts since last valid inbound frame
     */
    int getConsecutiveReconnectAttempts() {
        return consecutiveReconnectAttempts.get();
    }

    /**
     * Computes the exponential backoff reconnect delay in milliseconds for the given 1-based attempt number,
     * starting at {@link #INITIAL_FAST_RECONNECT_DELAY_MS} ({@code 50 ms}) and capped at {@code maxDelayMs}.
     *
     * @param attempt    1-based consecutive reconnect attempt counter
     * @param maxDelayMs maximum configured reconnect delay in milliseconds
     * @return reconnect delay in milliseconds
     */
    static long computeNextReconnectDelayMs(int attempt, long maxDelayMs) {
        int shift = Math.clamp(attempt - 1, 0, 16);
        long backoffMs = INITIAL_FAST_RECONNECT_DELAY_MS << shift;
        return Math.min(maxDelayMs, backoffMs);
    }

    /**
     * Computes the maximum allowed inbound silence duration in milliseconds before the read-idle/pong watchdog
     * forces a reconnect for a stream with the given {@code pingIntervalMs}.
     *
     * @param pingIntervalMs configured ping interval in milliseconds ({@code > 0})
     * @return read-idle watchdog threshold in milliseconds
     */
    static long computeReadIdleTimeoutMs(long pingIntervalMs) {
        return Math.max(MIN_READ_IDLE_TIMEOUT_MS, Math.multiplyExact(pingIntervalMs, 3L));
    }

    /**
     * Executes a bounded user-space {@link Thread#onSpinWait()} loop for up to {@link #HOT_SPIN_WINDOW_NS}
     * while {@code abortSpinCondition} evaluates to {@code false}, checking {@link System#nanoTime()} only
     * every {@code 64} iterations to avoid clock-read overhead.
     *
     * @param abortSpinCondition supplier returning {@code true} as soon as work arrives or shutdown is requested
     */
    public static void spinWaitBeforePark(BooleanSupplier abortSpinCondition) {
        long deadlineNs = System.nanoTime() + HOT_SPIN_WINDOW_NS;
        int iter = 0;
        while (!abortSpinCondition.getAsBoolean()) {
            Thread.onSpinWait();
            if ((++iter & (SPIN_CLOCK_CHECK_STRIDE - 1)) == 0 && System.nanoTime() - deadlineNs >= 0L) {
                return;
            }
        }
    }

    /**
     * Binds this stream's lock-free {@code inboundQueue} wakeups to an externally managed dedicated platform thread.
     *
     * @param workerThread dedicated platform thread responsible for draining this stream
     */
    public void bindExternalWorker(Thread workerThread) {
        this.externalWorkerThread = workerThread;
        this.workerParked.set(true);
        Thread internal = this.drainThread;
        this.drainThread = null;
        if (internal != null) {
            LockSupport.unpark(internal);
        }
    }

    /**
     * Starts the WebSocket connection, automatic reconnection loop, and (unless bound to an external worker via
     * {@link #bindExternalWorker(Thread)} or operating with {@code inboundQueueEnabled == false}) a dedicated
     * high-priority platform drain thread.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            state.set(ConnectionState.CONNECTING);
            if (externalWorkerThread == null && inboundQueueEnabled.get()) {
                startInternalDrainWorkerIfNeeded();
            }
            connect();
        }
    }

    private synchronized void startInternalDrainWorkerIfNeeded() {
        if (running.get() && externalWorkerThread == null && inboundQueueEnabled.get() && drainThread == null) {
            this.drainThread = Thread.ofPlatform()
                    .daemon(true)
                    .priority(Thread.MAX_PRIORITY)
                    .name("ws-drain-" + streamName)
                    .start(this::drainLoop);
        }
    }

    /**
     * Stops the WebSocket stream, wakes and terminates the drain worker, cancels timers,
     * and closes the underlying connection and HTTP client.
     */
    public void stop() {
        running.set(false);
        pendingRemoteIp.set("");
        inboundQueue.clear();
        Thread worker = this.drainThread;
        this.drainThread = null;
        if (worker != null) {
            workerParked.set(false);
            LockSupport.unpark(worker);
        }
        Thread ext = this.externalWorkerThread;
        if (ext != null) {
            workerParked.set(false);
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
        webSocketClient.close();
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
     * into {@code scratchBatch}, updates queue wait lag and processing time telemetry, and dispatches the
     * frames to the configured {@link RawWebSocketBatchListener} or {@link RawWebSocketListener}.
     *
     * @param scratchBatch reusable pre-allocated list owned by the calling worker thread
     * @return number of frames drained and processed in this pass
     */
    public int drainAvailableBatch(List<TimestampedFrame> scratchBatch) {
        scratchBatch.clear();
        int drainedCount = inboundQueue.drain(scratchBatch::add, DEFAULT_BATCH_LIMIT);
        if (drainedCount == 0) {
            return 0;
        }
        if (workerParked.getOpaque()) {
            workerParked.set(false);
        }

        TimestampedFrame first = scratchBatch.getFirst();
        long drainStartMs = System.currentTimeMillis();
        long oldestLagMs = Math.max(0L, drainStartMs - first.recvTs());
        lastQueueLagMs.set(oldestLagMs);
        if (oldestLagMs > maxQueueLagMs.getOpaque()) {
            updateMaxLong(maxQueueLagMs, oldestLagMs);
        }

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
                    messageListener.onMessageBytes(frame);
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
            if (elapsedUs > maxProcessTimeUs.getOpaque()) {
                updateMaxLong(maxProcessTimeUs, elapsedUs);
            }
            scratchBatch.clear();
        }
        return drainedCount;
    }

    /**
     * Marks the bound external worker as parked ({@code workerParked = true}) immediately prior to
     * {@link LockSupport#parkNanos(Object, long)} and returns {@code true} if frames arrived concurrently
     * during the transition (requiring an immediate drain rather than parking).
     *
     * @return {@code true} if {@code inboundQueue} is non-empty after arming {@code workerParked}
     */
    public boolean prepareToParkAndCheckQueuedFrames() {
        workerParked.set(true);
        if (!inboundQueue.isEmpty()) {
            workerParked.set(false);
            return true;
        }
        return false;
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
     * Returns the resolved remote endpoint address ({@code IP:port}) of the current or most recent connection, or {@code ""}.
     *
     * @return remote socket address string
     */
    public String getRemoteAddress() {
        return lastRemoteAddress.get();
    }

    /**
     * Returns an immutable point-in-time snapshot of this stream's queue depth, latency, and connection telemetry.
     *
     * @return {@link WebSocketStreamTelemetry} snapshot
     */
    public WebSocketStreamTelemetry getTelemetrySnapshot() {
        return buildTelemetrySnapshot(lastCloseCode.get(), lastCloseReason.get());
    }

    private WebSocketStreamTelemetry buildTelemetrySnapshot(int closeCode, String closeReason) {
        return new WebSocketStreamTelemetry(
                streamName,
                state.get(),
                lastRemoteAddress.get(),
                Math.max(0, inboundQueue.size()),
                maxQueueDepth.get(),
                lastQueueLagMs.get(),
                maxQueueLagMs.get(),
                lastProcessTimeUs.get(),
                maxProcessTimeUs.get(),
                totalFramesReceived.get(),
                coalescedFramesCount.get(),
                reconnectCount.get(),
                lastMessageTimestampMs.get(),
                closeCode,
                closeReason
        );
    }

    private void connect() {
        if (!running.get()) {
            return;
        }

        try {
            URI uri = URI.create(wsUrl);
            String host = uri.getHost();
            boolean isSsl = "wss".equalsIgnoreCase(uri.getScheme());
            int port = uri.getPort() == -1 ? (isSsl ? 443 : 80) : uri.getPort();
            String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
            if (uri.getRawQuery() != null) {
                path += "?" + uri.getRawQuery();
            }

            WebSocketConnectOptions connectOptions = new WebSocketConnectOptions()
                    .setHost(host)
                    .setPort(port)
                    .setSsl(isSsl)
                    .setURI(path);

            MarketWebSocketRuntime slotRuntime = this.addressSlotRuntime;
            int slot = this.preferredAddressSlot;
            int attempt = consecutiveReconnectAttempts.get();
            String failedIp = lastFailedIp.get();
            boolean forceRefresh = attempt > 0 || !failedIp.isEmpty();

            Future<Void> pinTargetFuture = (slotRuntime != null && slot >= 0)
                    ? slotRuntime.resolveIpv4Pool(host, forceRefresh)
                            .map(pool -> {
                                Supplier<String> peerSupplier = this.excludedPeerIpSupplier;
                                String peerIp = peerSupplier != null ? peerSupplier.get() : null;
                                String selectedIp = MarketWebSocketRuntime.selectIpv4ForSlot(
                                        pool, slot, attempt, peerIp, failedIp);
                                pendingRemoteIp.set(selectedIp != null ? selectedIp : "");
                                return null;
                            })
                    : Future.succeededFuture();

            state.set(ConnectionState.CONNECTING);
            pinTargetFuture
                    .compose(ignored -> {
                        if (!running.get()) {
                            pendingRemoteIp.set("");
                            return Future.failedFuture("Stream stopped before connect");
                        }
                        return webSocketClient.connect(connectOptions);
                    })
                    .onSuccess(this::onWebSocketConnected)
                    .onFailure(err -> {
                        if (!running.get()) {
                            pendingRemoteIp.set("");
                            return;
                        }
                        recordFailedIp();
                        lastCloseCode.set(-1);
                        lastCloseReason.set(err.getMessage() != null ? err.getMessage() : "connect_failed");
                        Log.errorf("[%s] WebSocket connect failed (%s): %s", streamName, wsUrl, err.getMessage());
                        scheduleReconnect();
                    });
        } catch (Exception e) {
            recordFailedIp();
            Log.errorf("[%s] Error initiating WebSocket connection (%s): %s", streamName, wsUrl, e.getMessage());
            scheduleReconnect();
        }
    }

    private void onWebSocketConnected(WebSocket ws) {
        if (!running.get() || ws.isClosed()) {
            pendingRemoteIp.set("");
            if (!ws.isClosed()) {
                ws.close();
            }
            return;
        }
        Thread currentThread = Thread.currentThread();
        applyDedicatedIoThreadTuning(currentThread);
        this.ioReaderThread = currentThread;
        this.currentWebSocket = ws;

        long connectedNowMs = System.currentTimeMillis();
        this.lastInboundActivityMs.set(connectedNowMs);
        this.lastCloseCode.set(0);
        this.lastCloseReason.set("");

        String remote = formatRemoteAddress(ws.remoteAddress());
        this.lastRemoteAddress.set(remote);
        this.pendingRemoteIp.set(extractHostPart(remote));
        this.state.set(ConnectionState.CONNECTED);
        Log.infof("[%s] WebSocket connected to %s (remote=%s) on thread %s (reconnectCount=%d)",
                streamName, wsUrl, remote.isEmpty() ? "unknown" : remote, currentThread.getName(), reconnectCount.get());

        final Buffer[] continuationBuffer = new Buffer[1];
        final AtomicBoolean disconnectHandled = new AtomicBoolean(false);

        ws.frameHandler(frame -> {
            this.ioReaderThread = Thread.currentThread();
            long recvTs = System.currentTimeMillis();
            lastMessageTimestampMs.set(recvTs);
            lastInboundActivityMs.set(recvTs);

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
            lastInboundActivityMs.set(nowMs);
            if (Log.isTraceEnabled()) {
                Log.tracef("[%s] Received WebSocket PONG at %d ms", streamName, nowMs);
            }
        });

        ws.closeHandler(v -> {
            if (!disconnectHandled.compareAndSet(false, true)) {
                return;
            }
            short closeCode = ws.closeStatusCode() != null ? ws.closeStatusCode() : -1;
            String closeReason = ws.closeReason() != null && !ws.closeReason().isEmpty() ? ws.closeReason() : "none";
            lastCloseCode.set(closeCode);
            lastCloseReason.set(closeReason);

            if (running.get()) {
                String remoteLabel = lastRemoteAddress.get().isEmpty() ? "unknown" : lastRemoteAddress.get();
                if (this.normalCloseListener != null && (closeCode == 1000 || !shouldReconnect())) {
                    if (closeCode == 1000) {
                        Log.infof("[%s] WebSocket connection closed normally with code=1000 (remote=%s, reason=%s, totalFrames=%d); finalizing stream without reconnect",
                                streamName, remoteLabel, closeReason, totalFramesReceived.get());
                    } else {
                        Log.infof("[%s] WebSocket connection closed after market interval end (remote=%s, code=%d, reason=%s, totalFrames=%d); skipping reconnect and finalizing stream",
                                streamName, remoteLabel, closeCode, closeReason, totalFramesReceived.get());
                    }
                    triggerTerminalClose(closeCode, closeReason);
                    return;
                }
                long nextDelayMs = computeNextReconnectDelayMs(
                        consecutiveReconnectAttempts.get() + 1, reconnectDelayMs);
                WebSocketStreamTelemetry eventSnapshot = buildTelemetrySnapshot(closeCode, closeReason);
                Log.warnf("[%s] WebSocket connection closed (remote=%s, code=%d, reason=%s, queueDepth=%d, maxQueueDepth=%d, lastQueueLagMs=%d, maxQueueLagMs=%d, lastProcessTimeUs=%d, maxProcessTimeUs=%d, totalFrames=%d, coalescedFrames=%d, reconnectCount=%d); scheduling reconnect in %d ms",
                        streamName, remoteLabel, closeCode, closeReason,
                        eventSnapshot.queueDepth(), eventSnapshot.maxQueueDepth(),
                        eventSnapshot.lastQueueLagMs(), eventSnapshot.maxQueueLagMs(),
                        eventSnapshot.lastProcessTimeUs(), eventSnapshot.maxProcessTimeUs(),
                        eventSnapshot.totalFramesReceived(), eventSnapshot.coalescedFramesCount(),
                        eventSnapshot.reconnectCount(), nextDelayMs);
                if (this.currentWebSocket == ws) {
                    notifyDisconnectListener(eventSnapshot);
                }
            } else if (Log.isDebugEnabled()) {
                Log.debugf("[%s] WebSocket connection closed cleanly on stop (code=%d)", streamName, closeCode);
            }
            onConnectionClosed(ws);
        });

        ws.exceptionHandler(err -> {
            if (!disconnectHandled.compareAndSet(false, true)) {
                return;
            }
            String msg = err.getMessage() != null && !err.getMessage().isEmpty() ? err.getMessage() : "exception";
            int code = -1;
            lastCloseCode.set(code);
            lastCloseReason.set(msg);

            if (running.get() && this.currentWebSocket == ws) {
                String remoteLabel = lastRemoteAddress.get().isEmpty() ? "unknown" : lastRemoteAddress.get();
                if (this.normalCloseListener != null && !shouldReconnect()) {
                    Log.infof("[%s] WebSocket error occurred after market interval end (remote=%s, reason=%s); skipping reconnect and finalizing stream",
                            streamName, remoteLabel, msg);
                    triggerTerminalClose(code, msg);
                    return;
                }
                long nextDelayMs = computeNextReconnectDelayMs(
                        consecutiveReconnectAttempts.get() + 1, reconnectDelayMs);
                WebSocketStreamTelemetry eventSnapshot = buildTelemetrySnapshot(code, msg);
                Log.warnf("[%s] WebSocket connection aborted (remote=%s, code=%d, reason=%s, queueDepth=%d, maxQueueDepth=%d, lastQueueLagMs=%d, maxQueueLagMs=%d, lastProcessTimeUs=%d, maxProcessTimeUs=%d, totalFrames=%d, coalescedFrames=%d, reconnectCount=%d); scheduling reconnect in %d ms",
                        streamName, remoteLabel, code, msg,
                        eventSnapshot.queueDepth(), eventSnapshot.maxQueueDepth(),
                        eventSnapshot.lastQueueLagMs(), eventSnapshot.maxQueueLagMs(),
                        eventSnapshot.lastProcessTimeUs(), eventSnapshot.maxProcessTimeUs(),
                        eventSnapshot.totalFramesReceived(), eventSnapshot.coalescedFramesCount(),
                        eventSnapshot.reconnectCount(), nextDelayMs);
                notifyDisconnectListener(eventSnapshot);
            }
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

        startPingTimer(ws, disconnectHandled);
    }

    static String formatRemoteAddress(SocketAddress addr) {
        if (addr == null) {
            return "";
        }
        String ip = addr.hostAddress();
        if (ip != null && !ip.isBlank()) {
            return ip + ":" + addr.port();
        }
        String host = addr.host();
        if (host != null && !host.isBlank()) {
            return host + ":" + addr.port();
        }
        return addr.toString();
    }

    private void notifyDisconnectListener(WebSocketStreamTelemetry snapshot) {
        WebSocketDisconnectListener listener = this.disconnectListener;
        if (listener != null && snapshot != null) {
            try {
                listener.onUnexpectedDisconnect(snapshot);
            } catch (Exception e) {
                Log.warnf("[%s] Exception in WebSocketDisconnectListener: %s", streamName, e.getMessage());
            }
        }
    }

    private void notifyNormalCloseListener(WebSocketStreamTelemetry snapshot) {
        Consumer<WebSocketStreamTelemetry> listener = this.normalCloseListener;
        if (listener != null && snapshot != null) {
            try {
                listener.accept(snapshot);
            } catch (Exception e) {
                Log.warnf("[%s] Exception in normalCloseListener: %s", streamName, e.getMessage());
            }
        }
    }

    private void closeHttpClientQuietly() {
        try {
            webSocketClient.close();
        } catch (Exception ignored) {
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
        Consumer<TimestampedFrame> tap = this.collectorTap;
        boolean enqueuePrimary = inboundQueueEnabled.getOpaque() && (messageListener != null || batchListener != null);
        if (tap != null || enqueuePrimary) {
            if (consecutiveReconnectAttempts.getOpaque() != 0) {
                consecutiveReconnectAttempts.set(0);
                lastFailedIp.set("");
            }
            byte[] rawUtf8Bytes = payloadBuffer.getBytes();
            if (Log.isTraceEnabled()) {
                Log.tracef("[%s] Received WebSocket frame at %d ms (%d bytes)", streamName, recvTs, rawUtf8Bytes.length);
            }
            TimestampedFrame frame = new TimestampedFrame(rawUtf8Bytes, recvTs);
            totalFramesReceived.incrementAndGet();
            if (tap != null) {
                tap.accept(frame);
            }
            if (enqueuePrimary) {
                inboundQueue.offer(frame);
                int depth = inboundQueue.size();
                if (depth > maxQueueDepth.getOpaque()) {
                    updateMaxInt(maxQueueDepth, depth);
                }

                if (workerParked.getOpaque() && workerParked.compareAndSet(true, false)) {
                    Thread target = this.externalWorkerThread != null ? this.externalWorkerThread : this.drainThread;
                    if (target != null) {
                        LockSupport.unpark(target);
                    }
                }
            }
        }
    }

    private void startPingTimer(WebSocket activeWs, AtomicBoolean disconnectHandled) {
        cancelPingTimer();
        if (pingIntervalMs > 0L && running.get()) {
            final long idleTimeoutMs = computeReadIdleTimeoutMs(pingIntervalMs);
            pingTimerId = vertx.setPeriodic(pingIntervalMs, id -> {
                WebSocket ws = this.currentWebSocket;
                if (ws != activeWs || ws == null || ws.isClosed() || !running.get()) {
                    return;
                }
                long nowMs = System.currentTimeMillis();
                long lastActivity = lastInboundActivityMs.get();
                long silenceMs = lastActivity > 0L ? Math.max(0L, nowMs - lastActivity) : 0L;
                if (silenceMs > idleTimeoutMs) {
                    if (disconnectHandled.compareAndSet(false, true)) {
                        String reason = "Read-idle watchdog timeout (" + silenceMs + " ms without inbound frame or PONG)";
                        lastCloseCode.set(-1);
                        lastCloseReason.set(reason);
                        String remoteLabel = lastRemoteAddress.get().isEmpty() ? "unknown" : lastRemoteAddress.get();
                        if (this.normalCloseListener != null && !shouldReconnect()) {
                            Log.infof("[%s] WebSocket read-idle watchdog expired after market end (remote=%s, silenceMs=%d); finalizing stream",
                                    streamName, remoteLabel, silenceMs);
                            triggerTerminalClose(-1, reason);
                            try {
                                ws.close();
                            } catch (Exception ignored) {
                            }
                            return;
                        }
                        long nextDelayMs = computeNextReconnectDelayMs(
                                consecutiveReconnectAttempts.get() + 1, reconnectDelayMs);
                        WebSocketStreamTelemetry snapshot = buildTelemetrySnapshot(-1, reason);
                        Log.warnf("[%s] WebSocket read-idle watchdog triggered (remote=%s, silenceMs=%d, timeoutMs=%d, reconnectCount=%d); forcing reconnect in %d ms",
                                streamName, remoteLabel, silenceMs, idleTimeoutMs, snapshot.reconnectCount(), nextDelayMs);
                        notifyDisconnectListener(snapshot);
                        onConnectionClosed(ws);
                        try {
                            ws.close();
                        } catch (Exception ignored) {
                        }
                    }
                    return;
                }

                if (textPingPayload != null && !textPingPayload.isBlank()) {
                    if (Log.isTraceEnabled()) {
                        Log.tracef("[%s] Sending WebSocket text ping: %s", streamName, textPingPayload);
                    }
                    ws.writeTextMessage(textPingPayload);
                } else {
                    if (Log.isTraceEnabled()) {
                        Log.tracef("[%s] Sending WebSocket protocol PING frame", streamName);
                    }
                    ws.writePing(PROTOCOL_PING_BUFFER);
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

    private boolean shouldReconnect() {
        BooleanSupplier supplier = this.shouldReconnectSupplier;
        if (supplier == null) {
            return true;
        }
        try {
            return supplier.getAsBoolean();
        } catch (Exception e) {
            Log.warnf("[%s] Exception in shouldReconnectSupplier: %s", streamName, e.getMessage());
            return true;
        }
    }

    private void triggerTerminalClose(int closeCode, String closeReason) {
        if (!normalCloseTriggered.compareAndSet(false, true)) {
            return;
        }
        running.set(false);
        pendingRemoteIp.set("");
        cancelPingTimer();
        this.currentWebSocket = null;
        state.set(ConnectionState.DISCONNECTED);
        Thread worker = this.drainThread;
        if (worker != null && externalWorkerThread == null) {
            workerParked.set(false);
            LockSupport.unpark(worker);
        } else {
            completeTerminalCloseDrain(closeCode, closeReason);
        }
    }

    private void completeTerminalCloseDrain(int closeCode, String closeReason) {
        if (!terminalCloseCompleted.compareAndSet(false, true)) {
            return;
        }
        List<TimestampedFrame> scratch = new ArrayList<>(64);
        while (!inboundQueue.isEmpty()) {
            drainAvailableBatch(scratch);
        }
        notifyNormalCloseListener(buildTelemetrySnapshot(closeCode, closeReason));
        closeHttpClientQuietly();
    }

    private void onConnectionClosed(WebSocket closedWs) {
        if (this.currentWebSocket == closedWs) {
            this.currentWebSocket = null;
            recordFailedIp();
            cancelPingTimer();
            scheduleReconnect();
        }
    }

    private void recordFailedIp() {
        String failed = pendingRemoteIp.getAndSet("");
        if (failed == null || failed.isEmpty()) {
            failed = extractHostPart(lastRemoteAddress.get());
        }
        if (failed != null && !failed.isEmpty()) {
            lastFailedIp.set(failed);
        }
    }

    private static String extractHostPart(String remoteAddr) {
        if (remoteAddr == null || remoteAddr.isEmpty()) {
            return "";
        }
        int lastColon = remoteAddr.lastIndexOf(':');
        if (lastColon > 0 && remoteAddr.indexOf(':') == lastColon) {
            return remoteAddr.substring(0, lastColon);
        }
        return remoteAddr;
    }

    private void scheduleReconnect() {
        if (!running.get()) {
            state.set(ConnectionState.DISCONNECTED);
            return;
        }
        if (this.normalCloseListener != null && !shouldReconnect()) {
            Log.infof("[%s] Skipping WebSocket reconnect because market interval has ended; finalizing stream", streamName);
            triggerTerminalClose(lastCloseCode.get(), lastCloseReason.get());
            return;
        }
        state.set(ConnectionState.RECONNECTING);
        reconnectCount.incrementAndGet();
        int attempt = consecutiveReconnectAttempts.incrementAndGet();
        long delayMs = computeNextReconnectDelayMs(attempt, reconnectDelayMs);
        vertx.setTimer(delayMs, timerId -> {
            if (running.get()) {
                if (this.normalCloseListener != null && !shouldReconnect()) {
                    Log.infof("[%s] Cancelling scheduled WebSocket reconnect because market interval has ended; finalizing stream", streamName);
                    triggerTerminalClose(lastCloseCode.get(), lastCloseReason.get());
                    return;
                }
                connect();
            }
        });
    }

    private void drainLoop() {
        List<TimestampedFrame> scratchBatch = new ArrayList<>(64);
        while (running.get() && externalWorkerThread == null && inboundQueueEnabled.get()) {
            int drained = drainAvailableBatch(scratchBatch);
            if (drained > 0) {
                continue;
            }
            spinWaitBeforePark(() -> !running.get() || externalWorkerThread != null || !inboundQueueEnabled.get() || !inboundQueue.isEmpty());
            if (!running.get() || externalWorkerThread != null || !inboundQueueEnabled.get() || !inboundQueue.isEmpty()) {
                continue;
            }
            if (prepareToParkAndCheckQueuedFrames()) {
                continue;
            }
            if (running.get() && externalWorkerThread == null && inboundQueueEnabled.get()) {
                LockSupport.parkNanos(this, IDLE_PARK_TIMEOUT_NS);
            }
            workerParked.set(false);
        }
        this.drainThread = null;
        if (normalCloseTriggered.get()) {
            completeTerminalCloseDrain(lastCloseCode.get(), lastCloseReason.get());
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
