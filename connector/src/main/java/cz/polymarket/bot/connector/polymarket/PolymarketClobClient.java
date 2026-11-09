package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.polymarket.auth.PolymarketCredentials;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketBatchListener;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketDisconnectListener;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import io.netty.handler.ssl.OpenSsl;
import io.quarkus.logging.Log;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Resilient WebSocket client for Polymarket CLOB ({@code wss://ws-subscriptions-clob.polymarket.com/ws/market}
 * and {@code wss://ws-subscriptions-clob.polymarket.com/ws/user}).
 * <p>
 * Isolates high-frequency market orderbook streams onto a dedicated single-thread Netty EventLoop
 * ({@code clob-ws-io-reader}) so that TLS decryption and socket draining for orderbook bursts never contend
 * with the HTTP server, PolyBolt streams, or the authenticated User Channel stream.
 */
@ApplicationScoped
public class PolymarketClobClient {

    /**
     * Connection state map key for the authenticated Polymarket CLOB User Channel WebSocket stream.
     */
    public static final String USER_STREAM_KEY = "USER";

    /**
     * Dedicated thread name for the isolated Netty EventLoop servicing CLOB market orderbook streams.
     */
    public static final String DEDICATED_MARKET_IO_THREAD_NAME = "clob-ws-io-reader";

    private final Vertx sharedVertx;
    private final Vertx clobMarketVertx;
    private final String wsUrl;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private final Map<String, ReconnectingWebSocketStream> marketStreams = new ConcurrentHashMap<>();
    private volatile ReconnectingWebSocketStream userStream;
    private volatile WebSocketDisconnectListener disconnectListener;

    // Cumulative telemetry preserved across 15m market stream rotations
    private long historicalMarketReconnects = 0L;
    private int lastMarketCloseCode = 0;
    private String lastMarketCloseReason = "";
    private int historicalMaxQueueDepth = 0;
    private long historicalMaxQueueLagMs = 0L;
    private long historicalMaxProcessTimeUs = 0L;

    /**
     * Constructs the Polymarket CLOB WebSocket client with a dedicated 1-thread Vert.x instance
     * exclusively for market orderbook streams.
     *
     * @param vertx            shared Quarkus Vert.x runtime instance (used by the low-rate User stream)
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
        this.sharedVertx = vertx;
        this.clobMarketVertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(1)
                .setPreferNativeTransport(true));
        if (!this.clobMarketVertx.isNativeTransportEnabled()) {
            Log.warnf("Native Linux Epoll transport is unavailable on clobMarketVertx (os=%s); falling back to JDK NIO EventLoop",
                    System.getProperty("os.name", "unknown"));
        } else {
            Log.infof("Initialized dedicated clobMarketVertx with native Linux Epoll transport (openSslAvailable=%b)",
                    OpenSsl.isAvailable());
        }
        this.wsUrl = wsUrl;
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    @PreDestroy
    void destroy() {
        stopAll();
        try {
            clobMarketVertx.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Returns whether native Linux Epoll transport is active on the dedicated CLOB market Vert.x instance.
     *
     * @return {@code true} if native transport is enabled
     */
    public boolean isNativeTransportEnabled() {
        return clobMarketVertx.isNativeTransportEnabled();
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
     * Registers a listener notified whenever any active CLOB market or user stream disconnects unexpectedly.
     *
     * @param disconnectListener listener invoked with the stream telemetry snapshot at the moment of disconnect
     */
    public synchronized void setDisconnectListener(WebSocketDisconnectListener disconnectListener) {
        this.disconnectListener = disconnectListener;
        for (ReconnectingWebSocketStream stream : marketStreams.values()) {
            stream.setDisconnectListener(disconnectListener);
        }
        ReconnectingWebSocketStream currentUser = this.userStream;
        if (currentUser != null) {
            currentUser.setDisconnectListener(disconnectListener);
        }
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
                clobMarketVertx,
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
        stream.setDedicatedIoThreadName(DEDICATED_MARKET_IO_THREAD_NAME);
        stream.setDisconnectListener(this.disconnectListener);
        stream.seedHistoricalTelemetry(
                historicalMarketReconnects,
                lastMarketCloseCode,
                lastMarketCloseReason,
                historicalMaxQueueDepth,
                historicalMaxQueueLagMs,
                historicalMaxProcessTimeUs
        );
        marketStreams.put(marketSlug, stream);
        Log.infof("Opening Polymarket CLOB WebSocket stream for market %s (tokens=%d)", marketSlug, tokenIds.size());
        stream.start();
    }

    /**
     * Opens a dedicated CLOB WebSocket connection for a single market with batch-draining listener support,
     * optionally bound to an external dedicated worker thread (e.g. {@code clob-book-worker}).
     *
     * @param marketSlug     Polymarket market slug
     * @param tokenIds       CLOB asset token IDs belonging to the market
     * @param batchListener  listener invoked with drained chronological frame batches
     * @param externalWorker optional dedicated platform worker thread to unpark on frame arrival
     */
    public synchronized void openMarketStreamBatch(
            String marketSlug,
            List<String> tokenIds,
            RawWebSocketBatchListener batchListener,
            Thread externalWorker) {
        if (marketSlug == null || marketSlug.isBlank() || tokenIds == null || tokenIds.isEmpty()) {
            return;
        }
        if (marketStreams.containsKey(marketSlug)) {
            return;
        }
        String subPayload = buildSubscriptionPayload(tokenIds);
        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                clobMarketVertx,
                "POLYMARKET-CLOB-" + marketSlug,
                wsUrl,
                () -> List.of(subPayload),
                null,
                batchListener,
                reconnectDelayMs,
                pingIntervalMs,
                "PING"
        );
        stream.setDedicatedIoThreadName(DEDICATED_MARKET_IO_THREAD_NAME);
        stream.setDisconnectListener(this.disconnectListener);
        stream.seedHistoricalTelemetry(
                historicalMarketReconnects,
                lastMarketCloseCode,
                lastMarketCloseReason,
                historicalMaxQueueDepth,
                historicalMaxQueueLagMs,
                historicalMaxProcessTimeUs
        );
        if (externalWorker != null) {
            stream.bindExternalWorker(externalWorker);
        }
        marketStreams.put(marketSlug, stream);
        Log.infof("Opening Polymarket CLOB WebSocket stream for market %s (tokens=%d, batchMode=true)", marketSlug, tokenIds.size());
        stream.start();
    }

    /**
     * Disconnects and removes the CLOB WebSocket stream for a market that has finished and resolved,
     * preserving its high-water mark and reconnect telemetry for subsequent market rotations.
     *
     * @param marketSlug Polymarket market slug to disconnect
     */
    public synchronized void closeMarketStream(String marketSlug) {
        if (marketSlug == null) {
            return;
        }
        ReconnectingWebSocketStream stream = marketStreams.remove(marketSlug);
        if (stream != null) {
            WebSocketStreamTelemetry snap = stream.getTelemetrySnapshot();
            historicalMarketReconnects = Math.max(historicalMarketReconnects, snap.reconnectCount());
            if (snap.lastCloseCode() != 0 && (snap.lastCloseCode() != -1 || lastMarketCloseCode == 0)) {
                lastMarketCloseCode = snap.lastCloseCode();
            }
            if (snap.lastCloseReason() != null && !snap.lastCloseReason().isEmpty()
                    && (!"none".equals(snap.lastCloseReason()) || lastMarketCloseReason.isEmpty())) {
                lastMarketCloseReason = snap.lastCloseReason();
            }
            historicalMaxQueueDepth = Math.max(historicalMaxQueueDepth, snap.maxQueueDepth());
            historicalMaxQueueLagMs = Math.max(historicalMaxQueueLagMs, snap.maxQueueLagMs());
            historicalMaxProcessTimeUs = Math.max(historicalMaxProcessTimeUs, snap.maxProcessTimeUs());
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
        openUserStream(credentials, listener, null);
    }

    /**
     * Opens an authenticated CLOB User Channel WebSocket stream ({@code /ws/user}) bound to an optional
     * external worker thread (e.g. {@code order-execution-io-worker}).
     *
     * @param credentials    Polymarket Level 2 API credentials
     * @param listener       listener receiving raw JSON frames and receipt timestamps
     * @param externalWorker optional dedicated platform worker thread responsible for draining user events
     */
    public synchronized void openUserStream(
            PolymarketCredentials credentials,
            RawWebSocketListener listener,
            Thread externalWorker) {
        if (credentials == null || !credentials.hasL2Credentials()) {
            throw new IllegalArgumentException("Valid Polymarket L2 credentials are required for the CLOB user stream");
        }
        closeUserStream();
        String userWsUrl = resolveUserWsUrl(this.wsUrl);
        String subPayload = buildUserSubscriptionPayload(credentials);
        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                sharedVertx,
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
        stream.setDisconnectListener(this.disconnectListener);
        if (externalWorker != null) {
            stream.bindExternalWorker(externalWorker);
        }
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
     * Drains all currently queued frames across active market CLOB streams into {@code scratchBatch}.
     * Called on the dedicated {@code clob-book-worker} thread.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return total number of frames drained
     */
    public int drainMarketStreams(List<TimestampedFrame> scratchBatch) {
        int total = 0;
        for (ReconnectingWebSocketStream stream : marketStreams.values()) {
            total += stream.drainAvailableBatch(scratchBatch);
        }
        return total;
    }

    /**
     * Drains all currently queued frames from the authenticated CLOB User stream into {@code scratchBatch}.
     * Called on the dedicated {@code order-execution-io-worker} thread.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return number of frames drained
     */
    public int drainUserStream(List<TimestampedFrame> scratchBatch) {
        ReconnectingWebSocketStream current = this.userStream;
        return current != null ? current.drainAvailableBatch(scratchBatch) : 0;
    }

    /**
     * Returns whether any active market CLOB stream currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if any market stream queue is non-empty
     */
    public boolean hasQueuedMarketFrames() {
        for (ReconnectingWebSocketStream stream : marketStreams.values()) {
            if (stream.hasQueuedFrames()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether the CLOB User stream currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if the user stream queue is non-empty
     */
    public boolean hasQueuedUserFrames() {
        ReconnectingWebSocketStream current = this.userStream;
        return current != null && current.hasQueuedFrames();
    }

    /**
     * Records additional fast-forwarded frames for the specified market slug.
     *
     * @param marketSlug market slug
     * @param delta      number of additional fast-forwarded frames
     */
    public void addMarketCoalescedFrames(String marketSlug, long delta) {
        if (marketSlug == null || delta <= 0L) {
            return;
        }
        ReconnectingWebSocketStream stream = marketStreams.get(marketSlug);
        if (stream != null) {
            stream.addCoalescedFrames(delta);
        }
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

    /**
     * Returns telemetry snapshots for all active market and user CLOB streams.
     *
     * @return list of {@link WebSocketStreamTelemetry} snapshots
     */
    public List<WebSocketStreamTelemetry> getStreamTelemetries() {
        List<WebSocketStreamTelemetry> result = new ArrayList<>(marketStreams.size() + 1);
        for (ReconnectingWebSocketStream stream : marketStreams.values()) {
            result.add(stream.getTelemetrySnapshot());
        }
        ReconnectingWebSocketStream currentUser = this.userStream;
        if (currentUser != null) {
            result.add(currentUser.getTelemetrySnapshot());
        }
        return result;
    }

    /**
     * Returns the dedicated Netty I/O reader thread observed by any active CLOB market stream,
     * or falls back to the User stream I/O thread if no market stream is connected yet.
     *
     * @return Netty I/O reader thread
     */
    public Thread getIoReaderThread() {
        for (ReconnectingWebSocketStream stream : marketStreams.values()) {
            Thread t = stream.getIoReaderThread();
            if (t != null) {
                return t;
            }
        }
        ReconnectingWebSocketStream currentUser = this.userStream;
        return currentUser != null ? currentUser.getIoReaderThread() : null;
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

    static String resolveUserWsUrl(String marketWsUrl) {
        if (marketWsUrl == null || marketWsUrl.isBlank()) {
            return "wss://ws-subscriptions-clob.polymarket.com/ws/user";
        }
        String trimmed = marketWsUrl.trim();
        if (trimmed.endsWith("/ws/market")) {
            return trimmed.substring(0, trimmed.length() - "/ws/market".length()) + "/ws/user";
        }
        if (trimmed.endsWith("/market")) {
            return trimmed.substring(0, trimmed.length() - "/market".length()) + "/user";
        }
        if (trimmed.endsWith("/clob")) {
            return trimmed.substring(0, trimmed.length() - "/clob".length()) + "/user";
        }
        return trimmed.endsWith("/") ? (trimmed + "user") : (trimmed + "/user");
    }
}
