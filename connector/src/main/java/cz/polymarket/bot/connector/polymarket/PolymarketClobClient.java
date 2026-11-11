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
import io.vertx.core.dns.AddressResolverOptions;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
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
    private final Map<String, ReconnectingWebSocketStream> backupMarketStreams = new ConcurrentHashMap<>();
    private final Map<String, PayloadDeduplicator> marketDeduplicators = new ConcurrentHashMap<>();
    private volatile ReconnectingWebSocketStream userStream;
    private volatile WebSocketDisconnectListener disconnectListener;

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
                .setPreferNativeTransport(true)
                .setAddressResolverOptions(new AddressResolverOptions()
                        .setRoundRobinInetAddress(true)
                        .setRotateServers(true)
                        .setCacheMaxTimeToLive(60)));
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
        for (ReconnectingWebSocketStream backup : backupMarketStreams.values()) {
            backup.setDisconnectListener(disconnectListener);
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
        openMarketStream(marketSlug, tokenIds, listener, null);
    }

    /**
     * Opens a dedicated CLOB WebSocket connection for a single market subscribed to its token IDs,
     * forwarding all raw frames directly to the listener without parsing and invoking {@code normalCloseListener}
     * when the server terminates the resolved market stream with {@code closeCode == 1000}.
     *
     * @param marketSlug          Polymarket market slug
     * @param tokenIds            CLOB asset token IDs belonging to the market
     * @param listener            listener receiving raw JSON frames and receipt timestamps
     * @param normalCloseListener optional listener invoked when the server cleanly closes the stream with {@code closeCode == 1000}
     */
    public synchronized void openMarketStream(
            String marketSlug,
            List<String> tokenIds,
            RawWebSocketListener listener,
            Consumer<WebSocketStreamTelemetry> normalCloseListener) {
        if (marketSlug == null || marketSlug.isBlank() || tokenIds == null || tokenIds.isEmpty()) {
            return;
        }
        if (marketStreams.containsKey(marketSlug)) {
            return;
        }
        String subPayload = buildSubscriptionPayload(tokenIds);
        RawWebSocketListener delegatingListener = new RawWebSocketListener() {
            @Override
            public void onMessage(String rawJson, long recvTs) {
                if (listener != null && rawJson != null && !rawJson.isBlank()) {
                    listener.onMessage(rawJson, recvTs);
                }
            }

            @Override
            public void onMessageBytes(TimestampedFrame frame) {
                if (listener != null && frame != null) {
                    listener.onMessageBytes(frame);
                }
            }
        };
        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                clobMarketVertx,
                "POLYMARKET-CLOB-" + marketSlug,
                wsUrl,
                () -> List.of(subPayload),
                delegatingListener,
                reconnectDelayMs,
                pingIntervalMs,
                "PING"
        );
        stream.setDedicatedIoThreadName(DEDICATED_MARKET_IO_THREAD_NAME);
        stream.setDisconnectListener(this.disconnectListener);
        if (normalCloseListener != null) {
            stream.setNormalCloseListener(telemetry -> {
                marketStreams.remove(marketSlug, stream);
                normalCloseListener.accept(telemetry);
            });
        }
        marketStreams.put(marketSlug, stream);
        Log.infof("Opening Polymarket CLOB WebSocket stream for market %s (tokens=%d)", marketSlug, tokenIds.size());
        stream.start();
    }

    /**
     * Opens a dual Active-Active pair of CLOB WebSocket connections ({@code PRIMARY} and {@code BACKUP})
     * for a single market with pre-parse {@code rawUtf8Bytes} fingerprint deduplication and batch-draining
     * listener support, optionally bound to an external dedicated worker thread (e.g. {@code clob-book-worker}).
     * <p>
     * Maintaining two independent WebSocket connections ensures {@code 0 ms} orderbook interruption if one
     * connection experiences an upstream CDN/proxy micro-stall or {@code code=1013} disconnect.
     *
     * @param marketSlug     Polymarket market slug
     * @param tokenIds       CLOB asset token IDs belonging to the market
     * @param batchListener  listener invoked with deduplicated chronological frame batches
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
        PayloadDeduplicator deduplicator = new PayloadDeduplicator();
        marketDeduplicators.put(marketSlug, deduplicator);

        ReconnectingWebSocketStream[] primaryHolder = new ReconnectingWebSocketStream[1];
        ReconnectingWebSocketStream[] backupHolder = new ReconnectingWebSocketStream[1];

        RawWebSocketBatchListener primaryDedupListener = batch ->
                dispatchDeduplicatedBatch(batch, primaryHolder[0], deduplicator, batchListener);
        RawWebSocketBatchListener backupDedupListener = batch ->
                dispatchDeduplicatedBatch(batch, backupHolder[0], deduplicator, batchListener);

        ReconnectingWebSocketStream primaryStream = new ReconnectingWebSocketStream(
                clobMarketVertx,
                "POLYMARKET-CLOB-" + marketSlug,
                wsUrl,
                () -> List.of(subPayload),
                null,
                primaryDedupListener,
                reconnectDelayMs,
                pingIntervalMs,
                "PING"
        );
        primaryHolder[0] = primaryStream;
        primaryStream.setDedicatedIoThreadName(DEDICATED_MARKET_IO_THREAD_NAME);
        primaryStream.setDisconnectListener(this.disconnectListener);

        ReconnectingWebSocketStream backupStream = new ReconnectingWebSocketStream(
                clobMarketVertx,
                "POLYMARKET-CLOB-" + marketSlug + "-BACKUP",
                wsUrl,
                () -> List.of(subPayload),
                null,
                backupDedupListener,
                reconnectDelayMs,
                pingIntervalMs,
                "PING"
        );
        backupHolder[0] = backupStream;
        backupStream.setDedicatedIoThreadName(DEDICATED_MARKET_IO_THREAD_NAME);
        backupStream.setDisconnectListener(this.disconnectListener);

        if (externalWorker != null) {
            primaryStream.bindExternalWorker(externalWorker);
            backupStream.bindExternalWorker(externalWorker);
        }
        marketStreams.put(marketSlug, primaryStream);
        backupMarketStreams.put(marketSlug, backupStream);
        Log.infof("Opening dual Active-Active Polymarket CLOB WebSocket streams for market %s (tokens=%d, batchMode=true)",
                marketSlug, tokenIds.size());
        primaryStream.start();
        backupStream.start();
    }

    private static void dispatchDeduplicatedBatch(
            List<TimestampedFrame> batch,
            ReconnectingWebSocketStream sourceStream,
            PayloadDeduplicator deduplicator,
            RawWebSocketBatchListener delegate) {
        if (delegate == null || batch == null || batch.isEmpty()) {
            return;
        }
        List<TimestampedFrame> uniqueBatch = deduplicator.filterUnique(batch);
        int duplicatesSkipped = batch.size() - uniqueBatch.size();
        if (duplicatesSkipped > 0 && sourceStream != null) {
            sourceStream.addCoalescedFrames(duplicatesSkipped);
        }
        if (!uniqueBatch.isEmpty()) {
            delegate.onMessageBatch(uniqueBatch);
        }
    }

    /**
     * Disconnects and removes both primary and backup CLOB WebSocket streams for a market that has finished.
     *
     * @param marketSlug Polymarket market slug to disconnect
     */
    public synchronized void closeMarketStream(String marketSlug) {
        if (marketSlug == null) {
            return;
        }
        marketDeduplicators.remove(marketSlug);
        ReconnectingWebSocketStream primary = marketStreams.remove(marketSlug);
        ReconnectingWebSocketStream backup = backupMarketStreams.remove(marketSlug);
        if (primary != null || backup != null) {
            Log.infof("Closing Polymarket CLOB WebSocket stream(s) for market %s", marketSlug);
        }
        if (primary != null) {
            primary.stop();
        }
        if (backup != null) {
            backup.stop();
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
     * Drains all currently queued frames across active primary and backup market CLOB streams into {@code scratchBatch}.
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
        for (ReconnectingWebSocketStream backup : backupMarketStreams.values()) {
            total += backup.drainAvailableBatch(scratchBatch);
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
     * Returns whether any active primary or backup market CLOB stream currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if any market stream queue is non-empty
     */
    public boolean hasQueuedMarketFrames() {
        for (ReconnectingWebSocketStream stream : marketStreams.values()) {
            if (stream.hasQueuedFrames()) {
                return true;
            }
        }
        for (ReconnectingWebSocketStream backup : backupMarketStreams.values()) {
            if (backup.hasQueuedFrames()) {
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
     * For dual Active-Active market streams, reports {@link ConnectionState#CONNECTED} as long as either
     * the primary or backup connection is active.
     *
     * @return map of stream key to {@link ConnectionState}
     */
    public Map<String, ConnectionState> getConnectionStates() {
        Map<String, ConnectionState> states = new LinkedHashMap<>();
        marketStreams.forEach((slug, primary) -> {
            ConnectionState primaryState = primary.getState();
            ConnectionState effectiveState = primaryState;
            if (primaryState != ConnectionState.CONNECTED) {
                ReconnectingWebSocketStream backup = backupMarketStreams.get(slug);
                if (backup != null) {
                    ConnectionState backupState = backup.getState();
                    if (backupState == ConnectionState.CONNECTED || primaryState == ConnectionState.DISCONNECTED) {
                        effectiveState = backupState;
                    }
                }
            }
            states.put(slug, effectiveState);
        });
        ReconnectingWebSocketStream currentUser = this.userStream;
        if (currentUser != null) {
            states.put(USER_STREAM_KEY, currentUser.getState());
        }
        return Collections.unmodifiableMap(states);
    }

    /**
     * Returns telemetry snapshots for all active primary, backup, and user CLOB streams.
     *
     * @return list of {@link WebSocketStreamTelemetry} snapshots
     */
    public List<WebSocketStreamTelemetry> getStreamTelemetries() {
        List<WebSocketStreamTelemetry> result = new ArrayList<>(marketStreams.size() + backupMarketStreams.size() + 1);
        for (ReconnectingWebSocketStream stream : marketStreams.values()) {
            result.add(stream.getTelemetrySnapshot());
        }
        for (ReconnectingWebSocketStream backup : backupMarketStreams.values()) {
            result.add(backup.getTelemetrySnapshot());
        }
        ReconnectingWebSocketStream currentUser = this.userStream;
        if (currentUser != null) {
            result.add(currentUser.getTelemetrySnapshot());
        }
        return result;
    }

    /**
     * Describes the real-time failover state of the peer WebSocket connection when one leg of a dual
     * Active-Active CLOB market stream disconnects.
     *
     * @param dualMarketStream  {@code true} if the stream belongs to a dual Active-Active market subscription
     * @param peerStreamName    identifier of the peer stream (or {@code ""} if single-leg stream)
     * @param peerState         current connection state of the peer stream
     * @param peerRemoteAddress remote endpoint address ({@code IP:port}) of the peer stream
     * @param failoverActive    {@code true} if the peer stream is currently {@link ConnectionState#CONNECTED}
     */
    public record MarketStreamRedundancyStatus(
            boolean dualMarketStream,
            String peerStreamName,
            ConnectionState peerState,
            String peerRemoteAddress,
            boolean failoverActive
    ) {
        /**
         * Sentinel status for single-leg streams (such as {@code USER} or PolyBolt streams) without a dual peer.
         */
        public static final MarketStreamRedundancyStatus NONE = new MarketStreamRedundancyStatus(
                false, "", ConnectionState.DISCONNECTED, "", false
        );
    }

    /**
     * Evaluates the live connection state of the peer leg for a dual Active-Active market WebSocket stream
     * (e.g. {@code POLYMARKET-CLOB-<slug>} vs {@code POLYMARKET-CLOB-<slug>-BACKUP}).
     *
     * @param streamName identifier of the stream that disconnected
     * @return {@link MarketStreamRedundancyStatus} describing whether the peer stream is actively covering failover
     */
    public MarketStreamRedundancyStatus describeMarketStreamRedundancy(String streamName) {
        if (streamName == null || !streamName.startsWith("POLYMARKET-CLOB-") || "POLYMARKET-CLOB-USER".equals(streamName)) {
            return MarketStreamRedundancyStatus.NONE;
        }
        String suffix = streamName.substring("POLYMARKET-CLOB-".length());
        ReconnectingWebSocketStream peer;
        if (suffix.endsWith("-BACKUP")) {
            String marketSlug = suffix.substring(0, suffix.length() - "-BACKUP".length());
            peer = marketStreams.get(marketSlug);
        } else {
            peer = backupMarketStreams.get(suffix);
        }
        if (peer == null) {
            return MarketStreamRedundancyStatus.NONE;
        }
        ConnectionState peerState = peer.getState();
        return new MarketStreamRedundancyStatus(
                true,
                peer.getStreamName(),
                peerState,
                peer.getRemoteAddress(),
                peerState == ConnectionState.CONNECTED
        );
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
        for (ReconnectingWebSocketStream backup : backupMarketStreams.values()) {
            Thread t = backup.getIoReaderThread();
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

    /**
     * Fast, fixed-footprint 16-way set-associative FIFO fingerprint cache (256 sets x 16 ways = 4,096 slots)
     * that deduplicates identical raw UTF-8 WebSocket frames across the primary and backup market streams
     * before {@link String} decoding or Jackson JSON parsing.
     */
    static final class PayloadDeduplicator {

        private static final int SET_COUNT = 256;
        private static final int SET_MASK = SET_COUNT - 1;
        private static final int WAYS_SHIFT = 4;
        private static final int WAYS_PER_SET = 1 << WAYS_SHIFT;
        private static final int WAY_MASK = WAYS_PER_SET - 1;
        private static final VarHandle LONG_ARRAY_HANDLE =
                MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

        private final long[] cache = new long[SET_COUNT * WAYS_PER_SET];
        private final byte[] wayCursors = new byte[SET_COUNT];
        private final List<TimestampedFrame> uniqueScratch = new ArrayList<>(64);

        synchronized List<TimestampedFrame> filterUnique(List<TimestampedFrame> batch) {
            uniqueScratch.clear();
            for (int i = 0, n = batch.size(); i < n; i++) {
                TimestampedFrame frame = batch.get(i);
                long fp = computeFingerprint(frame.rawUtf8Bytes());
                if (fp == 0L || !isDuplicateAndRecord(fp)) {
                    uniqueScratch.add(frame);
                }
            }
            return uniqueScratch;
        }

        private boolean isDuplicateAndRecord(long fp) {
            int setIdx = ((int) (fp ^ (fp >>> 32))) & SET_MASK;
            int baseOffset = setIdx << WAYS_SHIFT;
            for (int w = 0; w < WAYS_PER_SET; w++) {
                if (cache[baseOffset + w] == fp) {
                    return true;
                }
            }
            int nextWay = wayCursors[setIdx] & WAY_MASK;
            cache[baseOffset + nextWay] = fp;
            wayCursors[setIdx] = (byte) ((nextWay + 1) & WAY_MASK);
            return false;
        }

        static long computeFingerprint(byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return 0L;
            }
            int len = bytes.length;
            long h = 0xcbf29ce484222325L ^ ((long) len * 0x9e3779b97f4a7c15L);
            int i = 0;
            int limit = len - 8;
            while (i <= limit) {
                long word = (long) LONG_ARRAY_HANDLE.get(bytes, i);
                h ^= Long.rotateLeft(word * 0xbf58476d1ce4e5b9L, 31);
                h = Long.rotateLeft(h, 27) * 0x94d049bb133111ebL + 0x52dce729L;
                i += 8;
            }
            while (i < len) {
                h ^= (bytes[i] & 0xFFL);
                h *= 0x100000001b3L;
                i++;
            }
            h ^= (h >>> 33);
            h *= 0xff51afd7ed558ccdL;
            h ^= (h >>> 33);
            return h != 0L ? h : 1L;
        }
    }
}
