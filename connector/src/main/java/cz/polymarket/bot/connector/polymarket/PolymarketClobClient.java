package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.os.LinuxCpuAffinity;
import cz.polymarket.bot.connector.polymarket.auth.PolymarketCredentials;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.RawWebSocketBatchListener;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import cz.polymarket.bot.connector.ws.StreamRedundancyStatus;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketDisconnectListener;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import io.quarkus.logging.Log;
import io.vertx.core.Vertx;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jctools.queues.MpscUnboundedArrayQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Resilient WebSocket client for Polymarket CLOB ({@code wss://ws-subscriptions-clob.polymarket.com/ws/market}
 * and {@code wss://ws-subscriptions-clob.polymarket.com/ws/user}).
 * <p>
 * Unifies market WebSocket subscriptions on {@link MarketWebSocketRuntime} ({@code CPU #1}) so Data Collector
 * and Strategy share active market connections without redundant sockets or duplicate archive records:
 * <ul>
 *   <li><b>Active trading market ({@code LIVE} / {@code PAPER})</b>: 2 connections
 *       ({@code POLYMARKET-CLOB-<slug>} on edge IPv4 {@code slot 0} + {@code POLYMARKET-CLOB-<slug>-BACKUP}
 *       on distinct edge IPv4 {@code slot 1}), feeding Strategy ({@code CPU #2}) and Data Collector
 *       ({@code CPU #0}) through completely independent lock-free queues and independent
 *       {@link PayloadDeduplicator} instances (zero lock contention and zero duplicate lines in {@code .jsonl.zst}).</li>
 *   <li><b>Lookahead / post-end resolving market (and all markets in {@code OFF} mode)</b>: 1 single-leg
 *       connection ({@code POLYMARKET-CLOB-<slug>}) with Strategy queue disabled, keeping total CLOB market
 *       connections bounded to at most {@code 3} ({@code 2} active + {@code 1} lookahead/resolving) in
 *       {@code LIVE}/{@code PAPER} and {@code 2} in {@code OFF}.</li>
 * </ul>
 */
@ApplicationScoped
public class PolymarketClobClient {

    /**
     * Connection state map key for the authenticated Polymarket CLOB User Channel WebSocket stream.
     */
    public static final String USER_STREAM_KEY = "USER";

    private static final int COLLECTOR_QUEUE_CHUNK_SIZE = 1024;
    private static final int COLLECTOR_DRAIN_BATCH_LIMIT = 512;

    private record CollectorQueueItem(
            SharedMarketSubscription subscription,
            TimestampedFrame frame,
            WebSocketStreamTelemetry terminalCloseTelemetry
    ) {
    }

    private final Vertx sharedVertx;
    private final MarketWebSocketRuntime marketWsRuntime;
    private final String wsUrl;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private final Map<String, SharedMarketSubscription> marketSubscriptions = new ConcurrentHashMap<>();
    private final MpscUnboundedArrayQueue<CollectorQueueItem> collectorEventQueue =
            new MpscUnboundedArrayQueue<>(COLLECTOR_QUEUE_CHUNK_SIZE);
    private final AtomicBoolean collectorWorkerRunning = new AtomicBoolean(false);
    private final AtomicBoolean collectorWorkerParked = new AtomicBoolean(true);
    private volatile Thread collectorWorkerThread;

    private volatile ReconnectingWebSocketStream userStream;
    private volatile WebSocketDisconnectListener disconnectListener;

    /**
     * Constructs the Polymarket CLOB WebSocket client.
     *
     * @param vertx            shared Quarkus Vert.x runtime instance (used by the low-rate User stream on {@code CPU #0})
     * @param marketWsRuntime  shared 1-EventLoop market WebSocket runtime ({@code market-ws-io-reader} on {@code CPU #1})
     * @param wsUrl            Polymarket CLOB WebSocket URL
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    @Inject
    public PolymarketClobClient(
            Vertx vertx,
            MarketWebSocketRuntime marketWsRuntime,
            @ConfigProperty(name = "polymarket.connector.clob.ws-url", defaultValue = "wss://ws-subscriptions-clob.polymarket.com/ws/market")
            String wsUrl,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "10000")
            long pingIntervalMs) {
        if (vertx == null) {
            throw new IllegalArgumentException("vertx cannot be null");
        }
        if (marketWsRuntime == null) {
            throw new IllegalArgumentException("marketWsRuntime cannot be null");
        }
        this.sharedVertx = vertx;
        this.marketWsRuntime = marketWsRuntime;
        this.wsUrl = wsUrl;
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    @PreDestroy
    void destroy() {
        stopAll();
    }

    /**
     * Returns whether native Linux Epoll transport is active on the consolidated market WebSocket runtime.
     *
     * @return {@code true} if native transport is enabled
     */
    public boolean isNativeTransportEnabled() {
        return marketWsRuntime.isNativeTransportEnabled();
    }

    /**
     * Returns whether native OpenSSL/BoringSSL ({@code netty-tcnative}) is available on the runtime classpath.
     *
     * @return {@code true} if native OpenSSL/BoringSSL is available
     */
    public boolean isOpenSslAvailable() {
        return marketWsRuntime.isOpenSslAvailable();
    }

    /**
     * Registers a listener notified whenever any active CLOB market or user stream disconnects unexpectedly.
     *
     * @param disconnectListener listener invoked with the stream telemetry snapshot at the moment of disconnect
     */
    public synchronized void setDisconnectListener(WebSocketDisconnectListener disconnectListener) {
        this.disconnectListener = disconnectListener;
        for (SharedMarketSubscription sub : marketSubscriptions.values()) {
            sub.primaryStream.setDisconnectListener(disconnectListener);
            ReconnectingWebSocketStream backup = sub.backupStream;
            if (backup != null) {
                backup.setDisconnectListener(disconnectListener);
            }
        }
        ReconnectingWebSocketStream currentUser = this.userStream;
        if (currentUser != null) {
            currentUser.setDisconnectListener(disconnectListener);
        }
    }

    /**
     * Attaches Data Collector archival recording ({@code CPU #0}) to {@code marketSlug}, opening
     * {@code POLYMARKET-CLOB-<slug>} if not already open or sharing the active market's connection(s)
     * with deduplicated zero-copy fan-out so zero duplicate records are ever written to {@code .jsonl.zst}.
     *
     * @param marketSlug              Polymarket market slug
     * @param tokenIds                CLOB asset token IDs belonging to the market
     * @param listener                listener receiving deduplicated raw JSON frames and receipt timestamps
     * @param normalCloseListener     optional listener invoked upon terminal stream closure ({@code closeCode == 1000} or post-end)
     * @param shouldReconnectSupplier optional supplier returning {@code true} if reconnection is allowed on disconnect
     */
    public synchronized void openCollectorMarketStream(
            String marketSlug,
            List<String> tokenIds,
            RawWebSocketListener listener,
            Consumer<WebSocketStreamTelemetry> normalCloseListener,
            BooleanSupplier shouldReconnectSupplier) {
        if (marketSlug == null || marketSlug.isBlank() || tokenIds == null || tokenIds.isEmpty()) {
            return;
        }
        ensureCollectorWorkerStarted();
        SharedMarketSubscription sub = marketSubscriptions.get(marketSlug);
        if (sub != null && sub.collectorAttached) {
            return;
        }
        if (sub == null) {
            sub = createSharedMarketSubscription(marketSlug, tokenIds);
            marketSubscriptions.put(marketSlug, sub);
        }
        sub.attachCollector(listener, normalCloseListener, shouldReconnectSupplier);
        if (!sub.primaryStarted) {
            sub.primaryStarted = true;
            Log.infof("Opening Polymarket CLOB WebSocket stream for market %s (tokens=%d, mode=collector)", marketSlug, tokenIds.size());
            sub.primaryStream.start();
        } else {
            Log.infof("Attached Data Collector to existing Polymarket CLOB stream for market %s", marketSlug);
        }
    }

    /**
     * Detaches Data Collector recording from {@code marketSlug}, closing {@code POLYMARKET-CLOB-<slug>}
     * only if Strategy is not currently subscribed to {@code marketSlug}.
     *
     * @param marketSlug Polymarket market slug
     */
    public synchronized void closeCollectorMarketStream(String marketSlug) {
        if (marketSlug == null) {
            return;
        }
        SharedMarketSubscription sub = marketSubscriptions.get(marketSlug);
        if (sub == null) {
            return;
        }
        sub.detachCollector();
        if (!sub.strategyAttached) {
            marketSubscriptions.remove(marketSlug, sub);
            Log.infof("Closing Polymarket CLOB WebSocket stream for market %s", marketSlug);
            sub.stopAllStreams();
        }
    }

    /**
     * Detaches Data Collector from all tracked markets and closes any market streams not currently used by Strategy.
     */
    public synchronized void stopAllCollectorStreams() {
        for (String slug : List.copyOf(marketSubscriptions.keySet())) {
            closeCollectorMarketStream(slug);
        }
    }

    /**
     * Returns an unmodifiable snapshot of the effective connection states of all collector-attached CLOB market streams.
     * When an active market also has a {@code BACKUP} leg open, reports {@link ConnectionState#CONNECTED} as long as
     * either leg is connected.
     *
     * @return map of market slug to effective {@link ConnectionState}
     */
    public Map<String, ConnectionState> getCollectorConnectionStates() {
        Map<String, ConnectionState> states = new LinkedHashMap<>();
        marketSubscriptions.forEach((slug, sub) -> {
            if (sub.collectorAttached) {
                states.put(slug, sub.effectiveState());
            }
        });
        return Collections.unmodifiableMap(states);
    }

    /**
     * Attaches Strategy ({@code CPU #2}) to {@code marketSlug}, reusing {@code POLYMARKET-CLOB-<slug>} if already
     * opened by Data Collector (or starting it on edge IPv4 {@code slot 0}) and opening the sibling
     * {@code POLYMARKET-CLOB-<slug>-BACKUP} connection on distinct edge IPv4 {@code slot 1}.
     *
     * @param marketSlug     Polymarket market slug
     * @param tokenIds       CLOB asset token IDs belonging to the market
     * @param batchListener  listener invoked with deduplicated chronological frame batches on {@code CPU #2}
     * @param externalWorker optional dedicated platform worker thread ({@code market-strategy-worker})
     */
    public synchronized void openMarketStreamBatch(
            String marketSlug,
            List<String> tokenIds,
            RawWebSocketBatchListener batchListener,
            Thread externalWorker) {
        if (marketSlug == null || marketSlug.isBlank() || tokenIds == null || tokenIds.isEmpty()) {
            return;
        }
        SharedMarketSubscription sub = marketSubscriptions.get(marketSlug);
        if (sub != null && sub.strategyAttached) {
            return;
        }
        boolean createdNew = false;
        if (sub == null) {
            sub = createSharedMarketSubscription(marketSlug, tokenIds);
            marketSubscriptions.put(marketSlug, sub);
            createdNew = true;
        }

        ReconnectingWebSocketStream backup = createBackupStream(sub);
        sub.attachStrategy(batchListener, externalWorker, backup);

        if (!sub.primaryStarted) {
            sub.primaryStarted = true;
            Log.infof("Opening dual Active-Active Polymarket CLOB WebSocket streams for market %s (tokens=%d, edgeSlots=0/1)",
                    marketSlug, tokenIds.size());
            sub.primaryStream.start();
        } else if (!createdNew) {
            Log.infof("Promoting existing Polymarket CLOB stream %s to dual Active-Active (opening BACKUP on edgeSlot=1)",
                    marketSlug);
        }
        backup.start();
    }

    /**
     * Detaches Strategy from {@code marketSlug} and immediately closes {@code POLYMARKET-CLOB-<slug>-BACKUP}.
     * If Data Collector is still recording {@code marketSlug} (e.g. awaiting post-end {@code code=1000} resolution
     * or switching Strategy to {@code OFF}), {@code POLYMARKET-CLOB-<slug>} remains open with Strategy queueing disabled.
     *
     * @param marketSlug Polymarket market slug to detach from Strategy
     */
    public synchronized void closeMarketStream(String marketSlug) {
        if (marketSlug == null) {
            return;
        }
        SharedMarketSubscription sub = marketSubscriptions.get(marketSlug);
        if (sub == null || !sub.strategyAttached) {
            return;
        }
        ReconnectingWebSocketStream removedBackup = sub.detachStrategy();
        if (removedBackup != null) {
            removedBackup.stop();
        }
        if (sub.collectorAttached) {
            Log.infof("Detached Strategy from Polymarket CLOB market %s (closed BACKUP; keeping single PRIMARY leg for Collector)",
                    marketSlug);
        } else {
            marketSubscriptions.remove(marketSlug, sub);
            Log.infof("Closing Polymarket CLOB WebSocket stream(s) for market %s", marketSlug);
            sub.stopAllStreams();
        }
    }

    /**
     * Opens an authenticated CLOB User Channel WebSocket stream ({@code /ws/user}) bound to an optional
     * external worker thread (e.g. {@code clob-user-worker} on {@code CPU #0}).
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
     * Stops all Strategy market attachments (closing all {@code -BACKUP} legs and clearing Strategy queues)
     * and closes the User stream while leaving Data Collector's single-leg {@code PRIMARY} streams running uninterrupted.
     */
    public synchronized void stopAllStrategyStreams() {
        for (String slug : List.copyOf(marketSubscriptions.keySet())) {
            closeMarketStream(slug);
        }
        closeUserStream();
    }

    /**
     * Disconnects all active Strategy, User, and Data Collector CLOB WebSocket streams and stops the collector worker.
     */
    public synchronized void stopAll() {
        stopAllStrategyStreams();
        stopAllCollectorStreams();
        stopCollectorWorker();
    }

    /**
     * Drains all currently queued Strategy frames across active primary and backup market CLOB streams into {@code scratchBatch}.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return total number of frames drained
     */
    public int drainMarketStreams(List<TimestampedFrame> scratchBatch) {
        int total = 0;
        for (SharedMarketSubscription sub : marketSubscriptions.values()) {
            if (!sub.strategyAttached) {
                continue;
            }
            total += sub.primaryStream.drainAvailableBatch(scratchBatch);
            ReconnectingWebSocketStream backup = sub.backupStream;
            if (backup != null) {
                total += backup.drainAvailableBatch(scratchBatch);
            }
        }
        return total;
    }

    /**
     * Drains all currently queued frames from the authenticated CLOB User stream into {@code scratchBatch}.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return number of frames drained
     */
    public int drainUserStream(List<TimestampedFrame> scratchBatch) {
        ReconnectingWebSocketStream current = this.userStream;
        return current != null ? current.drainAvailableBatch(scratchBatch) : 0;
    }

    /**
     * Returns whether any active Strategy primary or backup market CLOB stream currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if any strategy market stream queue is non-empty
     */
    public boolean hasQueuedMarketFrames() {
        for (SharedMarketSubscription sub : marketSubscriptions.values()) {
            if (!sub.strategyAttached) {
                continue;
            }
            if (sub.primaryStream.hasQueuedFrames()) {
                return true;
            }
            ReconnectingWebSocketStream backup = sub.backupStream;
            if (backup != null && backup.hasQueuedFrames()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Marks all active Strategy primary and backup market streams as parked prior to {@code LockSupport.parkNanos}
     * and returns {@code true} if any strategy market stream received frames concurrently.
     *
     * @return {@code true} if frames arrived concurrently and parking must be aborted
     */
    public boolean prepareMarketStreamsToPark() {
        for (SharedMarketSubscription sub : marketSubscriptions.values()) {
            if (!sub.strategyAttached) {
                continue;
            }
            if (sub.primaryStream.prepareToParkAndCheckQueuedFrames()) {
                return true;
            }
            ReconnectingWebSocketStream backup = sub.backupStream;
            if (backup != null && backup.prepareToParkAndCheckQueuedFrames()) {
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
     * Marks the CLOB User stream as parked prior to {@code LockSupport.parkNanos} and returns {@code true}
     * if frames arrived concurrently.
     *
     * @return {@code true} if frames arrived concurrently and parking must be aborted
     */
    public boolean prepareUserStreamToPark() {
        ReconnectingWebSocketStream current = this.userStream;
        return current != null && current.prepareToParkAndCheckQueuedFrames();
    }

    /**
     * Records additional fast-forwarded frames for the specified Strategy market slug.
     *
     * @param marketSlug market slug
     * @param delta      number of additional fast-forwarded frames
     */
    public void addMarketCoalescedFrames(String marketSlug, long delta) {
        if (marketSlug == null || delta <= 0L) {
            return;
        }
        SharedMarketSubscription sub = marketSubscriptions.get(marketSlug);
        if (sub != null && sub.strategyAttached) {
            sub.primaryStream.addCoalescedFrames(delta);
        }
    }

    /**
     * Returns an unmodifiable snapshot of the connection states of all active Strategy market and user CLOB streams.
     * For dual Active-Active market streams, reports {@link ConnectionState#CONNECTED} as long as either
     * the primary or backup connection is active.
     *
     * @return map of stream key to {@link ConnectionState}
     */
    public Map<String, ConnectionState> getConnectionStates() {
        Map<String, ConnectionState> states = new LinkedHashMap<>();
        marketSubscriptions.forEach((slug, sub) -> {
            if (sub.strategyAttached) {
                states.put(slug, sub.effectiveState());
            }
        });
        ReconnectingWebSocketStream currentUser = this.userStream;
        if (currentUser != null) {
            states.put(USER_STREAM_KEY, currentUser.getState());
        }
        return Collections.unmodifiableMap(states);
    }

    /**
     * Returns telemetry snapshots for all open CLOB streams ({@code PRIMARY} for active + lookahead markets,
     * {@code BACKUP} for the active Strategy market, and {@code USER} in {@code LIVE} mode).
     *
     * @return list of {@link WebSocketStreamTelemetry} snapshots
     */
    public List<WebSocketStreamTelemetry> getStreamTelemetries() {
        List<WebSocketStreamTelemetry> result = new ArrayList<>(marketSubscriptions.size() * 2 + 1);
        for (SharedMarketSubscription sub : marketSubscriptions.values()) {
            result.add(sub.primaryStream.getTelemetrySnapshot());
            ReconnectingWebSocketStream backup = sub.backupStream;
            if (backup != null) {
                result.add(backup.getTelemetrySnapshot());
            }
        }
        ReconnectingWebSocketStream currentUser = this.userStream;
        if (currentUser != null) {
            result.add(currentUser.getTelemetrySnapshot());
        }
        return result;
    }

    /**
     * Evaluates the live connection state of the peer leg when a dual Active-Active market WebSocket stream
     * ({@code POLYMARKET-CLOB-<slug>} vs {@code POLYMARKET-CLOB-<slug>-BACKUP}) disconnects.
     * Single-leg lookahead or {@code OFF}-mode streams (where {@code BACKUP} is absent) return {@link StreamRedundancyStatus#NONE}.
     *
     * @param streamName identifier of the stream that disconnected
     * @return {@link StreamRedundancyStatus} describing whether the peer stream is actively covering failover
     */
    public StreamRedundancyStatus describeMarketStreamRedundancy(String streamName) {
        if (streamName == null || !streamName.startsWith("POLYMARKET-CLOB-") || "POLYMARKET-CLOB-USER".equals(streamName)) {
            return StreamRedundancyStatus.NONE;
        }
        String suffix = streamName.substring("POLYMARKET-CLOB-".length());
        ReconnectingWebSocketStream peer;
        if (suffix.endsWith("-BACKUP")) {
            String marketSlug = suffix.substring(0, suffix.length() - "-BACKUP".length());
            SharedMarketSubscription sub = marketSubscriptions.get(marketSlug);
            peer = sub != null ? sub.primaryStream : null;
        } else {
            SharedMarketSubscription sub = marketSubscriptions.get(suffix);
            peer = sub != null ? sub.backupStream : null;
        }
        if (peer == null) {
            return StreamRedundancyStatus.NONE;
        }
        ConnectionState peerState = peer.getState();
        return new StreamRedundancyStatus(
                true,
                peer.getStreamName(),
                peerState,
                peer.getRemoteAddress(),
                peerState == ConnectionState.CONNECTED
        );
    }

    /**
     * Returns the dedicated Netty I/O reader thread ({@code market-ws-io-reader}) servicing market streams.
     *
     * @return Netty I/O reader thread
     */
    public Thread getIoReaderThread() {
        return marketWsRuntime.getIoReaderThread();
    }

    private SharedMarketSubscription createSharedMarketSubscription(String marketSlug, List<String> tokenIds) {
        String subPayload = buildSubscriptionPayload(tokenIds);
        SharedMarketSubscription[] subHolder = new SharedMarketSubscription[1];

        RawWebSocketBatchListener primaryStrategyListener = batch -> {
            SharedMarketSubscription sub = subHolder[0];
            if (sub != null) {
                sub.dispatchStrategyBatch(batch, sub.primaryStream);
            }
        };

        ReconnectingWebSocketStream primary = new ReconnectingWebSocketStream(
                marketWsRuntime.vertx(),
                "POLYMARKET-CLOB-" + marketSlug,
                wsUrl,
                () -> List.of(subPayload),
                null,
                primaryStrategyListener,
                reconnectDelayMs,
                pingIntervalMs,
                "PING"
        );
        primary.setInboundQueueEnabled(false);
        primary.setPreferredAddressSlot(marketWsRuntime, 0, () -> {
            SharedMarketSubscription sub = subHolder[0];
            ReconnectingWebSocketStream backup = sub != null ? sub.backupStream : null;
            return backup != null ? backup.getActiveOrPendingRemoteIp() : "";
        });
        primary.setDisconnectListener(this.disconnectListener);
        primary.setCollectorTap(frame -> {
            SharedMarketSubscription sub = subHolder[0];
            if (sub != null && sub.collectorAttached) {
                enqueueCollectorItem(new CollectorQueueItem(sub, frame, null));
            }
        });
        primary.setShouldReconnectSupplier(() -> {
            SharedMarketSubscription sub = subHolder[0];
            return sub == null || sub.shouldReconnectPrimary();
        });

        SharedMarketSubscription subscription = new SharedMarketSubscription(marketSlug, subPayload, primary);
        subHolder[0] = subscription;
        return subscription;
    }

    private ReconnectingWebSocketStream createBackupStream(SharedMarketSubscription sub) {
        ReconnectingWebSocketStream[] backupHolder = new ReconnectingWebSocketStream[1];
        RawWebSocketBatchListener backupStrategyListener = batch ->
                sub.dispatchStrategyBatch(batch, backupHolder[0]);

        ReconnectingWebSocketStream backup = new ReconnectingWebSocketStream(
                marketWsRuntime.vertx(),
                "POLYMARKET-CLOB-" + sub.slug + "-BACKUP",
                wsUrl,
                () -> List.of(sub.subPayload),
                null,
                backupStrategyListener,
                reconnectDelayMs,
                pingIntervalMs,
                "PING"
        );
        backupHolder[0] = backup;
        backup.setPreferredAddressSlot(marketWsRuntime, 1, () -> sub.primaryStream.getActiveOrPendingRemoteIp());
        backup.setDisconnectListener(this.disconnectListener);
        backup.setCollectorTap(frame -> {
            if (sub.collectorAttached) {
                enqueueCollectorItem(new CollectorQueueItem(sub, frame, null));
            }
        });
        return backup;
    }

    private void ensureCollectorWorkerStarted() {
        if (collectorWorkerRunning.compareAndSet(false, true)) {
            collectorWorkerParked.set(true);
            this.collectorWorkerThread = Thread.ofPlatform()
                    .daemon(true)
                    .priority(Thread.NORM_PRIORITY)
                    .name("clob-collector-worker")
                    .start(this::collectorWorkerLoop);
        }
    }

    private void stopCollectorWorker() {
        if (collectorWorkerRunning.compareAndSet(true, false)) {
            collectorEventQueue.clear();
            collectorWorkerParked.set(false);
            Thread worker = this.collectorWorkerThread;
            this.collectorWorkerThread = null;
            if (worker != null) {
                LockSupport.unpark(worker);
                if (worker != Thread.currentThread()) {
                    try {
                        worker.join(1_000L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private void enqueueCollectorItem(CollectorQueueItem item) {
        if (!collectorWorkerRunning.get()) {
            return;
        }
        collectorEventQueue.offer(item);
        if (collectorWorkerParked.getOpaque() && collectorWorkerParked.compareAndSet(true, false)) {
            Thread worker = this.collectorWorkerThread;
            if (worker != null) {
                LockSupport.unpark(worker);
            }
        }
    }

    private void collectorWorkerLoop() {
        LinuxCpuAffinity.pinCurrentThreadIfSupported(0, "vCPU #0: CLOB Collector");
        List<CollectorQueueItem> batch = new ArrayList<>(64);
        while (collectorWorkerRunning.get()) {
            batch.clear();
            int drained = collectorEventQueue.drain(batch::add, COLLECTOR_DRAIN_BATCH_LIMIT);
            if (drained > 0) {
                if (collectorWorkerParked.getOpaque()) {
                    collectorWorkerParked.set(false);
                }
                for (int i = 0; i < drained; i++) {
                    processCollectorQueueItem(batch.get(i));
                }
                continue;
            }
            collectorWorkerParked.set(true);
            if (!collectorEventQueue.isEmpty() || !collectorWorkerRunning.get()) {
                collectorWorkerParked.set(false);
                continue;
            }
            LockSupport.parkNanos(this, ReconnectingWebSocketStream.IDLE_PARK_TIMEOUT_NS);
            collectorWorkerParked.set(false);
        }
    }

    private void processCollectorQueueItem(CollectorQueueItem item) {
        if (item == null || item.subscription() == null) {
            return;
        }
        SharedMarketSubscription sub = item.subscription();
        TimestampedFrame frame = item.frame();
        if (frame != null) {
            RawWebSocketListener listener = sub.collectorListener;
            if (sub.collectorAttached && listener != null && sub.collectorDeduplicator.shouldAcceptUnique(frame.rawUtf8Bytes())) {
                try {
                    listener.onMessageBytes(frame);
                } catch (Exception e) {
                    Log.errorf("[POLYMARKET-CLOB-%s] Unhandled exception in collector listener: %s", sub.slug, e.getMessage());
                }
            }
            return;
        }

        WebSocketStreamTelemetry closeTelemetry = item.terminalCloseTelemetry();
        if (closeTelemetry != null) {
            Consumer<WebSocketStreamTelemetry> closeListener;
            synchronized (this) {
                closeListener = sub.collectorNormalCloseListener;
                sub.detachCollector();
                if (!sub.strategyAttached) {
                    marketSubscriptions.remove(sub.slug, sub);
                }
            }
            if (closeListener != null) {
                try {
                    closeListener.accept(closeTelemetry);
                } catch (Exception e) {
                    Log.warnf("[POLYMARKET-CLOB-%s] Exception in collector normalCloseListener: %s", sub.slug, e.getMessage());
                }
            }
        }
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

    private final class SharedMarketSubscription {

        private final String slug;
        private final String subPayload;
        private final ReconnectingWebSocketStream primaryStream;
        private final PayloadDeduplicator collectorDeduplicator = new PayloadDeduplicator();

        private volatile boolean primaryStarted;
        private volatile boolean collectorAttached;
        private volatile RawWebSocketListener collectorListener;
        private volatile Consumer<WebSocketStreamTelemetry> collectorNormalCloseListener;
        private volatile BooleanSupplier collectorShouldReconnectSupplier;

        private volatile boolean strategyAttached;
        private volatile RawWebSocketBatchListener strategyBatchListener;
        private volatile PayloadDeduplicator strategyDeduplicator;
        private volatile ReconnectingWebSocketStream backupStream;

        private SharedMarketSubscription(String slug, String subPayload, ReconnectingWebSocketStream primaryStream) {
            this.slug = slug;
            this.subPayload = subPayload;
            this.primaryStream = primaryStream;
        }

        void attachCollector(
                RawWebSocketListener listener,
                Consumer<WebSocketStreamTelemetry> normalCloseListener,
                BooleanSupplier shouldReconnectSupplier) {
            this.collectorListener = listener;
            this.collectorNormalCloseListener = normalCloseListener;
            this.collectorShouldReconnectSupplier = shouldReconnectSupplier;
            this.collectorAttached = true;
            updatePrimaryTerminalCloseArming();
        }

        void detachCollector() {
            this.collectorAttached = false;
            this.collectorListener = null;
            this.collectorNormalCloseListener = null;
            this.collectorShouldReconnectSupplier = null;
            updatePrimaryTerminalCloseArming();
        }

        void attachStrategy(
                RawWebSocketBatchListener batchListener,
                Thread externalWorker,
                ReconnectingWebSocketStream backup) {
            this.strategyDeduplicator = new PayloadDeduplicator();
            this.strategyBatchListener = batchListener;
            this.backupStream = backup;
            this.strategyAttached = true;
            updatePrimaryTerminalCloseArming();

            if (externalWorker != null) {
                primaryStream.bindExternalWorker(externalWorker);
                backup.bindExternalWorker(externalWorker);
            }
            primaryStream.setInboundQueueEnabled(true);
            backup.setInboundQueueEnabled(true);
        }

        ReconnectingWebSocketStream detachStrategy() {
            this.strategyAttached = false;
            this.strategyBatchListener = null;
            this.strategyDeduplicator = null;
            ReconnectingWebSocketStream removedBackup = this.backupStream;
            this.backupStream = null;
            primaryStream.unbindExternalWorkerAndDisableQueue();
            updatePrimaryTerminalCloseArming();
            return removedBackup;
        }

        private void updatePrimaryTerminalCloseArming() {
            if (!strategyAttached && collectorAttached && collectorNormalCloseListener != null) {
                primaryStream.setNormalCloseListener(telemetry ->
                        enqueueCollectorItem(new CollectorQueueItem(this, null, telemetry)));
            } else {
                primaryStream.setNormalCloseListener(null);
            }
        }

        boolean shouldReconnectPrimary() {
            if (strategyAttached) {
                return true;
            }
            BooleanSupplier supplier = this.collectorShouldReconnectSupplier;
            if (supplier == null) {
                return true;
            }
            try {
                return supplier.getAsBoolean();
            } catch (Exception e) {
                Log.warnf("[POLYMARKET-CLOB-%s] Exception in collector shouldReconnectSupplier: %s", slug, e.getMessage());
                return true;
            }
        }

        void dispatchStrategyBatch(List<TimestampedFrame> batch, ReconnectingWebSocketStream sourceStream) {
            RawWebSocketBatchListener delegate = this.strategyBatchListener;
            PayloadDeduplicator dedup = this.strategyDeduplicator;
            if (!this.strategyAttached || delegate == null || dedup == null || batch == null || batch.isEmpty()) {
                return;
            }
            int duplicatesSkipped = dedup.filterUniqueInPlace(batch);
            if (duplicatesSkipped > 0 && sourceStream != null) {
                sourceStream.addCoalescedFrames(duplicatesSkipped);
            }
            if (!batch.isEmpty()) {
                delegate.onMessageBatch(batch);
            }
        }

        ConnectionState effectiveState() {
            ConnectionState primaryState = primaryStream.getState();
            if (primaryState == ConnectionState.CONNECTED) {
                return ConnectionState.CONNECTED;
            }
            ReconnectingWebSocketStream backup = this.backupStream;
            if (backup != null) {
                ConnectionState backupState = backup.getState();
                if (backupState == ConnectionState.CONNECTED || primaryState == ConnectionState.DISCONNECTED) {
                    return backupState;
                }
            }
            return primaryState;
        }

        void stopAllStreams() {
            ReconnectingWebSocketStream backup = this.backupStream;
            this.backupStream = null;
            primaryStream.stop();
            if (backup != null) {
                backup.stop();
            }
        }
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

        synchronized boolean shouldAcceptUnique(byte[] rawUtf8Bytes) {
            long fp = computeFingerprint(rawUtf8Bytes);
            return fp == 0L || !isDuplicateAndRecord(fp);
        }

        synchronized int filterUniqueInPlace(List<TimestampedFrame> batch) {
            int n = batch.size();
            int writeIdx = 0;
            for (int i = 0; i < n; i++) {
                TimestampedFrame frame = batch.get(i);
                long fp = computeFingerprint(frame.rawUtf8Bytes());
                if (fp == 0L || !isDuplicateAndRecord(fp)) {
                    if (writeIdx != i) {
                        batch.set(writeIdx, frame);
                    }
                    writeIdx++;
                }
            }
            if (writeIdx < n) {
                batch.subList(writeIdx, n).clear();
            }
            return n - writeIdx;
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
