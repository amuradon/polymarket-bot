package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketDisconnectListener;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import io.quarkus.logging.Log;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jctools.queues.MpscUnboundedArrayQueue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * Resilient WebSocket client for Polymarket PolyBolt v2 ({@code wss://ws-live-v2.polymarket.com/ws}).
 * <p>
 * Maintains single-stream authenticated connections for {@code price.crypto.twap} (60-second TWAP) and
 * {@code price.crypto} (spot reference price) on {@link MarketWebSocketRuntime} ({@code CPU #1}),
 * delivering raw frames continuously to the Data Collector ({@code CPU #0}) while optionally fanning out
 * to a dedicated lock-free Strategy {@link MpscUnboundedArrayQueue} ({@code CPU #2}) strictly when a
 * strategy consumer is attached. When the strategy consumer is detached ({@code OFF} mode), the strategy
 * queue is cleared and receives zero frames.
 */
@ApplicationScoped
public class PolymarketPolyBoltClient {

    private static final int STRATEGY_QUEUE_CHUNK_SIZE = 256;
    private static final int STRATEGY_DRAIN_BATCH_LIMIT = 256;

    private record StrategyBinding(RawWebSocketListener listener, Thread worker) {
    }

    private final MarketWebSocketRuntime marketWsRuntime;
    private final String wsUrl;
    private volatile String apiKey;
    private volatile String secret;
    private volatile String passphrase;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private final MpscUnboundedArrayQueue<TimestampedFrame> strategyQueue =
            new MpscUnboundedArrayQueue<>(STRATEGY_QUEUE_CHUNK_SIZE);
    private final AtomicBoolean strategyWorkerParked = new AtomicBoolean(true);
    private volatile StrategyBinding strategyBinding;

    private volatile ReconnectingWebSocketStream twapStream;
    private volatile ReconnectingWebSocketStream cryptoPriceStream;
    private volatile WebSocketDisconnectListener disconnectListener;

    /**
     * CDI constructor for the Polymarket PolyBolt WebSocket client.
     *
     * @param marketWsRuntime  consolidated 1-EventLoop market WebSocket runtime
     * @param wsUrl            Polymarket PolyBolt v2 WebSocket endpoint URL
     * @param apiKey           Polymarket CLOB API key required for gated PolyBolt channels
     * @param secret           Polymarket CLOB API secret required for gated PolyBolt channels
     * @param passphrase       Polymarket CLOB API passphrase required for gated PolyBolt channels
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    @Inject
    public PolymarketPolyBoltClient(
            MarketWebSocketRuntime marketWsRuntime,
            @ConfigProperty(name = "polymarket.connector.polybolt.ws-url", defaultValue = "wss://ws-live-v2.polymarket.com/ws")
            String wsUrl,
            @ConfigProperty(name = "polymarket.connector.api-key")
            Optional<String> apiKey,
            @ConfigProperty(name = "polymarket.connector.secret")
            Optional<String> secret,
            @ConfigProperty(name = "polymarket.connector.passphrase")
            Optional<String> passphrase,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "10000")
            long pingIntervalMs) {
        if (marketWsRuntime == null) {
            throw new IllegalArgumentException("marketWsRuntime cannot be null");
        }
        this.marketWsRuntime = marketWsRuntime;
        this.wsUrl = wsUrl;
        this.apiKey = apiKey.map(String::trim).orElse("");
        this.secret = secret.map(String::trim).orElse("");
        this.passphrase = passphrase.map(String::trim).orElse("");
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    @PreDestroy
    void destroy() {
        stop();
    }

    /**
     * Registers a listener notified whenever any active PolyBolt stream disconnects unexpectedly.
     *
     * @param disconnectListener listener invoked with the stream telemetry snapshot at the moment of disconnect
     */
    public synchronized void setDisconnectListener(WebSocketDisconnectListener disconnectListener) {
        this.disconnectListener = disconnectListener;
        ReconnectingWebSocketStream twap = this.twapStream;
        if (twap != null) {
            twap.setDisconnectListener(disconnectListener);
        }
        ReconnectingWebSocketStream spot = this.cryptoPriceStream;
        if (spot != null) {
            spot.setDisconnectListener(disconnectListener);
        }
    }

    /**
     * Explicitly configures L2 credentials used for the mandatory {@code op: "auth"} handshake.
     *
     * @param apiKey     Polymarket CLOB API key
     * @param secret     Polymarket CLOB API secret
     * @param passphrase Polymarket CLOB API passphrase
     */
    public synchronized void configureCredentials(String apiKey, String secret, String passphrase) {
        this.apiKey = apiKey != null ? apiKey.trim() : "";
        this.secret = secret != null ? secret.trim() : "";
        this.passphrase = passphrase != null ? passphrase.trim() : "";
    }

    /**
     * Starts both authenticated PolyBolt streams ({@code price.crypto} and {@code price.crypto.twap}) for
     * continuous Data Collector recording on {@code CPU #0}.
     *
     * @param symbol              trading symbol (e.g. {@code BTCUSDT}), normalized to PolyBolt symbol (e.g. {@code btcusd})
     * @param cryptoPriceListener collector listener receiving {@code price.crypto} frames
     * @param twapListener        collector listener receiving {@code price.crypto.twap} frames
     */
    public synchronized void startCollectorStreams(
            String symbol,
            RawWebSocketListener cryptoPriceListener,
            RawWebSocketListener twapListener) {
        stopCollectorStreams();
        String polyBoltSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(symbol);

        ReconnectingWebSocketStream newCryptoStream = createAuthenticatedStream(
                "POLYMARKET-POLYBOLT-CRYPTO-PRICE",
                buildCryptoPriceSubscribeMessage(polyBoltSymbol),
                () -> this.cryptoPriceStream,
                cryptoPriceListener
        );
        newCryptoStream.setDisconnectListener(this.disconnectListener);

        ReconnectingWebSocketStream newTwapStream = createAuthenticatedStream(
                "POLYMARKET-POLYBOLT-TWAP",
                buildTwapSubscribeMessage(polyBoltSymbol),
                () -> this.twapStream,
                twapListener
        );
        newTwapStream.setDisconnectListener(this.disconnectListener);

        this.cryptoPriceStream = newCryptoStream;
        this.twapStream = newTwapStream;
        newCryptoStream.start();
        newTwapStream.start();
    }

    /**
     * Attaches the Strategy consumer and optional dedicated {@code market-strategy-worker} thread ({@code CPU #2})
     * so incoming PolyBolt data frames are offered lock-free to the Strategy queue prior to collector disk I/O.
     *
     * @param strategyListener listener invoked when {@link #drainStreams(List)} runs on {@code CPU #2}
     * @param externalWorker   optional dedicated worker thread to unpark upon frame arrival
     */
    public synchronized void attachStrategyConsumer(RawWebSocketListener strategyListener, Thread externalWorker) {
        if (strategyListener == null) {
            detachStrategyConsumer();
            return;
        }
        strategyQueue.clear();
        strategyWorkerParked.set(true);
        this.strategyBinding = new StrategyBinding(strategyListener, externalWorker);
    }

    /**
     * Detaches the Strategy consumer (used when switching Strategy to {@code OFF} or shutting down) and immediately
     * clears the Strategy queue so zero PolyBolt frames accumulate in memory while Data Collector remains active.
     */
    public synchronized void detachStrategyConsumer() {
        StrategyBinding prev = this.strategyBinding;
        this.strategyBinding = null;
        strategyQueue.clear();
        strategyWorkerParked.set(false);
        if (prev != null && prev.worker() != null) {
            LockSupport.unpark(prev.worker());
        }
    }

    /**
     * Drains all currently queued Strategy PolyBolt frames ({@code TWAP} and {@code CRYPTO-PRICE})
     * into {@code scratchBatch} and invokes the attached Strategy listener.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return total number of frames drained
     */
    public int drainStreams(List<TimestampedFrame> scratchBatch) {
        StrategyBinding binding = this.strategyBinding;
        if (binding == null) {
            strategyQueue.clear();
            return 0;
        }
        scratchBatch.clear();
        int drained = strategyQueue.drain(scratchBatch::add, STRATEGY_DRAIN_BATCH_LIMIT);
        if (drained == 0) {
            return 0;
        }
        if (strategyWorkerParked.getOpaque()) {
            strategyWorkerParked.set(false);
        }
        try {
            RawWebSocketListener listener = binding.listener();
            for (int i = 0; i < drained; i++) {
                TimestampedFrame frame = scratchBatch.get(i);
                if (frame != null) {
                    listener.onMessageBytes(frame);
                }
            }
        } finally {
            scratchBatch.clear();
        }
        return drained;
    }

    /**
     * Returns whether the Strategy PolyBolt queue currently has frames waiting to be drained.
     *
     * @return {@code true} if a strategy consumer is attached and {@code strategyQueue} is non-empty
     */
    public boolean hasQueuedFrames() {
        return this.strategyBinding != null && !strategyQueue.isEmpty();
    }

    /**
     * Returns the current number of frames queued in the Strategy PolyBolt queue (always {@code 0} in {@code OFF} mode).
     *
     * @return strategy queue depth
     */
    public int getStrategyQueueDepth() {
        return Math.max(0, strategyQueue.size());
    }

    /**
     * Marks the Strategy PolyBolt queue as parked prior to {@code LockSupport.parkNanos} and returns {@code true}
     * if frames arrived concurrently.
     *
     * @return {@code true} if frames arrived concurrently and parking must be aborted
     */
    public boolean prepareStreamsToPark() {
        if (this.strategyBinding == null) {
            return false;
        }
        strategyWorkerParked.set(true);
        if (!strategyQueue.isEmpty()) {
            strategyWorkerParked.set(false);
            return true;
        }
        return false;
    }

    /**
     * Stops only the underlying PolyBolt WebSocket streams.
     */
    public synchronized void stopCollectorStreams() {
        ReconnectingWebSocketStream currentTwap = this.twapStream;
        this.twapStream = null;
        if (currentTwap != null) {
            currentTwap.stop();
        }
        ReconnectingWebSocketStream currentSpot = this.cryptoPriceStream;
        this.cryptoPriceStream = null;
        if (currentSpot != null) {
            currentSpot.stop();
        }
    }

    /**
     * Detaches the Strategy consumer and stops all active PolyBolt WebSocket connections.
     */
    public synchronized void stop() {
        detachStrategyConsumer();
        stopCollectorStreams();
    }

    /**
     * Returns the current connection state of the PolyBolt {@code price.crypto.twap} WebSocket stream.
     *
     * @return current {@link ConnectionState}
     */
    public ConnectionState getTwapState() {
        ReconnectingWebSocketStream current = this.twapStream;
        return current != null ? current.getState() : ConnectionState.DISCONNECTED;
    }

    /**
     * Returns the current connection state of the PolyBolt {@code price.crypto} WebSocket stream.
     *
     * @return current {@link ConnectionState}
     */
    public ConnectionState getCryptoPriceState() {
        ReconnectingWebSocketStream current = this.cryptoPriceStream;
        return current != null ? current.getState() : ConnectionState.DISCONNECTED;
    }

    /**
     * Returns telemetry snapshots for active PolyBolt WebSocket streams.
     *
     * @return list of {@link WebSocketStreamTelemetry} snapshots
     */
    public List<WebSocketStreamTelemetry> getStreamTelemetries() {
        List<WebSocketStreamTelemetry> result = new ArrayList<>(2);
        ReconnectingWebSocketStream twap = this.twapStream;
        if (twap != null) {
            result.add(twap.getTelemetrySnapshot());
        }
        ReconnectingWebSocketStream spot = this.cryptoPriceStream;
        if (spot != null) {
            result.add(spot.getTelemetrySnapshot());
        }
        return result;
    }

    /**
     * Returns the consolidated Netty I/O reader thread ({@code market-ws-io-reader}).
     *
     * @return Netty I/O reader thread
     */
    public Thread getIoReaderThread() {
        return marketWsRuntime.getIoReaderThread();
    }

    /**
     * Returns whether the underlying PolyBolt WebSocket streams are currently active.
     *
     * @return {@code true} if either {@code twapStream} or {@code cryptoPriceStream} is initialized
     */
    public boolean isStreamsActive() {
        return this.twapStream != null || this.cryptoPriceStream != null;
    }

    /**
     * Checks whether all required Polymarket CLOB API credentials ({@code apiKey}, {@code secret}, {@code passphrase})
     * are configured and non-blank.
     *
     * @return {@code true} if all three credentials are present
     */
    public boolean hasCredentials() {
        return !apiKey.isEmpty() && !secret.isEmpty() && !passphrase.isEmpty();
    }

    private void enqueueForStrategyIfAttached(TimestampedFrame frame) {
        StrategyBinding binding = this.strategyBinding;
        if (binding == null || frame == null) {
            return;
        }
        strategyQueue.offer(frame);
        // Re-check binding in case detachStrategyConsumer() ran concurrently right after offer
        if (this.strategyBinding == null) {
            strategyQueue.clear();
            return;
        }
        if (strategyWorkerParked.getOpaque() && strategyWorkerParked.compareAndSet(true, false)) {
            Thread worker = binding.worker();
            if (worker != null) {
                LockSupport.unpark(worker);
            }
        }
    }

    private ReconnectingWebSocketStream createAuthenticatedStream(
            String streamName,
            String subscribeMsg,
            Supplier<ReconnectingWebSocketStream> streamRef,
            RawWebSocketListener collectorListener) {
        if (!hasCredentials()) {
            String errorMsg = String.format(
                    "[%s] Missing required Polymarket CLOB API credentials (POLYMARKET_API_KEY, POLYMARKET_SECRET, POLYMARKET_PASSPHRASE). All streams must be recorded; aborting startup.",
                    streamName
            );
            Log.fatal(errorMsg);
            throw new IllegalStateException(errorMsg);
        }
        String initialMessage = buildAuthMessage();

        RawWebSocketListener internalListener = new RawWebSocketListener() {
            @Override
            public void onMessage(String rawJson, long recvTs) {
                if (rawJson == null || rawJson.isBlank()) {
                    return;
                }
                if (!rawJson.contains("\"op\"")) {
                    enqueueForStrategyIfAttached(new TimestampedFrame(rawJson.getBytes(StandardCharsets.UTF_8), recvTs));
                    if (collectorListener != null) {
                        collectorListener.onMessage(rawJson, recvTs);
                    }
                    return;
                }
                handleControlFrame(streamName, subscribeMsg, streamRef, collectorListener, rawJson, recvTs);
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
                if (!containsOpFieldBytes(rawBytes)) {
                    // Offer to Strategy queue & unpark CPU #2 FIRST so Collector Zstd disk I/O never delays Strategy
                    enqueueForStrategyIfAttached(frame);
                    if (collectorListener != null) {
                        collectorListener.onMessageBytes(frame);
                    }
                    return;
                }
                String rawJson = frame.rawText();
                if (rawJson != null && !rawJson.isBlank()) {
                    handleControlFrame(streamName, subscribeMsg, streamRef, collectorListener, rawJson, frame.recvTs());
                }
            }
        };

        return new ReconnectingWebSocketStream(
                marketWsRuntime.vertx(),
                streamName,
                wsUrl,
                () -> List.of(initialMessage),
                internalListener,
                reconnectDelayMs,
                pingIntervalMs,
                "{\"op\":\"ping\"}"
        );
    }

    private void handleControlFrame(
            String streamName,
            String subscribeMsg,
            Supplier<ReconnectingWebSocketStream> streamRef,
            RawWebSocketListener collectorListener,
            String rawJson,
            long recvTs) {
        if (isAuthedConfirmation(rawJson)) {
            Log.infof("[%s] PolyBolt session authenticated successfully; subscribing to channel", streamName);
            ReconnectingWebSocketStream current = streamRef.get();
            if (current != null) {
                current.sendText(subscribeMsg);
            }
            return;
        }
        if (isSubscribedConfirmation(rawJson)) {
            Log.infof("[%s] PolyBolt channel subscription confirmed: %s", streamName, rawJson);
            return;
        }
        if (isPongConfirmation(rawJson)) {
            if (Log.isTraceEnabled()) {
                Log.tracef("[%s] PolyBolt pong received: %s", streamName, rawJson);
            }
            return;
        }
        if (isErrorFrame(rawJson)) {
            Log.errorf("[%s] PolyBolt server returned error: %s", streamName, rawJson);
            if (isAuthRequiredError(rawJson)) {
                ReconnectingWebSocketStream current = streamRef.get();
                if (current != null) {
                    current.sendText(buildAuthMessage());
                }
            }
            return;
        }
        enqueueForStrategyIfAttached(new TimestampedFrame(rawJson.getBytes(StandardCharsets.UTF_8), recvTs));
        if (collectorListener != null) {
            collectorListener.onMessage(rawJson, recvTs);
        }
    }

    static boolean containsOpFieldBytes(byte[] rawBytes) {
        int limit = Math.min(rawBytes.length, 64) - 4;
        for (int i = 0; i <= limit; i++) {
            if (rawBytes[i] == '"' && rawBytes[i + 1] == 'o' && rawBytes[i + 2] == 'p' && rawBytes[i + 3] == '"') {
                return true;
            }
        }
        return false;
    }

    private String buildAuthMessage() {
        return "{\"op\":\"auth\",\"rid\":\"a1\",\"auth\":{\"apiKey\":\""
                + apiKey + "\",\"secret\":\"" + secret + "\",\"passphrase\":\"" + passphrase + "\"}}";
    }

    private static String buildTwapSubscribeMessage(String polyBoltSymbol) {
        return "{\"op\":\"subscribe\",\"rid\":\"s1\",\"subscriptions\":["
                + "{\"channel\":\"price.crypto.twap\",\"filter\":{\"symbol\":\"" + polyBoltSymbol + "\",\"window_seconds\":60}}"
                + "]}";
    }

    private static String buildCryptoPriceSubscribeMessage(String polyBoltSymbol) {
        return "{\"op\":\"subscribe\",\"rid\":\"s1\",\"subscriptions\":["
                + "{\"channel\":\"price.crypto\",\"filter\":{\"symbol\":\"" + polyBoltSymbol + "\"}}"
                + "]}";
    }

    private static boolean isAuthedConfirmation(String rawJson) {
        return rawJson.contains("\"op\":\"authed\"") || rawJson.contains("\"op\": \"authed\"");
    }

    private static boolean isSubscribedConfirmation(String rawJson) {
        return rawJson.contains("\"op\":\"subscribed\"") || rawJson.contains("\"op\": \"subscribed\"");
    }

    private static boolean isErrorFrame(String rawJson) {
        return rawJson.contains("\"op\":\"error\"") || rawJson.contains("\"op\": \"error\"");
    }

    private static boolean isAuthRequiredError(String rawJson) {
        return rawJson.contains("\"code\":\"auth_required\"") || rawJson.contains("\"code\": \"auth_required\"");
    }

    private static boolean isPongConfirmation(String rawJson) {
        return rawJson.contains("\"op\":\"pong\"") || rawJson.contains("\"op\": \"pong\"");
    }
}
