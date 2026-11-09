package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketDisconnectListener;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import io.quarkus.logging.Log;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.dns.AddressResolverOptions;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Resilient WebSocket client for Polymarket PolyBolt v2 ({@code wss://ws-live-v2.polymarket.com/ws}).
 * Manages dedicated authenticated streams for {@code price.crypto.twap} (60-second TWAP) and
 * {@code price.crypto} (spot reference price) on an isolated 1-thread Netty EventLoop
 * ({@code polybolt-ws-io-reader}), performing the mandatory {@code op: "auth"} ->
 * {@code op: "authed"} -> {@code op: "subscribe"} handshake before subscribing to any channel and
 * forwarding raw WebSocket JSON messages without parsing overhead.
 */
@ApplicationScoped
public class PolymarketPolyBoltClient {

    /**
     * Dedicated thread name for the isolated Netty EventLoop servicing PolyBolt v2 oracle streams.
     */
    public static final String DEDICATED_POLYBOLT_IO_THREAD_NAME = "polybolt-ws-io-reader";

    private final Vertx polyBoltVertx;
    private final String wsUrl;
    private volatile String apiKey;
    private volatile String secret;
    private volatile String passphrase;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private volatile ReconnectingWebSocketStream twapStream;
    private volatile ReconnectingWebSocketStream cryptoPriceStream;
    private volatile WebSocketDisconnectListener disconnectListener;

    /**
     * CDI constructor for the Polymarket PolyBolt WebSocket client.
     *
     * @param vertx            shared Vert.x runtime instance
     * @param wsUrl            Polymarket PolyBolt v2 WebSocket endpoint URL
     * @param apiKey           Polymarket CLOB API key required for gated PolyBolt channels
     * @param secret           Polymarket CLOB API secret required for gated PolyBolt channels
     * @param passphrase       Polymarket CLOB API passphrase required for gated PolyBolt channels
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    @Inject
    public PolymarketPolyBoltClient(
            Vertx vertx,
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
        this.polyBoltVertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(1)
                .setPreferNativeTransport(true)
                .setAddressResolverOptions(new AddressResolverOptions()
                        .setRoundRobinInetAddress(true)
                        .setRotateServers(true)
                        .setCacheMaxTimeToLive(60)));
        if (!this.polyBoltVertx.isNativeTransportEnabled()) {
            Log.warnf("Native Linux Epoll transport is unavailable on polyBoltVertx (os=%s); falling back to JDK NIO EventLoop",
                    System.getProperty("os.name", "unknown"));
        }
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
        try {
            polyBoltVertx.close();
        } catch (Exception ignored) {
        }
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
     * Starts the PolyBolt {@code price.crypto.twap} (60s window) WebSocket stream for the given asset symbol,
     * performing the {@code auth} -> {@code authed} -> {@code subscribe} handshake and forwarding raw messages
     * directly to the listener without parsing.
     *
     * @param symbol   trading symbol (e.g. BTCUSDT or BTC), normalized to PolyBolt symbol (e.g. btcusd)
     * @param listener raw WebSocket listener receiving unparsed JSON frames and local receipt timestamps
     */
    public synchronized void startTwapStream(String symbol, RawWebSocketListener listener) {
        startTwapStream(symbol, listener, null);
    }

    /**
     * Starts the PolyBolt {@code price.crypto.twap} (60s window) WebSocket stream bound to an optional
     * external dedicated worker thread (e.g. {@code strategy-calculator-worker}).
     *
     * @param symbol         trading symbol (e.g. BTCUSDT or BTC), normalized to PolyBolt symbol (e.g. btcusd)
     * @param listener       raw WebSocket listener receiving unparsed JSON frames and local receipt timestamps
     * @param externalWorker optional dedicated platform worker thread responsible for draining this stream
     */
    public synchronized void startTwapStream(String symbol, RawWebSocketListener listener, Thread externalWorker) {
        stopTwapStream();
        String polyBoltSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(symbol);
        String subscribeMsg = buildTwapSubscribeMessage(polyBoltSymbol);
        ReconnectingWebSocketStream newStream = createAuthenticatedStream(
                "POLYMARKET-POLYBOLT-TWAP",
                subscribeMsg,
                () -> this.twapStream,
                listener
        );
        newStream.setDisconnectListener(this.disconnectListener);
        if (externalWorker != null) {
            newStream.bindExternalWorker(externalWorker);
        }
        this.twapStream = newStream;
        newStream.start();
    }

    /**
     * Starts the PolyBolt {@code price.crypto} WebSocket stream for the given asset symbol,
     * performing the {@code auth} -> {@code authed} -> {@code subscribe} handshake and forwarding raw messages
     * directly to the listener without parsing.
     *
     * @param symbol   trading symbol (e.g. BTCUSDT or BTC), normalized to PolyBolt symbol (e.g. btcusd)
     * @param listener raw WebSocket listener receiving unparsed JSON frames and local receipt timestamps
     */
    public synchronized void startCryptoPriceStream(String symbol, RawWebSocketListener listener) {
        startCryptoPriceStream(symbol, listener, null);
    }

    /**
     * Starts the PolyBolt {@code price.crypto} WebSocket stream bound to an optional
     * external dedicated worker thread (e.g. {@code strategy-calculator-worker}).
     *
     * @param symbol         trading symbol (e.g. BTCUSDT or BTC), normalized to PolyBolt symbol (e.g. btcusd)
     * @param listener       raw WebSocket listener receiving unparsed JSON frames and local receipt timestamps
     * @param externalWorker optional dedicated platform worker thread responsible for draining this stream
     */
    public synchronized void startCryptoPriceStream(String symbol, RawWebSocketListener listener, Thread externalWorker) {
        stopCryptoPriceStream();
        String polyBoltSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(symbol);
        String subscribeMsg = buildCryptoPriceSubscribeMessage(polyBoltSymbol);
        ReconnectingWebSocketStream newStream = createAuthenticatedStream(
                "POLYMARKET-POLYBOLT-CRYPTO-PRICE",
                subscribeMsg,
                () -> this.cryptoPriceStream,
                listener
        );
        newStream.setDisconnectListener(this.disconnectListener);
        if (externalWorker != null) {
            newStream.bindExternalWorker(externalWorker);
        }
        this.cryptoPriceStream = newStream;
        newStream.start();
    }

    /**
     * Drains all currently queued frames from both PolyBolt streams ({@code TWAP} and {@code CRYPTO-PRICE})
     * into {@code scratchBatch}. Called on the dedicated {@code strategy-calculator-worker} thread.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return total number of frames drained across both streams
     */
    public int drainStreams(List<TimestampedFrame> scratchBatch) {
        int total = 0;
        ReconnectingWebSocketStream twap = this.twapStream;
        if (twap != null) {
            total += twap.drainAvailableBatch(scratchBatch);
        }
        ReconnectingWebSocketStream spot = this.cryptoPriceStream;
        if (spot != null) {
            total += spot.drainAvailableBatch(scratchBatch);
        }
        return total;
    }

    /**
     * Returns whether either PolyBolt stream currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if either PolyBolt queue is non-empty
     */
    public boolean hasQueuedFrames() {
        ReconnectingWebSocketStream twap = this.twapStream;
        if (twap != null && twap.hasQueuedFrames()) {
            return true;
        }
        ReconnectingWebSocketStream spot = this.cryptoPriceStream;
        return spot != null && spot.hasQueuedFrames();
    }

    /**
     * Stops the PolyBolt {@code price.crypto.twap} WebSocket connection.
     */
    public synchronized void stopTwapStream() {
        ReconnectingWebSocketStream current = this.twapStream;
        this.twapStream = null;
        if (current != null) {
            current.stop();
        }
    }

    /**
     * Stops the PolyBolt {@code price.crypto} WebSocket connection.
     */
    public synchronized void stopCryptoPriceStream() {
        ReconnectingWebSocketStream current = this.cryptoPriceStream;
        this.cryptoPriceStream = null;
        if (current != null) {
            current.stop();
        }
    }

    /**
     * Stops all active PolyBolt WebSocket connections.
     */
    public synchronized void stop() {
        stopTwapStream();
        stopCryptoPriceStream();
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
     * Returns the Netty I/O reader thread observed by either active PolyBolt stream, or {@code null}.
     *
     * @return Netty I/O reader thread
     */
    public Thread getIoReaderThread() {
        ReconnectingWebSocketStream spot = this.cryptoPriceStream;
        if (spot != null && spot.getIoReaderThread() != null) {
            return spot.getIoReaderThread();
        }
        ReconnectingWebSocketStream twap = this.twapStream;
        return twap != null ? twap.getIoReaderThread() : null;
    }

    /**
     * Checks whether all required Polymarket CLOB API credentials ({@code apiKey}, {@code secret}, {@code passphrase})
     * are configured and non-blank.
     *
     * @return true if all three credentials are present
     */
    public boolean hasCredentials() {
        return !apiKey.isEmpty() && !secret.isEmpty() && !passphrase.isEmpty();
    }

    private ReconnectingWebSocketStream createAuthenticatedStream(
            String streamName,
            String subscribeMsg,
            Supplier<ReconnectingWebSocketStream> streamRef,
            RawWebSocketListener listener) {
        if (!hasCredentials()) {
            String errorMsg = String.format(
                    "[%s] Missing required Polymarket CLOB API credentials (POLYMARKET_API_KEY, POLYMARKET_SECRET, POLYMARKET_PASSPHRASE). All streams must be recorded; aborting startup.",
                    streamName
            );
            Log.fatal(errorMsg);
            throw new IllegalStateException(errorMsg);
        }
        String initialMessage = buildAuthMessage();

        ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                polyBoltVertx,
                streamName,
                wsUrl,
                () -> List.of(initialMessage),
                (rawJson, recvTs) -> {
                    if (rawJson == null || rawJson.isBlank()) {
                        return;
                    }
                    if (!rawJson.contains("\"op\"")) {
                        if (listener != null) {
                            listener.onMessage(rawJson, recvTs);
                        }
                        return;
                    }
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
                    if (listener != null) {
                        listener.onMessage(rawJson, recvTs);
                    }
                },
                reconnectDelayMs,
                pingIntervalMs,
                "{\"op\":\"ping\"}"
        );
        stream.setDedicatedIoThreadName(DEDICATED_POLYBOLT_IO_THREAD_NAME);
        return stream;
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
