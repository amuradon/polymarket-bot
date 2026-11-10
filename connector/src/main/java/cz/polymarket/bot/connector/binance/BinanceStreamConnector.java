package cz.polymarket.bot.connector.binance;

import cz.polymarket.bot.connector.ws.ConnectionState;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Manages resilient WebSocket connections for all 5 Binance Spot and USD-M Futures data streams:
 * Spot aggTrades, Futures aggTrades, Futures Diff. Book Depth (100ms),
 * Futures Partial Book Depth (100ms), and Futures Liquidation Orders (forceOrder).
 * <p>
 * Isolates all 5 Binance streams onto a dedicated single-thread Netty EventLoop ({@code binance-ws-io-reader})
 * with native Linux Epoll transport, native BoringSSL ({@code netty-tcnative}), and CloudFront DNS round-robin
 * rotation, forwarding raw {@code byte[]} frames without UTF-8 {@link String} allocation overhead.
 */
@ApplicationScoped
public class BinanceStreamConnector {

    /**
     * Dedicated thread name for the isolated Netty EventLoop servicing all 5 Binance WebSocket streams.
     */
    public static final String DEDICATED_BINANCE_IO_THREAD_NAME = "binance-ws-io-reader";

    private final Vertx binanceVertx;
    private final String spotWsBaseUrl;
    private final String futuresWsBaseUrl;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;
    private final Map<BinanceStreamType, ReconnectingWebSocketStream> streams = new EnumMap<>(BinanceStreamType.class);
    private volatile WebSocketDisconnectListener disconnectListener;

    /**
     * Constructs the Binance multi-stream WebSocket connector with an isolated 1-thread Vert.x instance.
     *
     * @param vertx            shared Quarkus Vert.x instance
     * @param spotWsBaseUrl    Binance Spot WebSocket base URL
     * @param futuresWsBaseUrl Binance USD-M Futures WebSocket base URL
     * @param reconnectDelayMs delay in ms before reconnecting a dropped stream
     * @param pingIntervalMs   interval in ms for sending WebSocket ping frames
     */
    @Inject
    public BinanceStreamConnector(
            Vertx vertx,
            @ConfigProperty(name = "polymarket.connector.binance.spot-ws-base-url", defaultValue = "wss://stream.binance.com:9443/ws")
            String spotWsBaseUrl,
            @ConfigProperty(name = "polymarket.connector.binance.futures-ws-base-url", defaultValue = "wss://fstream.binance.com")
            String futuresWsBaseUrl,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "15000")
            long pingIntervalMs) {
        this.binanceVertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(1)
                .setPreferNativeTransport(true)
                .setAddressResolverOptions(new AddressResolverOptions()
                        .setRoundRobinInetAddress(true)
                        .setRotateServers(true)
                        .setCacheMaxTimeToLive(60)));
        if (!this.binanceVertx.isNativeTransportEnabled()) {
            Log.warnf("Native Linux Epoll transport is unavailable on binanceVertx (os=%s); falling back to JDK NIO EventLoop",
                    System.getProperty("os.name", "unknown"));
        } else {
            Log.infof("Initialized dedicated binanceVertx with native Linux Epoll transport (openSslAvailable=%b)",
                    OpenSsl.isAvailable());
        }
        this.spotWsBaseUrl = spotWsBaseUrl;
        this.futuresWsBaseUrl = futuresWsBaseUrl;
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    @PreDestroy
    public void destroy() {
        stop();
        try {
            binanceVertx.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Returns whether native Linux Epoll transport is active on the dedicated Binance Vert.x instance.
     *
     * @return {@code true} if native transport is enabled
     */
    public boolean isNativeTransportEnabled() {
        return binanceVertx.isNativeTransportEnabled();
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
     * Registers a listener notified whenever any active Binance stream disconnects unexpectedly.
     *
     * @param disconnectListener listener invoked with the stream telemetry snapshot at the moment of disconnect
     */
    public synchronized void setDisconnectListener(WebSocketDisconnectListener disconnectListener) {
        this.disconnectListener = disconnectListener;
        for (ReconnectingWebSocketStream stream : streams.values()) {
            stream.setDisconnectListener(disconnectListener);
        }
    }

    /**
     * Starts all 5 Binance WebSocket streams for the given symbol and routes messages to the listener.
     *
     * @param symbol   trading pair symbol (e.g. BTCUSDT)
     * @param listener callback receiving stream type, payload, and local receipt timestamp
     */
    public synchronized void start(String symbol, BinanceStreamListener listener) {
        start(symbol, listener, null);
    }

    /**
     * Starts all 5 Binance WebSocket streams for the given symbol, optionally bound to an external worker thread.
     *
     * @param symbol         trading pair symbol (e.g. BTCUSDT)
     * @param listener       callback receiving stream type, payload, and local receipt timestamp
     * @param externalWorker optional dedicated platform worker thread responsible for draining these streams
     */
    public synchronized void start(String symbol, BinanceStreamListener listener, Thread externalWorker) {
        stop();
        String normalizedSymbol = normalizeSymbol(symbol);

        for (BinanceStreamType type : BinanceStreamType.values()) {
            String wsUrl = type.buildWsUrl(spotWsBaseUrl, futuresWsBaseUrl, normalizedSymbol);
            String streamName = "BINANCE-" + type.name() + "-" + normalizedSymbol;
            RawWebSocketListener rawListener = new RawWebSocketListener() {
                @Override
                public void onMessage(String rawJson, long recvTs) {
                    if (listener != null && rawJson != null && !rawJson.isBlank()) {
                        listener.onStreamMessage(type, rawJson, recvTs);
                    }
                }

                @Override
                public void onMessageBytes(TimestampedFrame frame) {
                    if (listener != null && frame != null) {
                        listener.onStreamFrame(type, frame.rawUtf8Bytes(), frame.recvTs());
                    }
                }
            };
            ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                    binanceVertx,
                    streamName,
                    wsUrl,
                    List::of,
                    rawListener,
                    reconnectDelayMs,
                    pingIntervalMs
            );
            stream.setDedicatedIoThreadName(DEDICATED_BINANCE_IO_THREAD_NAME);
            stream.setDisconnectListener(this.disconnectListener);
            if (externalWorker != null) {
                stream.bindExternalWorker(externalWorker);
            }
            streams.put(type, stream);
            stream.start();
        }
    }

    /**
     * Drains all currently queued frames across the 5 Binance streams into {@code scratchBatch}.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return total number of frames drained across all 5 streams
     */
    public synchronized int drainStreams(List<TimestampedFrame> scratchBatch) {
        int total = 0;
        for (ReconnectingWebSocketStream stream : streams.values()) {
            total += stream.drainAvailableBatch(scratchBatch);
        }
        return total;
    }

    /**
     * Returns whether any of the 5 Binance streams currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if any Binance stream queue is non-empty
     */
    public synchronized boolean hasQueuedFrames() {
        for (ReconnectingWebSocketStream stream : streams.values()) {
            if (stream.hasQueuedFrames()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Stops and closes all active Binance WebSocket stream connections.
     */
    public synchronized void stop() {
        for (ReconnectingWebSocketStream stream : streams.values()) {
            stream.stop();
        }
        streams.clear();
    }

    /**
     * Returns an immutable snapshot of the connection state for each Binance stream type.
     *
     * @return map of stream type to its current {@link ConnectionState}
     */
    public synchronized Map<BinanceStreamType, ConnectionState> getConnectionStates() {
        Map<BinanceStreamType, ConnectionState> result = new EnumMap<>(BinanceStreamType.class);
        for (BinanceStreamType type : BinanceStreamType.values()) {
            ReconnectingWebSocketStream stream = streams.get(type);
            result.put(type, stream != null ? stream.getState() : ConnectionState.DISCONNECTED);
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Returns telemetry snapshots for all active Binance WebSocket streams.
     *
     * @return list of {@link WebSocketStreamTelemetry} snapshots
     */
    public synchronized List<WebSocketStreamTelemetry> getStreamTelemetries() {
        List<WebSocketStreamTelemetry> result = new ArrayList<>(streams.size());
        for (ReconnectingWebSocketStream stream : streams.values()) {
            result.add(stream.getTelemetrySnapshot());
        }
        return result;
    }

    /**
     * Returns the dedicated Netty I/O reader thread observed by any active Binance stream, or {@code null}.
     *
     * @return Netty I/O reader thread
     */
    public synchronized Thread getIoReaderThread() {
        for (ReconnectingWebSocketStream stream : streams.values()) {
            Thread t = stream.getIoReaderThread();
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    /**
     * Checks whether all 5 Binance streams are currently in the {@link ConnectionState#CONNECTED} state.
     *
     * @return true if all 5 streams are connected
     */
    public synchronized boolean isAllConnected() {
        if (streams.size() != BinanceStreamType.values().length) {
            return false;
        }
        for (ReconnectingWebSocketStream stream : streams.values()) {
            if (stream.getState() != ConnectionState.CONNECTED) {
                return false;
            }
        }
        return true;
    }

    /**
     * Normalizes a symbol such as "BTC" or "BTCUSDT" into a standard Binance pair symbol (defaulting to USDT quote).
     *
     * @param symbol input symbol
     * @return uppercase trading pair symbol (e.g. BTCUSDT)
     */
    public static String normalizeSymbol(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "BTCUSDT";
        }
        String clean = symbol.trim().toUpperCase().replace("/", "").replace("-", "");
        if (!clean.endsWith("USDT") && !clean.endsWith("USDC") && !clean.endsWith("USD")) {
            return clean + "USDT";
        }
        return clean;
    }
}
