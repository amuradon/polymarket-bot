package cz.polymarket.bot.connector.coinbase;

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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * Manages a resilient WebSocket connection to Coinbase Exchange / Coinbase Pro
 * ({@code wss://ws-feed.exchange.coinbase.com}) for real-time Spot {@code matches} (trades) streams
 * (e.g. {@code BTCUSD} / {@code BTC-USD}, {@code ETHUSD} / {@code ETH-USD}).
 * <p>
 * Isolates the Coinbase stream onto a dedicated single-thread Netty EventLoop ({@code coinbase-ws-io-reader})
 * with native Linux Epoll transport and BoringSSL ({@code netty-tcnative}), filtering out
 * {@code subscriptions} and initial {@code last_match} frames directly on raw {@code byte[]} without
 * {@link String} allocation overhead.
 */
@ApplicationScoped
public class CoinbaseStreamConnector {

    /**
     * Dedicated thread name for the isolated Netty EventLoop servicing the Coinbase Spot WebSocket stream.
     */
    public static final String DEDICATED_COINBASE_IO_THREAD_NAME = "coinbase-ws-io-reader";

    private static final byte[] TYPE_MATCH_COMPACT =
            "\"type\":\"match\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TYPE_MATCH_SPACED =
            "\"type\": \"match\"".getBytes(StandardCharsets.UTF_8);

    private final Vertx coinbaseVertx;
    private final String spotWsUrl;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private volatile ReconnectingWebSocketStream stream;
    private volatile String activeSymbol;
    private volatile WebSocketDisconnectListener disconnectListener;

    /**
     * Constructs the Coinbase Spot WebSocket connector with an isolated 1-thread Vert.x instance.
     *
     * @param vertx            shared Quarkus Vert.x instance
     * @param spotWsUrl        Coinbase Exchange WebSocket feed URL
     * @param reconnectDelayMs delay in ms before reconnecting a dropped stream
     * @param pingIntervalMs   interval in ms for sending WebSocket ping frames
     */
    @Inject
    public CoinbaseStreamConnector(
            Vertx vertx,
            @ConfigProperty(name = "polymarket.connector.coinbase.spot-ws-url", defaultValue = "wss://ws-feed.exchange.coinbase.com")
            String spotWsUrl,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "15000")
            long pingIntervalMs) {
        this.coinbaseVertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(1)
                .setPreferNativeTransport(true)
                .setAddressResolverOptions(new AddressResolverOptions()
                        .setRoundRobinInetAddress(true)
                        .setRotateServers(true)
                        .setCacheMaxTimeToLive(60)));
        if (!this.coinbaseVertx.isNativeTransportEnabled()) {
            Log.warnf("Native Linux Epoll transport is unavailable on coinbaseVertx (os=%s); falling back to JDK NIO EventLoop",
                    System.getProperty("os.name", "unknown"));
        } else {
            Log.infof("Initialized dedicated coinbaseVertx with native Linux Epoll transport (openSslAvailable=%b)",
                    OpenSsl.isAvailable());
        }
        this.spotWsUrl = spotWsUrl;
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    @PreDestroy
    public void destroy() {
        stop();
        try {
            coinbaseVertx.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Returns whether native Linux Epoll transport is active on the dedicated Coinbase Vert.x instance.
     *
     * @return {@code true} if native transport is enabled
     */
    public boolean isNativeTransportEnabled() {
        return coinbaseVertx.isNativeTransportEnabled();
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
     * Registers a listener notified whenever the Coinbase stream disconnects unexpectedly.
     *
     * @param disconnectListener listener invoked with the stream telemetry snapshot at the moment of disconnect
     */
    public synchronized void setDisconnectListener(WebSocketDisconnectListener disconnectListener) {
        this.disconnectListener = disconnectListener;
        ReconnectingWebSocketStream current = this.stream;
        if (current != null) {
            current.setDisconnectListener(disconnectListener);
        }
    }

    /**
     * Starts the Coinbase Spot {@code matches} WebSocket stream for the given symbol and routes live trade
     * payloads to {@code listener}.
     *
     * @param symbol   trading symbol (e.g. {@code BTCUSDT}, {@code BTCUSD}, {@code ETHUSD})
     * @param listener callback receiving normalized symbol, payload, and local receipt timestamp
     */
    public synchronized void start(String symbol, CoinbaseStreamListener listener) {
        start(symbol, listener, null);
    }

    /**
     * Starts the Coinbase Spot {@code matches} WebSocket stream for the given symbol, optionally bound
     * to an external worker thread.
     *
     * @param symbol         trading symbol (e.g. {@code BTCUSDT}, {@code BTCUSD}, {@code ETHUSD})
     * @param listener       callback receiving normalized symbol, payload, and local receipt timestamp
     * @param externalWorker optional dedicated platform worker thread responsible for draining this stream
     */
    public synchronized void start(String symbol, CoinbaseStreamListener listener, Thread externalWorker) {
        stop();
        String normSym = normalizeSymbol(symbol);
        String productId = toProductId(symbol);
        this.activeSymbol = normSym;
        String streamName = "COINBASE-SPOT-TRADES-" + normSym;
        String subscribeMsg = buildSubscribeMessage(productId);

        RawWebSocketListener rawListener = new RawWebSocketListener() {
            @Override
            public void onMessage(String rawJson, long recvTs) {
                if (listener != null && rawJson != null && !rawJson.isBlank()) {
                    byte[] bytes = rawJson.getBytes(StandardCharsets.UTF_8);
                    if (isMatchEventBytes(bytes)) {
                        listener.onStreamMessage(normSym, rawJson, recvTs);
                    }
                }
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
                if (isMatchEventBytes(rawBytes) && listener != null) {
                    listener.onStreamFrame(normSym, rawBytes, frame.recvTs());
                }
            }
        };

        ReconnectingWebSocketStream newStream = new ReconnectingWebSocketStream(
                coinbaseVertx,
                streamName,
                spotWsUrl,
                () -> List.of(subscribeMsg),
                rawListener,
                reconnectDelayMs,
                pingIntervalMs
        );
        newStream.setDedicatedIoThreadName(DEDICATED_COINBASE_IO_THREAD_NAME);
        newStream.setDisconnectListener(this.disconnectListener);
        if (externalWorker != null) {
            newStream.bindExternalWorker(externalWorker);
        }
        this.stream = newStream;
        newStream.start();
    }

    /**
     * Drains all currently queued frames from the Coinbase stream into {@code scratchBatch}.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return number of frames drained
     */
    public int drainStream(List<TimestampedFrame> scratchBatch) {
        ReconnectingWebSocketStream current = this.stream;
        return current != null ? current.drainAvailableBatch(scratchBatch) : 0;
    }

    /**
     * Returns whether the Coinbase stream currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if the Coinbase queue is non-empty
     */
    public boolean hasQueuedFrames() {
        ReconnectingWebSocketStream current = this.stream;
        return current != null && current.hasQueuedFrames();
    }

    /**
     * Stops and closes the Coinbase WebSocket stream.
     */
    public synchronized void stop() {
        ReconnectingWebSocketStream current = this.stream;
        this.stream = null;
        if (current != null) {
            current.stop();
        }
    }

    /**
     * Returns the current connection state of the Coinbase stream.
     *
     * @return current {@link ConnectionState}
     */
    public ConnectionState getState() {
        ReconnectingWebSocketStream current = this.stream;
        return current != null ? current.getState() : ConnectionState.DISCONNECTED;
    }

    /**
     * Checks whether the Coinbase stream is currently connected.
     *
     * @return {@code true} if connected
     */
    public boolean isConnected() {
        return getState() == ConnectionState.CONNECTED;
    }

    /**
     * Returns the currently active normalized symbol (e.g. {@code "BTCUSD"}).
     *
     * @return active normalized symbol, or {@code null} if not started
     */
    public String getActiveSymbol() {
        return activeSymbol;
    }

    /**
     * Returns telemetry snapshots for the active Coinbase WebSocket stream.
     *
     * @return list of {@link WebSocketStreamTelemetry} snapshots
     */
    public List<WebSocketStreamTelemetry> getStreamTelemetries() {
        ReconnectingWebSocketStream current = this.stream;
        return current != null ? List.of(current.getTelemetrySnapshot()) : List.of();
    }

    /**
     * Returns the dedicated Netty I/O reader thread observed by the active Coinbase stream, or {@code null}.
     *
     * @return Netty I/O reader thread
     */
    public Thread getIoReaderThread() {
        ReconnectingWebSocketStream current = this.stream;
        return current != null ? current.getIoReaderThread() : null;
    }

    /**
     * Normalizes a trading symbol (such as {@code "BTC"}, {@code "BTCUSDT"}, {@code "BTCUSD"}, or {@code "BTC-USD"})
     * into the canonical uppercase Coinbase folder/file symbol (e.g. {@code "BTCUSD"}, {@code "ETHUSD"}).
     *
     * @param symbol input symbol
     * @return canonical uppercase symbol without delimiters (e.g. {@code "BTCUSD"})
     */
    public static String normalizeSymbol(String symbol) {
        return toProductId(symbol).replace("-", "");
    }

    /**
     * Normalizes a trading symbol into the Coinbase Exchange product ID format ({@code "<BASE>-<QUOTE>"},
     * e.g. {@code "BTC-USD"}, {@code "ETH-USD"}, {@code "SOL-USD"}).
     *
     * @param symbol input symbol (e.g. {@code "BTCUSDT"}, {@code "BTCUSD"}, {@code "BTC-USD"}, {@code "BTC"})
     * @return Coinbase product ID (e.g. {@code "BTC-USD"})
     */
    public static String toProductId(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "BTC-USD";
        }
        String raw = symbol.trim().toUpperCase(Locale.ROOT).replace('/', '-').replace('_', '-');
        if (raw.contains("-")) {
            String[] parts = raw.split("-", 2);
            String base = normalizeBaseAlias(parts[0]);
            String quote = parts.length > 1 && !parts[1].isEmpty() ? parts[1] : "USD";
            if ("USDT".equals(quote) && !"USDT".equals(base)) {
                quote = "USD";
            }
            return base + "-" + quote;
        }
        String base;
        if (raw.endsWith("USDT") && raw.length() > 4) {
            base = raw.substring(0, raw.length() - 4);
        } else if (raw.endsWith("USDC") && raw.length() > 4) {
            base = raw.substring(0, raw.length() - 4);
        } else if (raw.endsWith("USD") && raw.length() > 3) {
            base = raw.substring(0, raw.length() - 3);
        } else {
            base = raw;
        }
        base = normalizeBaseAlias(base);
        return base + "-USD";
    }

    private static String normalizeBaseAlias(String base) {
        if ("XBT".equals(base) || "XXBT".equals(base)) {
            return "BTC";
        }
        if ("XDG".equals(base)) {
            return "DOGE";
        }
        return base;
    }

    static String buildSubscribeMessage(String productId) {
        return "{\"type\":\"subscribe\",\"product_ids\":[\"" + productId + "\"],\"channels\":[\"matches\"]}";
    }

    /**
     * Evaluates directly on UTF-8 {@code byte[]} whether the frame is a live Coinbase {@code match} event,
     * rejecting {@code subscriptions}, {@code last_match}, and {@code error} frames.
     *
     * @param rawBytes UTF-8 WebSocket frame bytes
     * @return {@code true} if the frame has {@code "type":"match"}
     */
    static boolean isMatchEventBytes(byte[] rawBytes) {
        int searchLimit = Math.min(rawBytes.length, 160);
        return containsAsciiPattern(rawBytes, searchLimit, TYPE_MATCH_COMPACT)
                || containsAsciiPattern(rawBytes, searchLimit, TYPE_MATCH_SPACED);
    }

    private static boolean containsAsciiPattern(byte[] haystack, int searchLimit, byte[] needle) {
        int nLen = needle.length;
        int lastStart = searchLimit - nLen;
        if (lastStart < 0) {
            return false;
        }
        byte first = needle[0];
        for (int i = 0; i <= lastStart; i++) {
            if (haystack[i] == first) {
                boolean match = true;
                for (int j = 1; j < nLen; j++) {
                    if (haystack[i + j] != needle[j]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    return true;
                }
            }
        }
        return false;
    }
}
