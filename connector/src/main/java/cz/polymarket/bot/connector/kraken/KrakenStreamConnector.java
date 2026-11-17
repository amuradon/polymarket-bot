package cz.polymarket.bot.connector.kraken;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.MarketWebSocketRuntime;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketDisconnectListener;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Manages resilient WebSocket v2 connections ({@code wss://ws.kraken.com/v2}) for Kraken Spot trade streams:
 * {@code USDTUSD} ({@code USDT/USD}) and the configured cryptocurrency symbol (e.g. {@code BTCUSD} / {@code BTC/USD},
 * {@code ETHUSD} / {@code ETH/USD}) on the consolidated {@link MarketWebSocketRuntime}
 * ({@code market-ws-io-reader} on {@code CPU #1}), filtering control, heartbeat, and snapshot frames directly
 * on raw {@code byte[]} without {@link String} allocation overhead.
 */
@ApplicationScoped
public class KrakenStreamConnector {

    /**
     * Canonical symbol identifier for the Kraken Spot USDT/USD stream.
     */
    public static final String USDTUSD_SYMBOL = "USDTUSD";

    private static final byte[] CHANNEL_TRADE_COMPACT =
            "\"channel\":\"trade\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CHANNEL_TRADE_SPACED =
            "\"channel\": \"trade\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TYPE_UPDATE_COMPACT =
            "\"type\":\"update\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TYPE_UPDATE_SPACED =
            "\"type\": \"update\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] METHOD_SUBSCRIBE_COMPACT =
            "\"method\":\"subscribe\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] METHOD_SUBSCRIBE_SPACED =
            "\"method\": \"subscribe\"".getBytes(StandardCharsets.UTF_8);

    private final MarketWebSocketRuntime marketWsRuntime;
    private final String spotWsUrl;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;
    private final Map<String, ReconnectingWebSocketStream> streams = new LinkedHashMap<>();
    private volatile WebSocketDisconnectListener disconnectListener;

    /**
     * Constructs the Kraken Spot multi-stream WebSocket connector backed by {@link MarketWebSocketRuntime}.
     *
     * @param marketWsRuntime  consolidated 1-EventLoop market WebSocket runtime
     * @param spotWsUrl        Kraken Spot WebSocket v2 endpoint URL
     * @param reconnectDelayMs delay in ms before reconnecting a dropped stream
     * @param pingIntervalMs   interval in ms for sending WebSocket ping frames
     */
    @Inject
    public KrakenStreamConnector(
            MarketWebSocketRuntime marketWsRuntime,
            @ConfigProperty(name = "polymarket.connector.kraken.spot-ws-url", defaultValue = "wss://ws.kraken.com/v2")
            String spotWsUrl,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "15000")
            long pingIntervalMs) {
        if (marketWsRuntime == null) {
            throw new IllegalArgumentException("marketWsRuntime cannot be null");
        }
        this.marketWsRuntime = marketWsRuntime;
        this.spotWsUrl = spotWsUrl;
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    /**
     * Stops active Kraken streams on bean destruction.
     */
    @PreDestroy
    public void destroy() {
        stop();
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
     * Registers a listener notified whenever any active Kraken stream disconnects unexpectedly.
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
     * Starts Kraken Spot trade WebSocket streams for {@code USDTUSD} and the normalized {@code symbol}
     * (e.g. {@code BTCUSD}, {@code ETHUSD}), routing live trade update payloads to {@code listener}.
     *
     * @param symbol   trading symbol (e.g. {@code BTCUSDT}, {@code BTCUSD}, {@code ETHUSD})
     * @param listener callback receiving normalized symbol, payload, and local receipt timestamp
     */
    public synchronized void start(String symbol, KrakenStreamListener listener) {
        start(symbol, true, listener, null);
    }

    /**
     * Starts Kraken Spot trade WebSocket streams for the normalized {@code symbol} and optionally {@code USDTUSD}.
     *
     * @param symbol         trading symbol (e.g. {@code BTCUSDT}, {@code BTCUSD}, {@code ETHUSD})
     * @param includeUsdtUsd whether to also subscribe to and collect the {@code USDTUSD} ({@code USDT/USD}) trade stream
     * @param listener       callback receiving normalized symbol, payload, and local receipt timestamp
     */
    public synchronized void start(String symbol, boolean includeUsdtUsd, KrakenStreamListener listener) {
        start(symbol, includeUsdtUsd, listener, null);
    }

    /**
     * Starts Kraken Spot trade WebSocket streams for the normalized {@code symbol} and optionally {@code USDTUSD},
     * optionally bound to an external worker thread.
     *
     * @param symbol         trading symbol (e.g. {@code BTCUSDT}, {@code BTCUSD}, {@code ETHUSD})
     * @param includeUsdtUsd whether to also subscribe to and collect the {@code USDTUSD} ({@code USDT/USD}) trade stream
     * @param listener       callback receiving normalized symbol, payload, and local receipt timestamp
     * @param externalWorker optional dedicated platform worker thread responsible for draining these streams
     */
    public synchronized void start(
            String symbol,
            boolean includeUsdtUsd,
            KrakenStreamListener listener,
            Thread externalWorker) {
        stop();

        Set<String> targetSymbols = new LinkedHashSet<>();
        if (includeUsdtUsd) {
            targetSymbols.add(USDTUSD_SYMBOL);
        }
        targetSymbols.add(normalizeSymbol(symbol));

        for (String normSym : targetSymbols) {
            String wsSymbol = toWsSymbol(normSym);
            String subscribeMsg = buildSubscribeMessage(wsSymbol);
            String streamName = "KRAKEN-SPOT-TRADES-" + normSym;

            RawWebSocketListener rawListener = new RawWebSocketListener() {
                @Override
                public void onMessage(String rawJson, long recvTs) {
                    if (listener != null && rawJson != null && !rawJson.isBlank()) {
                        byte[] bytes = rawJson.getBytes(StandardCharsets.UTF_8);
                        if (isTradeUpdateBytes(bytes)) {
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
                    if (isTradeUpdateBytes(rawBytes) && listener != null) {
                        listener.onStreamFrame(normSym, rawBytes, frame.recvTs());
                    }
                }
            };

            ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                    marketWsRuntime.vertx(),
                    streamName,
                    spotWsUrl,
                    () -> List.of(subscribeMsg),
                    rawListener,
                    reconnectDelayMs,
                    pingIntervalMs
            );
            stream.setDisconnectListener(this.disconnectListener);
            if (externalWorker != null) {
                stream.bindExternalWorker(externalWorker);
            }
            streams.put(normSym, stream);
            stream.start();
        }
    }

    /**
     * Drains all currently queued frames across all active Kraken streams into {@code scratchBatch}.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return total number of frames drained across all active Kraken streams
     */
    public synchronized int drainStreams(List<TimestampedFrame> scratchBatch) {
        int total = 0;
        for (ReconnectingWebSocketStream stream : streams.values()) {
            total += stream.drainAvailableBatch(scratchBatch);
        }
        return total;
    }

    /**
     * Returns whether any active Kraken stream currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if any Kraken stream queue is non-empty
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
     * Marks all active Kraken streams as parked prior to {@code LockSupport.parkNanos} and returns {@code true}
     * if frames arrived concurrently.
     *
     * @return {@code true} if frames arrived concurrently and parking must be aborted
     */
    public synchronized boolean prepareStreamsToPark() {
        for (ReconnectingWebSocketStream stream : streams.values()) {
            if (stream.prepareToParkAndCheckQueuedFrames()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Stops and closes all active Kraken WebSocket stream connections.
     */
    public synchronized void stop() {
        for (ReconnectingWebSocketStream stream : streams.values()) {
            stream.stop();
        }
        streams.clear();
    }

    /**
     * Returns an immutable snapshot of the connection state for each active Kraken stream symbol.
     *
     * @return map of symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"}) to its current {@link ConnectionState}
     */
    public synchronized Map<String, ConnectionState> getConnectionStates() {
        Map<String, ConnectionState> result = new LinkedHashMap<>();
        for (Map.Entry<String, ReconnectingWebSocketStream> entry : streams.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getState());
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Returns telemetry snapshots for all active Kraken WebSocket streams.
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
     * Returns the consolidated Netty I/O reader thread ({@code market-ws-io-reader}).
     *
     * @return Netty I/O reader thread
     */
    public Thread getIoReaderThread() {
        return marketWsRuntime.getIoReaderThread();
    }

    /**
     * Checks whether all active Kraken streams are currently in the {@link ConnectionState#CONNECTED} state.
     *
     * @return {@code true} if at least one stream is registered and all are connected
     */
    public synchronized boolean isAllConnected() {
        if (streams.isEmpty()) {
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
     * Normalizes a trading symbol (such as {@code "BTC"}, {@code "BTCUSDT"}, {@code "BTC/USD"}, {@code "XBT/USD"},
     * or {@code "USDTUSD"}) into the canonical uppercase Kraken folder/file symbol (e.g. {@code "BTCUSD"},
     * {@code "ETHUSD"}, {@code "USDTUSD"}).
     *
     * @param symbol input symbol
     * @return canonical uppercase symbol without delimiters (e.g. {@code "BTCUSD"})
     */
    public static String normalizeSymbol(String symbol) {
        String wsSym = toWsSymbol(symbol);
        return wsSym.replace("/", "");
    }

    /**
     * Normalizes a trading symbol into the Kraken WebSocket v2 pair format ({@code "<BASE>/<QUOTE>"},
     * e.g. {@code "BTC/USD"}, {@code "ETH/USD"}, {@code "USDT/USD"}).
     *
     * @param symbol input symbol (e.g. {@code "BTCUSDT"}, {@code "BTCUSD"}, {@code "BTC"}, {@code "USDTUSD"})
     * @return Kraken WS v2 symbol (e.g. {@code "BTC/USD"})
     */
    public static String toWsSymbol(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "BTC/USD";
        }
        String raw = symbol.trim().toUpperCase(Locale.ROOT).replace('-', '/').replace('_', '/');
        if (raw.contains("/")) {
            String[] parts = raw.split("/", 2);
            String base = normalizeBaseAlias(parts[0]);
            String quote = parts.length > 1 && !parts[1].isEmpty() ? parts[1] : "USD";
            if ("USDT".equals(quote) && !"USDT".equals(base)) {
                quote = "USD";
            }
            return base + "/" + quote;
        }
        if ("USDTUSD".equals(raw)) {
            return "USDT/USD";
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
        return base + "/USD";
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

    static String buildSubscribeMessage(String wsSymbol) {
        return "{\"method\":\"subscribe\",\"params\":{\"channel\":\"trade\",\"symbol\":[\""
                + wsSymbol + "\"],\"snapshot\":false}}";
    }

    /**
     * Evaluates directly on UTF-8 {@code byte[]} whether the frame is a Kraken WS v2 {@code trade} {@code update}
     * notification, rejecting {@code heartbeat}, {@code status}, {@code subscribe} confirmation, and {@code snapshot} frames.
     *
     * @param rawBytes UTF-8 WebSocket frame bytes
     * @return {@code true} if the frame is a live Kraken trade update message
     */
    static boolean isTradeUpdateBytes(byte[] rawBytes) {
        int len = rawBytes.length;
        int headLimit = Math.min(len, 96);
        if (containsAsciiPattern(rawBytes, 0, headLimit, METHOD_SUBSCRIBE_COMPACT)
                || containsAsciiPattern(rawBytes, 0, headLimit, METHOD_SUBSCRIBE_SPACED)) {
            return false;
        }
        boolean hasTradeChannel = containsAsciiPattern(rawBytes, 0, headLimit, CHANNEL_TRADE_COMPACT)
                || containsAsciiPattern(rawBytes, 0, headLimit, CHANNEL_TRADE_SPACED);
        if (!hasTradeChannel) {
            return false;
        }
        if (containsAsciiPattern(rawBytes, 0, headLimit, TYPE_UPDATE_COMPACT)
                || containsAsciiPattern(rawBytes, 0, headLimit, TYPE_UPDATE_SPACED)) {
            return true;
        }
        int tailStart = Math.max(0, len - 64);
        return containsAsciiPattern(rawBytes, tailStart, len, TYPE_UPDATE_COMPACT)
                || containsAsciiPattern(rawBytes, tailStart, len, TYPE_UPDATE_SPACED);
    }

    private static boolean containsAsciiPattern(byte[] haystack, int fromIdx, int toIdx, byte[] needle) {
        int nLen = needle.length;
        int lastStart = toIdx - nLen;
        if (lastStart < fromIdx) {
            return false;
        }
        byte first = needle[0];
        for (int i = fromIdx; i <= lastStart; i++) {
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
