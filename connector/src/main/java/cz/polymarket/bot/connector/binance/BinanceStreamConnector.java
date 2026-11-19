package cz.polymarket.bot.connector.binance;

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
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages resilient WebSocket connections for Binance Spot and USD-M Futures data streams on
 * {@link MarketWebSocketRuntime} ({@code market-ws-io-reader} on {@code CPU #1}), strictly isolating:
 * <ul>
 *   <li><b>Strategy {@code SPOT_AGG_TRADES} dual streams</b>: Two independent TCP/TLS connections pinned to
 *       distinct Tokyo edge IPv4 addresses ({@code slot 0} and {@code slot 1}) paired with a zero-allocation
 *       monotonic {@code aggTradeId} ({@code "a"}) fast-forward filter, drained lock-free on {@code CPU #2}.</li>
 *   <li><b>Collector {@code bookTicker} streams</b> ({@code SPOT_BOOK_TICKER}, {@code FUTURES_BOOK_TICKER}):
 *       Optional single-leg archival streams drained on {@code CPU #0} (inactive by default), completely
 *       isolated from Strategy's dual {@code SPOT_AGG_TRADES} lifecycle so zero duplicate frames can ever
 *       enter the Data Collector.</li>
 * </ul>
 */
@ApplicationScoped
public class BinanceStreamConnector {

    private static final Set<BinanceStreamType> SUPPORTED_BOOK_TICKER_TYPES = EnumSet.of(
            BinanceStreamType.SPOT_BOOK_TICKER,
            BinanceStreamType.FUTURES_BOOK_TICKER
    );

    private final MarketWebSocketRuntime marketWsRuntime;
    private final String spotWsBaseUrl;
    private final String futuresWsBaseUrl;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private final Map<BinanceStreamType, ReconnectingWebSocketStream> bookTickerStreams =
            new EnumMap<>(BinanceStreamType.class);
    private volatile ReconnectingWebSocketStream primarySpotAggTradesStream;
    private volatile ReconnectingWebSocketStream backupSpotAggTradesStream;
    private volatile WebSocketDisconnectListener disconnectListener;

    /**
     * Constructs the Binance multi-stream WebSocket connector backed by {@link MarketWebSocketRuntime}.
     *
     * @param marketWsRuntime  consolidated 1-EventLoop market WebSocket runtime
     * @param spotWsBaseUrl    Binance Spot WebSocket base URL
     * @param futuresWsBaseUrl Binance USD-M Futures WebSocket base URL
     * @param reconnectDelayMs delay in ms before reconnecting a dropped stream
     * @param pingIntervalMs   interval in ms for sending WebSocket ping frames
     */
    @Inject
    public BinanceStreamConnector(
            MarketWebSocketRuntime marketWsRuntime,
            @ConfigProperty(name = "polymarket.connector.binance.spot-ws-base-url", defaultValue = "wss://stream.binance.com:9443/ws")
            String spotWsBaseUrl,
            @ConfigProperty(name = "polymarket.connector.binance.futures-ws-base-url", defaultValue = "wss://fstream.binance.com")
            String futuresWsBaseUrl,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "15000")
            long pingIntervalMs) {
        if (marketWsRuntime == null) {
            throw new IllegalArgumentException("marketWsRuntime cannot be null");
        }
        this.marketWsRuntime = marketWsRuntime;
        this.spotWsBaseUrl = spotWsBaseUrl;
        this.futuresWsBaseUrl = futuresWsBaseUrl;
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    /**
     * Stops active Binance streams on bean destruction.
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
     * Registers a listener notified whenever any active Binance stream disconnects unexpectedly.
     *
     * @param disconnectListener listener invoked with the stream telemetry snapshot at the moment of disconnect
     */
    public synchronized void setDisconnectListener(WebSocketDisconnectListener disconnectListener) {
        this.disconnectListener = disconnectListener;
        for (ReconnectingWebSocketStream stream : bookTickerStreams.values()) {
            stream.setDisconnectListener(disconnectListener);
        }
        ReconnectingWebSocketStream primary = this.primarySpotAggTradesStream;
        if (primary != null) {
            primary.setDisconnectListener(disconnectListener);
        }
        ReconnectingWebSocketStream backup = this.backupSpotAggTradesStream;
        if (backup != null) {
            backup.setDisconnectListener(disconnectListener);
        }
    }

    /**
     * Starts single-leg Data Collector {@code bookTicker} WebSocket streams ({@code SPOT_BOOK_TICKER} and/or
     * {@code FUTURES_BOOK_TICKER}) on {@code CPU #0} without disturbing Strategy's {@code SPOT_AGG_TRADES} streams.
     *
     * @param symbol          trading pair symbol (e.g. {@code BTCUSDT})
     * @param bookTickerTypes subset of {@code SPOT_BOOK_TICKER} / {@code FUTURES_BOOK_TICKER}
     * @param listener        callback receiving stream type, payload, and local receipt timestamp
     */
    public synchronized void startBookTickerStreams(
            String symbol,
            Set<BinanceStreamType> bookTickerTypes,
            BinanceStreamListener listener) {
        stopBookTickerStreams();
        if (bookTickerTypes == null || bookTickerTypes.isEmpty()) {
            return;
        }
        for (BinanceStreamType type : bookTickerTypes) {
            if (!SUPPORTED_BOOK_TICKER_TYPES.contains(type)) {
                throw new IllegalArgumentException(
                        "startBookTickerStreams only supports SPOT_BOOK_TICKER and FUTURES_BOOK_TICKER, got: " + type);
            }
        }
        String normalizedSymbol = normalizeSymbol(symbol);
        for (BinanceStreamType type : bookTickerTypes) {
            String wsUrl = type.buildWsUrl(spotWsBaseUrl, futuresWsBaseUrl, normalizedSymbol);
            String streamName = "COLLECTOR-BINANCE-" + type.name() + "-" + normalizedSymbol;
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
                    marketWsRuntime.vertx(),
                    streamName,
                    wsUrl,
                    List::of,
                    rawListener,
                    reconnectDelayMs,
                    pingIntervalMs
            );
            stream.setDisconnectListener(this.disconnectListener);
            bookTickerStreams.put(type, stream);
            stream.start();
        }
    }

    /**
     * Stops only the Data Collector {@code bookTicker} streams without touching Strategy's {@code SPOT_AGG_TRADES}.
     */
    public synchronized void stopBookTickerStreams() {
        for (ReconnectingWebSocketStream stream : bookTickerStreams.values()) {
            stream.stop();
        }
        bookTickerStreams.clear();
    }

    /**
     * Returns an immutable snapshot of the connection states of enabled Data Collector {@code bookTicker} streams.
     *
     * @return map of {@code bookTicker} stream type to {@link ConnectionState}
     */
    public synchronized Map<BinanceStreamType, ConnectionState> getBookTickerConnectionStates() {
        Map<BinanceStreamType, ConnectionState> result = new EnumMap<>(BinanceStreamType.class);
        for (Map.Entry<BinanceStreamType, ReconnectingWebSocketStream> entry : bookTickerStreams.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getState());
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Starts a dual Active-Active pair of Strategy Binance Spot {@code aggTrades}
     * ({@link BinanceStreamType#SPOT_AGG_TRADES}) WebSocket connections pinned to distinct Tokyo edge IPv4
     * addresses ({@code slot 0} and {@code slot 1}) with monotonic {@code aggTradeId} ({@code "a"}) fast-forward
     * filtering, without touching Data Collector {@code bookTicker} streams.
     *
     * @param symbol         trading pair symbol (e.g. {@code BTCUSDT})
     * @param batchListener  listener invoked with strictly forward-progressing {@code aggTrade} frame batches
     * @param externalWorker optional dedicated platform worker thread (e.g. {@code market-strategy-worker})
     */
    public synchronized void startSpotAggTradesBatch(
            String symbol,
            RawWebSocketBatchListener batchListener,
            Thread externalWorker) {
        stopSpotAggTrades();
        String normalizedSymbol = normalizeSymbol(symbol);
        String wsUrl = BinanceStreamType.SPOT_AGG_TRADES.buildWsUrl(spotWsBaseUrl, futuresWsBaseUrl, normalizedSymbol);
        String primaryName = "BINANCE-" + BinanceStreamType.SPOT_AGG_TRADES.name() + "-" + normalizedSymbol;
        String backupName = primaryName + "-BACKUP";

        AggTradeFastForwardFilter fastForwardFilter = new AggTradeFastForwardFilter();
        ReconnectingWebSocketStream[] primaryHolder = new ReconnectingWebSocketStream[1];
        ReconnectingWebSocketStream[] backupHolder = new ReconnectingWebSocketStream[1];

        RawWebSocketBatchListener primaryBatchListener = batch ->
                dispatchFastForwardedBatch(batch, primaryHolder[0], fastForwardFilter, batchListener);
        RawWebSocketBatchListener backupBatchListener = batch ->
                dispatchFastForwardedBatch(batch, backupHolder[0], fastForwardFilter, batchListener);

        ReconnectingWebSocketStream primary = new ReconnectingWebSocketStream(
                marketWsRuntime.vertx(),
                primaryName,
                wsUrl,
                List::of,
                null,
                primaryBatchListener,
                reconnectDelayMs,
                pingIntervalMs,
                null
        );
        primaryHolder[0] = primary;
        primary.setPreferredAddressSlot(marketWsRuntime, 0, () -> {
            ReconnectingWebSocketStream peer = backupHolder[0];
            return peer != null ? peer.getActiveOrPendingRemoteIp() : "";
        });
        primary.setDisconnectListener(this.disconnectListener);

        ReconnectingWebSocketStream backup = new ReconnectingWebSocketStream(
                marketWsRuntime.vertx(),
                backupName,
                wsUrl,
                List::of,
                null,
                backupBatchListener,
                reconnectDelayMs,
                pingIntervalMs,
                null
        );
        backupHolder[0] = backup;
        backup.setPreferredAddressSlot(marketWsRuntime, 1, () -> {
            ReconnectingWebSocketStream peer = primaryHolder[0];
            return peer != null ? peer.getActiveOrPendingRemoteIp() : "";
        });
        backup.setDisconnectListener(this.disconnectListener);

        if (externalWorker != null) {
            primary.bindExternalWorker(externalWorker);
            backup.bindExternalWorker(externalWorker);
        }

        this.primarySpotAggTradesStream = primary;
        this.backupSpotAggTradesStream = backup;
        Log.infof("Opening dual multi-path Binance SPOT_AGG_TRADES WebSocket streams for %s (edgeSlots=0/1, monotonicFastForward=true)",
                normalizedSymbol);
        primary.start();
        backup.start();
    }

    /**
     * Stops and clears Strategy's dual {@code SPOT_AGG_TRADES} streams (used when switching Strategy to {@code OFF})
     * without disturbing Data Collector {@code bookTicker} streams.
     */
    public synchronized void stopSpotAggTrades() {
        ReconnectingWebSocketStream primary = this.primarySpotAggTradesStream;
        ReconnectingWebSocketStream backup = this.backupSpotAggTradesStream;
        this.primarySpotAggTradesStream = null;
        this.backupSpotAggTradesStream = null;
        if (primary != null) {
            primary.stop();
        }
        if (backup != null) {
            backup.stop();
        }
    }

    private static void dispatchFastForwardedBatch(
            List<TimestampedFrame> batch,
            ReconnectingWebSocketStream sourceStream,
            AggTradeFastForwardFilter filter,
            RawWebSocketBatchListener delegate) {
        if (delegate == null || batch == null || batch.isEmpty()) {
            return;
        }
        int skipped = filter.filterStrictlyNewerInPlace(batch);
        if (skipped > 0 && sourceStream != null) {
            sourceStream.addCoalescedFrames(skipped);
        }
        if (!batch.isEmpty()) {
            delegate.onMessageBatch(batch);
        }
    }

    /**
     * Drains all currently queued Strategy {@code SPOT_AGG_TRADES} frames across both primary and backup legs
     * into {@code scratchBatch} completely lock-free.
     *
     * @param scratchBatch reusable pre-allocated list owned by the caller
     * @return total number of frames drained
     */
    public int drainStreams(List<TimestampedFrame> scratchBatch) {
        int total = 0;
        ReconnectingWebSocketStream primary = this.primarySpotAggTradesStream;
        if (primary != null) {
            total += primary.drainAvailableBatch(scratchBatch);
        }
        ReconnectingWebSocketStream backup = this.backupSpotAggTradesStream;
        if (backup != null) {
            total += backup.drainAvailableBatch(scratchBatch);
        }
        return total;
    }

    /**
     * Returns whether either Strategy {@code SPOT_AGG_TRADES} leg currently has frames waiting in its inbound queue.
     *
     * @return {@code true} if either {@code SPOT_AGG_TRADES} queue is non-empty
     */
    public boolean hasQueuedFrames() {
        ReconnectingWebSocketStream primary = this.primarySpotAggTradesStream;
        if (primary != null && primary.hasQueuedFrames()) {
            return true;
        }
        ReconnectingWebSocketStream backup = this.backupSpotAggTradesStream;
        return backup != null && backup.hasQueuedFrames();
    }

    /**
     * Marks Strategy {@code SPOT_AGG_TRADES} legs as parked prior to {@code LockSupport.parkNanos} and returns
     * {@code true} if frames arrived concurrently.
     *
     * @return {@code true} if frames arrived concurrently and parking must be aborted
     */
    public boolean prepareStreamsToPark() {
        ReconnectingWebSocketStream primary = this.primarySpotAggTradesStream;
        if (primary != null && primary.prepareToParkAndCheckQueuedFrames()) {
            return true;
        }
        ReconnectingWebSocketStream backup = this.backupSpotAggTradesStream;
        return backup != null && backup.prepareToParkAndCheckQueuedFrames();
    }

    /**
     * Stops and closes all active Strategy and Data Collector Binance WebSocket stream connections.
     */
    public synchronized void stop() {
        stopSpotAggTrades();
        stopBookTickerStreams();
    }

    /**
     * Returns the effective connection state of Strategy's dual {@code SPOT_AGG_TRADES} stream, reporting
     * {@link ConnectionState#CONNECTED} when either leg is connected.
     *
     * @return effective {@link ConnectionState} of {@code SPOT_AGG_TRADES}
     */
    public ConnectionState getSpotAggTradesState() {
        ReconnectingWebSocketStream primary = this.primarySpotAggTradesStream;
        ConnectionState effective = primary != null ? primary.getState() : ConnectionState.DISCONNECTED;
        if (effective != ConnectionState.CONNECTED) {
            ReconnectingWebSocketStream backup = this.backupSpotAggTradesStream;
            if (backup != null && (backup.getState() == ConnectionState.CONNECTED || effective == ConnectionState.DISCONNECTED)) {
                effective = backup.getState();
            }
        }
        return effective;
    }

    /**
     * Returns telemetry snapshots for all active Strategy {@code SPOT_AGG_TRADES} and Collector {@code bookTicker} streams.
     *
     * @return list of {@link WebSocketStreamTelemetry} snapshots
     */
    public synchronized List<WebSocketStreamTelemetry> getStreamTelemetries() {
        List<WebSocketStreamTelemetry> result = new ArrayList<>(bookTickerStreams.size() + 2);
        ReconnectingWebSocketStream primary = this.primarySpotAggTradesStream;
        if (primary != null) {
            result.add(primary.getTelemetrySnapshot());
        }
        ReconnectingWebSocketStream backup = this.backupSpotAggTradesStream;
        if (backup != null) {
            result.add(backup.getTelemetrySnapshot());
        }
        for (ReconnectingWebSocketStream stream : bookTickerStreams.values()) {
            result.add(stream.getTelemetrySnapshot());
        }
        return result;
    }

    /**
     * Evaluates the live connection state of the peer leg when a dual multi-path Binance {@code SPOT_AGG_TRADES}
     * stream disconnects.
     *
     * @param streamName identifier of the stream that disconnected
     * @return {@link StreamRedundancyStatus} describing whether the peer leg is actively covering failover
     */
    public StreamRedundancyStatus describeStreamRedundancy(String streamName) {
        ReconnectingWebSocketStream primary = this.primarySpotAggTradesStream;
        ReconnectingWebSocketStream backup = this.backupSpotAggTradesStream;
        if (streamName == null || primary == null || backup == null) {
            return StreamRedundancyStatus.NONE;
        }
        ReconnectingWebSocketStream peer;
        if (streamName.equals(primary.getStreamName())) {
            peer = backup;
        } else if (streamName.equals(backup.getStreamName())) {
            peer = primary;
        } else {
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
     * Returns the consolidated Netty I/O reader thread ({@code market-ws-io-reader}).
     *
     * @return Netty I/O reader thread
     */
    public Thread getIoReaderThread() {
        return marketWsRuntime.getIoReaderThread();
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

    /**
     * Extracts the strictly monotonic integer {@code aggTrade} ID ({@code "a":<digits>}) directly from raw
     * UTF-8 JSON bytes without {@link String} or Jackson parser allocation.
     *
     * @param rawBytes UTF-8 JSON payload bytes
     * @return parsed non-negative {@code aggTradeId}, or {@code -1L} if not present
     */
    static long extractAggTradeId(byte[] rawBytes) {
        if (rawBytes == null || rawBytes.length < 6) {
            return -1L;
        }
        int limit = rawBytes.length - 4;
        for (int i = 0; i <= limit; i++) {
            if (rawBytes[i] == '"' && rawBytes[i + 1] == 'a' && rawBytes[i + 2] == '"') {
                int pos = i + 3;
                while (pos < rawBytes.length && (rawBytes[pos] == ' ' || rawBytes[pos] == '\t')) {
                    pos++;
                }
                if (pos >= rawBytes.length || rawBytes[pos] != ':') {
                    continue;
                }
                pos++;
                while (pos < rawBytes.length && (rawBytes[pos] == ' ' || rawBytes[pos] == '\t')) {
                    pos++;
                }
                if (pos >= rawBytes.length || rawBytes[pos] < '0' || rawBytes[pos] > '9') {
                    continue;
                }
                long val = 0L;
                while (pos < rawBytes.length) {
                    byte c = rawBytes[pos];
                    if (c < '0' || c > '9') {
                        break;
                    }
                    val = val * 10L + (c - '0');
                    pos++;
                }
                return val;
            }
        }
        return -1L;
    }

    /**
     * Monotonic {@code aggTradeId} fast-forward filter that drops duplicate frames across dual Tokyo TCP paths
     * as well as delayed/retransmitted out-of-order frames ({@code aggTradeId <= maxSeenAggTradeId}) released
     * after a TCP out-of-order stall.
     */
    static final class AggTradeFastForwardFilter {

        private long maxSeenAggTradeId = -1L;

        synchronized int filterStrictlyNewerInPlace(List<TimestampedFrame> batch) {
            int n = batch.size();
            int writeIdx = 0;
            for (int i = 0; i < n; i++) {
                TimestampedFrame frame = batch.get(i);
                long id = extractAggTradeId(frame.rawUtf8Bytes());
                if (id < 0L || id > maxSeenAggTradeId) {
                    if (id > maxSeenAggTradeId) {
                        maxSeenAggTradeId = id;
                    }
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
    }
}
