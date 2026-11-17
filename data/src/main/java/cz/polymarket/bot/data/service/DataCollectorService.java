package cz.polymarket.bot.data.service;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.binance.BinanceStreamListener;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import cz.polymarket.bot.data.storage.CollectorWriteCoordinator;
import cz.polymarket.bot.data.storage.CollectorWriteMode;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.data.web.CollectorStatusDto;
import cz.polymarket.bot.data.web.CutoverActivateResponseDto;
import cz.polymarket.bot.data.web.CutoverFreezeResponseDto;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Main orchestrator service for the Data Collector application.
 * Always starts recording Polymarket PolyBolt and CLOB WebSocket streams on application startup (plus optional
 * Binance Spot/Futures {@code bookTicker} streams when explicitly enabled) using zero-decode {@code byte[]}
 * Zstandard writing, supports Blue-Green STANDBY warm-up and atomic timestamp cutover via
 * {@link CollectorWriteCoordinator}, schedules periodic Polymarket market discovery/resolution checks and
 * Zstandard frame flushes, and exposes runtime status.
 */
@ApplicationScoped
public class DataCollectorService {

    private final BinanceStreamConnector binanceConnector;
    private final DailyBinanceStorageManager binanceStorage;
    private final DailyPolyBoltStorageManager polyBoltDailyStorage;
    private final PolymarketLifecycleCoordinator polymarketCoordinator;
    private final PolymarketMarketStorageManager polymarketStorage;
    private final PolymarketPolyBoltClient polyBoltClient;
    private final PolymarketClobClient clobClient;
    private final CollectorWriteCoordinator writeCoordinator;
    private final Vertx vertx;
    private final String symbol;
    private final String timeframe;
    private final boolean binanceSpotBookTickerEnabled;
    private final boolean binanceFuturesBookTickerEnabled;
    private final int publicPort;

    private volatile HttpServer publicHttpServer;

    /**
     * Constructs the Data Collector orchestrator service.
     *
     * @param binanceConnector                Binance {@code bookTicker} WebSocket connector (used when enabled)
     * @param binanceStorage                  daily Zstd storage manager for Binance {@code bookTicker} streams
     * @param polyBoltDailyStorage            daily Zstd storage manager for PolyBolt {@code price.crypto} and {@code price.crypto.twap} streams
     * @param polymarketCoordinator           lifecycle coordinator for Polymarket markets
     * @param polymarketStorage               per-market Zstd storage manager for Polymarket
     * @param polyBoltClient                  Polymarket PolyBolt v2 WebSocket client
     * @param clobClient                      Polymarket CLOB WebSocket client
     * @param writeCoordinator                Blue-Green write mode and standby buffer coordinator
     * @param vertx                           Vert.x instance used to bind the optional public status listener on port 443
     * @param symbol                          configured trading symbol (default BTCUSDT)
     * @param timeframe                       configured Polymarket market timeframe (default 15m)
     * @param binanceSpotBookTickerEnabled    whether to connect and persist Binance Spot {@code bookTicker} (default false)
     * @param binanceFuturesBookTickerEnabled whether to connect and persist Binance USD-M Futures {@code bookTicker} (default false)
     * @param publicPort                      optional public HTTP port for {@code /api/collector/status} bound only when ACTIVE (default 0 = disabled)
     */
    @Inject
    public DataCollectorService(
            BinanceStreamConnector binanceConnector,
            DailyBinanceStorageManager binanceStorage,
            DailyPolyBoltStorageManager polyBoltDailyStorage,
            PolymarketLifecycleCoordinator polymarketCoordinator,
            PolymarketMarketStorageManager polymarketStorage,
            PolymarketPolyBoltClient polyBoltClient,
            PolymarketClobClient clobClient,
            CollectorWriteCoordinator writeCoordinator,
            Vertx vertx,
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol,
            @ConfigProperty(name = "polymarket.collector.timeframe", defaultValue = "15m")
            String timeframe,
            @ConfigProperty(name = "polymarket.collector.binance.spot-book-ticker-enabled", defaultValue = "false")
            boolean binanceSpotBookTickerEnabled,
            @ConfigProperty(name = "polymarket.collector.binance.futures-book-ticker-enabled", defaultValue = "false")
            boolean binanceFuturesBookTickerEnabled,
            @ConfigProperty(name = "polymarket.collector.public-port", defaultValue = "0")
            int publicPort) {
        this.binanceConnector = binanceConnector;
        this.binanceStorage = binanceStorage;
        this.polyBoltDailyStorage = polyBoltDailyStorage;
        this.polymarketCoordinator = polymarketCoordinator;
        this.polymarketStorage = polymarketStorage;
        this.polyBoltClient = polyBoltClient;
        this.clobClient = clobClient;
        this.writeCoordinator = writeCoordinator;
        this.vertx = vertx;
        this.symbol = BinanceStreamConnector.normalizeSymbol(symbol);
        this.timeframe = timeframe;
        this.binanceSpotBookTickerEnabled = binanceSpotBookTickerEnabled;
        this.binanceFuturesBookTickerEnabled = binanceFuturesBookTickerEnabled;
        this.publicPort = publicPort;
        this.binanceConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.polyBoltClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.clobClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
    }

    void onStart(@Observes StartupEvent event) {
        Set<BinanceStreamType> enabledBinanceStreams = enabledBinanceStreams();
        Log.infof("Starting Data Collector for symbol=%s, polymarketTimeframe=%s, binanceSpotBookTickerEnabled=%s, binanceFuturesBookTickerEnabled=%s, writeMode=%s...",
                symbol, timeframe, binanceSpotBookTickerEnabled, binanceFuturesBookTickerEnabled, writeCoordinator.getWriteMode());
        try {
            polyBoltClient.startCryptoPriceStream(symbol, new RawWebSocketListener() {
                @Override
                public void onMessage(String rawJson, long recvTs) {
                    writeCoordinator.recordPolyBoltCryptoPrice(rawJson, recvTs);
                }

                @Override
                public void onMessageBytes(TimestampedFrame frame) {
                    if (frame != null) {
                        writeCoordinator.recordPolyBoltCryptoPriceBytes(frame.rawUtf8Bytes(), frame.recvTs());
                    }
                }
            });
            polyBoltClient.startTwapStream(symbol, new RawWebSocketListener() {
                @Override
                public void onMessage(String rawJson, long recvTs) {
                    writeCoordinator.recordPolyBoltTwap(rawJson, recvTs);
                }

                @Override
                public void onMessageBytes(TimestampedFrame frame) {
                    if (frame != null) {
                        writeCoordinator.recordPolyBoltTwapBytes(frame.rawUtf8Bytes(), frame.recvTs());
                    }
                }
            });
            if (!enabledBinanceStreams.isEmpty()) {
                binanceConnector.start(symbol, enabledBinanceStreams, new BinanceStreamListener() {
                    @Override
                    public void onStreamMessage(BinanceStreamType streamType, String rawJson, long recvTimestampMs) {
                        writeCoordinator.recordBinance(streamType, rawJson, recvTimestampMs);
                    }

                    @Override
                    public void onStreamFrame(BinanceStreamType streamType, byte[] rawUtf8Bytes, long recvTimestampMs) {
                        writeCoordinator.recordBinanceBytes(streamType, rawUtf8Bytes, recvTimestampMs);
                    }
                }, null);
            }
            polymarketCoordinator.start(Instant.now().getEpochSecond());

            if (writeCoordinator.getWriteMode() == CollectorWriteMode.ACTIVE) {
                bindPublicPortIfConfigured();
            }
        } catch (Exception e) {
            Log.errorf("Fatal error starting Data Collector: %s. Terminating application.", e.getMessage());
            Quarkus.asyncExit(1);
            throw e;
        }
    }

    void onStop(@Observes ShutdownEvent event) {
        Log.info("Shutting down Data Collector and closing Zstd streams...");
        unbindPublicPort();
        binanceConnector.stop();
        polymarketCoordinator.stop();
        polyBoltClient.stop();
        writeCoordinator.closeAll();
    }

    private void onStreamUnexpectedDisconnect(WebSocketStreamTelemetry snapshot) {
        if (snapshot == null) {
            return;
        }
        String remoteLabel = snapshot.remoteAddress().isEmpty() ? "unknown" : snapshot.remoteAddress();
        Log.warnf("Data Collector stream disconnect telemetry [%s]: remote=%s, closeCode=%d, reason=%s, queueDepth=%d, maxQueueDepth=%d, maxQueueLagMs=%d, totalFrames=%d, reconnectCount=%d",
                snapshot.streamName(),
                remoteLabel,
                snapshot.lastCloseCode(),
                snapshot.lastCloseReason(),
                snapshot.queueDepth(),
                snapshot.maxQueueDepth(),
                snapshot.maxQueueLagMs(),
                snapshot.totalFramesReceived(),
                snapshot.reconnectCount());
    }

    /**
     * Periodically checks ended Polymarket markets for resolution and opens streams for new future markets.
     */
    @Scheduled(every = "${polymarket.collector.discovery-interval:30s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void scheduledMarketDiscovery() {
        polymarketCoordinator.discoverAndRefreshMarkets(Instant.now().getEpochSecond());
    }

    /**
     * Periodically flushes active Zstandard frames to disk so all buffered records are safely persisted.
     */
    @Scheduled(every = "${polymarket.collector.flush-interval:5s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void scheduledFlush() {
        writeCoordinator.flushAllIfActive();
    }

    /**
     * Freezes this collector instance at {@code cutoverTimestampMs}, flushing and closing all open {@code .jsonl.zst} files
     * and releasing the public HTTP port so the incoming Green instance can take over cleanly.
     *
     * @param cutoverTimestampMs epoch millisecond cutoff timestamp
     * @return freeze result summary
     */
    public CutoverFreezeResponseDto freeze(long cutoverTimestampMs) {
        unbindPublicPort();
        return writeCoordinator.freeze(cutoverTimestampMs);
    }

    /**
     * Activates this collector instance from {@code STANDBY} into {@code ACTIVE}, replaying buffered events
     * with {@code recvTimestampMs > afterTimestampMs} and binding the public HTTP status listener.
     *
     * @param afterTimestampMs cutover timestamp from the frozen Blue instance
     * @return activation result summary
     */
    public CutoverActivateResponseDto activate(long afterTimestampMs) {
        CutoverActivateResponseDto response = writeCoordinator.activate(afterTimestampMs);
        bindPublicPortIfConfigured();
        return response;
    }

    /**
     * Builds a diagnostic status snapshot of all collector streams, write mode, readiness, and storage writers.
     *
     * @return current {@link CollectorStatusDto}
     */
    public CollectorStatusDto getStatus() {
        Set<BinanceStreamType> enabledBinanceStreams = enabledBinanceStreams();
        Map<BinanceStreamType, ConnectionState> rawBinanceStates = enabledBinanceStreams.isEmpty()
                ? Map.of()
                : binanceConnector.getConnectionStates();
        Map<String, String> binanceStates = new LinkedHashMap<>();
        if (rawBinanceStates != null) {
            for (BinanceStreamType type : enabledBinanceStreams) {
                ConnectionState state = rawBinanceStates.get(type);
                binanceStates.put(type.name(), state != null ? state.name() : ConnectionState.DISCONNECTED.name());
            }
        }

        Map<String, ConnectionState> rawClobStates = clobClient.getConnectionStates();
        Map<String, String> clobStates = new LinkedHashMap<>();
        if (rawClobStates != null) {
            rawClobStates.forEach((slug, state) -> clobStates.put(slug, state.name()));
        }

        ConnectionState polyBoltTwapState = polyBoltClient.getTwapState();
        ConnectionState polyBoltCryptoState = polyBoltClient.getCryptoPriceState();
        List<String> trackedSlugs = polymarketCoordinator.getTrackedMarketSlugs();

        long binanceWritten = binanceStorage.getTotalRecordsWritten();
        long polyBoltWritten = polyBoltDailyStorage.getTotalRecordsWritten();
        long polymarketWritten = polymarketStorage.getTotalRecordsWritten();
        long standbyBuffered = writeCoordinator.getStandbyBufferedRecords();
        long totalProcessedOrBuffered = binanceWritten + polyBoltWritten + polymarketWritten + standbyBuffered;

        boolean readyForCutover = evaluateReadyForCutover(
                enabledBinanceStreams,
                rawBinanceStates,
                polyBoltTwapState,
                polyBoltCryptoState,
                trackedSlugs,
                rawClobStates,
                totalProcessedOrBuffered
        );

        return new CollectorStatusDto(
                symbol,
                timeframe,
                writeCoordinator.getWriteMode().name(),
                readyForCutover,
                standbyBuffered,
                binanceSpotBookTickerEnabled,
                binanceFuturesBookTickerEnabled,
                binanceStates,
                polyBoltTwapState != null ? polyBoltTwapState.name() : ConnectionState.DISCONNECTED.name(),
                polyBoltCryptoState != null ? polyBoltCryptoState.name() : ConnectionState.DISCONNECTED.name(),
                clobStates,
                trackedSlugs,
                binanceWritten,
                polyBoltWritten,
                polymarketWritten
        );
    }

    private Set<BinanceStreamType> enabledBinanceStreams() {
        EnumSet<BinanceStreamType> streams = EnumSet.noneOf(BinanceStreamType.class);
        if (binanceSpotBookTickerEnabled) {
            streams.add(BinanceStreamType.SPOT_BOOK_TICKER);
        }
        if (binanceFuturesBookTickerEnabled) {
            streams.add(BinanceStreamType.FUTURES_BOOK_TICKER);
        }
        return streams;
    }

    private boolean evaluateReadyForCutover(
            Set<BinanceStreamType> enabledBinanceStreams,
            Map<BinanceStreamType, ConnectionState> binanceStates,
            ConnectionState polyBoltTwapState,
            ConnectionState polyBoltCryptoState,
            List<String> trackedSlugs,
            Map<String, ConnectionState> clobStates,
            long totalProcessedOrBuffered) {
        if (totalProcessedOrBuffered <= 0L) {
            return false;
        }
        if (!enabledBinanceStreams.isEmpty()) {
            if (binanceStates == null) {
                return false;
            }
            for (BinanceStreamType type : enabledBinanceStreams) {
                if (binanceStates.get(type) != ConnectionState.CONNECTED) {
                    return false;
                }
            }
        }
        if (polyBoltTwapState != ConnectionState.CONNECTED
                || polyBoltCryptoState != ConnectionState.CONNECTED) {
            return false;
        }
        if (trackedSlugs == null || trackedSlugs.isEmpty() || clobStates == null) {
            return false;
        }
        for (String slug : trackedSlugs) {
            if (clobStates.get(slug) != ConnectionState.CONNECTED) {
                return false;
            }
        }
        return true;
    }

    private synchronized void bindPublicPortIfConfigured() {
        if (publicPort <= 0 || publicHttpServer != null || vertx == null) {
            return;
        }
        try {
            HttpServer server = vertx.createHttpServer();
            if (server == null) {
                return;
            }
            server.requestHandler(req -> {
                if ("/api/collector/status".equals(req.path())) {
                    String json = Json.encode(getStatus());
                    req.response()
                            .setStatusCode(200)
                            .putHeader("Content-Type", "application/json")
                            .end(json);
                } else {
                    req.response().setStatusCode(404).end();
                }
            });
            server.listen(publicPort, "0.0.0.0")
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            this.publicHttpServer = server;
            Log.infof("Bound public collector status HTTP listener on 0.0.0.0:%d", publicPort);
        } catch (Exception e) {
            Log.warnf("Could not bind public collector status port %d: %s", publicPort, e.getMessage());
        }
    }

    private synchronized void unbindPublicPort() {
        HttpServer server = this.publicHttpServer;
        this.publicHttpServer = null;
        if (server != null) {
            try {
                server.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
                Log.infof("Released public collector status HTTP listener on port %d", publicPort);
            } catch (Exception e) {
                Log.warnf("Error closing public collector status listener on port %d: %s", publicPort, e.getMessage());
            }
        }
    }
}
