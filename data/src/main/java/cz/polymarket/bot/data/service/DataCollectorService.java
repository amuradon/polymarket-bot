package cz.polymarket.bot.data.service;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.binance.BinanceStreamListener;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamConnector;
import cz.polymarket.bot.connector.coinbase.CoinbaseStreamListener;
import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
import cz.polymarket.bot.connector.kraken.KrakenStreamListener;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import cz.polymarket.bot.data.storage.CollectorWriteCoordinator;
import cz.polymarket.bot.data.storage.CollectorWriteMode;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyCoinbaseStorageManager;
import cz.polymarket.bot.data.storage.DailyKrakenStorageManager;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Main orchestrator service for the Data Collector application.
 * Always starts recording Binance, Kraken, Coinbase, and Polymarket WebSocket streams on application startup
 * using zero-decode {@code byte[]} Zstandard writing, supports Blue-Green STANDBY warm-up and atomic timestamp
 * cutover via {@link CollectorWriteCoordinator}, schedules periodic Polymarket market discovery/resolution
 * checks and Zstandard frame flushes, and exposes runtime status.
 */
@ApplicationScoped
public class DataCollectorService {

    private final BinanceStreamConnector binanceConnector;
    private final DailyBinanceStorageManager binanceStorage;
    private final KrakenStreamConnector krakenConnector;
    private final DailyKrakenStorageManager krakenStorage;
    private final CoinbaseStreamConnector coinbaseConnector;
    private final DailyCoinbaseStorageManager coinbaseStorage;
    private final DailyPolyBoltStorageManager polyBoltDailyStorage;
    private final PolymarketLifecycleCoordinator polymarketCoordinator;
    private final PolymarketMarketStorageManager polymarketStorage;
    private final PolymarketPolyBoltClient polyBoltClient;
    private final PolymarketClobClient clobClient;
    private final CollectorWriteCoordinator writeCoordinator;
    private final Vertx vertx;
    private final String symbol;
    private final String krakenPrimarySymbol;
    private final String timeframe;
    private final boolean krakenUsdtUsdEnabled;
    private final int publicPort;

    private volatile HttpServer publicHttpServer;

    /**
     * Constructs the Data Collector orchestrator service.
     *
     * @param binanceConnector      Binance Spot & Futures {@code aggTrades} and {@code bookTicker} WebSocket connector
     * @param binanceStorage        daily Zstd storage manager for Binance streams
     * @param krakenConnector       Kraken Spot trade WebSocket connector
     * @param krakenStorage         daily Zstd storage manager for Kraken Spot trades
     * @param coinbaseConnector     Coinbase Spot trade WebSocket connector
     * @param coinbaseStorage       daily Zstd storage manager for Coinbase Spot trades
     * @param polyBoltDailyStorage  daily Zstd storage manager for PolyBolt {@code price.crypto} stream
     * @param polymarketCoordinator lifecycle coordinator for Polymarket markets
     * @param polymarketStorage     per-market Zstd storage manager for Polymarket
     * @param polyBoltClient        Polymarket PolyBolt v2 WebSocket client
     * @param clobClient            Polymarket CLOB WebSocket client
     * @param writeCoordinator      Blue-Green write mode and standby buffer coordinator
     * @param vertx                 Vert.x instance used to bind the optional public status listener on port 443
     * @param symbol                configured trading symbol (default BTCUSDT)
     * @param timeframe             configured Polymarket market timeframe (default 15m)
     * @param krakenUsdtUsdEnabled  whether to also listen and collect Kraken {@code USDTUSD} trade stream (default true)
     * @param publicPort            optional public HTTP port for {@code /api/collector/status} bound only when ACTIVE (default 0 = disabled)
     */
    @Inject
    public DataCollectorService(
            BinanceStreamConnector binanceConnector,
            DailyBinanceStorageManager binanceStorage,
            KrakenStreamConnector krakenConnector,
            DailyKrakenStorageManager krakenStorage,
            CoinbaseStreamConnector coinbaseConnector,
            DailyCoinbaseStorageManager coinbaseStorage,
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
            @ConfigProperty(name = "polymarket.collector.kraken.usdtusd-enabled", defaultValue = "true")
            boolean krakenUsdtUsdEnabled,
            @ConfigProperty(name = "polymarket.collector.public-port", defaultValue = "0")
            int publicPort) {
        this.binanceConnector = binanceConnector;
        this.binanceStorage = binanceStorage;
        this.krakenConnector = krakenConnector;
        this.krakenStorage = krakenStorage;
        this.coinbaseConnector = coinbaseConnector;
        this.coinbaseStorage = coinbaseStorage;
        this.polyBoltDailyStorage = polyBoltDailyStorage;
        this.polymarketCoordinator = polymarketCoordinator;
        this.polymarketStorage = polymarketStorage;
        this.polyBoltClient = polyBoltClient;
        this.clobClient = clobClient;
        this.writeCoordinator = writeCoordinator;
        this.vertx = vertx;
        this.symbol = BinanceStreamConnector.normalizeSymbol(symbol);
        this.krakenPrimarySymbol = KrakenStreamConnector.normalizeSymbol(symbol);
        this.timeframe = timeframe;
        this.krakenUsdtUsdEnabled = krakenUsdtUsdEnabled;
        this.publicPort = publicPort;
        this.binanceConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.krakenConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.coinbaseConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.polyBoltClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.clobClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
    }

    void onStart(@Observes StartupEvent event) {
        Log.infof("Starting Data Collector for symbol=%s, polymarketTimeframe=%s, krakenUsdtUsdEnabled=%s, writeMode=%s...",
                symbol, timeframe, krakenUsdtUsdEnabled, writeCoordinator.getWriteMode());
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
            binanceConnector.start(symbol, new BinanceStreamListener() {
                @Override
                public void onStreamMessage(BinanceStreamType streamType, String rawJson, long recvTimestampMs) {
                    writeCoordinator.recordBinance(streamType, rawJson, recvTimestampMs);
                }

                @Override
                public void onStreamFrame(BinanceStreamType streamType, byte[] rawUtf8Bytes, long recvTimestampMs) {
                    writeCoordinator.recordBinanceBytes(streamType, rawUtf8Bytes, recvTimestampMs);
                }
            });
            krakenConnector.start(symbol, krakenUsdtUsdEnabled, new KrakenStreamListener() {
                @Override
                public void onStreamMessage(String normalizedSymbol, String rawJson, long recvTimestampMs) {
                    writeCoordinator.recordKraken(normalizedSymbol, rawJson, recvTimestampMs);
                }

                @Override
                public void onStreamFrame(String normalizedSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
                    writeCoordinator.recordKrakenBytes(normalizedSymbol, rawUtf8Bytes, recvTimestampMs);
                }
            });
            coinbaseConnector.start(symbol, new CoinbaseStreamListener() {
                @Override
                public void onStreamMessage(String normalizedSymbol, String rawJson, long recvTimestampMs) {
                    writeCoordinator.recordCoinbase(normalizedSymbol, rawJson, recvTimestampMs);
                }

                @Override
                public void onStreamFrame(String normalizedSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
                    writeCoordinator.recordCoinbaseBytes(normalizedSymbol, rawUtf8Bytes, recvTimestampMs);
                }
            });
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
        krakenConnector.stop();
        coinbaseConnector.stop();
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
        Map<BinanceStreamType, ConnectionState> rawBinanceStates = binanceConnector.getConnectionStates();
        Map<String, String> binanceStates = new LinkedHashMap<>();
        if (rawBinanceStates != null) {
            rawBinanceStates.forEach((type, state) -> binanceStates.put(type.name(), state.name()));
        }

        Map<String, ConnectionState> rawKrakenStates = krakenConnector.getConnectionStates();
        Map<String, String> krakenStates = new LinkedHashMap<>();
        if (rawKrakenStates != null) {
            rawKrakenStates.forEach((sym, state) -> krakenStates.put(sym, state.name()));
        }

        Map<String, ConnectionState> rawClobStates = clobClient.getConnectionStates();
        Map<String, String> clobStates = new LinkedHashMap<>();
        if (rawClobStates != null) {
            rawClobStates.forEach((slug, state) -> clobStates.put(slug, state.name()));
        }

        ConnectionState coinbaseState = coinbaseConnector.getState();
        ConnectionState polyBoltTwapState = polyBoltClient.getTwapState();
        ConnectionState polyBoltCryptoState = polyBoltClient.getCryptoPriceState();
        List<String> trackedSlugs = polymarketCoordinator.getTrackedMarketSlugs();

        long binanceWritten = binanceStorage.getTotalRecordsWritten();
        long krakenWritten = krakenStorage.getTotalRecordsWritten();
        long coinbaseWritten = coinbaseStorage.getTotalRecordsWritten();
        long polyBoltWritten = polyBoltDailyStorage.getTotalRecordsWritten();
        long polymarketWritten = polymarketStorage.getTotalRecordsWritten();
        long standbyBuffered = writeCoordinator.getStandbyBufferedRecords();
        long totalProcessedOrBuffered = binanceWritten + krakenWritten + coinbaseWritten + polyBoltWritten + polymarketWritten + standbyBuffered;

        boolean readyForCutover = evaluateReadyForCutover(
                rawBinanceStates,
                rawKrakenStates,
                coinbaseState,
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
                binanceStates,
                krakenUsdtUsdEnabled,
                krakenStates,
                coinbaseState != null ? coinbaseState.name() : ConnectionState.DISCONNECTED.name(),
                polyBoltTwapState != null ? polyBoltTwapState.name() : ConnectionState.DISCONNECTED.name(),
                polyBoltCryptoState != null ? polyBoltCryptoState.name() : ConnectionState.DISCONNECTED.name(),
                clobStates,
                trackedSlugs,
                binanceWritten,
                krakenWritten,
                coinbaseWritten,
                polyBoltWritten,
                polymarketWritten
        );
    }

    private boolean evaluateReadyForCutover(
            Map<BinanceStreamType, ConnectionState> binanceStates,
            Map<String, ConnectionState> krakenStates,
            ConnectionState coinbaseState,
            ConnectionState polyBoltTwapState,
            ConnectionState polyBoltCryptoState,
            List<String> trackedSlugs,
            Map<String, ConnectionState> clobStates,
            long totalProcessedOrBuffered) {
        if (totalProcessedOrBuffered <= 0L) {
            return false;
        }
        if (binanceStates == null) {
            return false;
        }
        for (BinanceStreamType type : BinanceStreamType.values()) {
            if (binanceStates.get(type) != ConnectionState.CONNECTED) {
                return false;
            }
        }
        if (krakenStates == null || krakenStates.get(krakenPrimarySymbol) != ConnectionState.CONNECTED) {
            return false;
        }
        if (krakenUsdtUsdEnabled && krakenStates.get(KrakenStreamConnector.USDTUSD_SYMBOL) != ConnectionState.CONNECTED) {
            return false;
        }
        if (coinbaseState != ConnectionState.CONNECTED
                || polyBoltTwapState != ConnectionState.CONNECTED
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
