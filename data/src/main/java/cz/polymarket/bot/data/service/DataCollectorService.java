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
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyCoinbaseStorageManager;
import cz.polymarket.bot.data.storage.DailyKrakenStorageManager;
import cz.polymarket.bot.data.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.data.web.CollectorStatusDto;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Main orchestrator service for the Data Collector application.
 * Always starts recording Binance, Kraken, Coinbase, and Polymarket WebSocket streams on application startup
 * using zero-decode {@code byte[]} Zstandard writing, schedules periodic Polymarket market discovery/resolution
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
    private final String symbol;
    private final String timeframe;
    private final boolean krakenUsdtUsdEnabled;

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
     * @param symbol                configured trading symbol (default BTCUSDT)
     * @param timeframe             configured Polymarket market timeframe (default 15m)
     * @param krakenUsdtUsdEnabled  whether to also listen and collect Kraken {@code USDTUSD} trade stream (default true)
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
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol,
            @ConfigProperty(name = "polymarket.collector.timeframe", defaultValue = "15m")
            String timeframe,
            @ConfigProperty(name = "polymarket.collector.kraken.usdtusd-enabled", defaultValue = "true")
            boolean krakenUsdtUsdEnabled) {
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
        this.symbol = BinanceStreamConnector.normalizeSymbol(symbol);
        this.timeframe = timeframe;
        this.krakenUsdtUsdEnabled = krakenUsdtUsdEnabled;
        this.binanceConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.krakenConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.coinbaseConnector.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.polyBoltClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
        this.clobClient.setDisconnectListener(this::onStreamUnexpectedDisconnect);
    }

    void onStart(@Observes StartupEvent event) {
        Log.infof("Starting Data Collector for symbol=%s, polymarketTimeframe=%s, krakenUsdtUsdEnabled=%s...",
                symbol, timeframe, krakenUsdtUsdEnabled);
        try {
            polyBoltClient.startCryptoPriceStream(symbol, new RawWebSocketListener() {
                @Override
                public void onMessage(String rawJson, long recvTs) {
                    polyBoltDailyStorage.recordCryptoPrice(rawJson, recvTs);
                }

                @Override
                public void onMessageBytes(TimestampedFrame frame) {
                    if (frame != null) {
                        polyBoltDailyStorage.recordCryptoPriceBytes(frame.rawUtf8Bytes(), frame.recvTs());
                    }
                }
            });
            polyBoltClient.startTwapStream(symbol, new RawWebSocketListener() {
                @Override
                public void onMessage(String rawJson, long recvTs) {
                    polyBoltDailyStorage.recordTwap(rawJson, recvTs);
                }

                @Override
                public void onMessageBytes(TimestampedFrame frame) {
                    if (frame != null) {
                        polyBoltDailyStorage.recordTwapBytes(frame.rawUtf8Bytes(), frame.recvTs());
                    }
                }
            });
            binanceConnector.start(symbol, new BinanceStreamListener() {
                @Override
                public void onStreamMessage(BinanceStreamType streamType, String rawJson, long recvTimestampMs) {
                    binanceStorage.record(streamType, rawJson, recvTimestampMs);
                }

                @Override
                public void onStreamFrame(BinanceStreamType streamType, byte[] rawUtf8Bytes, long recvTimestampMs) {
                    binanceStorage.recordBytes(streamType, rawUtf8Bytes, recvTimestampMs);
                }
            });
            krakenConnector.start(symbol, krakenUsdtUsdEnabled, new KrakenStreamListener() {
                @Override
                public void onStreamMessage(String normalizedSymbol, String rawJson, long recvTimestampMs) {
                    krakenStorage.record(normalizedSymbol, rawJson, recvTimestampMs);
                }

                @Override
                public void onStreamFrame(String normalizedSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
                    krakenStorage.recordBytes(normalizedSymbol, rawUtf8Bytes, recvTimestampMs);
                }
            });
            coinbaseConnector.start(symbol, new CoinbaseStreamListener() {
                @Override
                public void onStreamMessage(String normalizedSymbol, String rawJson, long recvTimestampMs) {
                    coinbaseStorage.record(normalizedSymbol, rawJson, recvTimestampMs);
                }

                @Override
                public void onStreamFrame(String normalizedSymbol, byte[] rawUtf8Bytes, long recvTimestampMs) {
                    coinbaseStorage.recordBytes(normalizedSymbol, rawUtf8Bytes, recvTimestampMs);
                }
            });
            polymarketCoordinator.start(Instant.now().getEpochSecond());
        } catch (Exception e) {
            Log.errorf("Fatal error starting Data Collector: %s. Terminating application.", e.getMessage());
            Quarkus.asyncExit(1);
            throw e;
        }
    }

    void onStop(@Observes ShutdownEvent event) {
        Log.info("Shutting down Data Collector and closing Zstd streams...");
        binanceConnector.stop();
        krakenConnector.stop();
        coinbaseConnector.stop();
        polymarketCoordinator.stop();
        polyBoltClient.stop();
        binanceStorage.closeAll();
        krakenStorage.closeAll();
        coinbaseStorage.closeAll();
        polyBoltDailyStorage.closeAll();
    }

    private void onStreamUnexpectedDisconnect(WebSocketStreamTelemetry snapshot) {
        if (snapshot == null) {
            return;
        }
        String remoteLabel = snapshot.remoteAddress().isEmpty() ? "unknown" : snapshot.remoteAddress();
        if (snapshot.lastCloseCode() == 1000) {
            Log.infof("Data Collector stream closed normally [%s]: remote=%s, closeCode=%d, reason=%s, queueDepth=%d, maxQueueDepth=%d, maxQueueLagMs=%d, totalFrames=%d, reconnectCount=%d",
                    snapshot.streamName(),
                    remoteLabel,
                    snapshot.lastCloseCode(),
                    snapshot.lastCloseReason(),
                    snapshot.queueDepth(),
                    snapshot.maxQueueDepth(),
                    snapshot.maxQueueLagMs(),
                    snapshot.totalFramesReceived(),
                    snapshot.reconnectCount());
            return;
        }
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
        binanceStorage.flushAll();
        krakenStorage.flushAll();
        coinbaseStorage.flushAll();
        polyBoltDailyStorage.flushAll();
        polymarketStorage.flushAll();
    }

    /**
     * Builds a diagnostic status snapshot of all collector streams and storage writers.
     *
     * @return current {@link CollectorStatusDto}
     */
    public CollectorStatusDto getStatus() {
        Map<String, String> binanceStates = new LinkedHashMap<>();
        binanceConnector.getConnectionStates().forEach((type, state) -> binanceStates.put(type.name(), state.name()));

        Map<String, String> krakenStates = new LinkedHashMap<>();
        krakenConnector.getConnectionStates().forEach((sym, state) -> krakenStates.put(sym, state.name()));

        Map<String, String> clobStates = new LinkedHashMap<>();
        clobClient.getConnectionStates().forEach((slug, state) -> clobStates.put(slug, state.name()));

        return new CollectorStatusDto(
                symbol,
                timeframe,
                binanceStates,
                krakenUsdtUsdEnabled,
                krakenStates,
                coinbaseConnector.getState().name(),
                polyBoltClient.getTwapState().name(),
                polyBoltClient.getCryptoPriceState().name(),
                clobStates,
                polymarketCoordinator.getTrackedMarketSlugs(),
                binanceStorage.getTotalRecordsWritten(),
                krakenStorage.getTotalRecordsWritten(),
                coinbaseStorage.getTotalRecordsWritten(),
                polyBoltDailyStorage.getTotalRecordsWritten(),
                polymarketStorage.getTotalRecordsWritten()
        );
    }
}
