package cz.polymarket.bot.data.service;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.data.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.data.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.data.web.CollectorStatusDto;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Main orchestrator service for the Data Collector application.
 * Always starts recording Binance and Polymarket WebSocket streams on application startup,
 * schedules periodic Polymarket market discovery/resolution checks and Zstandard frame flushes,
 * and exposes runtime status.
 */
@ApplicationScoped
public class DataCollectorService {

    private static final Logger LOG = Logger.getLogger(DataCollectorService.class);

    private final BinanceStreamConnector binanceConnector;
    private final DailyBinanceStorageManager binanceStorage;
    private final DailyPolyBoltStorageManager polyBoltDailyStorage;
    private final PolymarketLifecycleCoordinator polymarketCoordinator;
    private final PolymarketMarketStorageManager polymarketStorage;
    private final PolymarketPolyBoltClient polyBoltClient;
    private final PolymarketClobClient clobClient;
    private final String symbol;
    private final String timeframe;

    /**
     * Constructs the Data Collector orchestrator service.
     *
     * @param binanceConnector      Binance 5-stream WebSocket connector
     * @param binanceStorage        daily Zstd storage manager for Binance streams
     * @param polyBoltDailyStorage  daily Zstd storage manager for PolyBolt {@code price.crypto} stream
     * @param polymarketCoordinator lifecycle coordinator for Polymarket markets
     * @param polymarketStorage     per-market Zstd storage manager for Polymarket
     * @param polyBoltClient        Polymarket PolyBolt v2 WebSocket client
     * @param clobClient            Polymarket CLOB WebSocket client
     * @param symbol                configured trading symbol (default BTCUSDT)
     * @param timeframe             configured Polymarket market timeframe (default 15m)
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
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol,
            @ConfigProperty(name = "polymarket.collector.timeframe", defaultValue = "15m")
            String timeframe) {
        this.binanceConnector = binanceConnector;
        this.binanceStorage = binanceStorage;
        this.polyBoltDailyStorage = polyBoltDailyStorage;
        this.polymarketCoordinator = polymarketCoordinator;
        this.polymarketStorage = polymarketStorage;
        this.polyBoltClient = polyBoltClient;
        this.clobClient = clobClient;
        this.symbol = BinanceStreamConnector.normalizeSymbol(symbol);
        this.timeframe = timeframe;
    }

    void onStart(@Observes StartupEvent event) {
        LOG.infof("Starting Data Collector for symbol=%s, polymarketTimeframe=%s...", symbol, timeframe);
        try {
            polyBoltClient.startCryptoPriceStream(symbol, polyBoltDailyStorage::recordCryptoPrice);
            polyBoltClient.startTwapStream(symbol, polyBoltDailyStorage::recordTwap);
            binanceConnector.start(symbol, binanceStorage::record);
            polymarketCoordinator.start(Instant.now().getEpochSecond());
        } catch (Exception e) {
            LOG.errorf("Fatal error starting Data Collector: %s. Terminating application.", e.getMessage());
            Quarkus.asyncExit(1);
            throw e;
        }
    }

    void onStop(@Observes ShutdownEvent event) {
        LOG.info("Shutting down Data Collector and closing Zstd streams...");
        binanceConnector.stop();
        polymarketCoordinator.stop();
        polyBoltClient.stop();
        binanceStorage.closeAll();
        polyBoltDailyStorage.closeAll();
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

        Map<String, String> clobStates = new LinkedHashMap<>();
        clobClient.getConnectionStates().forEach((slug, state) -> clobStates.put(slug, state.name()));

        return new CollectorStatusDto(
                symbol,
                timeframe,
                binanceStates,
                polyBoltClient.getTwapState().name(),
                polyBoltClient.getCryptoPriceState().name(),
                clobStates,
                polymarketCoordinator.getTrackedMarketSlugs(),
                binanceStorage.getTotalRecordsWritten(),
                polyBoltDailyStorage.getTotalRecordsWritten(),
                polymarketStorage.getTotalRecordsWritten()
        );
    }
}
