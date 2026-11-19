package cz.polymarket.bot.collector;

import cz.polymarket.bot.collector.storage.DailyBinanceStorageManager;
import cz.polymarket.bot.collector.storage.DailyPolyBoltStorageManager;
import cz.polymarket.bot.collector.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.binance.BinanceStreamListener;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.polymarket.PolymarketPolyBoltClient;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.web.CollectorStatusDto;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates continuous Zstandard ({@code .jsonl.zst}) recording of Polymarket PolyBolt
 * ({@code price.crypto} and {@code price.crypto.twap}) and deduplicated per-market CLOB streams
 * ({@code current + lookahead-count}, default {@code lookahead-count = 1}), plus optional (default inactive)
 * Binance Spot/Futures {@code bookTicker} streams.
 * <p>
 * Runs exclusively on {@code CPU #0} off the strategy hot path and deduplicates shared active-market CLOB
 * frames independently on {@code CPU #0}, guaranteeing zero duplicate records in archives.
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
    private final String symbol;
    private final String timeframe;
    private final long durationSeconds;
    private final boolean streamsEnabled;
    private final boolean binanceSpotBookTickerEnabled;
    private final boolean binanceFuturesBookTickerEnabled;

    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Constructs the Data Collector orchestrator service.
     *
     * @param binanceConnector                Binance WebSocket connector (used for optional {@code bookTicker} streams)
     * @param binanceStorage                  daily Zstd storage manager for Binance {@code bookTicker} streams
     * @param polyBoltDailyStorage            daily Zstd storage manager for PolyBolt streams
     * @param polymarketCoordinator           lifecycle coordinator for Polymarket markets
     * @param polymarketStorage               per-market Zstd storage manager for Polymarket
     * @param polyBoltClient                  Polymarket PolyBolt v2 WebSocket client
     * @param clobClient                      Polymarket CLOB WebSocket client
     * @param symbol                          configured trading symbol (default BTCUSDT)
     * @param timeframe                       configured Polymarket market timeframe (default 15m)
     * @param streamsEnabled                  whether collector streams are enabled (default true)
     * @param binanceSpotBookTickerEnabled    whether to connect and persist Binance Spot {@code bookTicker} (default false)
     * @param binanceFuturesBookTickerEnabled whether to connect and persist Binance USD-M Futures {@code bookTicker} (default false)
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
            String timeframe,
            @ConfigProperty(name = "polymarket.collector.streams-enabled", defaultValue = "true")
            boolean streamsEnabled,
            @ConfigProperty(name = "polymarket.collector.binance.spot-book-ticker-enabled", defaultValue = "false")
            boolean binanceSpotBookTickerEnabled,
            @ConfigProperty(name = "polymarket.collector.binance.futures-book-ticker-enabled", defaultValue = "false")
            boolean binanceFuturesBookTickerEnabled) {
        this.binanceConnector = binanceConnector;
        this.binanceStorage = binanceStorage;
        this.polyBoltDailyStorage = polyBoltDailyStorage;
        this.polymarketCoordinator = polymarketCoordinator;
        this.polymarketStorage = polymarketStorage;
        this.polyBoltClient = polyBoltClient;
        this.clobClient = clobClient;
        this.symbol = BinanceStreamConnector.normalizeSymbol(symbol);
        this.timeframe = timeframe;
        this.durationSeconds = PolymarketMarketInfo.parseTimeframeSeconds(timeframe);
        this.streamsEnabled = streamsEnabled;
        this.binanceSpotBookTickerEnabled = binanceSpotBookTickerEnabled;
        this.binanceFuturesBookTickerEnabled = binanceFuturesBookTickerEnabled;
    }

    /**
     * Starts Data Collector WebSocket streams if {@code streamsEnabled} is {@code true}.
     *
     * @param nowEpochSec current epoch timestamp in seconds
     */
    public synchronized void startStreams(long nowEpochSec) {
        if (!streamsEnabled || !running.compareAndSet(false, true)) {
            return;
        }
        Set<BinanceStreamType> enabledBookTickers = enabledBinanceBookTickerStreams();
        Log.infof("Starting Data Collector for symbol=%s, timeframe=%s, lookaheadCount=%d, binanceSpotBookTickerEnabled=%b, binanceFuturesBookTickerEnabled=%b...",
                symbol, timeframe, polymarketCoordinator.getLookaheadCount(), binanceSpotBookTickerEnabled, binanceFuturesBookTickerEnabled);

        polyBoltClient.startCollectorStreams(
                symbol,
                new RawWebSocketListener() {
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
                },
                new RawWebSocketListener() {
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
                }
        );

        if (!enabledBookTickers.isEmpty()) {
            binanceConnector.startBookTickerStreams(symbol, enabledBookTickers, new BinanceStreamListener() {
                @Override
                public void onStreamMessage(BinanceStreamType streamType, String rawJson, long recvTimestampMs) {
                    binanceStorage.record(streamType, rawJson, recvTimestampMs);
                }

                @Override
                public void onStreamFrame(BinanceStreamType streamType, byte[] rawUtf8Bytes, long recvTimestampMs) {
                    binanceStorage.recordBytes(streamType, rawUtf8Bytes, recvTimestampMs);
                }
            });
        }

        polymarketCoordinator.start(nowEpochSec);
    }

    /**
     * Stops Data Collector WebSocket streams and flushes and closes all open {@code .jsonl.zst} files.
     */
    public synchronized void stopAndCloseAll() {
        running.set(false);
        binanceConnector.stopBookTickerStreams();
        polymarketCoordinator.stop();
        polyBoltClient.stopCollectorStreams();
        flushAll();
        binanceStorage.closeAll();
        polyBoltDailyStorage.closeAll();
        polymarketStorage.closeAll();
    }

    /**
     * Periodically checks ended Polymarket markets for resolution and opens streams for new future markets.
     */
    @Scheduled(every = "${polymarket.collector.discovery-interval:30s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void scheduledMarketDiscovery() {
        if (!streamsEnabled || !running.get()) {
            return;
        }
        polymarketCoordinator.discoverAndRefreshMarkets(Instant.now().getEpochSecond());
    }

    /**
     * Periodically flushes active Zstandard frames to disk so all buffered records are safely persisted.
     */
    @Scheduled(every = "${polymarket.collector.flush-interval:5s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void scheduledFlush() {
        if (!running.get()) {
            return;
        }
        flushAll();
    }

    /**
     * Flushes all open Zstandard writers across PolyBolt, Polymarket CLOB, and Binance {@code bookTicker}.
     */
    public void flushAll() {
        binanceStorage.flushAll();
        polyBoltDailyStorage.flushAll();
        polymarketStorage.flushAll();
    }

    /**
     * Returns whether collector streams are enabled by configuration.
     *
     * @return {@code true} if collector streams are enabled
     */
    public boolean isStreamsEnabled() {
        return streamsEnabled;
    }

    /**
     * Builds a diagnostic status snapshot of all collector streams, tracked markets, and storage counters.
     *
     * @return current {@link CollectorStatusDto}
     */
    public CollectorStatusDto getStatus() {
        Set<BinanceStreamType> enabledBinanceStreams = enabledBinanceBookTickerStreams();
        Map<BinanceStreamType, ConnectionState> rawBinanceStates = enabledBinanceStreams.isEmpty()
                ? Map.of()
                : binanceConnector.getBookTickerConnectionStates();
        Map<String, String> binanceStates = new LinkedHashMap<>();
        if (rawBinanceStates != null) {
            for (BinanceStreamType type : enabledBinanceStreams) {
                ConnectionState state = rawBinanceStates.get(type);
                binanceStates.put(type.name(), state != null ? state.name() : ConnectionState.DISCONNECTED.name());
            }
        }

        Map<String, ConnectionState> rawClobStates = clobClient.getCollectorConnectionStates();
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
        long totalWritten = binanceWritten + polyBoltWritten + polymarketWritten;

        long nowEpochSec = Instant.now().getEpochSecond();
        String activeSlug = PolymarketMarketInfo.buildMarketSlug(
                symbol,
                timeframe,
                PolymarketMarketInfo.alignCycleStart(nowEpochSec, durationSeconds)
        );

        boolean healthy = evaluateHealthy(
                enabledBinanceStreams,
                rawBinanceStates,
                polyBoltTwapState,
                polyBoltCryptoState,
                trackedSlugs,
                rawClobStates,
                activeSlug,
                totalWritten
        );

        return new CollectorStatusDto(
                symbol,
                timeframe,
                polymarketCoordinator.getLookaheadCount(),
                healthy,
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

    private Set<BinanceStreamType> enabledBinanceBookTickerStreams() {
        EnumSet<BinanceStreamType> streams = EnumSet.noneOf(BinanceStreamType.class);
        if (binanceSpotBookTickerEnabled) {
            streams.add(BinanceStreamType.SPOT_BOOK_TICKER);
        }
        if (binanceFuturesBookTickerEnabled) {
            streams.add(BinanceStreamType.FUTURES_BOOK_TICKER);
        }
        return streams;
    }

    private boolean evaluateHealthy(
            Set<BinanceStreamType> enabledBinanceStreams,
            Map<BinanceStreamType, ConnectionState> binanceStates,
            ConnectionState polyBoltTwapState,
            ConnectionState polyBoltCryptoState,
            List<String> trackedSlugs,
            Map<String, ConnectionState> clobStates,
            String activeSlug,
            long totalWritten) {
        if (!streamsEnabled) {
            return true;
        }
        if (totalWritten <= 0L) {
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
        if (clobStates.get(activeSlug) != ConnectionState.CONNECTED) {
            // Fall back to checking that all tracked slugs are connected when clock alignment just rolled
            for (String slug : trackedSlugs) {
                if (clobStates.get(slug) != ConnectionState.CONNECTED) {
                    return false;
                }
            }
        }
        return true;
    }
}
