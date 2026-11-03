package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.strategy.ExternalStreamType;
import cz.polymarket.bot.strategy.TradeDirection;
import io.quarkus.logging.Log;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * High-performance chronological event multiplexer and streamer for backtesting.
 * <p>
 * Streams captured events from collector Parquet archives ordered strictly by the local EC2
 * capture timestamp {@code ts} ({@link BacktestEvent#timestampMs()}).
 * <p>
 * Optimizes multi-candle backtesting over millions of rows by:
 * <ul>
 *   <li>Reusing a single DuckDB connection across candles,</li>
 *   <li>Reading daily stream Parquet files at most once per UTC day and slicing candle intervals
 *       via {@code O(log N)} binary search, and</li>
 *   <li>Loading external (Binance, Deribit) streams strictly when declared in
 *       {@code requiredExternalStreams}.</li>
 * </ul>
 */
public class ChronologicalEventStreamer implements AutoCloseable {

    private static final BacktestEvent[] EMPTY_EVENTS = new BacktestEvent[0];

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            Log.fatal("Fatal error: DuckDB JDBC driver not found on classpath; event streamer cannot run", e);
        }
    }

    private final PriorityQueue<BacktestEvent> queue = new PriorityQueue<>(4096);
    private Connection sharedConnection;

    // Rolling single-day cache for daily Parquet streams to avoid re-scanning 96 times per UTC day
    private String cachedDateStr;
    private String cachedSymbol;
    private Set<ExternalStreamType> cachedExternalStreams;
    private BacktestEvent[] cachedDailyStreamEvents = EMPTY_EVENTS;

    /**
     * Checks if there are pending events in the priority queue.
     *
     * @return true if more events exist
     */
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    /**
     * Retrieves and removes the next chronological event from the priority queue.
     *
     * @return next event, or null if queue is empty
     */
    public BacktestEvent next() {
        return queue.poll();
    }

    /**
     * Schedules a future or immediate event into the chronological queue.
     *
     * @param event BacktestEvent to schedule
     */
    public void scheduleEvent(BacktestEvent event) {
        if (event != null) {
            queue.add(event);
        }
    }

    /**
     * Clears all pending events from the active candle queue.
     */
    public void clear() {
        queue.clear();
    }

    /**
     * Loads all shared daily stream events ({@code polymarket/price_crypto}, {@code polymarket/price_crypto_twap_60s},
     * and any strategy-requested external Binance/Deribit streams) for a single UTC day into a sorted,
     * immutable array that can be shared read-only across all parallel 15-minute market workers of that day.
     *
     * @param datasetService collector Parquet dataset service
     * @param symbol trading asset symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @param requiredExternalStreams set of external streams required by the strategy
     * @return sorted array of daily stream events
     */
    public static BacktestEvent[] loadSharedDailyStreams(
            CollectorParquetDatasetService datasetService,
            String symbol,
            String dateStr,
            Set<ExternalStreamType> requiredExternalStreams) {

        if (datasetService == null || dateStr == null || dateStr.isBlank()) {
            return EMPTY_EVENTS;
        }

        Set<ExternalStreamType> extStreams = requiredExternalStreams != null ? requiredExternalStreams : Set.of();

        record DailyLoaderTask(Path file, BiConsumer<Statement, List<BacktestEvent>> loader) {}
        List<DailyLoaderTask> tasks = new ArrayList<>(8);

        Path priceFile = datasetService.getPolymarketCryptoPriceFile(symbol, dateStr);
        if (priceFile != null && Files.exists(priceFile)) {
            tasks.add(new DailyLoaderTask(priceFile, (stmt, out) -> loadDailyPolymarketCryptoPrice(stmt, priceFile, out)));
        }

        Path twapFile = datasetService.getPolymarketCryptoTwap60sFile(symbol, dateStr);
        if (twapFile != null && Files.exists(twapFile)) {
            tasks.add(new DailyLoaderTask(twapFile, (stmt, out) -> loadDailyPolymarketCryptoTwap60s(stmt, twapFile, out)));
        }

        if (extStreams.contains(ExternalStreamType.BINANCE_SPOT_AGG_TRADES)) {
            Path f = datasetService.getBinanceSpotAggTradesFile(symbol, dateStr);
            if (f != null && Files.exists(f)) {
                tasks.add(new DailyLoaderTask(f, (stmt, out) -> loadDailyBinanceSpotTrades(stmt, f, out)));
            }
        }
        if (extStreams.contains(ExternalStreamType.BINANCE_FUTURES_AGG_TRADES)) {
            Path f = datasetService.getBinanceFuturesAggTradesFile(symbol, dateStr);
            if (f != null && Files.exists(f)) {
                tasks.add(new DailyLoaderTask(f, (stmt, out) -> loadDailyBinanceFuturesTrades(stmt, f, out)));
            }
        }
        if (extStreams.contains(ExternalStreamType.BINANCE_FUTURES_DEPTH_PARTIAL)) {
            Path f = datasetService.getBinanceFuturesDepthPartialFile(symbol, dateStr);
            if (f != null && Files.exists(f)) {
                tasks.add(new DailyLoaderTask(f, (stmt, out) -> loadDailyBinanceFuturesDepth(stmt, f, out)));
            }
        }
        if (extStreams.contains(ExternalStreamType.BINANCE_FUTURES_DEPTH_DIFF)) {
            Path f = datasetService.getBinanceFuturesDepthDiffFile(symbol, dateStr);
            if (f != null && Files.exists(f)) {
                tasks.add(new DailyLoaderTask(f, (stmt, out) -> loadDailyBinanceFuturesDepth(stmt, f, out)));
            }
        }
        if (extStreams.contains(ExternalStreamType.BINANCE_FUTURES_FORCE_ORDER)) {
            Path f = datasetService.getBinanceFuturesForceOrderFile(symbol, dateStr);
            if (f != null && Files.exists(f)) {
                tasks.add(new DailyLoaderTask(f, (stmt, out) -> loadDailyBinanceFuturesForceOrders(stmt, f, out)));
            }
        }
        if (extStreams.contains(ExternalStreamType.DERIBIT_FUTURES_TRADES)) {
            Path f = datasetService.getDeribitFuturesTradesFile(symbol, dateStr);
            if (f != null && Files.exists(f)) {
                tasks.add(new DailyLoaderTask(f, (stmt, out) -> loadDailyDeribitTrades(stmt, f, out)));
            }
        }

        if (tasks.isEmpty()) {
            if (Log.isDebugEnabled()) {
                Log.debugf("No daily stream Parquet files found for symbol=%s on date=%s", symbol, dateStr);
            }
            return EMPTY_EVENTS;
        }

        if (tasks.size() == 1) {
            List<BacktestEvent> singleList = new ArrayList<>(16_384);
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                 Statement stmt = conn.createStatement()) {
                tasks.getFirst().loader().accept(stmt, singleList);
            } catch (SQLException e) {
                Log.warnf("Failed loading daily stream file %s: %s", tasks.getFirst().file(), e.getMessage());
            }
            BacktestEvent[] arr = singleList.toArray(EMPTY_EVENTS);
            Arrays.parallelSort(arr);
            if (Log.isDebugEnabled()) {
                Log.debugf("Loaded %d shared daily stream events from 1 file for symbol=%s on date=%s",
                        arr.length, symbol, dateStr);
            }
            return arr;
        }

        @SuppressWarnings("unchecked")
        CompletableFuture<List<BacktestEvent>>[] futures = new CompletableFuture[tasks.size()];
        for (int i = 0; i < tasks.size(); i++) {
            DailyLoaderTask task = tasks.get(i);
            futures[i] = CompletableFuture.supplyAsync(() -> {
                List<BacktestEvent> localEvents = new ArrayList<>(16_384);
                try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                     Statement stmt = conn.createStatement()) {
                    task.loader().accept(stmt, localEvents);
                } catch (SQLException e) {
                    Log.warnf("Failed loading daily stream file %s: %s", task.file(), e.getMessage());
                }
                return localEvents;
            });
        }

        int totalSize = 0;
        List<List<BacktestEvent>> loadedLists = new ArrayList<>(tasks.size());
        for (CompletableFuture<List<BacktestEvent>> future : futures) {
            List<BacktestEvent> part = future.join();
            loadedLists.add(part);
            totalSize += part.size();
        }

        BacktestEvent[] combined = new BacktestEvent[totalSize];
        int offset = 0;
        for (List<BacktestEvent> part : loadedLists) {
            for (int i = 0, n = part.size(); i < n; i++) {
                combined[offset++] = part.get(i);
            }
        }
        Arrays.parallelSort(combined);
        if (Log.isDebugEnabled()) {
            Log.debugf("Loaded %d shared daily stream events from %d files for symbol=%s on date=%s",
                    combined.length, tasks.size(), symbol, dateStr);
        }
        return combined;
    }

    /**
     * Builds a completed 15-minute {@link MarketCandle} directly from the pre-loaded daily stream array
     * using {@code O(log N)} binary search over {@link BacktestEvent.BinanceSpotTradeEvent} and
     * {@link BacktestEvent.BinanceFuturesTradeEvent} entries.
     *
     * @param sharedDailyEvents sorted array of daily stream events
     * @param tStart candle start epoch seconds
     * @param tEnd candle end epoch seconds
     * @return constructed MarketCandle
     */
    public static MarketCandle buildSpotCandleFromDailyEvents(BacktestEvent[] sharedDailyEvents, long tStart, long tEnd) {
        if (sharedDailyEvents == null || sharedDailyEvents.length == 0) {
            return null;
        }
        long tStartMs = tStart * 1000L;
        long tEndMs = tEnd * 1000L;
        int startIdx = findFirstIndexAtOrAfter(sharedDailyEvents, tStartMs);

        double sOpen = 0.0;
        double sHigh = 0.0;
        double sLow = 0.0;
        double sClose = 0.0;
        double spotVolBtc = 0.0;
        double spotVolUsd = 0.0;
        double spotDeltaBtc = 0.0;

        double fOpen = 0.0;
        double fHigh = 0.0;
        double fLow = 0.0;
        double fClose = 0.0;
        double futVolBtc = 0.0;
        double futVolUsd = 0.0;
        double futDeltaBtc = 0.0;

        for (int i = startIdx; i < sharedDailyEvents.length; i++) {
            BacktestEvent ev = sharedDailyEvents[i];
            if (ev.timestampMs() > tEndMs) {
                break;
            }
            if (ev instanceof BacktestEvent.BinanceSpotTradeEvent spot) {
                double p = spot.price();
                double q = spot.quantity();
                if (sOpen <= 0.0) {
                    sOpen = p;
                    sHigh = p;
                    sLow = p;
                } else {
                    if (p > sHigh) sHigh = p;
                    if (p < sLow) sLow = p;
                }
                sClose = p;
                spotVolBtc += q;
                spotVolUsd += p * q;
                spotDeltaBtc += spot.isBuyerMaker() ? -q : q;
            } else if (ev instanceof BacktestEvent.BinanceFuturesTradeEvent fut) {
                double p = fut.price();
                double q = fut.quantity();
                if (fOpen <= 0.0) {
                    fOpen = p;
                    fHigh = p;
                    fLow = p;
                } else {
                    if (p > fHigh) fHigh = p;
                    if (p < fLow) fLow = p;
                }
                fClose = p;
                futVolBtc += q;
                futVolUsd += p * q;
                futDeltaBtc += fut.isBuyerMaker() ? -q : q;
            }
        }

        if (sOpen <= 0.0 || sHigh <= 0.0 || sLow <= 0.0 || sClose <= 0.0) {
            return null;
        }

        double basisBps = (sClose > 0.0 && fClose > 0.0)
                ? ((fClose - sClose) / sClose) * 10000.0
                : 0.0;

        return new MarketCandle(
                tStart,
                tEnd,
                sOpen, sHigh, sLow, sClose,
                spotVolBtc, spotVolUsd, spotDeltaBtc,
                fOpen > 0.0 ? fOpen : sOpen,
                fHigh > 0.0 ? fHigh : sHigh,
                fLow > 0.0 ? fLow : sLow,
                fClose > 0.0 ? fClose : sClose,
                futVolBtc, futVolUsd, futDeltaBtc,
                basisBps
        );
    }

    /**
     * Loads all chronological events for a specific 15-minute market interval using pre-loaded
     * shared daily stream events for the UTC day and loading only the market-specific Polymarket CLOB file.
     *
     * @param metadata market metadata descriptor
     * @param sharedDailyStreamEvents pre-loaded sorted daily stream events for the market's UTC day
     */
    public void loadCandleEvents(
            CollectorMarketMetadata metadata,
            BacktestEvent[] sharedDailyStreamEvents) {

        if (metadata == null) {
            return;
        }

        long tStart = metadata.tStart();
        long tEnd = metadata.tEnd();
        long tStartMs = tStart * 1000L;
        long tEndMs = tEnd * 1000L;
        TradeDirection actualOutcome = metadata.actualOutcome() != null ? metadata.actualOutcome() : TradeDirection.UP;

        scheduleMandatoryLifecycleEvents(metadata, tStart, tEnd, tStartMs, tEndMs, actualOutcome);

        try {
            Statement stmt = getOrCreateStatement();
            loadPolymarketClobEvents(stmt, metadata.marketParquetFile(), tEndMs);
        } catch (SQLException e) {
            Log.warnf("Failed to stream collector Parquet CLOB events for candle %d: %s", tStart, e.getMessage());
        }

        if (sharedDailyStreamEvents != null && sharedDailyStreamEvents.length > 0) {
            scheduleDailySlice(sharedDailyStreamEvents, tStartMs, tEndMs, metadata, tStart, tEnd);
        }
    }

    /**
     * Loads all chronological events for a specific 15-minute market interval from the collector
     * Parquet archives, loading only the external streams requested by the active strategy.
     *
     * @param metadata market metadata descriptor
     * @param datasetService collector Parquet dataset service
     * @param symbol trading asset symbol
     * @param requiredExternalStreams set of external streams required by the strategy
     */
    public void loadCandleEvents(
            CollectorMarketMetadata metadata,
            CollectorParquetDatasetService datasetService,
            String symbol,
            Set<ExternalStreamType> requiredExternalStreams) {

        if (metadata == null) {
            return;
        }

        long tStart = metadata.tStart();
        long tEnd = metadata.tEnd();
        long tStartMs = tStart * 1000L;
        long tEndMs = tEnd * 1000L;
        TradeDirection actualOutcome = metadata.actualOutcome() != null ? metadata.actualOutcome() : TradeDirection.UP;
        Set<ExternalStreamType> extStreams = requiredExternalStreams != null ? requiredExternalStreams : Set.of();

        scheduleMandatoryLifecycleEvents(metadata, tStart, tEnd, tStartMs, tEndMs, actualOutcome);

        if (datasetService == null) {
            return;
        }

        try {
            Statement stmt = getOrCreateStatement();

            // Load 15m Polymarket CLOB market file (including pre-open subscription snapshots <= tEndMs)
            loadPolymarketClobEvents(stmt, metadata.marketParquetFile(), tEndMs);

            // Slice daily PolyBolt & External streams for [tStartMs, tEndMs] from day cache
            String dateStr = metadata.dateStr();
            if (dateStr != null && !dateStr.isBlank()) {
                ensureDailyStreamsLoaded(datasetService, symbol, dateStr, extStreams);
                scheduleDailySlice(this.cachedDailyStreamEvents, tStartMs, tEndMs, metadata, tStart, tEnd);
            }
        } catch (SQLException e) {
            Log.warnf("Failed to stream collector Parquet events for candle %d: %s", tStart, e.getMessage());
        }
    }

    private void scheduleMandatoryLifecycleEvents(
            CollectorMarketMetadata metadata,
            long tStart,
            long tEnd,
            long tStartMs,
            long tEndMs,
            TradeDirection actualOutcome) {

        scheduleEvent(new BacktestEvent.CandleLifecycleEvent(
                tStartMs,
                BacktestEvent.CandleLifecycleType.CANDLE_START,
                tStart, tEnd, actualOutcome, null
        ));

        scheduleEvent(new BacktestEvent.CandleLifecycleEvent(
                (tStart + 60L) * 1000L,
                BacktestEvent.CandleLifecycleType.TWAP_60S_SAMPLE,
                tStart, tEnd, actualOutcome, null
        ));

        scheduleEvent(new BacktestEvent.CandleLifecycleEvent(
                tEndMs,
                BacktestEvent.CandleLifecycleType.CANDLE_CLOSE,
                tStart, tEnd, actualOutcome, null
        ));

        if (!Double.isNaN(metadata.twapOpen()) && metadata.twapOpen() > 0.0) {
            BigDecimal openBd = BigDecimal.valueOf(metadata.twapOpen());
            scheduleEvent(new BacktestEvent.TwapUpdateEvent(
                    tStartMs,
                    new TwapUpdate(
                            Timeframe.FIFTEEN_MINUTES,
                            tStart,
                            tEnd,
                            openBd,
                            new TwapPoint(tStart, openBd, openBd),
                            true
                    )
            ));
        }
    }

    private void loadPolymarketClobEvents(Statement stmt, Path marketFile, long tEndMs) {
        if (marketFile == null || !Files.exists(marketFile)) {
            Log.warnf("Polymarket CLOB market file does not exist: %s", marketFile);
            return;
        }
        String normPath = marketFile.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT ts, event_type, asset_id, COALESCE(price, 0.0), COALESCE(size, 0.0), side, CAST(payload AS VARCHAR) " +
                "FROM read_parquet('" + normPath + "') " +
                "WHERE ts <= " + tEndMs +
                " AND event_type IN ('clob', 'book', 'price_change', 'last_trade_price') " +
                "ORDER BY ts ASC";

        long seq = 0L;
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                long ts = rs.getLong(1);
                String eventType = rs.getString(2);
                String assetId = rs.getString(3);
                double price = rs.getDouble(4);
                double size = rs.getDouble(5);
                String side = rs.getString(6);
                String payload = rs.getString(7);
                if ("clob".equals(eventType) && (payload == null || "[]".equals(payload.trim()))) {
                    continue;
                }
                scheduleEvent(new BacktestEvent.PolymarketClobEvent(
                        ts, ++seq, eventType, assetId, price, size, side, payload
                ));
            }
        } catch (SQLException e) {
            Log.warnf("Could not stream Polymarket CLOB events from %s: %s", marketFile, e.getMessage());
        }
    }

    private void ensureDailyStreamsLoaded(
            CollectorParquetDatasetService datasetService,
            String symbol,
            String dateStr,
            Set<ExternalStreamType> extStreams) {

        String symKey = CollectorParquetDatasetService.extractBaseAsset(symbol);
        if (dateStr.equals(cachedDateStr)
                && symKey.equals(cachedSymbol)
                && extStreams.equals(cachedExternalStreams)) {
            return;
        }

        this.cachedDateStr = dateStr;
        this.cachedSymbol = symKey;
        this.cachedExternalStreams = Set.copyOf(extStreams);
        this.cachedDailyStreamEvents = loadSharedDailyStreams(datasetService, symbol, dateStr, extStreams);
    }

    private void scheduleDailySlice(
            BacktestEvent[] events,
            long tStartMs,
            long tEndMs,
            CollectorMarketMetadata metadata,
            long tStart,
            long tEnd) {

        if (events == null || events.length == 0) {
            return;
        }

        int startIdx = findFirstIndexAtOrAfter(events, tStartMs);
        double lastObservedPrice = Double.NaN;
        double twapOpen = metadata.twapOpen();
        long t60Ms = (tStart + 60L) * 1000L;
        boolean emitted60sTwapUpdate = false;

        for (int i = startIdx; i < events.length; i++) {
            BacktestEvent ev = events[i];
            long ts = ev.timestampMs();
            if (ts > tEndMs) {
                break;
            }
            scheduleEvent(ev);

            if (ev instanceof BacktestEvent.PolymarketCryptoTwap60sEvent twapEv) {
                if (Double.isNaN(twapOpen) || twapOpen <= 0.0) {
                    twapOpen = twapEv.twapPrice();
                }
                lastObservedPrice = twapEv.twapPrice();
            } else if (ev instanceof BacktestEvent.PolymarketCryptoPriceEvent priceEv) {
                lastObservedPrice = priceEv.price();
            } else if (ev instanceof BacktestEvent.BinanceSpotTradeEvent spotEv) {
                lastObservedPrice = spotEv.price();
            }

            if (!emitted60sTwapUpdate && ts >= t60Ms && !Double.isNaN(twapOpen) && twapOpen > 0.0 && !Double.isNaN(lastObservedPrice)) {
                emitted60sTwapUpdate = true;
                BigDecimal openBd = BigDecimal.valueOf(twapOpen);
                BigDecimal spotBd = BigDecimal.valueOf(lastObservedPrice);
                scheduleEvent(new BacktestEvent.TwapUpdateEvent(
                        ts,
                        new TwapUpdate(
                                Timeframe.FIFTEEN_MINUTES,
                                tStart,
                                tEnd,
                                openBd,
                                new TwapPoint(ts / 1000L, spotBd, spotBd),
                                false
                        )
                ));
            }
        }

        if (!emitted60sTwapUpdate && !Double.isNaN(twapOpen) && twapOpen > 0.0 && !Double.isNaN(lastObservedPrice)) {
            BigDecimal openBd = BigDecimal.valueOf(twapOpen);
            BigDecimal spotBd = BigDecimal.valueOf(lastObservedPrice);
            scheduleEvent(new BacktestEvent.TwapUpdateEvent(
                    t60Ms,
                    new TwapUpdate(
                            Timeframe.FIFTEEN_MINUTES,
                            tStart,
                            tEnd,
                            openBd,
                            new TwapPoint(tStart + 60L, spotBd, spotBd),
                            false
                    )
            ));
        }
    }

    private static int findFirstIndexAtOrAfter(BacktestEvent[] events, long targetTsMs) {
        int low = 0;
        int high = events.length - 1;
        int result = events.length;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (events[mid].timestampMs() >= targetTsMs) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return result;
    }

    private static void loadDailyPolymarketCryptoPrice(Statement stmt, Path file, List<BacktestEvent> out) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = file.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT ts, COALESCE(seq, 0), symbol, source, price, COALESCE(price_timestamp, ts) " +
                "FROM read_parquet('" + norm + "') WHERE price IS NOT NULL ORDER BY ts ASC";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new BacktestEvent.PolymarketCryptoPriceEvent(
                        rs.getLong(1),
                        rs.getLong(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getDouble(5),
                        rs.getLong(6)
                ));
            }
        } catch (SQLException e) {
            Log.warnf("Could not load Polymarket price_crypto file %s: %s", file, e.getMessage());
        }
    }

    private static void loadDailyPolymarketCryptoTwap60s(Statement stmt, Path file, List<BacktestEvent> out) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = file.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT ts, COALESCE(seq, 0), symbol, source, twap_price, COALESCE(window_seconds, 60), COALESCE(twap_timestamp, ts) " +
                "FROM read_parquet('" + norm + "') WHERE twap_price IS NOT NULL ORDER BY ts ASC";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new BacktestEvent.PolymarketCryptoTwap60sEvent(
                        rs.getLong(1),
                        rs.getLong(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getDouble(5),
                        rs.getInt(6),
                        rs.getLong(7)
                ));
            }
        } catch (SQLException e) {
            Log.warnf("Could not load Polymarket price_crypto_twap_60s file %s: %s", file, e.getMessage());
        }
    }

    private static void loadDailyBinanceSpotTrades(Statement stmt, Path file, List<BacktestEvent> out) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = file.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT ts, COALESCE(aggregate_trade_id, 0), price, quantity, is_buyer_maker " +
                "FROM read_parquet('" + norm + "') WHERE price IS NOT NULL ORDER BY ts ASC";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new BacktestEvent.BinanceSpotTradeEvent(
                        rs.getLong(1),
                        rs.getLong(2),
                        rs.getDouble(3),
                        rs.getDouble(4),
                        rs.getBoolean(5)
                ));
            }
        } catch (SQLException e) {
            Log.warnf("Could not load Binance Spot aggTrades file %s: %s", file, e.getMessage());
        }
    }

    private static void loadDailyBinanceFuturesTrades(Statement stmt, Path file, List<BacktestEvent> out) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = file.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT ts, COALESCE(aggregate_trade_id, 0), price, quantity, is_buyer_maker " +
                "FROM read_parquet('" + norm + "') WHERE price IS NOT NULL ORDER BY ts ASC";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new BacktestEvent.BinanceFuturesTradeEvent(
                        rs.getLong(1),
                        rs.getLong(2),
                        rs.getDouble(3),
                        rs.getDouble(4),
                        rs.getBoolean(5)
                ));
            }
        } catch (SQLException e) {
            Log.warnf("Could not load Binance Futures aggTrades file %s: %s", file, e.getMessage());
        }
    }

    private static void loadDailyBinanceFuturesDepth(Statement stmt, Path file, List<BacktestEvent> out) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = file.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT ts, COALESCE(final_update_id, 0), bids[1].price, asks[1].price, " +
                "COALESCE(list_sum([x.quantity FOR x IN bids]), 0.0), " +
                "COALESCE(list_sum([x.quantity FOR x IN asks]), 0.0) " +
                "FROM read_parquet('" + norm + "') WHERE len(bids) > 0 AND len(asks) > 0 ORDER BY ts ASC";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                long ts = rs.getLong(1);
                long seq = rs.getLong(2);
                double bestBid = rs.getDouble(3);
                double bestAsk = rs.getDouble(4);
                double depthBids = rs.getDouble(5);
                double depthAsks = rs.getDouble(6);
                double total = depthBids + depthAsks;
                double obi = total > 0.0 ? ((depthBids - depthAsks) / total) : 0.0;
                out.add(new BacktestEvent.BinanceFuturesOrderBookEvent(
                        ts, seq, bestBid, bestAsk, depthBids, depthAsks, obi
                ));
            }
        } catch (SQLException e) {
            Log.warnf("Could not load Binance Futures depth file %s: %s", file, e.getMessage());
        }
    }

    private static void loadDailyBinanceFuturesForceOrders(Statement stmt, Path file, List<BacktestEvent> out) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = file.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT ts, side, COALESCE(price, 0.0), COALESCE(average_price, 0.0), COALESCE(accumulated_quantity, original_quantity, 0.0) " +
                "FROM read_parquet('" + norm + "') ORDER BY ts ASC";
        long seq = 0L;
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new BacktestEvent.BinanceFuturesForceOrderEvent(
                        rs.getLong(1),
                        ++seq,
                        rs.getString(2),
                        rs.getDouble(3),
                        rs.getDouble(4),
                        rs.getDouble(5)
                ));
            }
        } catch (SQLException e) {
            Log.warnf("Could not load Binance Futures forceOrder file %s: %s", file, e.getMessage());
        }
    }

    private static void loadDailyDeribitTrades(Statement stmt, Path file, List<BacktestEvent> out) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = file.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT ts, COALESCE(trade_seq, 0), instrument_name, direction, price, COALESCE(amount, 0.0), " +
                "COALESCE(mark_price, 0.0), COALESCE(index_price, 0.0) " +
                "FROM read_parquet('" + norm + "') WHERE price IS NOT NULL ORDER BY ts ASC";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new BacktestEvent.DeribitFuturesTradeEvent(
                        rs.getLong(1),
                        rs.getLong(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getDouble(5),
                        rs.getDouble(6),
                        rs.getDouble(7),
                        rs.getDouble(8)
                ));
            }
        } catch (SQLException e) {
            Log.warnf("Could not load Deribit trades file %s: %s", file, e.getMessage());
        }
    }

    private Statement getOrCreateStatement() throws SQLException {
        if (sharedConnection == null || sharedConnection.isClosed()) {
            sharedConnection = DriverManager.getConnection("jdbc:duckdb:");
        }
        return sharedConnection.createStatement();
    }

    @Override
    public void close() {
        queue.clear();
        cachedDailyStreamEvents = EMPTY_EVENTS;
        cachedDateStr = null;
        if (sharedConnection != null) {
            try {
                sharedConnection.close();
            } catch (SQLException ignored) {
            }
            sharedConnection = null;
        }
    }
}
