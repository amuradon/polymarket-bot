package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.strategy.ExternalStreamType;
import cz.polymarket.bot.strategy.TradeDirection;
import org.jboss.logging.Logger;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

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

    private static final Logger LOG = Logger.getLogger(ChronologicalEventStreamer.class);
    private static final BacktestEvent[] EMPTY_EVENTS = new BacktestEvent[0];

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            LOG.warn("DuckDB JDBC driver not found on classpath", e);
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

        // 1. Mandatory Candle Lifecycle Milestones
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

        if (datasetService == null) {
            return;
        }

        try {
            Statement stmt = getOrCreateStatement();

            // 2. Load 15m Polymarket CLOB market file (including pre-open subscription snapshots <= tEndMs)
            loadPolymarketClobEvents(stmt, metadata.marketParquetFile(), tEndMs);

            // 3. Slice daily PolyBolt & External streams for [tStartMs, tEndMs] from day cache
            String dateStr = metadata.dateStr();
            if (dateStr != null && !dateStr.isBlank()) {
                ensureDailyStreamsLoaded(stmt, datasetService, symbol, dateStr, extStreams);
                scheduleDailySlice(tStartMs, tEndMs, metadata, tStart, tEnd);
            }
        } catch (SQLException e) {
            LOG.warnf("Failed to stream collector Parquet events for candle %d: %s", tStart, e.getMessage());
        }
    }

    private void loadPolymarketClobEvents(Statement stmt, Path marketFile, long tEndMs) {
        if (marketFile == null || !Files.exists(marketFile)) {
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
            LOG.warnf("Could not stream Polymarket CLOB events from %s: %s", marketFile, e.getMessage());
        }
    }

    private void ensureDailyStreamsLoaded(
            Statement stmt,
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

        List<BacktestEvent> dayEvents = new ArrayList<>(65_536);

        // Always load Polymarket crypto price and 60s TWAP streams
        loadDailyPolymarketCryptoPrice(stmt, datasetService.getPolymarketCryptoPriceFile(symbol, dateStr), dayEvents);
        loadDailyPolymarketCryptoTwap60s(stmt, datasetService.getPolymarketCryptoTwap60sFile(symbol, dateStr), dayEvents);

        // Conditionally load external streams only if required by the strategy
        if (extStreams.contains(ExternalStreamType.BINANCE_SPOT_AGG_TRADES)) {
            loadDailyBinanceSpotTrades(stmt, datasetService.getBinanceSpotAggTradesFile(symbol, dateStr), dayEvents);
        }
        if (extStreams.contains(ExternalStreamType.BINANCE_FUTURES_AGG_TRADES)) {
            loadDailyBinanceFuturesTrades(stmt, datasetService.getBinanceFuturesAggTradesFile(symbol, dateStr), dayEvents);
        }
        if (extStreams.contains(ExternalStreamType.BINANCE_FUTURES_DEPTH_PARTIAL)) {
            loadDailyBinanceFuturesDepth(stmt, datasetService.getBinanceFuturesDepthPartialFile(symbol, dateStr), dayEvents);
        }
        if (extStreams.contains(ExternalStreamType.BINANCE_FUTURES_DEPTH_DIFF)) {
            loadDailyBinanceFuturesDepth(stmt, datasetService.getBinanceFuturesDepthDiffFile(symbol, dateStr), dayEvents);
        }
        if (extStreams.contains(ExternalStreamType.BINANCE_FUTURES_FORCE_ORDER)) {
            loadDailyBinanceFuturesForceOrders(stmt, datasetService.getBinanceFuturesForceOrderFile(symbol, dateStr), dayEvents);
        }
        if (extStreams.contains(ExternalStreamType.DERIBIT_FUTURES_TRADES)) {
            loadDailyDeribitTrades(stmt, datasetService.getDeribitFuturesTradesFile(symbol, dateStr), dayEvents);
        }

        dayEvents.sort(Comparator.naturalOrder());
        this.cachedDateStr = dateStr;
        this.cachedSymbol = symKey;
        this.cachedExternalStreams = Set.copyOf(extStreams);
        this.cachedDailyStreamEvents = dayEvents.toArray(EMPTY_EVENTS);
    }

    private void scheduleDailySlice(
            long tStartMs,
            long tEndMs,
            CollectorMarketMetadata metadata,
            long tStart,
            long tEnd) {

        BacktestEvent[] events = this.cachedDailyStreamEvents;
        if (events.length == 0) {
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

    private void loadDailyPolymarketCryptoPrice(Statement stmt, Path file, List<BacktestEvent> out) {
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
            LOG.warnf("Could not load Polymarket price_crypto file %s: %s", file, e.getMessage());
        }
    }

    private void loadDailyPolymarketCryptoTwap60s(Statement stmt, Path file, List<BacktestEvent> out) {
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
            LOG.warnf("Could not load Polymarket price_crypto_twap_60s file %s: %s", file, e.getMessage());
        }
    }

    private void loadDailyBinanceSpotTrades(Statement stmt, Path file, List<BacktestEvent> out) {
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
            LOG.warnf("Could not load Binance Spot aggTrades file %s: %s", file, e.getMessage());
        }
    }

    private void loadDailyBinanceFuturesTrades(Statement stmt, Path file, List<BacktestEvent> out) {
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
            LOG.warnf("Could not load Binance Futures aggTrades file %s: %s", file, e.getMessage());
        }
    }

    private void loadDailyBinanceFuturesDepth(Statement stmt, Path file, List<BacktestEvent> out) {
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
            LOG.warnf("Could not load Binance Futures depth file %s: %s", file, e.getMessage());
        }
    }

    private void loadDailyBinanceFuturesForceOrders(Statement stmt, Path file, List<BacktestEvent> out) {
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
            LOG.warnf("Could not load Binance Futures forceOrder file %s: %s", file, e.getMessage());
        }
    }

    private void loadDailyDeribitTrades(Statement stmt, Path file, List<BacktestEvent> out) {
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
            LOG.warnf("Could not load Deribit trades file %s: %s", file, e.getMessage());
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
