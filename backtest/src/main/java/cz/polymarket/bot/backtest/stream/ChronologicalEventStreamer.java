package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.calculator.CalculatedSpotTwapEngine;
import cz.polymarket.bot.strategy.ExternalStreamType;
import cz.polymarket.bot.strategy.TradeDirection;
import io.quarkus.logging.Log;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * High-performance chronological event multiplexer and streamer for backtesting.
 * <p>
 * Streams captured events from typed collector Parquet archives ordered strictly by the local EC2
 * capture timestamp {@code ts} ({@link BacktestEvent#timestampMs()}) using a zero-copy 3-way merge
 * over:
 * <ol>
 *   <li>Lazy DuckDB streaming cursor grouping typed Polymarket CLOB {@code frame_id} rows,</li>
 *   <li>Direct array cursor over shared daily stream events ({@code O(log N)} binary search slice), and</li>
 *   <li>Small priority queue for candle lifecycle milestones and dynamically scheduled pending orders.</li>
 * </ol>
 */
public class ChronologicalEventStreamer implements AutoCloseable {

    private static final BacktestEvent[] EMPTY_EVENTS = new BacktestEvent[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];
    private static final int[] EMPTY_INTS = new int[0];
    private static final double[] EMPTY_DOUBLES = new double[0];
    private static final long BOUNDARY_TWAP_OFFSET_MS = 10_000L;
    private static final long FAST_SNAPSHOT_LOOKBACK_MS = 30_000L;
    private static final long SPOT_TWAP_WARMUP_MS = 65_000L;
    private static final long KRAKEN_USDT_WARMUP_MS = 1_800_000L;

    private final DuckDbSessionManager duckDbSessionManager;
    private final long binanceDelayMs;
    private final long krakenDelayMs;
    private final long coinbaseDelayMs;
    private final PriorityQueue<BacktestEvent> queue = new PriorityQueue<>(16);
    private final CalculatedSpotTwapEngine calculatedSpotTwapEngine = new CalculatedSpotTwapEngine();

    private Connection sharedConnection;
    private Statement clobStatement;
    private ResultSet clobResultSet;
    private Path activeClobFile;
    private long clobSeq;
    private BacktestEvent.PolymarketClobFrameEvent nextClobEvent;

    private final SpotVenueCursor[] venueCursors = {
            new SpotVenueCursor(CalculatedSpotTwapEngine.SpotVenue.BINANCE),
            new SpotVenueCursor(CalculatedSpotTwapEngine.SpotVenue.COINBASE),
            new SpotVenueCursor(CalculatedSpotTwapEngine.SpotVenue.KRAKEN)
    };
    private Connection calcKrakenConnection;
    private Statement calcKrakenStatement;
    private ResultSet calcKrakenResultSet;
    private boolean hasNextKrakenRow;
    private long nextKrakenTsMs;
    private double nextKrakenPrice;
    private String calcCryptoSymbol;
    private long calcSpotSeq;
    private BacktestEvent.CalculatedSpotTwapEvent nextCalculatedSpotTwapEvent;

    private final class SpotVenueCursor {
        final CalculatedSpotTwapEngine.SpotVenue venue;
        Connection connection;
        Statement statement;
        ResultSet resultSet;
        boolean hasNextRow;
        long nextTsMs;
        double nextPrice;
        double nextQty;

        SpotVenueCursor(CalculatedSpotTwapEngine.SpotVenue venue) {
            this.venue = venue;
        }

        void open(String sql) throws SQLException {
            closeStream();
            if (connection == null || connection.isClosed()) {
                connection = duckDbSessionManager.openSession();
            }
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            advance();
        }

        void advance() throws SQLException {
            if (resultSet != null && resultSet.next()) {
                nextTsMs = resultSet.getLong(1);
                nextPrice = resultSet.getDouble(2);
                nextQty = resultSet.getDouble(3);
                hasNextRow = true;
            } else {
                hasNextRow = false;
            }
        }

        void closeStream() {
            hasNextRow = false;
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException ignored) {
                }
                resultSet = null;
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ignored) {
                }
                statement = null;
            }
        }

        void closeConnection() {
            closeStream();
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
                connection = null;
            }
        }
    }

    // 1-row lookahead state and reusable scratch buffers for grouping consecutive frame_id rows
    private boolean hasLookaheadClobRow;
    private boolean clobResultSetExhausted;
    private long laTs;
    private long laServerTs;
    private long laFrameId;
    private byte laEventCode;
    private boolean laIsUp;
    private boolean laIsUpNull;
    private boolean laIsBid;
    private boolean laIsBidNull;
    private int laTick;
    private boolean laTickNull;
    private double laSize;
    private boolean laSizeNull;

    private boolean[] scratchUpFlags = new boolean[128];
    private boolean[] scratchBidFlags = new boolean[128];
    private int[] scratchTicks = new int[128];
    private double[] scratchSizes = new double[128];

    // Zero-copy daily event array slice cursor
    private BacktestEvent[] activeDailyEvents = EMPTY_EVENTS;
    private int dailyCursorIdx = 0;
    private int dailyEndIdx = 0;

    /**
     * Constructs a chronological event streamer backed by the shared DuckDB session manager and
     * per-exchange simulated feed delays.
     *
     * @param duckDbSessionManager shared DuckDB session manager providing streaming connections
     * @param binanceDelayMs simulated Binance one-way feed delay in milliseconds
     * @param krakenDelayMs simulated Kraken one-way feed delay in milliseconds
     * @param coinbaseDelayMs simulated Coinbase one-way feed delay in milliseconds
     */
    public ChronologicalEventStreamer(
            DuckDbSessionManager duckDbSessionManager,
            long binanceDelayMs,
            long krakenDelayMs,
            long coinbaseDelayMs) {
        if (duckDbSessionManager == null) {
            Log.error("Invalid argument in ChronologicalEventStreamer: duckDbSessionManager cannot be null");
            throw new IllegalArgumentException("duckDbSessionManager cannot be null");
        }
        if (binanceDelayMs < 0L) {
            Log.error("Invalid argument in ChronologicalEventStreamer: binanceDelayMs must be non-negative");
            throw new IllegalArgumentException("binanceDelayMs must be non-negative");
        }
        if (krakenDelayMs < 0L) {
            Log.error("Invalid argument in ChronologicalEventStreamer: krakenDelayMs must be non-negative");
            throw new IllegalArgumentException("krakenDelayMs must be non-negative");
        }
        if (coinbaseDelayMs < 0L) {
            Log.error("Invalid argument in ChronologicalEventStreamer: coinbaseDelayMs must be non-negative");
            throw new IllegalArgumentException("coinbaseDelayMs must be non-negative");
        }
        this.duckDbSessionManager = duckDbSessionManager;
        this.binanceDelayMs = binanceDelayMs;
        this.krakenDelayMs = krakenDelayMs;
        this.coinbaseDelayMs = coinbaseDelayMs;
    }

    /**
     * Checks if there are pending events in the CLOB stream, calculated Spot/TWAP stream, daily slice cursor, or priority queue.
     *
     * @return true if more events exist
     */
    public boolean hasNext() {
        return nextClobEvent != null || nextCalculatedSpotTwapEvent != null || dailyCursorIdx < dailyEndIdx || !queue.isEmpty();
    }

    /**
     * Retrieves and removes the next chronological event via {@code O(1)} merge across
     * the active CLOB cursor, the zero-copy daily stream cursor, the calculated Spot/TWAP cursor, and the dynamic priority queue.
     *
     * @return next event, or null if no events remain
     */
    public BacktestEvent next() {
        BacktestEvent a = nextClobEvent;
        BacktestEvent b = (dailyCursorIdx < dailyEndIdx) ? activeDailyEvents[dailyCursorIdx] : null;
        BacktestEvent c = nextCalculatedSpotTwapEvent;
        BacktestEvent d = queue.peek();

        int winner = 0;
        BacktestEvent best = null;
        if (a != null) {
            best = a;
            winner = 1;
        }
        if (b != null && (best == null || b.compareTo(best) < 0)) {
            best = b;
            winner = 2;
        }
        if (c != null && (best == null || c.compareTo(best) < 0)) {
            best = c;
            winner = 3;
        }
        if (d != null && (best == null || d.compareTo(best) < 0)) {
            winner = 4;
        }

        return switch (winner) {
            case 1 -> {
                advanceClobCursor();
                yield a;
            }
            case 2 -> {
                dailyCursorIdx++;
                yield b;
            }
            case 3 -> {
                advanceCalculatedSpotTwapCursor();
                yield c;
            }
            case 4 -> queue.poll();
            default -> null;
        };
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
     * Clears all pending events, resets the daily slice cursor, and closes any active CLOB and calculated Spot/TWAP cursors.
     */
    public void clear() {
        closeClobStream();
        closeCalculatedSpotTwapStream();
        activeDailyEvents = EMPTY_EVENTS;
        dailyCursorIdx = 0;
        dailyEndIdx = 0;
        queue.clear();
    }

    /**
     * Loads all shared daily stream events ({@code polymarket/price_crypto}, {@code polymarket/price_crypto_twap_60s},
     * and any strategy-requested external Binance streams) for a single UTC day into a sorted,
     * immutable array shared read-only across all parallel 15-minute market workers of that day.
     * <p>
     * External Binance aggTrades timestamps are shifted forward by {@code binanceDelayMs} relative to
     * the server timestamp ({@code COALESCE(event_time, trade_time)}) to simulate feed delivery latency.
     *
     * @param datasetService collector Parquet dataset service
     * @param symbol trading asset symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @param requiredExternalStreams set of external streams required by the strategy
     * @param binanceDelayMs simulated Binance one-way feed delay in milliseconds
     * @return sorted array of daily stream events
     */
    public static BacktestEvent[] loadSharedDailyStreams(
            CollectorParquetDatasetService datasetService,
            String symbol,
            String dateStr,
            Set<ExternalStreamType> requiredExternalStreams,
            long binanceDelayMs) {

        if (binanceDelayMs < 0L) {
            Log.error("Invalid argument in loadSharedDailyStreams: binanceDelayMs must be non-negative");
            throw new IllegalArgumentException("binanceDelayMs must be non-negative");
        }
        if (datasetService == null || dateStr == null || dateStr.isBlank()) {
            return EMPTY_EVENTS;
        }

        DuckDbSessionManager sessionManager = datasetService.getDuckDbSessionManager();
        Set<ExternalStreamType> extStreams = requiredExternalStreams != null ? requiredExternalStreams : Set.of();

        record DailyLoaderTask(Path file, BiConsumer<Statement, List<BacktestEvent>> loader) {}
        List<DailyLoaderTask> tasks = new ArrayList<>(4);

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
                tasks.add(new DailyLoaderTask(f, (stmt, out) -> loadDailyBinanceTrades(stmt, f, binanceDelayMs, true, out)));
            }
        }
        if (extStreams.contains(ExternalStreamType.BINANCE_FUTURES_AGG_TRADES)) {
            Path f = datasetService.getBinanceFuturesAggTradesFile(symbol, dateStr);
            if (f != null && Files.exists(f)) {
                tasks.add(new DailyLoaderTask(f, (stmt, out) -> loadDailyBinanceTrades(stmt, f, binanceDelayMs, false, out)));
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
            try (Connection conn = sessionManager.openSession();
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
                try (Connection conn = sessionManager.openSession();
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
     * Loads chronological events for a specific 15-minute market interval using pre-loaded shared daily
     * stream events (always covering the full {@code [tStartMs, tEndMs]} interval including boundary 60s TWAP)
     * while optionally pruning Polymarket CLOB frames to the final {@code clobLookbackMs} window seeded
     * from the latest pre-window {@code book} snapshots.
     *
     * @param metadata market metadata descriptor
     * @param sharedDailyStreamEvents pre-loaded sorted daily stream events for the market's UTC day
     * @param clobLookbackMs lookback window before {@code tEndMs} in milliseconds (or {@link Long#MAX_VALUE} for full stream)
     */
    public void loadCandleEvents(
            CollectorMarketMetadata metadata,
            BacktestEvent[] sharedDailyStreamEvents,
            long clobLookbackMs) {
        loadCandleEvents(metadata, sharedDailyStreamEvents, null, null, clobLookbackMs);
    }

    /**
     * Loads all chronological events for a specific 15-minute market interval using pre-loaded
     * shared daily stream events for the UTC day and lazily streaming the Polymarket CLOB file
     * (optionally pruned to {@code clobLookbackMs}) plus the calculated USD Spot &amp; 60s TWAP
     * from Binance, Coinbase, and Kraken Spot Parquet files.
     *
     * @param metadata market metadata descriptor
     * @param sharedDailyStreamEvents pre-loaded sorted daily stream events for the market's UTC day
     * @param datasetService optional collector Parquet dataset service for streaming Binance/Coinbase/Kraken files
     * @param symbol optional trading asset symbol
     * @param clobLookbackMs lookback window before {@code tEndMs} in milliseconds (or {@link Long#MAX_VALUE} for full stream)
     */
    public void loadCandleEvents(
            CollectorMarketMetadata metadata,
            BacktestEvent[] sharedDailyStreamEvents,
            CollectorParquetDatasetService datasetService,
            String symbol,
            long clobLookbackMs) {

        if (metadata == null) {
            return;
        }

        long tStart = metadata.tStart();
        long tEnd = metadata.tEnd();
        long tStartMs = tStart * 1000L;
        long tEndMs = tEnd * 1000L;
        TradeDirection actualOutcome = metadata.actualOutcome() != null ? metadata.actualOutcome() : TradeDirection.UP;

        scheduleMandatoryLifecycleEvents(tStart, tEnd, tStartMs, tEndMs, actualOutcome);
        prepareDailySlice(sharedDailyStreamEvents, tStartMs, tEndMs, metadata);
        openPolymarketClobStream(metadata.marketParquetFile(), tStartMs, tEndMs, clobLookbackMs);
        if (datasetService != null && metadata.dateStr() != null && !metadata.dateStr().isBlank()) {
            openCalculatedSpotTwapStream(datasetService, symbol, metadata.dateStr(), tStartMs, tEndMs);
        }
    }

    private void scheduleMandatoryLifecycleEvents(
            long tStart,
            long tEnd,
            long tStartMs,
            long tEndMs,
            TradeDirection actualOutcome) {

        scheduleEvent(new BacktestEvent.CandleLifecycleEvent(
                tStartMs,
                BacktestEvent.CandleLifecycleType.CANDLE_START,
                tStart, tEnd, actualOutcome
        ));

        scheduleEvent(new BacktestEvent.CandleLifecycleEvent(
                tEndMs,
                BacktestEvent.CandleLifecycleType.CANDLE_CLOSE,
                tStart, tEnd, actualOutcome
        ));
    }

    private void openCalculatedSpotTwapStream(
            CollectorParquetDatasetService datasetService,
            String symbol,
            String dateStr,
            long tStartMs,
            long tEndMs) {
        closeCalculatedSpotTwapStream();
        if (datasetService == null || dateStr == null || dateStr.isBlank()) {
            return;
        }

        String prevDateStr = null;
        try {
            prevDateStr = LocalDate.parse(dateStr.trim()).minusDays(1).toString();
        } catch (Exception ignored) {
        }

        Path curBinanceFile = datasetService.getBinanceSpotAggTradesFile(symbol, dateStr);
        Path prevBinanceFile = prevDateStr != null ? datasetService.getBinanceSpotAggTradesFile(symbol, prevDateStr) : null;
        String binanceReadExpr = buildParquetReadExpression(prevBinanceFile, curBinanceFile);

        Path curCoinbaseFile = datasetService.getCoinbaseSpotTradesFile(symbol, dateStr);
        Path prevCoinbaseFile = prevDateStr != null ? datasetService.getCoinbaseSpotTradesFile(symbol, prevDateStr) : null;
        String coinbaseReadExpr = buildParquetReadExpression(prevCoinbaseFile, curCoinbaseFile);

        Path curKrakenSpotFile = datasetService.getKrakenSpotTradesFile(symbol, dateStr);
        Path prevKrakenSpotFile = prevDateStr != null ? datasetService.getKrakenSpotTradesFile(symbol, prevDateStr) : null;
        String krakenSpotReadExpr = buildParquetReadExpression(prevKrakenSpotFile, curKrakenSpotFile);

        if (binanceReadExpr == null && coinbaseReadExpr == null && krakenSpotReadExpr == null) {
            return;
        }

        long spotWarmupStartMs = Math.max(0L, tStartMs - SPOT_TWAP_WARMUP_MS);
        long krakenWarmupStartMs = Math.max(0L, tStartMs - KRAKEN_USDT_WARMUP_MS);

        Path curKrakenUsdtFile = datasetService.getKrakenSpotUsdtUsdTradesFile(dateStr);
        Path prevKrakenUsdtFile = prevDateStr != null ? datasetService.getKrakenSpotUsdtUsdTradesFile(prevDateStr) : null;
        String krakenUsdtReadExpr = buildParquetReadExpression(prevKrakenUsdtFile, curKrakenUsdtFile);

        this.calculatedSpotTwapEngine.reset();
        this.calcCryptoSymbol = CollectorParquetDatasetService.extractBaseAsset(symbol).toLowerCase(Locale.ROOT) + "usd";
        this.calcSpotSeq = 0L;

        try {
            if (krakenUsdtReadExpr != null && tEndMs >= krakenDelayMs) {
                long minKrakenUsdtServerMs = Math.max(0L, krakenWarmupStartMs - krakenDelayMs);
                long maxKrakenUsdtServerMs = tEndMs - krakenDelayMs;
                String krakenUsdtSql = "SELECT (CAST(trade_timestamp AS BIGINT) + " + krakenDelayMs + ") AS eff_ts, price FROM " +
                        krakenUsdtReadExpr +
                        " WHERE trade_timestamp >= " + minKrakenUsdtServerMs +
                        " AND trade_timestamp <= " + maxKrakenUsdtServerMs +
                        " AND price IS NOT NULL" +
                        " ORDER BY eff_ts ASC, trade_id ASC";
                this.calcKrakenStatement = getOrCreateCalcKrakenStatement();
                this.calcKrakenResultSet = this.calcKrakenStatement.executeQuery(krakenUsdtSql);
                advanceKrakenRow();
            }

            String binanceSql = buildSpotTradesSql(
                    binanceReadExpr, "COALESCE(event_time, trade_time)", "COALESCE(quantity, 1.0)",
                    "aggregate_trade_id", spotWarmupStartMs, tEndMs, binanceDelayMs);
            if (binanceSql != null) {
                venueCursors[0].open(binanceSql);
            }

            String coinbaseSql = buildSpotTradesSql(
                    coinbaseReadExpr, "trade_timestamp", "COALESCE(qty, 1.0)",
                    "trade_id", spotWarmupStartMs, tEndMs, coinbaseDelayMs);
            if (coinbaseSql != null) {
                venueCursors[1].open(coinbaseSql);
            }

            String krakenSpotSql = buildSpotTradesSql(
                    krakenSpotReadExpr, "trade_timestamp", "COALESCE(qty, 1.0)",
                    "trade_id", spotWarmupStartMs, tEndMs, krakenDelayMs);
            if (krakenSpotSql != null) {
                venueCursors[2].open(krakenSpotSql);
            }

            // Advance through the pre-candle warm-up window [spotWarmupStartMs, tStartMs) and stop at first event >= tStartMs
            while (true) {
                SpotVenueCursor bestCursor = pickEarliestVenueCursor();
                if (bestCursor == null) {
                    break;
                }
                long ts = bestCursor.nextTsMs;
                double price = bestCursor.nextPrice;
                double qty = bestCursor.nextQty;
                CalculatedSpotTwapEngine.SpotVenue venue = bestCursor.venue;
                bestCursor.advance();

                consumeKrakenUpTo(ts);
                double twap60s = calculatedSpotTwapEngine.onVenueTrade(venue, ts, price, qty);
                if (ts >= tStartMs) {
                    double spotUsd = calculatedSpotTwapEngine.getLastSpotPriceUsd();
                    nextCalculatedSpotTwapEvent = new BacktestEvent.CalculatedSpotTwapEvent(
                            ts, ++calcSpotSeq, calcCryptoSymbol, spotUsd, twap60s
                    );
                    return;
                }
            }
            closeCalculatedSpotTwapStream();
        } catch (SQLException e) {
            Log.warnf("Could not stream multi-exchange spot TWAP events for %s on %s: %s", symbol, dateStr, e.getMessage());
            closeCalculatedSpotTwapStream();
        }
    }

    private void advanceCalculatedSpotTwapCursor() {
        try {
            SpotVenueCursor bestCursor = pickEarliestVenueCursor();
            if (bestCursor != null) {
                long ts = bestCursor.nextTsMs;
                double price = bestCursor.nextPrice;
                double qty = bestCursor.nextQty;
                CalculatedSpotTwapEngine.SpotVenue venue = bestCursor.venue;
                bestCursor.advance();

                consumeKrakenUpTo(ts);
                double twap60s = calculatedSpotTwapEngine.onVenueTrade(venue, ts, price, qty);
                double spotUsd = calculatedSpotTwapEngine.getLastSpotPriceUsd();
                nextCalculatedSpotTwapEvent = new BacktestEvent.CalculatedSpotTwapEvent(
                        ts, ++calcSpotSeq, calcCryptoSymbol, spotUsd, twap60s
                );
                return;
            }
        } catch (SQLException e) {
            Log.warnf("Error advancing multi-exchange spot TWAP stream: %s", e.getMessage());
        }
        closeCalculatedSpotTwapStream();
    }

    private SpotVenueCursor pickEarliestVenueCursor() {
        SpotVenueCursor best = null;
        for (int i = 0; i < venueCursors.length; i++) {
            SpotVenueCursor c = venueCursors[i];
            if (c.hasNextRow && (best == null || c.nextTsMs < best.nextTsMs)) {
                best = c;
            }
        }
        return best;
    }

    private void consumeKrakenUpTo(long targetTimestampMs) throws SQLException {
        while (hasNextKrakenRow && nextKrakenTsMs <= targetTimestampMs) {
            calculatedSpotTwapEngine.onUsdtUsdTrade(nextKrakenTsMs, nextKrakenPrice);
            advanceKrakenRow();
        }
    }

    private void advanceKrakenRow() throws SQLException {
        if (calcKrakenResultSet != null && calcKrakenResultSet.next()) {
            nextKrakenTsMs = calcKrakenResultSet.getLong(1);
            nextKrakenPrice = calcKrakenResultSet.getDouble(2);
            hasNextKrakenRow = true;
        } else {
            hasNextKrakenRow = false;
        }
    }

    private static String buildSpotTradesSql(
            String readExpr,
            String serverTsExpr,
            String qtyExpr,
            String tieBreakerCol,
            long warmupStartMs,
            long tEndMs,
            long delayMs) {
        if (readExpr == null || tEndMs < delayMs) {
            return null;
        }
        long minServerMs = Math.max(0L, warmupStartMs - delayMs);
        long maxServerMs = tEndMs - delayMs;
        return "SELECT (CAST(" + serverTsExpr + " AS BIGINT) + " + delayMs + ") AS eff_ts, " +
                "price, " + qtyExpr + " FROM " + readExpr +
                " WHERE " + serverTsExpr + " >= " + minServerMs +
                " AND " + serverTsExpr + " <= " + maxServerMs +
                " AND price IS NOT NULL" +
                " ORDER BY eff_ts ASC, " + tieBreakerCol + " ASC";
    }

    private static String buildParquetReadExpression(Path prevFile, Path curFile) {
        boolean hasPrev = prevFile != null && Files.exists(prevFile);
        boolean hasCur = curFile != null && Files.exists(curFile);
        if (!hasPrev && !hasCur) {
            return null;
        }
        if (hasPrev && hasCur) {
            String p1 = prevFile.toAbsolutePath().toString().replace('\\', '/');
            String p2 = curFile.toAbsolutePath().toString().replace('\\', '/');
            return "read_parquet(['" + p1 + "', '" + p2 + "'])";
        }
        Path single = hasCur ? curFile : prevFile;
        String norm = single.toAbsolutePath().toString().replace('\\', '/');
        return "read_parquet('" + norm + "')";
    }

    private void closeCalculatedSpotTwapStream() {
        nextCalculatedSpotTwapEvent = null;
        hasNextKrakenRow = false;
        for (int i = 0; i < venueCursors.length; i++) {
            venueCursors[i].closeStream();
        }
        if (calcKrakenResultSet != null) {
            try {
                calcKrakenResultSet.close();
            } catch (SQLException ignored) {
            }
            calcKrakenResultSet = null;
        }
        if (calcKrakenStatement != null) {
            try {
                calcKrakenStatement.close();
            } catch (SQLException ignored) {
            }
            calcKrakenStatement = null;
        }
    }

    private void prepareDailySlice(
            BacktestEvent[] events,
            long tStartMs,
            long tEndMs,
            CollectorMarketMetadata metadata) {

        if (events == null || events.length == 0) {
            this.activeDailyEvents = EMPTY_EVENTS;
            this.dailyCursorIdx = 0;
            this.dailyEndIdx = 0;
            if (!Double.isNaN(metadata.twapOpen()) && metadata.twapOpen() > 0.0) {
                scheduleBoundaryTwapEvent(tStartMs, metadata.twapOpen(), resolveCryptoSymbol(metadata));
            }
            return;
        }

        int startIdx = findFirstIndexAtOrAfter(events, tStartMs);
        int endIdx = findFirstIndexAfter(events, tEndMs);
        this.activeDailyEvents = events;
        this.dailyCursorIdx = startIdx;
        this.dailyEndIdx = endIdx;

        boolean hasOpeningTwapSample = false;
        String observedCryptoSymbol = null;

        for (int i = startIdx; i < endIdx; i++) {
            BacktestEvent ev = events[i];
            long ts = ev.timestampMs();

            if (ev instanceof BacktestEvent.PolymarketCryptoTwap60sEvent twapEv) {
                if (observedCryptoSymbol == null && twapEv.symbol() != null && !twapEv.symbol().isBlank()) {
                    observedCryptoSymbol = twapEv.symbol();
                }
                long effTs = twapEv.twapTimestampMs() > 0L ? twapEv.twapTimestampMs() : ts;
                if (effTs >= tStartMs && effTs <= tStartMs + BOUNDARY_TWAP_OFFSET_MS) {
                    hasOpeningTwapSample = true;
                }
            } else if (ev instanceof BacktestEvent.PolymarketCryptoPriceEvent priceEv) {
                if (observedCryptoSymbol == null && priceEv.symbol() != null && !priceEv.symbol().isBlank()) {
                    observedCryptoSymbol = priceEv.symbol();
                }
            }
        }

        if (!hasOpeningTwapSample && !Double.isNaN(metadata.twapOpen()) && metadata.twapOpen() > 0.0) {
            String sym = observedCryptoSymbol != null ? observedCryptoSymbol : resolveCryptoSymbol(metadata);
            scheduleBoundaryTwapEvent(tStartMs, metadata.twapOpen(), sym);
        }
    }

    private void scheduleBoundaryTwapEvent(long tStartMs, double twapOpen, String cryptoSymbol) {
        scheduleEvent(new BacktestEvent.PolymarketCryptoTwap60sEvent(
                tStartMs,
                0L,
                cryptoSymbol,
                "chainlink",
                twapOpen,
                60,
                tStartMs
        ));
    }

    private static String resolveCryptoSymbol(CollectorMarketMetadata metadata) {
        if (metadata != null && metadata.marketParquetFile() != null) {
            Path fileName = metadata.marketParquetFile().getFileName();
            if (fileName != null) {
                String name = fileName.toString();
                int dashIdx = name.indexOf('-');
                if (dashIdx > 0) {
                    return name.substring(0, dashIdx).toLowerCase(Locale.ROOT) + "usd";
                }
            }
        }
        return "btcusd";
    }

    private void openPolymarketClobStream(Path marketFile, long tStartMs, long tEndMs, long clobLookbackMs) {
        closeClobStream();
        if (marketFile == null || !Files.exists(marketFile)) {
            Log.warnf("Polymarket CLOB market file does not exist: %s", marketFile);
            return;
        }
        String normPath = marketFile.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");

        try {
            Statement stmt = getOrCreateStatement();
            long seedStartTs = 0L;
            if (clobLookbackMs > 0L && clobLookbackMs < (tEndMs - tStartMs)) {
                long clobWindowStartMs = Math.max(tStartMs, tEndMs - clobLookbackMs);
                seedStartTs = resolveClobSeedStartTs(stmt, normPath, clobWindowStartMs);
            }

            String sql = "SELECT ts, server_ts, frame_id, event_code, is_up, is_bid, tick, size " +
                    "FROM read_parquet('" + normPath + "') " +
                    "WHERE ts >= " + seedStartTs + " AND ts <= " + tEndMs + " AND event_code > 0";

            this.activeClobFile = marketFile;
            this.clobSeq = 0L;
            this.hasLookaheadClobRow = false;
            this.clobResultSetExhausted = false;
            this.clobStatement = stmt;
            this.clobResultSet = stmt.executeQuery(sql);
            advanceClobCursor();
        } catch (SQLException e) {
            Log.warnf("Could not stream Polymarket CLOB events from %s: %s", marketFile, e.getMessage());
            closeClobStream();
        }
    }

    private static long resolveClobSeedStartTs(Statement stmt, String normPath, long clobWindowStartMs) throws SQLException {
        long fastFloorMs = Math.max(0L, clobWindowStartMs - FAST_SNAPSHOT_LOOKBACK_MS);
        String fastSql = "SELECT MAX(ts) FILTER (WHERE is_up = true), MAX(ts) FILTER (WHERE is_up = false) " +
                "FROM read_parquet('" + normPath + "') " +
                "WHERE ts >= " + fastFloorMs + " AND ts <= " + clobWindowStartMs + " AND event_code = 1";

        try (ResultSet rs = stmt.executeQuery(fastSql)) {
            if (rs.next()) {
                long upTs = rs.getLong(1);
                boolean upNull = rs.wasNull();
                long downTs = rs.getLong(2);
                boolean downNull = rs.wasNull();
                if (!upNull && !downNull && upTs > 0L && downTs > 0L) {
                    return Math.min(upTs, downTs);
                }
            }
        }

        String fullPreSql = "SELECT " +
                "MAX(ts) FILTER (WHERE event_code = 1 AND is_up = true), " +
                "BOOL_OR(is_up = true), " +
                "MAX(ts) FILTER (WHERE event_code = 1 AND is_up = false), " +
                "BOOL_OR(is_up = false) " +
                "FROM read_parquet('" + normPath + "') " +
                "WHERE ts <= " + clobWindowStartMs + " AND event_code IN (1, 2)";

        try (ResultSet rs = stmt.executeQuery(fullPreSql)) {
            if (rs.next()) {
                long upBookTs = rs.getLong(1);
                boolean upBookNull = rs.wasNull();
                boolean hasUp = rs.getBoolean(2) && !rs.wasNull();
                long downBookTs = rs.getLong(3);
                boolean downBookNull = rs.wasNull();
                boolean hasDown = rs.getBoolean(4) && !rs.wasNull();

                if (!hasUp && !hasDown) {
                    return clobWindowStartMs;
                }
                long reqTs = clobWindowStartMs;
                if (hasUp) {
                    if (upBookNull || upBookTs <= 0L) {
                        return 0L;
                    }
                    reqTs = Math.min(reqTs, upBookTs);
                }
                if (hasDown) {
                    if (downBookNull || downBookTs <= 0L) {
                        return 0L;
                    }
                    reqTs = Math.min(reqTs, downBookTs);
                }
                return reqTs;
            }
        }
        return 0L;
    }

    private boolean readNextClobRowIntoLookahead() throws SQLException {
        if (clobResultSet == null || clobResultSetExhausted) {
            hasLookaheadClobRow = false;
            return false;
        }
        if (!clobResultSet.next()) {
            clobResultSetExhausted = true;
            hasLookaheadClobRow = false;
            return false;
        }
        laTs = clobResultSet.getLong(1);
        laServerTs = clobResultSet.getLong(2);
        laFrameId = clobResultSet.getLong(3);
        laEventCode = clobResultSet.getByte(4);
        laIsUp = clobResultSet.getBoolean(5);
        laIsUpNull = clobResultSet.wasNull();
        laIsBid = clobResultSet.getBoolean(6);
        laIsBidNull = clobResultSet.wasNull();
        laTick = clobResultSet.getInt(7);
        laTickNull = clobResultSet.wasNull();
        laSize = clobResultSet.getDouble(8);
        laSizeNull = clobResultSet.wasNull();
        hasLookaheadClobRow = true;
        return true;
    }

    private void ensureScratchCapacity(int required) {
        if (required <= scratchTicks.length) {
            return;
        }
        int newCap = Math.max(required, scratchTicks.length << 1);
        scratchUpFlags = Arrays.copyOf(scratchUpFlags, newCap);
        scratchBidFlags = Arrays.copyOf(scratchBidFlags, newCap);
        scratchTicks = Arrays.copyOf(scratchTicks, newCap);
        scratchSizes = Arrays.copyOf(scratchSizes, newCap);
    }

    private void advanceClobCursor() {
        if (clobResultSet == null) {
            nextClobEvent = null;
            return;
        }
        try {
            if (!hasLookaheadClobRow && !readNextClobRowIntoLookahead()) {
                closeClobStream();
                return;
            }

            long frameTs = laTs;
            long frameServerTs = laServerTs;
            long frameId = laFrameId;
            byte eventCode = laEventCode;
            boolean bookIsUp = !laIsUpNull && laIsUp;
            int levelCount = 0;

            while (hasLookaheadClobRow
                    && laFrameId == frameId
                    && laTs == frameTs
                    && laEventCode == eventCode) {
                if ((eventCode == BacktestEvent.PolymarketClobFrameEvent.EVENT_BOOK
                        || eventCode == BacktestEvent.PolymarketClobFrameEvent.EVENT_PRICE_CHANGE)
                        && !laIsUpNull && !laIsBidNull && !laTickNull && !laSizeNull) {
                    ensureScratchCapacity(levelCount + 1);
                    scratchUpFlags[levelCount] = laIsUp;
                    scratchBidFlags[levelCount] = laIsBid;
                    scratchTicks[levelCount] = laTick;
                    scratchSizes[levelCount] = laSize;
                    levelCount++;
                }
                readNextClobRowIntoLookahead();
            }

            nextClobEvent = new BacktestEvent.PolymarketClobFrameEvent(
                    frameTs,
                    frameServerTs,
                    ++clobSeq,
                    eventCode,
                    bookIsUp,
                    levelCount,
                    levelCount == 0 ? EMPTY_BOOLEANS : Arrays.copyOf(scratchUpFlags, levelCount),
                    levelCount == 0 ? EMPTY_BOOLEANS : Arrays.copyOf(scratchBidFlags, levelCount),
                    levelCount == 0 ? EMPTY_INTS : Arrays.copyOf(scratchTicks, levelCount),
                    levelCount == 0 ? EMPTY_DOUBLES : Arrays.copyOf(scratchSizes, levelCount)
            );
            return;
        } catch (SQLException e) {
            Log.warnf("Could not stream Polymarket CLOB events from %s: %s", activeClobFile, e.getMessage());
        }
        closeClobStream();
    }

    private void closeClobStream() {
        nextClobEvent = null;
        activeClobFile = null;
        hasLookaheadClobRow = false;
        clobResultSetExhausted = false;
        if (clobResultSet != null) {
            try {
                clobResultSet.close();
            } catch (SQLException ignored) {
            }
            clobResultSet = null;
        }
        if (clobStatement != null) {
            try {
                clobStatement.close();
            } catch (SQLException ignored) {
            }
            clobStatement = null;
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

    private static int findFirstIndexAfter(BacktestEvent[] events, long targetTsMs) {
        int low = 0;
        int high = events.length - 1;
        int result = events.length;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (events[mid].timestampMs() > targetTsMs) {
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
        String norm = file.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
        String sql = "SELECT ts, COALESCE(seq, 0), symbol, source, price, COALESCE(price_timestamp, ts) " +
                "FROM read_parquet('" + norm + "') WHERE price IS NOT NULL";
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
        String norm = file.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
        String sql = "SELECT ts, COALESCE(seq, 0), symbol, source, twap_price, COALESCE(window_seconds, 60), COALESCE(twap_timestamp, ts) " +
                "FROM read_parquet('" + norm + "') WHERE twap_price IS NOT NULL";
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

    private static void loadDailyBinanceTrades(
            Statement stmt,
            Path file,
            long binanceDelayMs,
            boolean spot,
            List<BacktestEvent> out) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = file.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
        String sql = "SELECT (CAST(COALESCE(event_time, trade_time) AS BIGINT) + " + binanceDelayMs + "), " +
                "COALESCE(aggregate_trade_id, 0), price, quantity, is_buyer_maker " +
                "FROM read_parquet('" + norm + "') " +
                "WHERE price IS NOT NULL AND COALESCE(event_time, trade_time) IS NOT NULL";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                long effTs = rs.getLong(1);
                long seq = rs.getLong(2);
                double price = rs.getDouble(3);
                double qty = rs.getDouble(4);
                boolean isBuyerMaker = rs.getBoolean(5);
                out.add(spot
                        ? new BacktestEvent.BinanceSpotTradeEvent(effTs, seq, price, qty, isBuyerMaker)
                        : new BacktestEvent.BinanceFuturesTradeEvent(effTs, seq, price, qty, isBuyerMaker));
            }
        } catch (SQLException e) {
            Log.warnf("Could not load Binance %s aggTrades file %s: %s",
                    spot ? "Spot" : "Futures", file, e.getMessage());
        }
    }

    private Statement getOrCreateStatement() throws SQLException {
        if (sharedConnection == null || sharedConnection.isClosed()) {
            sharedConnection = duckDbSessionManager.openSession();
        }
        return sharedConnection.createStatement();
    }

    private Statement getOrCreateCalcKrakenStatement() throws SQLException {
        if (calcKrakenConnection == null || calcKrakenConnection.isClosed()) {
            calcKrakenConnection = duckDbSessionManager.openSession();
        }
        return calcKrakenConnection.createStatement();
    }

    @Override
    public void close() {
        clear();
        if (sharedConnection != null) {
            try {
                sharedConnection.close();
            } catch (SQLException ignored) {
            }
            sharedConnection = null;
        }
        for (int i = 0; i < venueCursors.length; i++) {
            venueCursors[i].closeConnection();
        }
        if (calcKrakenConnection != null) {
            try {
                calcKrakenConnection.close();
            } catch (SQLException ignored) {
            }
            calcKrakenConnection = null;
        }
    }
}
