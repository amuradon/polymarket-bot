package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.indicator.DailyIndicatorTrack;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.strategy.TradeDirection;
import io.quarkus.logging.Log;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * High-performance chronological event multiplexer and streamer for backtesting.
 * <p>
 * Streams captured events from typed collector Parquet archives and cached daily indicator tracks
 * ordered strictly by the local EC2 capture or simulated arrival timestamp {@code ts}
 * ({@link BacktestEvent#timestampMs()}) using a zero-copy 4-way merge over:
 * <ol>
 *   <li>Lazy DuckDB streaming cursor grouping typed Polymarket CLOB {@code frame_id} rows,</li>
 *   <li>Direct array cursor over shared daily Polymarket stream events ({@code O(log N)} binary search slice),</li>
 *   <li>Direct columnar cursors over cached daily indicator tracks ({@code O(log N)} binary search slice), and</li>
 *   <li>Small priority queue for candle lifecycle milestones and dynamically scheduled pending orders.</li>
 * </ol>
 */
public class ChronologicalEventStreamer implements AutoCloseable {

    private static final BacktestEvent[] EMPTY_EVENTS = new BacktestEvent[0];
    private static final IndicatorSliceCursor[] EMPTY_INDICATOR_CURSORS = new IndicatorSliceCursor[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];
    private static final int[] EMPTY_INTS = new int[0];
    private static final double[] EMPTY_DOUBLES = new double[0];
    private static final long BOUNDARY_TWAP_OFFSET_MS = 10_000L;
    private static final long FAST_SNAPSHOT_LOOKBACK_MS = 30_000L;

    private final DuckDbSessionManager duckDbSessionManager;
    private final PriorityQueue<BacktestEvent> queue = new PriorityQueue<>(16);

    private Connection sharedConnection;
    private Statement clobStatement;
    private ResultSet clobResultSet;
    private Path activeClobFile;
    private long clobSeq;
    private BacktestEvent.PolymarketClobFrameEvent nextClobEvent;

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

    // Zero-copy daily Polymarket event array slice cursor
    private BacktestEvent[] activeDailyEvents = EMPTY_EVENTS;
    private int dailyCursorIdx = 0;
    private int dailyEndIdx = 0;

    // Zero-copy daily indicator track slice cursors
    private IndicatorSliceCursor[] activeIndicatorCursors = EMPTY_INDICATOR_CURSORS;
    private BacktestEvent nextIndicatorEvent;

    private static final class IndicatorSliceCursor {
        final DailyIndicatorTrack track;
        final int endIdx;
        int cursorIdx;
        BacktestEvent headEvent;

        IndicatorSliceCursor(DailyIndicatorTrack track, int startIdx, int endIdx) {
            this.track = track;
            this.cursorIdx = startIdx;
            this.endIdx = endIdx;
            this.headEvent = startIdx < endIdx ? track.eventAt(startIdx) : null;
        }

        void advance() {
            cursorIdx++;
            headEvent = cursorIdx < endIdx ? track.eventAt(cursorIdx) : null;
        }
    }

    /**
     * Constructs a chronological event streamer backed by the shared DuckDB session manager.
     *
     * @param duckDbSessionManager shared DuckDB session manager providing streaming connections
     */
    public ChronologicalEventStreamer(DuckDbSessionManager duckDbSessionManager) {
        if (duckDbSessionManager == null) {
            Log.error("Invalid argument in ChronologicalEventStreamer: duckDbSessionManager cannot be null");
            throw new IllegalArgumentException("duckDbSessionManager cannot be null");
        }
        this.duckDbSessionManager = duckDbSessionManager;
    }

    /**
     * Checks if there are pending events in the CLOB stream, indicator track cursors, daily slice cursor, or priority queue.
     *
     * @return true if more events exist
     */
    public boolean hasNext() {
        return nextClobEvent != null || nextIndicatorEvent != null || dailyCursorIdx < dailyEndIdx || !queue.isEmpty();
    }

    /**
     * Retrieves and removes the next chronological event via {@code O(1)} merge across
     * the active CLOB cursor, the zero-copy daily stream cursor, the daily indicator track cursors,
     * and the dynamic priority queue.
     *
     * @return next event, or null if no events remain
     */
    public BacktestEvent next() {
        BacktestEvent a = nextClobEvent;
        BacktestEvent b = (dailyCursorIdx < dailyEndIdx) ? activeDailyEvents[dailyCursorIdx] : null;
        BacktestEvent c = nextIndicatorEvent;
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
                advanceIndicatorCursor();
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
     * Clears all pending events, resets the daily and indicator slice cursors, and closes any active CLOB cursor.
     */
    public void clear() {
        closeClobStream();
        activeIndicatorCursors = EMPTY_INDICATOR_CURSORS;
        nextIndicatorEvent = null;
        activeDailyEvents = EMPTY_EVENTS;
        dailyCursorIdx = 0;
        dailyEndIdx = 0;
        queue.clear();
    }

    /**
     * Loads all shared daily Polymarket stream events ({@code polymarket/price_crypto} and
     * {@code polymarket/price_crypto_twap_60s}) for a single UTC day into a sorted,
     * immutable array shared read-only across all parallel 15-minute market workers of that day.
     *
     * @param datasetService collector Parquet dataset service
     * @param symbol trading asset symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return sorted array of daily stream events
     */
    public static BacktestEvent[] loadSharedDailyStreams(
            CollectorParquetDatasetService datasetService,
            String symbol,
            String dateStr) {

        if (datasetService == null || dateStr == null || dateStr.isBlank()) {
            return EMPTY_EVENTS;
        }

        DuckDbSessionManager sessionManager = datasetService.getDuckDbSessionManager();

        record DailyLoaderTask(Path file, BiConsumer<Statement, List<BacktestEvent>> loader) {}
        List<DailyLoaderTask> tasks = new ArrayList<>(2);

        Path priceFile = datasetService.getPolymarketCryptoPriceFile(symbol, dateStr);
        if (priceFile != null && Files.exists(priceFile)) {
            tasks.add(new DailyLoaderTask(priceFile, (stmt, out) -> loadDailyPolymarketCryptoPrice(stmt, priceFile, out)));
        }

        Path twapFile = datasetService.getPolymarketCryptoTwap60sFile(symbol, dateStr);
        if (twapFile != null && Files.exists(twapFile)) {
            tasks.add(new DailyLoaderTask(twapFile, (stmt, out) -> loadDailyPolymarketCryptoTwap60s(stmt, twapFile, out)));
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
     * Loads all chronological events for a specific 15-minute market interval using pre-loaded
     * shared daily Polymarket stream events, pre-loaded or cached daily indicator tracks, and lazily
     * streaming the Polymarket CLOB file (optionally pruned to {@code clobLookbackMs}).
     *
     * @param metadata market metadata descriptor
     * @param sharedDailyStreamEvents pre-loaded sorted daily Polymarket stream events for the market's UTC day
     * @param dailyIndicatorTracks pre-loaded or cached daily indicator tracks for the market's UTC day
     * @param clobLookbackMs lookback window before {@code tEndMs} in milliseconds (or {@link Long#MAX_VALUE} for full stream)
     */
    public void loadCandleEvents(
            CollectorMarketMetadata metadata,
            BacktestEvent[] sharedDailyStreamEvents,
            List<DailyIndicatorTrack> dailyIndicatorTracks,
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
        prepareIndicatorSlices(dailyIndicatorTracks, tStartMs, tEndMs);
        openPolymarketClobStream(metadata.marketParquetFile(), tStartMs, tEndMs, clobLookbackMs);
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

    private void prepareIndicatorSlices(List<DailyIndicatorTrack> tracks, long tStartMs, long tEndMs) {
        if (tracks == null || tracks.isEmpty()) {
            this.activeIndicatorCursors = EMPTY_INDICATOR_CURSORS;
            this.nextIndicatorEvent = null;
            return;
        }

        int trackCount = tracks.size();
        IndicatorSliceCursor[] cursors = new IndicatorSliceCursor[trackCount];
        int activeCount = 0;

        for (int i = 0; i < trackCount; i++) {
            DailyIndicatorTrack track = tracks.get(i);
            if (track == null || track.isEmpty()) {
                continue;
            }
            int startIdx = track.findFirstIndexAtOrAfter(tStartMs);
            int endIdx = track.findFirstIndexAfter(tEndMs);
            if (startIdx < endIdx) {
                cursors[activeCount++] = new IndicatorSliceCursor(track, startIdx, endIdx);
            }
        }

        if (activeCount == 0) {
            this.activeIndicatorCursors = EMPTY_INDICATOR_CURSORS;
            this.nextIndicatorEvent = null;
            return;
        }

        this.activeIndicatorCursors = activeCount == trackCount
                ? cursors
                : Arrays.copyOf(cursors, activeCount);
        advanceIndicatorCursor();
    }

    private void advanceIndicatorCursor() {
        IndicatorSliceCursor[] cursors = this.activeIndicatorCursors;
        int len = cursors.length;
        if (len == 0) {
            nextIndicatorEvent = null;
            return;
        }
        if (len == 1) {
            IndicatorSliceCursor single = cursors[0];
            BacktestEvent ev = single.headEvent;
            if (ev != null) {
                single.advance();
            }
            nextIndicatorEvent = ev;
            return;
        }

        IndicatorSliceCursor bestCursor = null;
        BacktestEvent bestEvent = null;
        for (int i = 0; i < len; i++) {
            IndicatorSliceCursor c = cursors[i];
            BacktestEvent ev = c.headEvent;
            if (ev != null && (bestEvent == null || ev.compareTo(bestEvent) < 0)) {
                bestEvent = ev;
                bestCursor = c;
            }
        }

        if (bestCursor != null) {
            bestCursor.advance();
        }
        nextIndicatorEvent = bestEvent;
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

    private Statement getOrCreateStatement() throws SQLException {
        if (sharedConnection == null || sharedConnection.isClosed()) {
            sharedConnection = duckDbSessionManager.openSession();
        }
        return sharedConnection.createStatement();
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
    }
}
