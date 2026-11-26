package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.data.ExchangeDelays;
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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * High-performance chronological event multiplexer and streamer for continuous backtesting.
 * <p>
 * Streams captured historical market data directly from converted collector Parquet archives
 * ordered strictly by local EC2 capture or simulated arrival timestamp {@code ts}
 * ({@link BacktestEvent#timestampMs()}) using an {@code O(1)} 4-way merge over:
 * <ol>
 *   <li>Lazy DuckDB streaming cursor grouping typed Polymarket CLOB {@code frame_id} rows
 *       ({@code book}, {@code price_change}, {@code last_trade_price}, and {@code bbo_hint}),</li>
 *   <li>Direct array cursor over daily PolyBolt {@code price_crypto} and {@code price_crypto_twap_60s}
 *       full-accuracy oracle streams ({@code O(log N)} binary search slice over half-open {@code [startMs, endMs)}),</li>
 *   <li>Lazy DuckDB daily streaming cursor over raw multi-exchange spot trades (Kraken {@code USDT/USD},
 *       Binance {@code aggTrades}, Coinbase {@code trades}, Kraken {@code <ASSET>/USD}) shifted by configured
 *       one-way WebSocket feed delays ({@link ExchangeDelays}), and</li>
 *   <li>Small priority queue for candle lifecycle milestones and dynamically scheduled
 *       {@code SimulatedClobExecutionEngine} match/report callbacks.</li>
 * </ol>
 */
public class ChronologicalEventStreamer implements AutoCloseable {

    private static final BacktestEvent[] EMPTY_EVENTS = new BacktestEvent[0];
    private static final boolean[] EMPTY_BOOLEANS = new boolean[0];
    private static final int[] EMPTY_INTS = new int[0];
    private static final double[] EMPTY_DOUBLES = new double[0];
    private static final long BOUNDARY_TWAP_OFFSET_MS = 10_000L;
    private static final long FAST_SNAPSHOT_LOOKBACK_MS = 30_000L;

    private final DuckDbSessionManager duckDbSessionManager;
    private final PriorityQueue<BacktestEvent> queue = new PriorityQueue<>(16);

    // Polymarket CLOB per-market streaming cursor
    private Connection clobConnection;
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

    // Zero-copy PolyBolt event array slice cursor over [sliceStartMs, sliceEndMs)
    private BacktestEvent[] activeDailyEvents = EMPTY_EVENTS;
    private int dailyCursorIdx = 0;
    private int dailyEndIdx = 0;

    // Raw multi-exchange CEX spot daily streaming cursor sliced across [sliceStartMs, activeSpotSliceEndMs)
    private Connection spotConnection;
    private Statement spotStatement;
    private ResultSet spotResultSet;
    private BacktestEvent.ExchangeSpotTradeEvent seededSpotEvent;
    private BacktestEvent.ExchangeSpotTradeEvent cursorSpotEvent;
    private BacktestEvent.ExchangeSpotTradeEvent nextSpotEvent;
    private long activeSpotSliceEndMs = Long.MAX_VALUE;

    private long scheduledActionSeq = 0L;

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
     * Checks if there are pending events in the active slice across CLOB, PolyBolt, CEX spot, or scheduled queue.
     *
     * @return {@code true} if more events exist in the current slice
     */
    public boolean hasNext() {
        return nextClobEvent != null
                || dailyCursorIdx < dailyEndIdx
                || (nextSpotEvent != null && nextSpotEvent.timestampMs() < activeSpotSliceEndMs)
                || !queue.isEmpty();
    }

    /**
     * Peeks at the next chronological event without removing it from the stream.
     *
     * @return next chronological {@link BacktestEvent}, or {@code null} if exhausted
     */
    public BacktestEvent peek() {
        BacktestEvent a = nextClobEvent;
        BacktestEvent b = (dailyCursorIdx < dailyEndIdx) ? activeDailyEvents[dailyCursorIdx] : null;
        BacktestEvent c = (nextSpotEvent != null && nextSpotEvent.timestampMs() < activeSpotSliceEndMs)
                ? nextSpotEvent
                : null;
        BacktestEvent d = queue.peek();

        BacktestEvent best = a;
        if (b != null && (best == null || b.compareTo(best) < 0)) {
            best = b;
        }
        if (c != null && (best == null || c.compareTo(best) < 0)) {
            best = c;
        }
        if (d != null && (best == null || d.compareTo(best) < 0)) {
            best = d;
        }
        return best;
    }

    /**
     * Retrieves and removes the next chronological event via {@code O(1)} merge across
     * the active CLOB cursor, the PolyBolt daily slice cursor, the daily CEX spot stream cursor,
     * and the dynamic priority queue.
     *
     * @return next event, or {@code null} if no events remain in the current slice
     */
    public BacktestEvent next() {
        BacktestEvent a = nextClobEvent;
        BacktestEvent b = (dailyCursorIdx < dailyEndIdx) ? activeDailyEvents[dailyCursorIdx] : null;
        BacktestEvent c = (nextSpotEvent != null && nextSpotEvent.timestampMs() < activeSpotSliceEndMs)
                ? nextSpotEvent
                : null;
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
                advanceSpotCursor();
                yield c;
            }
            case 4 -> queue.poll();
            default -> null;
        };
    }

    /**
     * Schedules a future or immediate event into the chronological priority queue.
     *
     * @param event {@link BacktestEvent} to schedule
     */
    public void scheduleEvent(BacktestEvent event) {
        if (event != null) {
            queue.add(event);
        }
    }

    /**
     * Schedules a delayed {@code SimulatedClobExecutionEngine} callback at {@code targetTimestampMs}.
     *
     * @param targetTimestampMs target simulated timestamp in epoch milliseconds UTC
     * @param action            callback to run when popped
     */
    public void scheduleActionAt(long targetTimestampMs, Runnable action) {
        if (action != null) {
            queue.add(new BacktestEvent.ScheduledEngineActionEvent(
                    targetTimestampMs,
                    action,
                    ++scheduledActionSeq
            ));
        }
    }

    /**
     * Clears only the per-candle CLOB cursor, PolyBolt slice indices, and scheduled queue while keeping
     * the open daily CEX spot stream positioned for the next chronological slice.
     */
    public void clearCandleSlice() {
        closeClobStream();
        activeDailyEvents = EMPTY_EVENTS;
        dailyCursorIdx = 0;
        dailyEndIdx = 0;
        activeSpotSliceEndMs = 0L;
        queue.clear();
    }

    /**
     * Clears all pending events and closes both the active CLOB stream and any open daily CEX spot stream.
     */
    public void clear() {
        clearCandleSlice();
        closeDailySpotStream();
    }

    /**
     * Loads all shared daily Polymarket PolyBolt stream events ({@code polymarket/price_crypto} and
     * {@code polymarket/price_crypto_twap_60s}) for a single UTC day into a sorted array, preferring
     * full-accuracy string prices ({@code full_accuracy_price} / {@code full_accuracy_twap_price}) when present.
     *
     * @param datasetService collector Parquet dataset service
     * @param symbol         trading asset symbol
     * @param dateStr        UTC date string ({@code yyyy-MM-dd})
     * @return sorted array of daily PolyBolt stream events
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
        return combined;
    }

    /**
     * Merges pre-loaded previous-day, current-day, and next-day PolyBolt arrays over {@code [minTsMs, maxTsMs]}
     * so 15-minute warm-up windows before {@code 00:00:00 UTC} and closing {@code 24:00:00 UTC} settlement TWAPs
     * are available without re-reading Parquet files.
     *
     * @param prevDayEvents sorted PolyBolt events for {@code date - 1}
     * @param curDayEvents  sorted PolyBolt events for {@code date}
     * @param nextDayEvents sorted PolyBolt events for {@code date + 1}
     * @param minTsMs       inclusive minimum timestamp filter for {@code prevDayEvents}
     * @param maxTsMs       inclusive maximum timestamp filter for {@code nextDayEvents}
     * @return sorted merged PolyBolt event array
     */
    public static BacktestEvent[] mergePolyBoltWindow(
            BacktestEvent[] prevDayEvents,
            BacktestEvent[] curDayEvents,
            BacktestEvent[] nextDayEvents,
            long minTsMs,
            long maxTsMs) {
        BacktestEvent[] prev = prevDayEvents != null ? prevDayEvents : EMPTY_EVENTS;
        BacktestEvent[] cur = curDayEvents != null ? curDayEvents : EMPTY_EVENTS;
        BacktestEvent[] next = nextDayEvents != null ? nextDayEvents : EMPTY_EVENTS;

        int prevStart = prev.length > 0 ? findFirstIndexAtOrAfter(prev, minTsMs) : 0;
        int prevLen = prev.length - prevStart;
        int nextEnd = next.length > 0 ? findFirstIndexAfter(next, maxTsMs) : 0;

        if (prevLen <= 0 && nextEnd <= 0) {
            return cur;
        }
        int total = prevLen + cur.length + nextEnd;
        BacktestEvent[] merged = new BacktestEvent[total];
        int pos = 0;
        if (prevLen > 0) {
            System.arraycopy(prev, prevStart, merged, pos, prevLen);
            pos += prevLen;
        }
        if (cur.length > 0) {
            System.arraycopy(cur, 0, merged, pos, cur.length);
            pos += cur.length;
        }
        if (nextEnd > 0) {
            System.arraycopy(next, 0, merged, pos, nextEnd);
        }
        Arrays.sort(merged);
        return merged;
    }

    /**
     * Opens a lazy DuckDB daily streaming cursor over raw multi-exchange spot trades
     * (Kraken {@code USDT/USD}, Binance {@code aggTrades}, Coinbase {@code trades}, Kraken {@code <ASSET>/USD})
     * across {@code [windowStartMs, windowEndMs)}, applying per-exchange arrival delays ({@link ExchangeDelays})
     * and optionally seeding the most recent prior {@code USDT/USD} peg trade before {@code windowStartMs}.
     *
     * @param datasetService   collector Parquet dataset service
     * @param symbol           traded symbol (e.g. {@code "BTCUSDT"})
     * @param dateStr          active UTC date ({@code yyyy-MM-dd})
     * @param delays           per-exchange simulated WebSocket one-way delays
     * @param windowStartMs    inclusive window start in epoch milliseconds UTC
     * @param windowEndMs      exclusive window end in epoch milliseconds UTC
     * @param seedPriorUsdtPeg {@code true} to seed the latest prior Kraken {@code USDT/USD} trade before {@code windowStartMs}
     */
    public void openDailySpotStream(
            CollectorParquetDatasetService datasetService,
            String symbol,
            String dateStr,
            ExchangeDelays delays,
            long windowStartMs,
            long windowEndMs,
            boolean seedPriorUsdtPeg) {
        closeDailySpotStream();
        if (datasetService == null || dateStr == null || dateStr.isBlank() || windowEndMs <= windowStartMs) {
            return;
        }

        ExchangeDelays eff = delays != null ? delays : new ExchangeDelays(100L, 50L, 50L);
        String prevDateStr = LocalDate.parse(dateStr).minusDays(1).toString();

        Path prevUsdt = datasetService.getKrakenSpotUsdtUsdTradesFile(prevDateStr);
        Path curUsdt = datasetService.getKrakenSpotUsdtUsdTradesFile(dateStr);
        Path prevBinance = datasetService.getBinanceSpotAggTradesFile(symbol, prevDateStr);
        Path curBinance = datasetService.getBinanceSpotAggTradesFile(symbol, dateStr);
        Path prevCoinbase = datasetService.getCoinbaseSpotTradesFile(symbol, prevDateStr);
        Path curCoinbase = datasetService.getCoinbaseSpotTradesFile(symbol, dateStr);
        Path prevKraken = datasetService.getKrakenSpotTradesFile(symbol, prevDateStr);
        Path curKraken = datasetService.getKrakenSpotTradesFile(symbol, dateStr);

        List<String> unionParts = new ArrayList<>(8);
        addKrakenUsdtUnionPart(unionParts, prevUsdt, eff.krakenDelayMs(), windowStartMs, windowEndMs);
        addKrakenUsdtUnionPart(unionParts, curUsdt, eff.krakenDelayMs(), windowStartMs, windowEndMs);
        addBinanceUnionPart(unionParts, prevBinance, eff.binanceDelayMs(), windowStartMs, windowEndMs);
        addBinanceUnionPart(unionParts, curBinance, eff.binanceDelayMs(), windowStartMs, windowEndMs);
        addCoinbaseUnionPart(unionParts, prevCoinbase, eff.coinbaseDelayMs(), windowStartMs, windowEndMs);
        addCoinbaseUnionPart(unionParts, curCoinbase, eff.coinbaseDelayMs(), windowStartMs, windowEndMs);
        addKrakenAssetUnionPart(unionParts, prevKraken, eff.krakenDelayMs(), windowStartMs, windowEndMs);
        addKrakenAssetUnionPart(unionParts, curKraken, eff.krakenDelayMs(), windowStartMs, windowEndMs);

        boolean hasUsdtFiles = (prevUsdt != null && Files.exists(prevUsdt)) || (curUsdt != null && Files.exists(curUsdt));
        if (unionParts.isEmpty() && !(seedPriorUsdtPeg && hasUsdtFiles)) {
            return;
        }

        try {
            Statement stmt = getOrCreateSpotStatement();
            if (seedPriorUsdtPeg && hasUsdtFiles) {
                Double priorPeg = queryLatestPriorUsdtPeg(stmt, prevUsdt, curUsdt, eff.krakenDelayMs(), windowStartMs);
                if (priorPeg != null && Double.isFinite(priorPeg) && priorPeg > 0.0) {
                    this.seededSpotEvent = new BacktestEvent.ExchangeSpotTradeEvent(
                            windowStartMs,
                            0L,
                            BacktestEvent.ExchangeSpotTradeEvent.VENUE_KRAKEN_USDT,
                            priorPeg,
                            1.0,
                            true
                    );
                }
            }

            if (!unionParts.isEmpty()) {
                String sql = "SELECT ts, venue_code, seq, price, qty, is_taker_buy FROM ("
                        + String.join(" UNION ALL ", unionParts)
                        + ") AS spot_union ORDER BY ts ASC, prio ASC, seq ASC";
                this.spotStatement = stmt;
                this.spotResultSet = stmt.executeQuery(sql);
                readNextSpotRowIntoCursor();
            } else {
                stmt.close();
            }
            refreshNextSpotHead();
        } catch (SQLException e) {
            Log.warnf("Could not open daily CEX spot stream for symbol=%s date=%s: %s", symbol, dateStr, e.getMessage());
            closeDailySpotStream();
        }
    }

    /**
     *Closes any open daily CEX spot stream cursor and resets spot slice state.
     */
    public void closeDailySpotStream() {
        seededSpotEvent = null;
        cursorSpotEvent = null;
        nextSpotEvent = null;
        activeSpotSliceEndMs = 0L;
        if (spotResultSet != null) {
            try {
                spotResultSet.close();
            } catch (SQLException ignored) {
            }
            spotResultSet = null;
        }
        if (spotStatement != null) {
            try {
                spotStatement.close();
            } catch (SQLException ignored) {
            }
            spotStatement = null;
        }
    }

    /**
     * Prepares a continuous warm-up or inter-market gap slice over {@code [sliceStartMs, sliceEndMs)}
     * streaming PolyBolt oracle events and CEX spot trades without Polymarket CLOB events.
     *
     * @param polyBoltWindowEvents sorted PolyBolt events covering the window
     * @param sliceStartMs         inclusive slice start in epoch milliseconds UTC
     * @param sliceEndMs           exclusive slice end in epoch milliseconds UTC
     */
    public void loadWarmupSlice(
            BacktestEvent[] polyBoltWindowEvents,
            long sliceStartMs,
            long sliceEndMs) {
        clearCandleSlice();
        if (sliceEndMs <= sliceStartMs) {
            return;
        }
        if (polyBoltWindowEvents != null && polyBoltWindowEvents.length > 0) {
            this.activeDailyEvents = polyBoltWindowEvents;
            this.dailyCursorIdx = findFirstIndexAtOrAfter(polyBoltWindowEvents, sliceStartMs);
            this.dailyEndIdx = findFirstIndexAtOrAfter(polyBoltWindowEvents, sliceEndMs);
        }
        prepareSpotSlice(sliceStartMs, sliceEndMs);
    }

    /**
     * Loads all chronological events for a 15-minute Polymarket market over half-open {@code [tStartMs, tEndMs)}
     * using pre-loaded PolyBolt stream events, the open daily CEX spot cursor, and lazy streaming of the
     * Polymarket CLOB Parquet archive (optionally pruned to {@code clobLookbackMs}).
     *
     * @param metadata             market metadata descriptor
     * @param polyBoltWindowEvents pre-loaded sorted PolyBolt stream events covering the market's window
     * @param clobLookbackMs       lookback window before {@code tEndMs} in ms (or {@link Long#MAX_VALUE} for full stream)
     */
    public void loadCandleEvents(
            CollectorMarketMetadata metadata,
            BacktestEvent[] polyBoltWindowEvents,
            long clobLookbackMs) {
        clearCandleSlice();
        if (metadata == null) {
            return;
        }

        long tStart = metadata.tStart();
        long tEnd = metadata.tEnd();
        long tStartMs = tStart * 1_000L;
        long tEndMs = tEnd * 1_000L;
        TradeDirection actualOutcome = metadata.actualOutcome() != null ? metadata.actualOutcome() : TradeDirection.UP;

        scheduleMandatoryLifecycleEvents(tStart, tEnd, tStartMs, tEndMs, actualOutcome);
        prepareDailySlice(polyBoltWindowEvents, tStartMs, tEndMs, metadata);
        prepareSpotSlice(tStartMs, tEndMs);
        openPolymarketClobStream(metadata.marketParquetFile(), tStartMs, tEndMs, clobLookbackMs);
    }

    private void prepareSpotSlice(long sliceStartMs, long sliceEndMs) {
        this.activeSpotSliceEndMs = sliceEndMs;
        while (nextSpotEvent != null && nextSpotEvent.timestampMs() < sliceStartMs) {
            advanceSpotCursor();
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
                tStart,
                tEnd,
                actualOutcome
        ));
        scheduleEvent(new BacktestEvent.CandleLifecycleEvent(
                tEndMs,
                BacktestEvent.CandleLifecycleType.CANDLE_CLOSE,
                tStart,
                tEnd,
                actualOutcome
        ));
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
        // Half-open [tStartMs, tEndMs) ensures boundary events at tEndMs are processed strictly once at the next candle's open
        int endIdx = findFirstIndexAtOrAfter(events, tEndMs);
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
        String normPath = normalizeSqlPath(marketFile);

        try {
            Statement stmt = getOrCreateClobStatement();
            long seedStartTs = 0L;
            if (clobLookbackMs > 0L && clobLookbackMs < (tEndMs - tStartMs)) {
                long clobWindowStartMs = Math.max(tStartMs, tEndMs - clobLookbackMs);
                seedStartTs = resolveClobSeedStartTs(stmt, normPath, clobWindowStartMs);
            }

            String sql = "SELECT ts, server_ts, frame_id, event_code, is_up, is_bid, tick, size "
                    + "FROM read_parquet('" + normPath + "') "
                    + "WHERE ts >= " + seedStartTs + " AND ts <= " + tEndMs + " AND event_code > 0";

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
        String fastSql = "SELECT MAX(ts) FILTER (WHERE is_up = true), MAX(ts) FILTER (WHERE is_up = false) "
                + "FROM read_parquet('" + normPath + "') "
                + "WHERE ts >= " + fastFloorMs + " AND ts <= " + clobWindowStartMs + " AND event_code = 1";

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

        String fullPreSql = "SELECT "
                + "MAX(ts) FILTER (WHERE event_code = 1 AND is_up = true), "
                + "BOOL_OR(is_up = true), "
                + "MAX(ts) FILTER (WHERE event_code = 1 AND is_up = false), "
                + "BOOL_OR(is_up = false) "
                + "FROM read_parquet('" + normPath + "') "
                + "WHERE ts <= " + clobWindowStartMs + " AND event_code IN (1, 2, 4)";

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
                        || eventCode == BacktestEvent.PolymarketClobFrameEvent.EVENT_PRICE_CHANGE
                        || eventCode == BacktestEvent.PolymarketClobFrameEvent.EVENT_BBO_HINT)
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

    private void readNextSpotRowIntoCursor() throws SQLException {
        if (spotResultSet == null) {
            cursorSpotEvent = null;
            return;
        }
        if (!spotResultSet.next()) {
            cursorSpotEvent = null;
            try {
                spotResultSet.close();
            } catch (SQLException ignored) {
            }
            spotResultSet = null;
            if (spotStatement != null) {
                try {
                    spotStatement.close();
                } catch (SQLException ignored) {
                }
                spotStatement = null;
            }
            return;
        }
        cursorSpotEvent = new BacktestEvent.ExchangeSpotTradeEvent(
                spotResultSet.getLong(1),
                spotResultSet.getLong(3),
                spotResultSet.getByte(2),
                spotResultSet.getDouble(4),
                spotResultSet.getDouble(5),
                spotResultSet.getBoolean(6)
        );
    }

    private void refreshNextSpotHead() {
        if (seededSpotEvent != null) {
            nextSpotEvent = seededSpotEvent;
        } else {
            nextSpotEvent = cursorSpotEvent;
        }
    }

    private void advanceSpotCursor() {
        if (seededSpotEvent != null) {
            seededSpotEvent = null;
            nextSpotEvent = cursorSpotEvent;
            return;
        }
        try {
            readNextSpotRowIntoCursor();
            nextSpotEvent = cursorSpotEvent;
        } catch (SQLException e) {
            Log.warnf("Failed advancing daily CEX spot stream: %s", e.getMessage());
            closeDailySpotStream();
        }
    }

    private static void addKrakenUsdtUnionPart(
            List<String> out,
            Path file,
            long delayMs,
            long windowStartMs,
            long windowEndMs) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = normalizeSqlPath(file);
        long minSrvTs = windowStartMs - delayMs;
        long maxSrvTsExclusive = windowEndMs - delayMs;
        out.add("SELECT CAST(trade_timestamp AS BIGINT) + " + delayMs + " AS ts, "
                + BacktestEvent.ExchangeSpotTradeEvent.VENUE_KRAKEN_USDT + "::TINYINT AS venue_code, "
                + "6 AS prio, COALESCE(CAST(trade_id AS BIGINT), 0) AS seq, "
                + "CAST(price AS DOUBLE) AS price, COALESCE(CAST(qty AS DOUBLE), 1.0) AS qty, true AS is_taker_buy "
                + "FROM read_parquet('" + norm + "') "
                + "WHERE trade_timestamp IS NOT NULL AND price IS NOT NULL AND price > 0 "
                + "AND CAST(trade_timestamp AS BIGINT) >= " + minSrvTs
                + " AND CAST(trade_timestamp AS BIGINT) < " + maxSrvTsExclusive);
    }

    private static void addBinanceUnionPart(
            List<String> out,
            Path file,
            long delayMs,
            long windowStartMs,
            long windowEndMs) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = normalizeSqlPath(file);
        long minSrvTs = windowStartMs - delayMs;
        long maxSrvTsExclusive = windowEndMs - delayMs;
        String srvTsExpr = "COALESCE(CAST(event_time AS BIGINT), CAST(trade_time AS BIGINT))";
        out.add("SELECT " + srvTsExpr + " + " + delayMs + " AS ts, "
                + BacktestEvent.ExchangeSpotTradeEvent.VENUE_BINANCE + "::TINYINT AS venue_code, "
                + "7 AS prio, COALESCE(CAST(aggregate_trade_id AS BIGINT), 0) AS seq, "
                + "CAST(price AS DOUBLE) AS price, COALESCE(CAST(quantity AS DOUBLE), 1.0) AS qty, "
                + "NOT COALESCE(is_buyer_maker, false) AS is_taker_buy "
                + "FROM read_parquet('" + norm + "') "
                + "WHERE " + srvTsExpr + " IS NOT NULL AND price IS NOT NULL AND price > 0 "
                + "AND " + srvTsExpr + " >= " + minSrvTs
                + " AND " + srvTsExpr + " < " + maxSrvTsExclusive);
    }

    private static void addCoinbaseUnionPart(
            List<String> out,
            Path file,
            long delayMs,
            long windowStartMs,
            long windowEndMs) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = normalizeSqlPath(file);
        long minSrvTs = windowStartMs - delayMs;
        long maxSrvTsExclusive = windowEndMs - delayMs;
        // Coinbase 'matches' reports the maker order side ('sell' maker => taker was buying)
        out.add("SELECT CAST(trade_timestamp AS BIGINT) + " + delayMs + " AS ts, "
                + BacktestEvent.ExchangeSpotTradeEvent.VENUE_COINBASE + "::TINYINT AS venue_code, "
                + "7 AS prio, COALESCE(CAST(trade_id AS BIGINT), 0) AS seq, "
                + "CAST(price AS DOUBLE) AS price, COALESCE(CAST(qty AS DOUBLE), 1.0) AS qty, "
                + "COALESCE(LOWER(CAST(side AS VARCHAR)) = 'sell', true) AS is_taker_buy "
                + "FROM read_parquet('" + norm + "') "
                + "WHERE trade_timestamp IS NOT NULL AND price IS NOT NULL AND price > 0 "
                + "AND CAST(trade_timestamp AS BIGINT) >= " + minSrvTs
                + " AND CAST(trade_timestamp AS BIGINT) < " + maxSrvTsExclusive);
    }

    private static void addKrakenAssetUnionPart(
            List<String> out,
            Path file,
            long delayMs,
            long windowStartMs,
            long windowEndMs) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = normalizeSqlPath(file);
        long minSrvTs = windowStartMs - delayMs;
        long maxSrvTsExclusive = windowEndMs - delayMs;
        out.add("SELECT CAST(trade_timestamp AS BIGINT) + " + delayMs + " AS ts, "
                + BacktestEvent.ExchangeSpotTradeEvent.VENUE_KRAKEN_ASSET + "::TINYINT AS venue_code, "
                + "7 AS prio, COALESCE(CAST(trade_id AS BIGINT), 0) AS seq, "
                + "CAST(price AS DOUBLE) AS price, COALESCE(CAST(qty AS DOUBLE), 1.0) AS qty, "
                + "COALESCE(LOWER(CAST(side AS VARCHAR)) NOT IN ('sell', 's'), true) AS is_taker_buy "
                + "FROM read_parquet('" + norm + "') "
                + "WHERE trade_timestamp IS NOT NULL AND price IS NOT NULL AND price > 0 "
                + "AND CAST(trade_timestamp AS BIGINT) >= " + minSrvTs
                + " AND CAST(trade_timestamp AS BIGINT) < " + maxSrvTsExclusive);
    }

    private static Double queryLatestPriorUsdtPeg(
            Statement stmt,
            Path prevUsdt,
            Path curUsdt,
            long krakenDelayMs,
            long windowStartMs) {
        long maxSrvTsExclusive = windowStartMs - krakenDelayMs;
        List<String> parts = new ArrayList<>(2);
        if (prevUsdt != null && Files.exists(prevUsdt)) {
            parts.add("SELECT CAST(trade_timestamp AS BIGINT) AS srv_ts, CAST(price AS DOUBLE) AS price "
                    + "FROM read_parquet('" + normalizeSqlPath(prevUsdt) + "') "
                    + "WHERE trade_timestamp IS NOT NULL AND price IS NOT NULL AND price > 0 "
                    + "AND CAST(trade_timestamp AS BIGINT) < " + maxSrvTsExclusive);
        }
        if (curUsdt != null && Files.exists(curUsdt)) {
            parts.add("SELECT CAST(trade_timestamp AS BIGINT) AS srv_ts, CAST(price AS DOUBLE) AS price "
                    + "FROM read_parquet('" + normalizeSqlPath(curUsdt) + "') "
                    + "WHERE trade_timestamp IS NOT NULL AND price IS NOT NULL AND price > 0 "
                    + "AND CAST(trade_timestamp AS BIGINT) < " + maxSrvTsExclusive);
        }
        if (parts.isEmpty()) {
            return null;
        }
        String sql = "SELECT price FROM (" + String.join(" UNION ALL ", parts) + ") ORDER BY srv_ts DESC LIMIT 1";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                double val = rs.getDouble(1);
                return rs.wasNull() ? null : val;
            }
        } catch (SQLException ignored) {
        }
        return null;
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

    private static boolean hasParquetColumn(Statement stmt, String normPath, String columnName) {
        String sql = "DESCRIBE SELECT * FROM read_parquet('" + normPath + "')";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                if (columnName.equalsIgnoreCase(rs.getString(1))) {
                    return true;
                }
            }
        } catch (SQLException ignored) {
        }
        return false;
    }

    private static void loadDailyPolymarketCryptoPrice(Statement stmt, Path file, List<BacktestEvent> out) {
        if (file == null || !Files.exists(file)) {
            return;
        }
        String norm = normalizeSqlPath(file);
        String priceExpr = hasParquetColumn(stmt, norm, "full_accuracy_price")
                ? "COALESCE(TRY_CAST(full_accuracy_price AS DOUBLE), price)"
                : "price";
        String sql = "SELECT ts, COALESCE(seq, 0), symbol, source, " + priceExpr + ", COALESCE(price_timestamp, ts) "
                + "FROM read_parquet('" + norm + "') WHERE " + priceExpr + " IS NOT NULL";
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
        String norm = normalizeSqlPath(file);
        String twapExpr = hasParquetColumn(stmt, norm, "full_accuracy_twap_price")
                ? "COALESCE(TRY_CAST(full_accuracy_twap_price AS DOUBLE), twap_price)"
                : "twap_price";
        String sql = "SELECT ts, COALESCE(seq, 0), symbol, source, " + twapExpr
                + ", COALESCE(window_seconds, 60), COALESCE(twap_timestamp, ts) "
                + "FROM read_parquet('" + norm + "') WHERE " + twapExpr + " IS NOT NULL";
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

    private static String normalizeSqlPath(Path file) {
        return file.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
    }

    private Statement getOrCreateClobStatement() throws SQLException {
        if (clobConnection == null || clobConnection.isClosed()) {
            clobConnection = duckDbSessionManager.openSession();
        }
        return clobConnection.createStatement();
    }

    private Statement getOrCreateSpotStatement() throws SQLException {
        if (spotConnection == null || spotConnection.isClosed()) {
            spotConnection = duckDbSessionManager.openSession();
        }
        return spotConnection.createStatement();
    }

    @Override
    public void close() {
        clear();
        if (clobConnection != null) {
            try {
                clobConnection.close();
            } catch (SQLException ignored) {
            }
            clobConnection = null;
        }
        if (spotConnection != null) {
            try {
                spotConnection.close();
            } catch (SQLException ignored) {
            }
            spotConnection = null;
        }
    }
}
