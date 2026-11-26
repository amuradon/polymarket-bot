package cz.polymarket.bot.backtest.engine;

import cz.polymarket.bot.backtest.data.BacktestProgressListener;
import cz.polymarket.bot.backtest.data.BacktestRunConfig;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.data.ExchangeDelays;
import cz.polymarket.bot.backtest.data.MarketEquityPoint;
import cz.polymarket.bot.backtest.indicator.DailyIndicatorTrack;
import cz.polymarket.bot.backtest.indicator.IndicatorCacheService;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService.DateBounds;
import cz.polymarket.bot.backtest.service.StrategyParameterService;
import cz.polymarket.bot.backtest.service.StrategyParameterService.ConfiguredStrategy;
import cz.polymarket.bot.backtest.stream.BacktestEvent;
import cz.polymarket.bot.backtest.stream.ChronologicalEventStreamer;
import cz.polymarket.bot.backtest.stream.PolymarketOrderBook;
import cz.polymarket.bot.backtest.stream.SimulatedMatchingEngine;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.indicators.FastSpotMicroListener;
import cz.polymarket.bot.indicators.SpotTwapListener;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.logging.Log;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance event-driven backtesting engine streaming historical market data directly from
 * converted collector Parquet archives ({@code /data/polymarket/collector}) and cached
 * indicator Parquet archives ({@code /data/polymarket/cache/<indicator>/<hash>}), and persisting
 * completed simulation runs into DuckDB history ({@link BacktestHistoryRepository}).
 * <p>
 * Orders all events strictly by the local EC2 receipt timestamp ({@code ts}), simulates
 * time-varying execution latency derived from Polymarket CLOB server-vs-receipt timestamps
 * scaled by {@code latencyFactor}, supports optional pre-seeded CLOB lookback windows while
 * preserving full-candle 60s TWAP / crypto price streams, and executes parallel market workers
 * on a bounded physical-core thread pool backed by a single shared DuckDB buffer manager.
 */
@ApplicationScoped
public class BacktestEngine {

    private final CollectorParquetDatasetService datasetService;
    private final IndicatorCacheService indicatorCacheService;
    private final StrategyParameterService strategyParameterService;
    private final PerformanceMetricsCalculator metricsCalculator;
    private final BacktestHistoryRepository historyRepository;
    private final String defaultStrategy;
    private final String defaultStartDate;
    private final double defaultCapital;
    private final double defaultLatencyFactor;
    private final int workerCount;
    private final ExchangeDelays defaultExchangeDelays;
    private final ExecutorService workerPool;

    /**
     * Primary constructor injecting all backtest engine dependencies, strategy parameter resolver,
     * DuckDB history repository, worker pool sizing, and default exchange feed delays.
     *
     * @param datasetService collector Parquet dataset discovery and path resolution service
     * @param indicatorCacheService deterministic indicator Parquet cache and single-pass computation service
     * @param strategyParameterService service resolving and instantiating configured strategies
     * @param metricsCalculator performance metrics calculation service
     * @param historyRepository persistent DuckDB history repository
     * @param defaultStrategy default trading strategy name configured in application properties
     * @param defaultStartDate default inclusive start date ({@code yyyy-MM-dd}) configured in application properties
     * @param defaultCapital default initial trading balance in USD
     * @param defaultLatencyFactor default multiplier for simulated order execution latency (default {@code 1.0})
     * @param configuredWorkers number of parallel simulation workers (default {@code 4} physical cores)
     * @param binanceDelayMs default simulated one-way feed delay for Binance in milliseconds (default {@code 100})
     * @param krakenDelayMs default simulated one-way feed delay for Kraken in milliseconds (default {@code 50})
     * @param coinbaseDelayMs default simulated one-way feed delay for Coinbase in milliseconds (default {@code 50})
     */
    @Inject
    public BacktestEngine(
            CollectorParquetDatasetService datasetService,
            IndicatorCacheService indicatorCacheService,
            StrategyParameterService strategyParameterService,
            PerformanceMetricsCalculator metricsCalculator,
            BacktestHistoryRepository historyRepository,
            @ConfigProperty(name = "polymarket.backtest.strategy", defaultValue = "OraclePulseLockStrategy")
            String defaultStrategy,
            @ConfigProperty(name = "polymarket.backtest.from", defaultValue = "2026-10-12")
            String defaultStartDate,
            @ConfigProperty(name = "polymarket.backtest.initial-capital", defaultValue = "10000.0")
            double defaultCapital,
            @ConfigProperty(name = "polymarket.backtest.latency-factor", defaultValue = "1.0")
            double defaultLatencyFactor,
            @ConfigProperty(name = "polymarket.backtest.workers", defaultValue = "4")
            int configuredWorkers,
            @ConfigProperty(name = "backtest.binance.delay-ms", defaultValue = "100")
            long binanceDelayMs,
            @ConfigProperty(name = "backtest.kraken.delay-ms", defaultValue = "50")
            long krakenDelayMs,
            @ConfigProperty(name = "backtest.coinbase.delay-ms", defaultValue = "50")
            long coinbaseDelayMs) {
        if (datasetService == null) {
            Log.fatal("Fatal initialization error in BacktestEngine: datasetService cannot be null");
            throw new IllegalArgumentException("datasetService cannot be null");
        }
        if (indicatorCacheService == null) {
            Log.fatal("Fatal initialization error in BacktestEngine: indicatorCacheService cannot be null");
            throw new IllegalArgumentException("indicatorCacheService cannot be null");
        }
        if (strategyParameterService == null) {
            Log.fatal("Fatal initialization error in BacktestEngine: strategyParameterService cannot be null");
            throw new IllegalArgumentException("strategyParameterService cannot be null");
        }
        if (metricsCalculator == null) {
            Log.fatal("Fatal initialization error in BacktestEngine: metricsCalculator cannot be null");
            throw new IllegalArgumentException("metricsCalculator cannot be null");
        }
        if (historyRepository == null) {
            Log.fatal("Fatal initialization error in BacktestEngine: historyRepository cannot be null");
            throw new IllegalArgumentException("historyRepository cannot be null");
        }
        this.defaultExchangeDelays = new ExchangeDelays(binanceDelayMs, krakenDelayMs, coinbaseDelayMs);
        this.datasetService = datasetService;
        this.indicatorCacheService = indicatorCacheService;
        this.strategyParameterService = strategyParameterService;
        this.metricsCalculator = metricsCalculator;
        this.historyRepository = historyRepository;
        this.defaultStrategy = (defaultStrategy != null && !defaultStrategy.isBlank())
                ? defaultStrategy.trim()
                : "OraclePulseLockStrategy";
        this.defaultStartDate = (defaultStartDate != null && !defaultStartDate.isBlank())
                ? defaultStartDate.trim()
                : "2026-10-12";
        this.defaultCapital = defaultCapital;
        this.defaultLatencyFactor = defaultLatencyFactor >= 0.0 ? defaultLatencyFactor : 1.0;
        this.workerCount = configuredWorkers > 0 ? configuredWorkers : 4;

        AtomicInteger threadSeq = new AtomicInteger(0);
        this.workerPool = Executors.newFixedThreadPool(this.workerCount, runnable -> {
            Thread t = new Thread(runnable, "backtest-worker-" + threadSeq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Shuts down the dedicated parallel market worker pool upon application teardown.
     */
    @PreDestroy
    public void shutdown() {
        workerPool.shutdownNow();
    }

    /**
     * Executes an event-driven backtest simulation using the provided run request, reporting live
     * progress and honoring cooperative cancellation via {@code progressListener}, and persists
     * the completed run to DuckDB history.
     *
     * @param runId optional pre-assigned persistent run ID (random UUID generated when null or blank)
     * @param request validated or raw {@link BacktestRunRequest} specifying strategy, overrides, dates, and engine settings
     * @param progressListener optional progress &amp; cancellation listener (defaults to {@link BacktestProgressListener#NOOP})
     * @return completed {@link BacktestResult}
     */
    public BacktestResult runBacktest(
            String runId,
            BacktestRunRequest request,
            BacktestProgressListener progressListener) {
        if (request == null) {
            Log.error("Cannot start backtest: request cannot be null");
            throw new IllegalArgumentException("request cannot be null");
        }
        BacktestProgressListener listener = progressListener != null ? progressListener : BacktestProgressListener.NOOP;
        long startWallNs = System.nanoTime();
        String resolvedRunId = (runId != null && !runId.isBlank()) ? runId.trim() : UUID.randomUUID().toString();

        String resolvedStrategyName = (request.strategyName() != null && !request.strategyName().isBlank())
                ? request.strategyName().trim()
                : defaultStrategy;

        ConfiguredStrategy configuredStrategy;
        try {
            configuredStrategy = strategyParameterService.instantiateStrategy(resolvedStrategyName, request.strategyParams());
        } catch (IllegalArgumentException e) {
            Log.errorf("Cannot start backtest: %s", e.getMessage());
            throw e;
        }
        TradingStrategy strategy = configuredStrategy.strategy();
        strategy.reset();

        String sym = (request.symbol() != null && !request.symbol().isBlank())
                ? request.symbol().trim().toUpperCase(Locale.ROOT)
                : "BTCUSDT";

        double capital = (request.initialCapital() != null) ? request.initialCapital() : defaultCapital;
        if (Double.isNaN(capital) || Double.isInfinite(capital) || capital <= 0.0) {
            Log.errorf("Cannot start backtest: initialCapital must be greater than 0 (received %s)", capital);
            throw new IllegalArgumentException("initialCapital must be greater than 0");
        }

        double resolvedLatencyFactor = (request.latencyFactor() != null) ? request.latencyFactor() : defaultLatencyFactor;
        if (Double.isNaN(resolvedLatencyFactor) || Double.isInfinite(resolvedLatencyFactor) || resolvedLatencyFactor < 0.0) {
            Log.errorf("Cannot start backtest: latencyFactor must be non-negative (received %s)", resolvedLatencyFactor);
            throw new IllegalArgumentException("latencyFactor must be non-negative");
        }

        ExchangeDelays effectiveDelays = new ExchangeDelays(
                request.binanceDelayMs() != null ? request.binanceDelayMs() : defaultExchangeDelays.binanceDelayMs(),
                request.krakenDelayMs() != null ? request.krakenDelayMs() : defaultExchangeDelays.krakenDelayMs(),
                request.coinbaseDelayMs() != null ? request.coinbaseDelayMs() : defaultExchangeDelays.coinbaseDelayMs()
        );

        String resolvedStartDate;
        String resolvedEndDate;
        LocalDate parsedStart;
        LocalDate parsedEnd;
        if (Boolean.TRUE.equals(request.fullRange())) {
            DateBounds bounds = datasetService.resolveDateBounds(sym).orElse(null);
            if (bounds == null) {
                String msg = "No market rows available for backtest for symbol " + sym;
                Log.error(msg);
                throw new IllegalArgumentException(msg);
            }
            resolvedStartDate = bounds.minDate();
            resolvedEndDate = bounds.maxDate();
            parsedStart = LocalDate.parse(resolvedStartDate);
            parsedEnd = LocalDate.parse(resolvedEndDate);
        } else {
            resolvedStartDate = (request.startDate() != null && !request.startDate().isBlank())
                    ? request.startDate().trim()
                    : defaultStartDate;
            resolvedEndDate = (request.endDate() != null && !request.endDate().isBlank())
                    ? request.endDate().trim()
                    : LocalDate.now(ZoneOffset.UTC).toString();

            try {
                parsedStart = LocalDate.parse(resolvedStartDate);
            } catch (DateTimeParseException e) {
                Log.errorf("Cannot start backtest: invalid startDate format '%s'", resolvedStartDate);
                throw new IllegalArgumentException("Invalid startDate format: " + resolvedStartDate, e);
            }
            try {
                parsedEnd = LocalDate.parse(resolvedEndDate);
            } catch (DateTimeParseException e) {
                Log.errorf("Cannot start backtest: invalid endDate format '%s'", resolvedEndDate);
                throw new IllegalArgumentException("Invalid endDate format: " + resolvedEndDate, e);
            }
            if (parsedStart.isAfter(parsedEnd)) {
                Log.errorf("Cannot start backtest: startDate %s must not be after endDate %s",
                        resolvedStartDate, resolvedEndDate);
                throw new IllegalArgumentException("startDate must not be after endDate");
            }
        }

        String resolvedLabel = (request.label() != null && !request.label().isBlank())
                ? request.label().trim()
                : strategy.getName() + " (" + resolvedStartDate + " .. " + resolvedEndDate + ")";

        if (Log.isInfoEnabled()) {
            Log.infof("Starting backtest: runId=%s, label=%s, strategy=%s, symbol=%s, startDate=%s, endDate=%s, initialCapital=%.2f, latencyFactor=%.2f, workers=%d, delaysMs=[binance=%d, kraken=%d, coinbase=%d]",
                    resolvedRunId, resolvedLabel, strategy.getName(), sym, resolvedStartDate, resolvedEndDate,
                    capital, resolvedLatencyFactor, workerCount,
                    effectiveDelays.binanceDelayMs(), effectiveDelays.krakenDelayMs(), effectiveDelays.coinbaseDelayMs());
        }

        List<CollectorMarketMetadata> markets = datasetService.discoverMarkets(sym, resolvedStartDate, resolvedEndDate);
        if (markets.isEmpty()) {
            String msg = "No market rows available for backtest for symbol " + sym;
            Log.error(msg);
            throw new IllegalArgumentException(msg);
        }

        // Group markets by UTC day so shared daily Polymarket streams and cached indicators are loaded strictly once per day
        Map<String, List<CollectorMarketMetadata>> marketsByDay = new LinkedHashMap<>();
        for (CollectorMarketMetadata market : markets) {
            String dayKey = market.dateStr() != null ? market.dateStr() : "";
            marketsByDay.computeIfAbsent(dayKey, k -> new ArrayList<>()).add(market);
        }

        listener.onDiscovered(marketsByDay.size(), markets.size());

        TradingStrategy probeInstance = strategy.createInstance();
        boolean parallelCapable = (probeInstance != null && probeInstance != strategy);

        List<TradeRecord> trades = new ArrayList<>();
        List<MarketEquityPoint> marketPoints = new ArrayList<>(markets.size());
        double currentBalance = capital;
        double brierSum = 0.0;
        int evaluatedCount = 0;
        int completedDays = 0;
        int completedMarkets = 0;

        for (Map.Entry<String, List<CollectorMarketMetadata>> dayEntry : marketsByDay.entrySet()) {
            if (listener.isCancelled()) {
                throw new BacktestCancelledException("Backtest run " + resolvedRunId + " cancelled");
            }
            String dateStr = dayEntry.getKey();
            List<CollectorMarketMetadata> dayMarkets = dayEntry.getValue();
            int dayMarketCount = dayMarkets.size();

            listener.onDayStarted(dateStr);

            if (Log.isInfoEnabled()) {
                Log.infof("Processing UTC day %s (%d markets) for symbol %s",
                        !dateStr.isBlank() ? dateStr : "UNKNOWN", dayMarketCount, sym);
            }

            // 1. Load shared daily Polymarket streams strictly ONCE for this UTC day
            BacktestEvent[] sharedDailyEvents = !dateStr.isBlank()
                    ? ChronologicalEventStreamer.loadSharedDailyStreams(datasetService, sym, dateStr)
                    : new BacktestEvent[0];

            // 2. Load cached daily indicator tracks consumed by this strategy (computing missing ones in a single raw pass)
            List<DailyIndicatorTrack> dailyIndicatorTracks = !dateStr.isBlank()
                    ? indicatorCacheService.loadOrComputeDailyIndicators(
                            datasetService, sym, dateStr, strategy, effectiveDelays)
                    : List.of();

            // 3. Process the day's 15-minute Polymarket markets across the dedicated worker pool
            MarketSimulationOutcome[] dayOutcomes = new MarketSimulationOutcome[dayMarketCount];
            int numWorkers = parallelCapable ? Math.min(dayMarketCount, workerCount) : 1;

            if (numWorkers <= 1) {
                executeWorkerLoop(
                        new AtomicInteger(0),
                        dayMarkets,
                        sharedDailyEvents,
                        dailyIndicatorTracks,
                        dayOutcomes,
                        strategy,
                        parallelCapable,
                        resolvedLatencyFactor,
                        listener
                );
            } else {
                AtomicInteger cursor = new AtomicInteger(0);
                CompletableFuture<?>[] workers = new CompletableFuture<?>[numWorkers];
                for (int w = 0; w < numWorkers; w++) {
                    workers[w] = CompletableFuture.runAsync(() -> executeWorkerLoop(
                            cursor,
                            dayMarkets,
                            sharedDailyEvents,
                            dailyIndicatorTracks,
                            dayOutcomes,
                            strategy,
                            true,
                            resolvedLatencyFactor,
                            listener
                    ), workerPool);
                }
                CompletableFuture.allOf(workers).join();
            }

            if (listener.isCancelled()) {
                throw new BacktestCancelledException("Backtest run " + resolvedRunId + " cancelled");
            }

            // 4. Deterministic chronological reduction of trades, per-market equity points, balance, and Brier score
            boolean traceEnabled = Log.isTraceEnabled();
            for (int i = 0; i < dayMarketCount; i++) {
                CollectorMarketMetadata discoveredMarket = dayMarkets.get(i);
                MarketSimulationOutcome outcome = dayOutcomes[i];
                if (outcome == null) {
                    continue;
                }
                double marketNetPnl = 0.0;
                double marketFees = 0.0;
                int marketTradesCount = 0;
                int marketWinningTrades = 0;

                for (TradeRecord rawTrade : outcome.rawTrades()) {
                    currentBalance += rawTrade.netPnl();
                    marketNetPnl += rawTrade.netPnl();
                    marketFees += rawTrade.feeUsd();
                    marketTradesCount++;
                    if (rawTrade.isWin()) {
                        marketWinningTrades++;
                    }
                    TradeRecord finalizedTrade = new TradeRecord(
                            rawTrade.entryTimeUtc(),
                            rawTrade.intervalStartSec(),
                            rawTrade.side(),
                            rawTrade.entryPrice(),
                            rawTrade.exitPrice(),
                            rawTrade.sizeUsd(),
                            rawTrade.shares(),
                            rawTrade.feeUsd(),
                            rawTrade.netPnl(),
                            rawTrade.exitReason(),
                            rawTrade.modelProbability(),
                            rawTrade.marketPrice(),
                            rawTrade.edge(),
                            rawTrade.isWin(),
                            currentBalance
                    );
                    trades.add(finalizedTrade);

                    if (traceEnabled) {
                        Log.tracef("Trade executed: marketStart=%d (%s), side=%s, entryPrice=%.4f, exitPrice=%.4f, shares=%.2f, sizeUsd=%.2f, feeUsd=%.4f, netPnl=%.2f, win=%s, reason=%s, balanceAfter=%.2f",
                                finalizedTrade.intervalStartSec(),
                                finalizedTrade.entryTimeUtc(),
                                finalizedTrade.side(),
                                finalizedTrade.entryPrice(),
                                finalizedTrade.exitPrice(),
                                finalizedTrade.shares(),
                                finalizedTrade.sizeUsd(),
                                finalizedTrade.feeUsd(),
                                finalizedTrade.netPnl(),
                                finalizedTrade.isWin(),
                                finalizedTrade.exitReason(),
                                finalizedTrade.balanceAfterTrade());
                    }
                }
                if (outcome.evaluated()) {
                    brierSum += outcome.brierDiffSq();
                    evaluatedCount++;
                    marketPoints.add(new MarketEquityPoint(
                            discoveredMarket.tStart(),
                            dateStr,
                            marketTradesCount,
                            marketWinningTrades,
                            marketNetPnl,
                            marketFees,
                            outcome.brierDiffSq(),
                            true,
                            currentBalance
                    ));
                }
            }

            completedDays++;
            completedMarkets += dayMarketCount;
            listener.onDayCompleted(completedDays, completedMarkets, dateStr);
        }

        double avgBrier = evaluatedCount > 0 ? (brierSum / evaluatedCount) : 0.25;
        double durationDays = Math.max(1.0, ChronoUnit.DAYS.between(parsedStart, parsedEnd) + 1.0);
        PerformanceMetrics metrics = metricsCalculator.calculate(trades, capital, avgBrier, durationDays);
        long durationMs = Math.max(1L, (System.nanoTime() - startWallNs) / 1_000_000L);

        BacktestRunConfig runConfig = new BacktestRunConfig(
                strategy.getName(),
                sym,
                resolvedStartDate,
                resolvedEndDate,
                capital,
                resolvedLatencyFactor,
                effectiveDelays.binanceDelayMs(),
                effectiveDelays.krakenDelayMs(),
                effectiveDelays.coinbaseDelayMs(),
                configuredStrategy.effectiveParams(),
                configuredStrategy.appliedOverrides()
        );

        BacktestResult result = new BacktestResult(
                resolvedRunId,
                resolvedLabel,
                strategy.getName(),
                sym,
                Instant.now(),
                durationMs,
                markets.size(),
                runConfig,
                metrics,
                Collections.unmodifiableList(marketPoints),
                Collections.unmodifiableList(trades)
        );

        historyRepository.saveRun(result);
        if (Log.isInfoEnabled()) {
            Log.infof("Completed backtest runId=%s for strategy=%s on %s: totalMarkets=%d, evaluatedMarkets=%d, totalTrades=%d, netPnl=%.2f, winRate=%.2f%%, durationMs=%d",
                    resolvedRunId, strategy.getName(), sym, markets.size(), evaluatedCount, trades.size(),
                    metrics.totalNetPnl(), metrics.winRatePct(), durationMs);
        }
        return result;
    }

    private void executeWorkerLoop(
            AtomicInteger cursor,
            List<CollectorMarketMetadata> dayMarkets,
            BacktestEvent[] sharedDailyEvents,
            List<DailyIndicatorTrack> dailyIndicatorTracks,
            MarketSimulationOutcome[] dayOutcomes,
            TradingStrategy templateStrategy,
            boolean createWorkerStrategyCopy,
            double resolvedLatencyFactor,
            BacktestProgressListener listener) {

        int total = dayMarkets.size();
        PolymarketOrderBook orderBook = new PolymarketOrderBook();
        SimulatedMatchingEngine matchingEngine = new SimulatedMatchingEngine(orderBook, resolvedLatencyFactor);

        try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(datasetService.getDuckDbSessionManager())) {
            matchingEngine.setEventScheduler(streamer::scheduleEvent);
            BacktestSimulationContext context = new BacktestSimulationContext(matchingEngine);

            TradingStrategy activeStrategy = createWorkerStrategyCopy
                    ? templateStrategy.createInstance()
                    : templateStrategy;
            activeStrategy.reset();
            activeStrategy.init(context);
            long clobLookbackMs = activeStrategy.getRequiredClobLookbackMs();

            while (!listener.isCancelled()) {
                int idx = cursor.getAndIncrement();
                if (idx >= total) {
                    break;
                }

                CollectorMarketMetadata discoveredMarket = dayMarkets.get(idx);
                CollectorMarketMetadata market = datasetService.loadMarketMetadata(discoveredMarket, sharedDailyEvents)
                        .orElse(null);
                if (market == null) {
                    dayOutcomes[idx] = new MarketSimulationOutcome(List.of(), 0.0, false);
                    continue;
                }

                if (Log.isDebugEnabled()) {
                    Log.debugf("Processing market at %d (%s): twapOpen=%.2f, twapClose=%.2f, outcome=%s",
                            market.tStart(), market.datetimeUtc(), market.twapOpen(), market.twapClose(), market.actualOutcome());
                }

                activeStrategy.reset();

                long tStart = market.tStart();
                long tEnd = market.tEnd();
                long tStartMs = tStart * 1000L;
                long tEndMs = tEnd * 1000L;
                boolean windowedClob = clobLookbackMs > 0L && clobLookbackMs < (tEndMs - tStartMs);
                long quoteStartMs = windowedClob
                        ? Math.max(tStartMs, tEndMs - clobLookbackMs)
                        : 0L;
                TradeDirection actualOutcome = market.actualOutcome() != null ? market.actualOutcome() : TradeDirection.UP;

                orderBook.clear();
                orderBook.setMarketTokenIds(market.upTokenId(), market.downTokenId());
                context.resetForCandle();

                streamer.clear();
                streamer.loadCandleEvents(market, sharedDailyEvents, dailyIndicatorTracks, clobLookbackMs);

                List<TradeRecord> marketTrades = List.of();
                double brierDiffSq = 0.0;
                boolean evaluated = false;
                long lastSeededClobTs = -1L;
                boolean quoteWindowEntered = !windowedClob;

                while (streamer.hasNext()) {
                    BacktestEvent event = streamer.next();
                    long evTs = event.timestampMs();
                    context.setCurrentTimestampMs(evTs);

                    if (!quoteWindowEntered && evTs >= quoteStartMs) {
                        quoteWindowEntered = true;
                        if (!(event instanceof BacktestEvent.PolymarketClobFrameEvent) && lastSeededClobTs >= 0L) {
                            activeStrategy.onOrderBookQuote(orderBook.toOrderBookQuote(lastSeededClobTs));
                            lastSeededClobTs = -1L;
                        }
                    }

                    switch (event) {
                        case BacktestEvent.PolymarketClobFrameEvent e -> {
                            orderBook.applyClobFrame(e);
                            if (evTs >= quoteStartMs) {
                                lastSeededClobTs = -1L;
                                OrderBookQuote quote = orderBook.toOrderBookQuote(evTs);
                                activeStrategy.onOrderBookQuote(quote);
                            } else {
                                lastSeededClobTs = evTs;
                            }
                        }
                        case BacktestEvent.CalculatedFastSpotMicroEvent e -> {
                            context.setLastReferencePrice(e.fastDirectUsd());
                            if (activeStrategy instanceof FastSpotMicroListener listenerStrategy) {
                                listenerStrategy.onFastSpotMicro(
                                        evTs, e.symbol(), e.fastDirectUsd(), e.signedOfi1s(), e.syncMove500Usd());
                            }
                        }
                        case BacktestEvent.CalculatedSpotTwapEvent e -> {
                            context.setLastReferencePrice(e.spotPrice());
                            if (activeStrategy instanceof SpotTwapListener listenerStrategy) {
                                listenerStrategy.onSpotTwap(evTs, e.symbol(), e.spotPrice(), e.twap60sPrice());
                            }
                        }
                        case BacktestEvent.PolymarketCryptoPriceEvent e -> {
                            context.setLastReferencePrice(e.price());
                            activeStrategy.onPolymarketCryptoPrice(evTs, e.symbol(), e.source(), e.price(), e.priceTimestampMs());
                        }
                        case BacktestEvent.PolymarketCryptoTwap60sEvent e -> {
                            context.setLastReferencePrice(e.twapPrice());
                            activeStrategy.onPolymarketCryptoTwap60s(
                                    evTs, e.symbol(), e.source(), e.twapPrice(), e.windowSeconds(), e.twapTimestampMs());
                        }
                        case BacktestEvent.PendingOrderExecutionEvent e -> {
                            ExecutionReport report = matchingEngine.executePendingOrder(e);
                            if (report != null) {
                                activeStrategy.onExecutionReport(report);
                                if ("FILLED".equalsIgnoreCase(report.status()) && evTs >= quoteStartMs) {
                                    activeStrategy.onOrderBookQuote(orderBook.toOrderBookQuote(evTs));
                                }
                            }
                        }
                        case BacktestEvent.CandleLifecycleEvent e -> {
                            if (e.type() == BacktestEvent.CandleLifecycleType.CANDLE_CLOSE) {
                                activeStrategy.onCandleResolution(actualOutcome);

                                if (context.hasActiveTrade()) {
                                    marketTrades = context.finalizeTrades(market, actualOutcome, 0.0);
                                }

                                double actualUp = (actualOutcome == TradeDirection.UP) ? 1.0 : 0.0;
                                double pModelUp = context.getEstimatedModelProbUp(market);
                                double diff = pModelUp - actualUp;
                                brierDiffSq = diff * diff;
                                evaluated = true;
                            }
                        }
                    }
                }

                if (Log.isDebugEnabled()) {
                    double marketNetPnl = 0.0;
                    int winningTrades = 0;
                    for (int t = 0; t < marketTrades.size(); t++) {
                        TradeRecord tr = marketTrades.get(t);
                        marketNetPnl += tr.netPnl();
                        if (tr.isWin()) {
                            winningTrades++;
                        }
                    }
                    Log.debugf("Market summary at %d (%s): outcome=%s, trades=%d, wins=%d, netPnl=%.2f, brierDiffSq=%.4f",
                            market.tStart(), market.datetimeUtc(), actualOutcome,
                            marketTrades.size(), winningTrades, marketNetPnl, brierDiffSq);
                }

                dayOutcomes[idx] = new MarketSimulationOutcome(marketTrades, brierDiffSq, evaluated);
            }
        }
    }

    private record MarketSimulationOutcome(
            List<TradeRecord> rawTrades,
            double brierDiffSq,
            boolean evaluated
    ) {}
}
