package cz.polymarket.bot.backtest.engine;

import cz.polymarket.bot.backtest.data.BacktestProgressListener;
import cz.polymarket.bot.backtest.data.BacktestRunConfig;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.data.ExchangeDelays;
import cz.polymarket.bot.backtest.data.MarketEquityPoint;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService.DateBounds;
import cz.polymarket.bot.backtest.service.StrategyParameterService;
import cz.polymarket.bot.backtest.service.StrategyParameterService.ConfiguredStrategy;
import cz.polymarket.bot.backtest.stream.BacktestEvent;
import cz.polymarket.bot.backtest.stream.ChronologicalEventStreamer;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.engine.CoreTradingEngine;
import cz.polymarket.bot.engine.SimulatedClobExecutionEngine;
import cz.polymarket.bot.indicators.FastSpotMicroIndicator;
import cz.polymarket.bot.indicators.FastSpotMicroIndicatorConfig;
import cz.polymarket.bot.indicators.FastSpotMicroListener;
import cz.polymarket.bot.indicators.SpotTwapIndicator;
import cz.polymarket.bot.indicators.SpotTwapListener;
import cz.polymarket.bot.orderbook.PolymarketOrderBook;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.logging.Log;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Continuous chronological event-driven backtesting engine powered by the shared {@link CoreTradingEngine},
 * {@link PolymarketOrderBook}, and {@link SimulatedClobExecutionEngine} in {@code common}.
 * <p>
 * Eliminates cross-candle state resets, parallel candle isolation, and indicator bucket caching so
 * historical simulations execute identical bytecode to live and paper trading:
 * <ul>
 *   <li>Replays a 15-minute ({@code 900_000 ms}) pre-run warm-up window in {@code warmupMode} before the
 *       first evaluated candle so rolling spot TWAP, OFI, volatility, and strategy buffers start warm,</li>
 *   <li>Streams raw multi-exchange spot trades, full-accuracy PolyBolt oracle feeds, and typed Polymarket
 *       CLOB L2 frames + BBO hints continuously without resetting strategy state between candles,</li>
 *   <li>Simulates two-phase Polymarket CLOB {@code POST /order} speed-bump matching and L2 liquidity
 *       depletion via {@link SimulatedClobExecutionEngine}, and</li>
 *   <li>Persists completed and incrementally appended runs into {@link BacktestHistoryRepository}.</li>
 * </ul>
 */
@ApplicationScoped
public class BacktestEngine {

    /** Pre-run warm-up duration in milliseconds (15 minutes) replayed before the first evaluated market. */
    public static final long PRE_RUN_WARMUP_MS = 900_000L;

    private static final long BOUNDARY_MARGIN_MS = 30_000L;

    private final CollectorParquetDatasetService datasetService;
    private final StrategyParameterService strategyParameterService;
    private final PerformanceMetricsCalculator metricsCalculator;
    private final BacktestHistoryRepository historyRepository;
    private final String defaultStrategy;
    private final String defaultStartDate;
    private final double defaultCapital;
    private final long defaultOrderGatewayDelayMs;
    private final ExchangeDelays defaultExchangeDelays;

    /**
     * Primary constructor injecting all backtest engine dependencies and default simulation configuration.
     *
     * @param datasetService             collector Parquet dataset discovery and path resolution service
     * @param strategyParameterService   service resolving and instantiating configured strategies
     * @param metricsCalculator          performance metrics calculation service
     * @param historyRepository          persistent DuckDB history repository
     * @param defaultStrategy            default trading strategy name configured in application properties
     * @param defaultStartDate           default inclusive start date ({@code yyyy-MM-dd}) configured in application properties
     * @param defaultCapital             default initial trading balance in USD
     * @param defaultOrderGatewayDelayMs default Polymarket CLOB order gateway round-trip delay in ms (default {@code 220})
     * @param binanceDelayMs             default simulated one-way feed delay for Binance in ms (default {@code 100})
     * @param krakenDelayMs              default simulated one-way feed delay for Kraken in ms (default {@code 50})
     * @param coinbaseDelayMs            default simulated one-way feed delay for Coinbase in ms (default {@code 50})
     */
    @Inject
    public BacktestEngine(
            CollectorParquetDatasetService datasetService,
            StrategyParameterService strategyParameterService,
            PerformanceMetricsCalculator metricsCalculator,
            BacktestHistoryRepository historyRepository,
            @ConfigProperty(name = "polymarket.backtest.strategy", defaultValue = "OraclePulseLockStrategy")
            String defaultStrategy,
            @ConfigProperty(name = "polymarket.backtest.from", defaultValue = "2026-10-12")
            String defaultStartDate,
            @ConfigProperty(name = "polymarket.backtest.initial-capital", defaultValue = "10000.0")
            double defaultCapital,
            @ConfigProperty(name = "polymarket.backtest.order-gateway-delay-ms", defaultValue = "220")
            long defaultOrderGatewayDelayMs,
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
        if (defaultOrderGatewayDelayMs < 0L) {
            Log.fatal("Fatal initialization error in BacktestEngine: defaultOrderGatewayDelayMs must be non-negative");
            throw new IllegalArgumentException("defaultOrderGatewayDelayMs must be non-negative");
        }
        this.defaultExchangeDelays = new ExchangeDelays(binanceDelayMs, krakenDelayMs, coinbaseDelayMs);
        this.datasetService = datasetService;
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
        this.defaultOrderGatewayDelayMs = defaultOrderGatewayDelayMs;
    }

    /**
     * Returns the configured default Polymarket CLOB order gateway round-trip delay in milliseconds.
     *
     * @return default order gateway delay in milliseconds
     */
    public long getDefaultOrderGatewayDelayMs() {
        return defaultOrderGatewayDelayMs;
    }

    /**
     * Returns the configured default per-exchange one-way WebSocket feed delays.
     *
     * @return default {@link ExchangeDelays}
     */
    public ExchangeDelays getDefaultExchangeDelays() {
        return defaultExchangeDelays;
    }

    /**
     * Executes a continuous chronological backtest simulation using the provided run request, reporting live
     * progress and honoring cooperative cancellation via {@code progressListener}, and persists the completed
     * run to DuckDB history.
     *
     * @param runId            optional pre-assigned persistent run ID (random UUID generated when null or blank)
     * @param request          {@link BacktestRunRequest} specifying strategy, overrides, dates, and engine settings
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
        TradingStrategy baseStrategy = configuredStrategy.strategy();
        TradingStrategy activeStrategy = resolveFreshStrategyInstance(baseStrategy);
        activeStrategy.reset();

        String sym = (request.symbol() != null && !request.symbol().isBlank())
                ? request.symbol().trim().toUpperCase(Locale.ROOT)
                : "BTCUSDT";

        double capital = (request.initialCapital() != null) ? request.initialCapital() : defaultCapital;
        if (Double.isNaN(capital) || Double.isInfinite(capital) || capital <= 0.0) {
            Log.errorf("Cannot start backtest: initialCapital must be greater than 0 (received %s)", capital);
            throw new IllegalArgumentException("initialCapital must be greater than 0");
        }

        long resolvedOrderGatewayDelayMs = (request.orderGatewayDelayMs() != null)
                ? request.orderGatewayDelayMs()
                : defaultOrderGatewayDelayMs;
        if (resolvedOrderGatewayDelayMs < 0L) {
            Log.errorf("Cannot start backtest: orderGatewayDelayMs must be non-negative (received %d)",
                    resolvedOrderGatewayDelayMs);
            throw new IllegalArgumentException("orderGatewayDelayMs must be non-negative");
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

        String explicitLabel = (request.label() != null && !request.label().isBlank())
                ? request.label().trim()
                : null;
        String configHash = BacktestRunConfig.computeConfigHash(
                activeStrategy.getName(),
                sym,
                capital,
                resolvedOrderGatewayDelayMs,
                effectiveDelays.binanceDelayMs(),
                effectiveDelays.krakenDelayMs(),
                effectiveDelays.coinbaseDelayMs(),
                configuredStrategy.effectiveParams()
        );

        boolean forceNew = Boolean.TRUE.equals(request.forceNewRun());
        boolean overwriteExisting = Boolean.TRUE.equals(request.overwriteExistingDays());
        Optional<BacktestHistoryRepository.RunSummaryDto> existingMatch = forceNew
                ? Optional.empty()
                : historyRepository.findLatestRunByConfigHash(configHash);

        boolean merging = existingMatch.isPresent();
        String targetRunId = merging ? existingMatch.get().runId() : resolvedRunId;
        String resolvedLabel = explicitLabel != null
                ? explicitLabel
                : (merging ? existingMatch.get().label() : activeStrategy.getName() + " (" + resolvedStartDate + " .. " + resolvedEndDate + ")");

        if (Log.isInfoEnabled()) {
            Log.infof("Starting continuous backtest: runId=%s, merging=%s, overwriteExisting=%s, label=%s, strategy=%s, symbol=%s, startDate=%s, endDate=%s, initialCapital=%.2f, orderGatewayDelayMs=%d, delaysMs=[binance=%d, kraken=%d, coinbase=%d]",
                    targetRunId, merging, overwriteExisting, resolvedLabel, activeStrategy.getName(), sym,
                    resolvedStartDate, resolvedEndDate, capital, resolvedOrderGatewayDelayMs,
                    effectiveDelays.binanceDelayMs(), effectiveDelays.krakenDelayMs(), effectiveDelays.coinbaseDelayMs());
        }

        List<CollectorMarketMetadata> markets = datasetService.discoverMarkets(sym, resolvedStartDate, resolvedEndDate);
        if (markets.isEmpty()) {
            String msg = "No market rows available for backtest for symbol " + sym;
            Log.error(msg);
            throw new IllegalArgumentException(msg);
        }

        Map<String, List<CollectorMarketMetadata>> marketsByDay = groupMarketsByDay(markets);
        int skippedDays = 0;
        Set<String> overwrittenDates = new LinkedHashSet<>();
        double startingBalance = capital;

        if (merging) {
            Set<String> storedDates = historyRepository.getStoredDatesForRun(targetRunId);
            if (!overwriteExisting) {
                Iterator<Map.Entry<String, List<CollectorMarketMetadata>>> it = marketsByDay.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, List<CollectorMarketMetadata>> entry = it.next();
                    if (storedDates.contains(entry.getKey())) {
                        skippedDays++;
                        it.remove();
                    }
                }
                if (!marketsByDay.isEmpty() && !storedDates.isEmpty()) {
                    String maxStoredDate = Collections.max(storedDates);
                    String minRemainingDate = marketsByDay.keySet().iterator().next();
                    if (minRemainingDate.compareTo(maxStoredDate) > 0) {
                        // Pure chronological tail append: continue compounding from existing run's final balance
                        startingBalance = existingMatch.get().metrics().finalBalance();
                    } else {
                        // Non-tail backfill: re-simulate union date span continuously from initial capital
                        String minUnion = minDate(resolvedStartDate, Collections.min(storedDates));
                        String maxUnion = maxDate(resolvedEndDate, maxStoredDate);
                        resolvedStartDate = minUnion;
                        resolvedEndDate = maxUnion;
                        parsedStart = LocalDate.parse(minUnion);
                        parsedEnd = LocalDate.parse(maxUnion);
                        markets = datasetService.discoverMarkets(sym, resolvedStartDate, resolvedEndDate);
                        marketsByDay = groupMarketsByDay(markets);
                        overwrittenDates.addAll(storedDates);
                        skippedDays = 0;
                    }
                }
            } else if (!storedDates.isEmpty()) {
                // Overwrite requested: re-simulate full union date span continuously from initial capital
                String minUnion = minDate(resolvedStartDate, Collections.min(storedDates));
                String maxUnion = maxDate(resolvedEndDate, Collections.max(storedDates));
                resolvedStartDate = minUnion;
                resolvedEndDate = maxUnion;
                parsedStart = LocalDate.parse(minUnion);
                parsedEnd = LocalDate.parse(maxUnion);
                markets = datasetService.discoverMarkets(sym, resolvedStartDate, resolvedEndDate);
                marketsByDay = groupMarketsByDay(markets);
                overwrittenDates.addAll(storedDates);
                skippedDays = 0;
            }
        }

        listener.onRunResolved(targetRunId, resolvedLabel, merging, skippedDays);

        if (merging && marketsByDay.isEmpty()) {
            if (Log.isInfoEnabled()) {
                Log.infof("All %d requested UTC day(s) already exist in run %s; skipping simulation",
                        skippedDays, targetRunId);
            }
            if (explicitLabel != null) {
                historyRepository.updateLabel(targetRunId, explicitLabel);
            }
            return historyRepository.loadFullResult(targetRunId, true, skippedDays)
                    .orElseThrow(() -> new IllegalStateException("Existing run disappeared during skip: " + targetRunId));
        }

        int remainingMarketCount = 0;
        for (List<CollectorMarketMetadata> dayList : marketsByDay.values()) {
            remainingMarketCount += dayList.size();
        }
        listener.onDiscovered(marketsByDay.size(), remainingMarketCount);

        boolean needsSpot = (activeStrategy instanceof SpotTwapListener)
                || (activeStrategy instanceof FastSpotMicroListener);
        long clobLookbackMs = activeStrategy.getRequiredClobLookbackMs();

        PolymarketOrderBook orderBook = new PolymarketOrderBook();
        SpotTwapIndicator spotTwapIndicator = new SpotTwapIndicator();
        FastSpotMicroIndicator fastSpotMicroIndicator = new FastSpotMicroIndicator(
                FastSpotMicroIndicatorConfig.defaults());
        CoreTradingEngine coreEngine = new CoreTradingEngine(
                orderBook,
                spotTwapIndicator,
                fastSpotMicroIndicator,
                activeStrategy,
                sym,
                startingBalance
        );
        SimulatedClobExecutionEngine executionEngine = new SimulatedClobExecutionEngine(
                orderBook,
                resolvedOrderGatewayDelayMs
        );
        coreEngine.setExecutionGateway(executionEngine);

        List<TradeRecord> currentMarketTrades = new ArrayList<>(8);
        coreEngine.setTradeLifecycleListener(new CoreTradingEngine.TradeLifecycleListener() {
            @Override
            public void onTradeClosed(TradeRecord tradeRecord) {
                currentMarketTrades.add(tradeRecord);
            }
        });

        List<TradeRecord> trades = new ArrayList<>();
        List<MarketEquityPoint> marketPoints = new ArrayList<>(remainingMarketCount);
        double brierSum = 0.0;
        int evaluatedCount = 0;
        int completedDays = 0;
        int completedMarkets = 0;

        Map<String, BacktestEvent[]> polyBoltDayCache = new HashMap<>(4);
        long simulatedUpToMs = -1L;

        try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(datasetService.getDuckDbSessionManager())) {
            executionEngine.bind(streamer::scheduleActionAt, coreEngine::onExecutionReport);

            for (Map.Entry<String, List<CollectorMarketMetadata>> dayEntry : marketsByDay.entrySet()) {
                if (listener.isCancelled()) {
                    throw new BacktestCancelledException("Backtest run " + targetRunId + " cancelled");
                }
                String dateStr = dayEntry.getKey();
                List<CollectorMarketMetadata> dayMarkets = dayEntry.getValue();
                int dayMarketCount = dayMarkets.size();

                listener.onDayStarted(dateStr);

                if (Log.isInfoEnabled()) {
                    Log.infof("Processing UTC day %s (%d markets) for symbol %s",
                            !dateStr.isBlank() ? dateStr : "UNKNOWN", dayMarketCount, sym);
                }

                long firstDayMarketStartMs = dayMarkets.getFirst().tStart() * 1_000L;
                long lastDayMarketEndMs = dayMarkets.getLast().tEnd() * 1_000L;
                boolean isFirstSimulatedDay = (simulatedUpToMs < 0L);
                long dayStreamStartMs = isFirstSimulatedDay
                        ? (firstDayMarketStartMs - PRE_RUN_WARMUP_MS)
                        : Math.min(simulatedUpToMs, firstDayMarketStartMs);

                BacktestEvent[] polyBoltWindowEvents = loadPolyBoltWindowWithCache(
                        polyBoltDayCache, sym, dateStr, dayStreamStartMs - BOUNDARY_MARGIN_MS, lastDayMarketEndMs + BOUNDARY_MARGIN_MS);

                if (needsSpot && !dateStr.isBlank()) {
                    streamer.openDailySpotStream(
                            datasetService,
                            sym,
                            dateStr,
                            effectiveDelays,
                            dayStreamStartMs,
                            lastDayMarketEndMs,
                            isFirstSimulatedDay
                    );
                } else {
                    streamer.closeDailySpotStream();
                }

                boolean traceEnabled = Log.isTraceEnabled();

                for (int i = 0; i < dayMarketCount; i++) {
                    if (listener.isCancelled()) {
                        throw new BacktestCancelledException("Backtest run " + targetRunId + " cancelled");
                    }

                    CollectorMarketMetadata discoveredMarket = dayMarkets.get(i);
                    CollectorMarketMetadata market = datasetService.loadMarketMetadata(discoveredMarket, polyBoltWindowEvents)
                            .orElse(null);
                    if (market == null) {
                        continue;
                    }

                    long tStart = market.tStart();
                    long tEnd = market.tEnd();
                    long tStartMs = tStart * 1_000L;
                    long tEndMs = tEnd * 1_000L;
                    TradeDirection actualOutcome = market.actualOutcome() != null
                            ? market.actualOutcome()
                            : TradeDirection.UP;

                    if (simulatedUpToMs < 0L) {
                        simulatedUpToMs = tStartMs - PRE_RUN_WARMUP_MS;
                    }
                    if (simulatedUpToMs < tStartMs) {
                        playWarmupSlice(streamer, coreEngine, polyBoltWindowEvents, simulatedUpToMs, tStartMs);
                        simulatedUpToMs = tStartMs;
                    }

                    if (Log.isDebugEnabled()) {
                        Log.debugf("Processing market at %d (%s): twapOpen=%.2f, twapClose=%.2f, outcome=%s",
                                tStart, market.datetimeUtc(), market.twapOpen(), market.twapClose(), actualOutcome);
                    }

                    currentMarketTrades.clear();
                    coreEngine.setWarmupMode(false);

                    boolean windowedClob = clobLookbackMs > 0L && clobLookbackMs < (tEndMs - tStartMs);
                    long quoteStartMs = windowedClob
                            ? Math.max(tStartMs, tEndMs - clobLookbackMs)
                            : 0L;

                    streamer.loadCandleEvents(market, polyBoltWindowEvents, clobLookbackMs);

                    double brierDiffSq = 0.0;
                    boolean evaluated = false;
                    long lastSeededClobTs = -1L;
                    boolean quoteWindowEntered = !windowedClob;

                    while (streamer.hasNext()) {
                        BacktestEvent event = streamer.next();
                        long evTs = event.timestampMs();

                        if (evTs > tEndMs) {
                            break;
                        }

                        if (!quoteWindowEntered && evTs >= quoteStartMs) {
                            quoteWindowEntered = true;
                            if (!(event instanceof BacktestEvent.PolymarketClobFrameEvent) && lastSeededClobTs >= 0L) {
                                coreEngine.publishSeededOrderBook(lastSeededClobTs);
                                lastSeededClobTs = -1L;
                            }
                        }

                        switch (event) {
                            case BacktestEvent.CandleLifecycleEvent e -> {
                                if (e.type() == BacktestEvent.CandleLifecycleType.CANDLE_START) {
                                    coreEngine.setCurrentClockMs(evTs);
                                    coreEngine.beginCandleCycle(
                                            tStart,
                                            tEnd,
                                            market.twapOpen(),
                                            market.upTokenId(),
                                            market.downTokenId(),
                                            true
                                    );
                                } else if (e.type() == BacktestEvent.CandleLifecycleType.CANDLE_CLOSE) {
                                    double pModelUp = coreEngine.estimateCurrentModelProbUp();
                                    coreEngine.resolveCandle(actualOutcome, market.twapClose(), evTs);
                                    double actualUp = (actualOutcome == TradeDirection.UP) ? 1.0 : 0.0;
                                    double diff = pModelUp - actualUp;
                                    brierDiffSq = diff * diff;
                                    evaluated = true;
                                }
                            }
                            case BacktestEvent.ScheduledEngineActionEvent e -> {
                                coreEngine.setCurrentClockMs(evTs);
                                e.action().run();
                            }
                            case BacktestEvent.PolymarketClobFrameEvent e -> {
                                coreEngine.setCurrentClockMs(evTs);
                                boolean mutated = applyClobFrameBatch(streamer, orderBook, e);
                                if (mutated) {
                                    if (evTs >= quoteStartMs) {
                                        lastSeededClobTs = -1L;
                                        coreEngine.onOrderBookUpdated(evTs);
                                    } else {
                                        lastSeededClobTs = evTs;
                                    }
                                }
                            }
                            case BacktestEvent.ExchangeSpotTradeEvent e -> dispatchSpotTrade(coreEngine, e);
                            case BacktestEvent.PolymarketCryptoPriceEvent e -> coreEngine.onPolyBoltCryptoPrice(
                                    evTs, e.symbol(), e.source(), e.price(), e.priceTimestampMs());
                            case BacktestEvent.PolymarketCryptoTwap60sEvent e -> coreEngine.onPolyBoltCryptoTwap60s(
                                    evTs, e.symbol(), e.source(), e.twapPrice(), e.windowSeconds(), e.twapTimestampMs());
                        }
                    }

                    streamer.clearCandleSlice();
                    simulatedUpToMs = tEndMs;

                    double marketNetPnl = 0.0;
                    double marketFees = 0.0;
                    int marketTradesCount = currentMarketTrades.size();
                    int marketWinningTrades = 0;

                    for (int t = 0; t < marketTradesCount; t++) {
                        TradeRecord finalizedTrade = currentMarketTrades.get(t);
                        marketNetPnl += finalizedTrade.netPnl();
                        marketFees += finalizedTrade.feeUsd();
                        if (finalizedTrade.isWin()) {
                            marketWinningTrades++;
                        }
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

                    if (evaluated) {
                        brierSum += brierDiffSq;
                        evaluatedCount++;
                        marketPoints.add(new MarketEquityPoint(
                                tStart,
                                dateStr,
                                marketTradesCount,
                                marketWinningTrades,
                                marketNetPnl,
                                marketFees,
                                brierDiffSq,
                                true,
                                coreEngine.getRealizedEquityUsd()
                        ));
                    }

                    if (Log.isDebugEnabled()) {
                        Log.debugf("Market summary at %d (%s): outcome=%s, trades=%d, wins=%d, netPnl=%.2f, brierDiffSq=%.4f",
                                tStart, market.datetimeUtc(), actualOutcome,
                                marketTradesCount, marketWinningTrades, marketNetPnl, brierDiffSq);
                    }
                }

                streamer.closeDailySpotStream();
                completedDays++;
                completedMarkets += dayMarketCount;
                listener.onDayCompleted(completedDays, completedMarkets, dateStr);
            }
        }

        long durationMs = Math.max(1L, (System.nanoTime() - startWallNs) / 1_000_000L);

        BacktestRunConfig runConfig = new BacktestRunConfig(
                activeStrategy.getName(),
                sym,
                resolvedStartDate,
                resolvedEndDate,
                capital,
                resolvedOrderGatewayDelayMs,
                effectiveDelays.binanceDelayMs(),
                effectiveDelays.krakenDelayMs(),
                effectiveDelays.coinbaseDelayMs(),
                configuredStrategy.effectiveParams(),
                configuredStrategy.appliedOverrides()
        );

        if (merging) {
            return historyRepository.mergeRunDays(
                    targetRunId,
                    explicitLabel,
                    runConfig,
                    marketPoints,
                    trades,
                    overwrittenDates,
                    durationMs,
                    skippedDays
            );
        }

        double avgBrier = evaluatedCount > 0 ? (brierSum / evaluatedCount) : 0.25;
        double durationDays = Math.max(1.0, ChronoUnit.DAYS.between(parsedStart, parsedEnd) + 1.0);
        PerformanceMetrics metrics = metricsCalculator.calculate(trades, capital, avgBrier, durationDays);

        BacktestResult result = new BacktestResult(
                targetRunId,
                resolvedLabel,
                activeStrategy.getName(),
                sym,
                Instant.now(),
                durationMs,
                markets.size(),
                runConfig,
                metrics,
                Collections.unmodifiableList(marketPoints),
                Collections.unmodifiableList(trades),
                false,
                0
        );

        historyRepository.saveRun(result);
        if (Log.isInfoEnabled()) {
            Log.infof("Completed backtest runId=%s for strategy=%s on %s: totalMarkets=%d, evaluatedMarkets=%d, totalTrades=%d, netPnl=%.2f, winRate=%.2f%%, durationMs=%d",
                    targetRunId, activeStrategy.getName(), sym, markets.size(), evaluatedCount, trades.size(),
                    metrics.totalNetPnl(), metrics.winRatePct(), durationMs);
        }
        return result;
    }

    private BacktestEvent[] loadPolyBoltWindowWithCache(
            Map<String, BacktestEvent[]> polyBoltDayCache,
            String symbol,
            String dateStr,
            long minTsMs,
            long maxTsMs) {
        if (dateStr == null || dateStr.isBlank()) {
            return new BacktestEvent[0];
        }
        LocalDate curDate = LocalDate.parse(dateStr);
        String prevDateStr = curDate.minusDays(1).toString();
        String nextDateStr = curDate.plusDays(1).toString();

        // Evict stale cached days older than prevDateStr
        polyBoltDayCache.keySet().removeIf(k ->
                !k.equals(prevDateStr) && !k.equals(dateStr) && !k.equals(nextDateStr));

        BacktestEvent[] prev = polyBoltDayCache.computeIfAbsent(
                prevDateStr, d -> ChronologicalEventStreamer.loadSharedDailyStreams(datasetService, symbol, d));
        BacktestEvent[] cur = polyBoltDayCache.computeIfAbsent(
                dateStr, d -> ChronologicalEventStreamer.loadSharedDailyStreams(datasetService, symbol, d));
        BacktestEvent[] next = polyBoltDayCache.computeIfAbsent(
                nextDateStr, d -> ChronologicalEventStreamer.loadSharedDailyStreams(datasetService, symbol, d));

        return ChronologicalEventStreamer.mergePolyBoltWindow(prev, cur, next, minTsMs, maxTsMs);
    }

    private static void playWarmupSlice(
            ChronologicalEventStreamer streamer,
            CoreTradingEngine coreEngine,
            BacktestEvent[] polyBoltWindowEvents,
            long sliceStartMs,
            long sliceEndMs) {
        coreEngine.setWarmupMode(true);
        streamer.loadWarmupSlice(polyBoltWindowEvents, sliceStartMs, sliceEndMs);
        while (streamer.hasNext()) {
            BacktestEvent event = streamer.next();
            long evTs = event.timestampMs();
            switch (event) {
                case BacktestEvent.ExchangeSpotTradeEvent e -> dispatchSpotTrade(coreEngine, e);
                case BacktestEvent.PolymarketCryptoPriceEvent e -> coreEngine.onPolyBoltCryptoPrice(
                        evTs, e.symbol(), e.source(), e.price(), e.priceTimestampMs());
                case BacktestEvent.PolymarketCryptoTwap60sEvent e -> coreEngine.onPolyBoltCryptoTwap60s(
                        evTs, e.symbol(), e.source(), e.twapPrice(), e.windowSeconds(), e.twapTimestampMs());
                default -> {
                }
            }
        }
        streamer.clearCandleSlice();
    }

    private static void dispatchSpotTrade(CoreTradingEngine coreEngine, BacktestEvent.ExchangeSpotTradeEvent e) {
        long ts = e.timestampMs();
        switch (e.venueCode()) {
            case BacktestEvent.ExchangeSpotTradeEvent.VENUE_KRAKEN_USDT ->
                    coreEngine.onKrakenUsdtUsdTrade(ts, e.price());
            case BacktestEvent.ExchangeSpotTradeEvent.VENUE_BINANCE ->
                    coreEngine.onBinanceUsdtTrade(ts, e.price(), e.quantity(), e.isTakerBuy());
            case BacktestEvent.ExchangeSpotTradeEvent.VENUE_COINBASE ->
                    coreEngine.onCoinbaseUsdTrade(ts, e.price(), e.quantity(), e.isTakerBuy());
            case BacktestEvent.ExchangeSpotTradeEvent.VENUE_KRAKEN_ASSET ->
                    coreEngine.onKrakenUsdTrade(ts, e.price(), e.quantity(), e.isTakerBuy());
            default -> {
            }
        }
    }

    /**
     * Applies a typed Polymarket CLOB frame (and any co-located {@code EVENT_BBO_HINT} frame sharing the
     * identical receipt timestamp) to {@link PolymarketOrderBook} using identical per-side server timestamp
     * guards and non-fabricating BBO pruning as live {@code PolymarketStreamParser}.
     *
     * @param streamer  active chronological event streamer
     * @param orderBook canonical L2 order book
     * @param frame     typed CLOB frame event
     * @return {@code true} if book levels or BBO hints were applied
     */
    public static boolean applyClobFrameBatch(
            ChronologicalEventStreamer streamer,
            PolymarketOrderBook orderBook,
            BacktestEvent.PolymarketClobFrameEvent frame) {
        boolean mutated = applySingleClobFrame(orderBook, frame);
        if (frame.eventCode() == BacktestEvent.PolymarketClobFrameEvent.EVENT_PRICE_CHANGE && streamer != null) {
            while (streamer.peek() instanceof BacktestEvent.PolymarketClobFrameEvent nextFrame
                    && nextFrame.timestampMs() == frame.timestampMs()
                    && nextFrame.eventCode() == BacktestEvent.PolymarketClobFrameEvent.EVENT_BBO_HINT) {
                streamer.next();
                // Co-located BBO hint shares the same server_ts as its parent price_change frame
                mutated |= applyBboHintLevels(orderBook, nextFrame, false);
            }
        }
        return mutated;
    }

    private static boolean applySingleClobFrame(
            PolymarketOrderBook orderBook,
            BacktestEvent.PolymarketClobFrameEvent frame) {
        long receiptTs = frame.timestampMs();
        long serverTs = frame.serverTimestampMs();
        orderBook.updateObservedLatency(receiptTs, serverTs);

        return switch (frame.eventCode()) {
            case BacktestEvent.PolymarketClobFrameEvent.EVENT_BOOK -> {
                boolean isUp = frame.bookIsUp();
                if (!orderBook.tryAcceptServerTimestamp(isUp, serverTs)) {
                    yield false;
                }
                orderBook.clearSideForSnapshot(isUp);
                boolean[] isBidFlags = frame.isBidFlags();
                int[] ticks = frame.ticks();
                double[] sizes = frame.sizes();
                for (int i = 0, n = frame.levelCount(); i < n; i++) {
                    orderBook.setSnapshotLevel(isUp, isBidFlags[i], ticks[i], sizes[i]);
                }
                orderBook.finishBookSnapshot(isUp, receiptTs);
                yield true;
            }
            case BacktestEvent.PolymarketClobFrameEvent.EVENT_PRICE_CHANGE -> {
                int n = frame.levelCount();
                if (n <= 0) {
                    yield false;
                }
                boolean[] isUpFlags = frame.isUpFlags();
                boolean[] isBidFlags = frame.isBidFlags();
                int[] ticks = frame.ticks();
                double[] sizes = frame.sizes();

                boolean hasUp = false;
                boolean hasDown = false;
                for (int i = 0; i < n; i++) {
                    if (isUpFlags[i]) {
                        hasUp = true;
                    } else {
                        hasDown = true;
                    }
                }
                boolean acceptUp = hasUp && orderBook.tryAcceptServerTimestamp(true, serverTs);
                boolean acceptDown = hasDown && orderBook.tryAcceptServerTimestamp(false, serverTs);
                if (!acceptUp && !acceptDown) {
                    yield false;
                }
                for (int i = 0; i < n; i++) {
                    boolean sideUp = isUpFlags[i];
                    if ((sideUp && acceptUp) || (!sideUp && acceptDown)) {
                        orderBook.applyTickDelta(sideUp, isBidFlags[i], ticks[i], sizes[i], receiptTs);
                    }
                }
                yield true;
            }
            case BacktestEvent.PolymarketClobFrameEvent.EVENT_BBO_HINT ->
                    applyBboHintLevels(orderBook, frame, true);
            default -> false;
        };
    }

    private static boolean applyBboHintLevels(
            PolymarketOrderBook orderBook,
            BacktestEvent.PolymarketClobFrameEvent frame,
            boolean checkServerTimestamp) {
        int n = frame.levelCount();
        if (n <= 0) {
            return false;
        }
        boolean[] isUpFlags = frame.isUpFlags();
        boolean[] isBidFlags = frame.isBidFlags();
        int[] ticks = frame.ticks();

        int upBid = -1;
        int upAsk = -1;
        int downBid = -1;
        int downAsk = -1;
        boolean hasUp = false;
        boolean hasDown = false;

        for (int i = 0; i < n; i++) {
            if (isUpFlags[i]) {
                hasUp = true;
                if (isBidFlags[i]) {
                    upBid = ticks[i];
                } else {
                    upAsk = ticks[i];
                }
            } else {
                hasDown = true;
                if (isBidFlags[i]) {
                    downBid = ticks[i];
                } else {
                    downAsk = ticks[i];
                }
            }
        }

        boolean applied = false;
        if (hasUp && (!checkServerTimestamp || orderBook.tryAcceptServerTimestamp(true, frame.serverTimestampMs()))) {
            orderBook.applyBboHint(true, upBid, upAsk, frame.timestampMs());
            applied = true;
        }
        if (hasDown && (!checkServerTimestamp || orderBook.tryAcceptServerTimestamp(false, frame.serverTimestampMs()))) {
            orderBook.applyBboHint(false, downBid, downAsk, frame.timestampMs());
            applied = true;
        }
        return applied;
    }

    private static TradingStrategy resolveFreshStrategyInstance(TradingStrategy strategy) {
        TradingStrategy created = strategy.createInstance();
        return created != null ? created : strategy;
    }

    private static Map<String, List<CollectorMarketMetadata>> groupMarketsByDay(List<CollectorMarketMetadata> markets) {
        Map<String, List<CollectorMarketMetadata>> marketsByDay = new LinkedHashMap<>();
        for (CollectorMarketMetadata market : markets) {
            String dayKey = market.dateStr() != null ? market.dateStr() : "";
            marketsByDay.computeIfAbsent(dayKey, k -> new ArrayList<>()).add(market);
        }
        return marketsByDay;
    }

    private static String minDate(String a, String b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static String maxDate(String a, String b) {
        return a.compareTo(b) >= 0 ? a : b;
    }
}
