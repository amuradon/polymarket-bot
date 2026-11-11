package cz.polymarket.bot.backtest.engine;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.export.BacktestJsonExporter;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.stream.BacktestEvent;
import cz.polymarket.bot.backtest.stream.ChronologicalEventStreamer;
import cz.polymarket.bot.backtest.stream.DynamicIndicatorEngine;
import cz.polymarket.bot.backtest.stream.PolymarketOrderBook;
import cz.polymarket.bot.backtest.stream.SimulatedMatchingEngine;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExternalStreamType;
import cz.polymarket.bot.strategy.IndicatorType;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance event-driven backtesting engine streaming historical market data directly from
 * converted collector Parquet archives ({@code D:\Crypto\data\Polymarket\collector}).
 * <p>
 * Orders all events strictly by the local EC2 receipt timestamp ({@code ts}), simulates
 * time-varying execution latency derived from Polymarket CLOB server-vs-receipt timestamps
 * scaled by {@code latencyFactor}, and loads external (Binance, Deribit) streams and indicators
 * strictly when requested by the active {@link TradingStrategy}.
 */
@ApplicationScoped
public class BacktestEngine {

    private final CollectorParquetDatasetService datasetService;
    private final StrategyRegistry strategyRegistry;
    private final PerformanceMetricsCalculator metricsCalculator;
    private final BacktestJsonExporter jsonExporter;
    private final String defaultStrategy;
    private final String defaultStartDate;
    private final String defaultOutputDir;
    private final double defaultCapital;
    private final double defaultLatencyFactor;

    /**
     * Primary constructor with all dependencies injected.
     *
     * @param datasetService collector Parquet dataset discovery and path resolution service
     * @param strategyRegistry registry resolving requested strategies
     * @param metricsCalculator performance metrics calculation service
     * @param jsonExporter exporter serializing backtest results to disk
     * @param defaultStrategy default trading strategy name configured in application properties
     * @param defaultStartDate default inclusive start date ({@code yyyy-MM-dd}) configured in application properties
     * @param defaultOutputDir default destination directory for output JSON artifacts
     * @param defaultCapital default initial trading balance in USD
     * @param defaultLatencyFactor default multiplier for simulated order execution latency (default {@code 1.0})
     */
    @Inject
    public BacktestEngine(
            CollectorParquetDatasetService datasetService,
            StrategyRegistry strategyRegistry,
            PerformanceMetricsCalculator metricsCalculator,
            BacktestJsonExporter jsonExporter,
            @ConfigProperty(name = "polymarket.backtest.strategy", defaultValue = "UtterParameciumStrategy")
            String defaultStrategy,
            @ConfigProperty(name = "polymarket.backtest.from", defaultValue = "2026-10-12")
            String defaultStartDate,
            @ConfigProperty(name = "polymarket.backtest.output-dir", defaultValue = "D:/Crypto/data/Polymarket/backtesting")
            String defaultOutputDir,
            @ConfigProperty(name = "polymarket.backtest.initial-capital", defaultValue = "10000.0")
            double defaultCapital,
            @ConfigProperty(name = "polymarket.backtest.latency-factor", defaultValue = "1.0")
            double defaultLatencyFactor) {
        if (datasetService == null) {
            Log.fatal("Fatal initialization error in BacktestEngine: datasetService cannot be null");
            throw new IllegalArgumentException("datasetService cannot be null");
        }
        if (strategyRegistry == null) {
            Log.fatal("Fatal initialization error in BacktestEngine: strategyRegistry cannot be null");
            throw new IllegalArgumentException("strategyRegistry cannot be null");
        }
        if (metricsCalculator == null) {
            Log.fatal("Fatal initialization error in BacktestEngine: metricsCalculator cannot be null");
            throw new IllegalArgumentException("metricsCalculator cannot be null");
        }
        if (jsonExporter == null) {
            Log.fatal("Fatal initialization error in BacktestEngine: jsonExporter cannot be null");
            throw new IllegalArgumentException("jsonExporter cannot be null");
        }
        this.datasetService = datasetService;
        this.strategyRegistry = strategyRegistry;
        this.metricsCalculator = metricsCalculator;
        this.jsonExporter = jsonExporter;
        this.defaultStrategy = (defaultStrategy != null && !defaultStrategy.isBlank())
                ? defaultStrategy.trim()
                : "UtterParameciumStrategy";
        this.defaultStartDate = (defaultStartDate != null && !defaultStartDate.isBlank())
                ? defaultStartDate.trim()
                : "2026-10-12";
        this.defaultOutputDir = defaultOutputDir;
        this.defaultCapital = defaultCapital;
        this.defaultLatencyFactor = defaultLatencyFactor >= 0.0 ? defaultLatencyFactor : 1.0;
    }

    /**
     * Executes an event-driven backtest simulation using the default latency factor.
     *
     * @param strategyName registered name of trading strategy (defaults to configured strategy if null/blank)
     * @param symbol trading asset pair (e.g. {@code "BTCUSDT"})
     * @param startDate optional start date string ({@code yyyy-MM-dd})
     * @param endDate optional end date string ({@code yyyy-MM-dd})
     * @param initialCapital optional initial account balance in USD
     * @param outputDirectory optional directory path to store output JSON
     * @return BacktestResult containing summary metrics, trade records, and exported file path
     */
    public BacktestResult runBacktest(
            String strategyName,
            String symbol,
            String startDate,
            String endDate,
            Double initialCapital,
            String outputDirectory) {
        return runBacktest(strategyName, symbol, startDate, endDate, initialCapital, outputDirectory, null);
    }

    /**
     * Executes an event-driven backtest simulation for a specified strategy over collector Parquet archives.
     *
     * @param strategyName registered name of trading strategy (defaults to configured strategy if null/blank)
     * @param symbol trading asset pair (e.g. {@code "BTCUSDT"})
     * @param startDate optional start date string ({@code yyyy-MM-dd}, defaults to configured {@code polymarket.backtest.from})
     * @param endDate optional end date string ({@code yyyy-MM-dd}, defaults to current UTC date)
     * @param initialCapital optional initial account balance in USD (defaults to configured {@code polymarket.backtest.initial-capital})
     * @param outputDirectory optional directory path to store output JSON (defaults to configured {@code polymarket.backtest.output-dir})
     * @param latencyFactor optional multiplier for simulated order execution latency (defaults to configured {@code polymarket.backtest.latency-factor})
     * @return BacktestResult containing summary metrics, trade records, and exported file path
     */
    public BacktestResult runBacktest(
            String strategyName,
            String symbol,
            String startDate,
            String endDate,
            Double initialCapital,
            String outputDirectory,
            Double latencyFactor) {

        String resolvedStrategyName = (strategyName != null && !strategyName.isBlank())
                ? strategyName.trim()
                : defaultStrategy;

        TradingStrategy strategy;
        try {
            strategy = strategyRegistry.getStrategy(resolvedStrategyName);
        } catch (IllegalArgumentException e) {
            Log.errorf("Cannot start backtest: %s", e.getMessage());
            throw e;
        }
        strategy.reset();

        String sym = (symbol != null && !symbol.isBlank()) ? symbol.toUpperCase() : "BTCUSDT";
        double capital = (initialCapital != null) ? initialCapital : defaultCapital;
        if (Double.isNaN(capital) || capital <= 0.0) {
            Log.errorf("Cannot start backtest: initialCapital must be greater than 0 (received %s)", capital);
            throw new IllegalArgumentException("initialCapital must be greater than 0");
        }

        String resolvedStartDate = (startDate != null && !startDate.isBlank())
                ? startDate.trim()
                : defaultStartDate;
        String resolvedEndDate = (endDate != null && !endDate.isBlank())
                ? endDate.trim()
                : LocalDate.now(ZoneOffset.UTC).toString();

        LocalDate parsedStart;
        try {
            parsedStart = LocalDate.parse(resolvedStartDate);
        } catch (DateTimeParseException e) {
            Log.errorf("Cannot start backtest: invalid startDate format '%s'", resolvedStartDate);
            throw new IllegalArgumentException("Invalid startDate format: " + resolvedStartDate, e);
        }

        LocalDate parsedEnd;
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

        String resolvedOutputDir = (outputDirectory != null && !outputDirectory.isBlank()) ? outputDirectory : defaultOutputDir;
        double resolvedLatencyFactor = (latencyFactor != null) ? latencyFactor : defaultLatencyFactor;
        if (Double.isNaN(resolvedLatencyFactor) || resolvedLatencyFactor < 0.0) {
            Log.errorf("Cannot start backtest: latencyFactor must be non-negative (received %s)", resolvedLatencyFactor);
            throw new IllegalArgumentException("latencyFactor must be non-negative");
        }

        if (Log.isInfoEnabled()) {
            Log.infof("Starting backtest: strategy=%s, symbol=%s, startDate=%s, endDate=%s, initialCapital=%.2f, latencyFactor=%.2f",
                    strategy.getName(), sym, resolvedStartDate, resolvedEndDate,
                    capital, resolvedLatencyFactor);
        }

        List<CollectorMarketMetadata> markets = datasetService.discoverMarkets(sym, resolvedStartDate, resolvedEndDate);
        if (markets.isEmpty()) {
            String msg = "No market rows available for backtest for symbol " + sym;
            Log.error(msg);
            throw new IllegalArgumentException(msg);
        }

        Set<IndicatorType> reqIndicatorsSet = strategy.getRequiredIndicators() != null
                ? strategy.getRequiredIndicators()
                : Set.of();
        IndicatorType[] requiredIndicators = reqIndicatorsSet.toArray(new IndicatorType[0]);
        boolean hasIndicators = requiredIndicators.length > 0;
        boolean needBasis = reqIndicatorsSet.contains(IndicatorType.BASIS);
        boolean needObi = reqIndicatorsSet.contains(IndicatorType.ORDER_BOOK_IMBALANCE);
        boolean needMicroPrice = reqIndicatorsSet.contains(IndicatorType.MICRO_PRICE);
        boolean needCandleHistory = reqIndicatorsSet.contains(IndicatorType.VOLATILITY_4H)
                || reqIndicatorsSet.contains(IndicatorType.VWAP_ZSCORE);

        Set<ExternalStreamType> requiredExternalStreams = strategy.getRequiredExternalStreams() != null
                ? strategy.getRequiredExternalStreams()
                : Set.of();

        // Group markets by UTC day so shared daily streams (PolyBolt, Binance, Deribit) are loaded strictly once per day
        Map<String, List<CollectorMarketMetadata>> marketsByDay = new LinkedHashMap<>();
        for (CollectorMarketMetadata market : markets) {
            String dayKey = market.dateStr() != null ? market.dateStr() : "";
            marketsByDay.computeIfAbsent(dayKey, k -> new ArrayList<>()).add(market);
        }

        TradingStrategy probeInstance = strategy.createInstance();
        boolean parallelCapable = (probeInstance != null && probeInstance != strategy);

        List<TradeRecord> trades = new ArrayList<>();
        double currentBalance = capital;
        double brierSum = 0.0;
        int evaluatedCount = 0;

        List<MarketCandle> rollingCandleHistory = needCandleHistory ? new ArrayList<>(markets.size()) : List.of();

        for (Map.Entry<String, List<CollectorMarketMetadata>> dayEntry : marketsByDay.entrySet()) {
            String dateStr = dayEntry.getKey();
            List<CollectorMarketMetadata> dayMarkets = dayEntry.getValue();
            int dayMarketCount = dayMarkets.size();

            if (Log.isInfoEnabled()) {
                Log.infof("Processing UTC day %s (%d markets) for symbol %s",
                        !dateStr.isBlank() ? dateStr : "UNKNOWN", dayMarketCount, sym);
            }

            // 1. Load shared daily streams strictly ONCE for this UTC day
            BacktestEvent[] sharedDailyEvents = !dateStr.isBlank()
                    ? ChronologicalEventStreamer.loadSharedDailyStreams(datasetService, sym, dateStr, requiredExternalStreams)
                    : new BacktestEvent[0];

            // 2. Pre-build prior MarketCandle snapshots if strategy uses multi-candle indicators
            @SuppressWarnings("unchecked")
            List<MarketCandle>[] priorCandlesPerMarket = needCandleHistory ? new List[dayMarketCount] : null;
            if (needCandleHistory) {
                for (int i = 0; i < dayMarketCount; i++) {
                    CollectorMarketMetadata m = dayMarkets.get(i);
                    priorCandlesPerMarket[i] = List.copyOf(rollingCandleHistory);
                    MarketCandle built = ChronologicalEventStreamer.buildSpotCandleFromDailyEvents(
                            sharedDailyEvents, m.tStart(), m.tEnd());
                    if (built != null) {
                        rollingCandleHistory.add(built);
                    }
                }
            }

            // 3. Process the day's 15-minute Polymarket markets in parallel across CPU cores
            MarketSimulationOutcome[] dayOutcomes = new MarketSimulationOutcome[dayMarketCount];
            int numWorkers = parallelCapable
                    ? Math.min(dayMarketCount, Math.max(1, Runtime.getRuntime().availableProcessors()))
                    : 1;

            if (numWorkers <= 1) {
                executeWorkerLoop(
                        new AtomicInteger(0),
                        dayMarkets,
                        sharedDailyEvents,
                        priorCandlesPerMarket,
                        dayOutcomes,
                        strategy,
                        parallelCapable,
                        reqIndicatorsSet,
                        requiredIndicators,
                        hasIndicators,
                        needBasis,
                        needObi,
                        needMicroPrice,
                        resolvedLatencyFactor,
                        sym
                );
            } else {
                AtomicInteger cursor = new AtomicInteger(0);
                CompletableFuture<?>[] workers = new CompletableFuture<?>[numWorkers];
                for (int w = 0; w < numWorkers; w++) {
                    workers[w] = CompletableFuture.runAsync(() -> executeWorkerLoop(
                            cursor,
                            dayMarkets,
                            sharedDailyEvents,
                            priorCandlesPerMarket,
                            dayOutcomes,
                            strategy,
                            true,
                            reqIndicatorsSet,
                            requiredIndicators,
                            hasIndicators,
                            needBasis,
                            needObi,
                            needMicroPrice,
                            resolvedLatencyFactor,
                            sym
                    ), ForkJoinPool.commonPool());
                }
                CompletableFuture.allOf(workers).join();
            }

            // 4. Deterministic chronological reduction of trades, account balance, and Brier score
            boolean traceEnabled = Log.isTraceEnabled();
            for (int i = 0; i < dayMarketCount; i++) {
                MarketSimulationOutcome outcome = dayOutcomes[i];
                if (outcome == null) {
                    continue;
                }
                for (TradeRecord rawTrade : outcome.rawTrades()) {
                    currentBalance += rawTrade.netPnl();
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
                }
            }
        }

        double avgBrier = evaluatedCount > 0 ? (brierSum / evaluatedCount) : 0.25;
        PerformanceMetrics metrics = metricsCalculator.calculate(trades, capital, avgBrier);

        BacktestResult rawResult = new BacktestResult(
                strategy.getName(),
                sym,
                Instant.now(),
                markets.size(),
                metrics,
                trades,
                null
        );

        try {
            Path exportedPath = jsonExporter.export(rawResult, resolvedOutputDir);
            String absPath = exportedPath.toAbsolutePath().toString();
            if (Log.isInfoEnabled()) {
                Log.infof("Completed backtest for strategy=%s on %s: totalMarkets=%d, evaluatedMarkets=%d, totalTrades=%d, netPnl=%.2f, winRate=%.2f%%, report=%s",
                        strategy.getName(), sym, markets.size(), evaluatedCount, trades.size(),
                        metrics.totalNetPnl(), metrics.winRatePct(), absPath);
            }
            return rawResult.withJsonFilePath(absPath);
        } catch (IOException e) {
            Log.errorf(e, "Failed to export backtest results to JSON in %s: %s", resolvedOutputDir, e.getMessage());
            throw new RuntimeException("Failed to export backtest results to JSON: " + e.getMessage(), e);
        }
    }

    private void executeWorkerLoop(
            AtomicInteger cursor,
            List<CollectorMarketMetadata> dayMarkets,
            BacktestEvent[] sharedDailyEvents,
            List<MarketCandle>[] priorCandlesPerMarket,
            MarketSimulationOutcome[] dayOutcomes,
            TradingStrategy templateStrategy,
            boolean createFreshStrategyPerMarket,
            Set<IndicatorType> reqIndicatorsSet,
            IndicatorType[] requiredIndicators,
            boolean hasIndicators,
            boolean needBasis,
            boolean needObi,
            boolean needMicroPrice,
            double resolvedLatencyFactor,
            String symbol) {

        int total = dayMarkets.size();
        PolymarketOrderBook orderBook = new PolymarketOrderBook();
        SimulatedMatchingEngine matchingEngine = new SimulatedMatchingEngine(orderBook, resolvedLatencyFactor);
        DynamicIndicatorEngine indicatorEngine = new DynamicIndicatorEngine(reqIndicatorsSet);

        try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer()) {
            matchingEngine.setEventScheduler(streamer::scheduleEvent);
            BacktestSimulationContext context = new BacktestSimulationContext(matchingEngine, indicatorEngine);

            TradingStrategy workerStrategy = createFreshStrategyPerMarket ? null : templateStrategy;
            if (workerStrategy != null) {
                workerStrategy.init(context);
            }

            while (true) {
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

                if (Log.isInfoEnabled()) {
                    Log.infof("Processing market at %d (%s): twapOpen=%.2f, twapClose=%.2f, outcome=%s",
                            market.tStart(), market.datetimeUtc(), market.twapOpen(), market.twapClose(), market.actualOutcome());
                }

                TradingStrategy activeStrategy = createFreshStrategyPerMarket
                        ? templateStrategy.createInstance()
                        : workerStrategy;
                if (createFreshStrategyPerMarket) {
                    activeStrategy.reset();
                    activeStrategy.init(context);
                }

                long tStart = market.tStart();
                long tStartMs = tStart * 1000L;
                TradeDirection actualOutcome = market.actualOutcome() != null ? market.actualOutcome() : TradeDirection.UP;

                orderBook.clear();
                orderBook.setMarketTokenIds(market.upTokenId(), market.downTokenId());
                context.resetForCandle();
                indicatorEngine.resetActiveCandle();
                if (priorCandlesPerMarket != null && priorCandlesPerMarket[idx] != null) {
                    indicatorEngine.setCandleHistory(priorCandlesPerMarket[idx]);
                }

                if (hasIndicators) {
                    notifyIndicators(activeStrategy, indicatorEngine, requiredIndicators, tStartMs);
                }

                streamer.clear();
                streamer.loadCandleEvents(market, sharedDailyEvents, datasetService, symbol);

                List<TradeRecord> marketTrades = List.of();
                double brierDiffSq = 0.0;
                boolean evaluated = false;

                while (streamer.hasNext()) {
                    BacktestEvent event = streamer.next();
                    long evTs = event.timestampMs();
                    context.setCurrentTimestampMs(evTs);

                    switch (event) {
                        case BacktestEvent.PolymarketClobEvent e -> {
                            orderBook.applyCollectorEvent(e);
                            if (evTs >= tStartMs) {
                                OrderBookQuote quote = orderBook.toOrderBookQuote(evTs);
                                activeStrategy.onOrderBookQuote(quote);
                            }
                        }
                        case BacktestEvent.CalculatedSpotTwapEvent e -> {
                            context.setLastReferencePrice(e.spotPrice());
                            activeStrategy.onCalculatedSpotTwap(evTs, e.symbol(), e.spotPrice(), e.twap60sPrice());
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
                        case BacktestEvent.BinanceSpotTradeEvent e -> {
                            context.setLastReferencePrice(e.price());
                            indicatorEngine.onSpotTrade(evTs, e.price(), e.quantity(), e.isBuyerMaker());
                            if (hasIndicators) {
                                notifyIndicators(activeStrategy, indicatorEngine, requiredIndicators, evTs);
                            }
                            activeStrategy.onBinanceSpotTrade(evTs, e.price(), e.quantity(), e.isBuyerMaker());
                        }
                        case BacktestEvent.BinanceFuturesTradeEvent e -> {
                            indicatorEngine.onFuturesTrade(evTs, e.price(), e.quantity(), e.isBuyerMaker());
                            if (needBasis) {
                                double val = indicatorEngine.getIndicatorValue(IndicatorType.BASIS);
                                if (!Double.isNaN(val)) {
                                    activeStrategy.onIndicatorUpdate(IndicatorType.BASIS, val, evTs);
                                }
                            }
                            activeStrategy.onBinanceFuturesTrade(evTs, e.price(), e.quantity(), e.isBuyerMaker());
                        }
                        case BacktestEvent.BinanceFuturesOrderBookEvent e -> {
                            indicatorEngine.onFuturesOrderBook(evTs, e.bestBid(), e.bestAsk(), e.depthBids(), e.depthAsks(), e.obi());
                            if (needObi) {
                                activeStrategy.onIndicatorUpdate(IndicatorType.ORDER_BOOK_IMBALANCE,
                                        indicatorEngine.getIndicatorValue(IndicatorType.ORDER_BOOK_IMBALANCE), evTs);
                            }
                            if (needMicroPrice) {
                                activeStrategy.onIndicatorUpdate(IndicatorType.MICRO_PRICE,
                                        indicatorEngine.getIndicatorValue(IndicatorType.MICRO_PRICE), evTs);
                            }
                            activeStrategy.onBinanceFuturesOrderBook(evTs, e.bestBid(), e.bestAsk(), e.depthBids(), e.depthAsks(), e.obi());
                        }
                        case BacktestEvent.BinanceFuturesForceOrderEvent e ->
                                activeStrategy.onBinanceFuturesForceOrder(evTs, e.side(), e.price(), e.averagePrice(), e.quantity());
                        case BacktestEvent.DeribitFuturesTradeEvent e ->
                                activeStrategy.onDeribitFuturesTrade(
                                        evTs, e.instrumentName(), e.direction(), e.price(), e.amount(), e.markPrice(), e.indexPrice());
                        case BacktestEvent.TwapUpdateEvent e ->
                                activeStrategy.onTwapUpdate(e.update());
                        case BacktestEvent.PendingOrderExecutionEvent e -> {
                            ExecutionReport report = matchingEngine.executePendingOrder(e);
                            if (report != null) {
                                activeStrategy.onExecutionReport(report);
                                if ("FILLED".equalsIgnoreCase(report.status()) && evTs >= tStartMs) {
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

                                MarketCandle completedCandle = (e.completedCandle() != null)
                                        ? e.completedCandle()
                                        : indicatorEngine.buildCompletedCandle(market.tStart(), market.tEnd());
                                if (completedCandle != null) {
                                    indicatorEngine.onMarketCandleCompleted(completedCandle);
                                    activeStrategy.onMarketCandleCompleted(completedCandle);
                                }
                                if (hasIndicators) {
                                    notifyIndicators(activeStrategy, indicatorEngine, requiredIndicators, evTs);
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

    private static void notifyIndicators(
            TradingStrategy strategy,
            DynamicIndicatorEngine indicatorEngine,
            IndicatorType[] requiredIndicators,
            long timestampMs) {
        for (int i = 0; i < requiredIndicators.length; i++) {
            IndicatorType type = requiredIndicators[i];
            double val = indicatorEngine.getIndicatorValue(type);
            if (!Double.isNaN(val)) {
                strategy.onIndicatorUpdate(type, val, timestampMs);
            }
        }
    }
}
