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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
            @ConfigProperty(name = "polymarket.backtest.output-dir", defaultValue = "D:/Crypto/data/Polymarket/backtesting")
            String defaultOutputDir,
            @ConfigProperty(name = "polymarket.backtest.initial-capital", defaultValue = "10000.0")
            double defaultCapital,
            @ConfigProperty(name = "polymarket.backtest.latency-factor", defaultValue = "1.0")
            double defaultLatencyFactor) {
        if (datasetService == null) {
            throw new IllegalArgumentException("datasetService cannot be null");
        }
        if (strategyRegistry == null) {
            throw new IllegalArgumentException("strategyRegistry cannot be null");
        }
        if (metricsCalculator == null) {
            throw new IllegalArgumentException("metricsCalculator cannot be null");
        }
        if (jsonExporter == null) {
            throw new IllegalArgumentException("jsonExporter cannot be null");
        }
        this.datasetService = datasetService;
        this.strategyRegistry = strategyRegistry;
        this.metricsCalculator = metricsCalculator;
        this.jsonExporter = jsonExporter;
        this.defaultOutputDir = defaultOutputDir;
        this.defaultCapital = defaultCapital;
        this.defaultLatencyFactor = defaultLatencyFactor >= 0.0 ? defaultLatencyFactor : 1.0;
    }

    /**
     * Executes an event-driven backtest simulation using the default latency factor.
     *
     * @param strategyName registered name of trading strategy
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
     * @param strategyName registered name of trading strategy (e.g. {@code "TWAPArbitrageStrategy"})
     * @param symbol trading asset pair (e.g. {@code "BTCUSDT"})
     * @param startDate optional start date string ({@code yyyy-MM-dd})
     * @param endDate optional end date string ({@code yyyy-MM-dd})
     * @param initialCapital optional initial account balance in USD (default 10,000)
     * @param outputDirectory optional directory path to store output JSON
     * @param latencyFactor optional multiplier for simulated order execution latency (default 1.0)
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

        if (strategyName == null || strategyName.isBlank()) {
            throw new IllegalArgumentException("Strategy name is mandatory");
        }

        TradingStrategy strategy = strategyRegistry.getStrategy(strategyName);
        strategy.reset();

        String sym = (symbol != null && !symbol.isBlank()) ? symbol.toUpperCase() : "BTCUSDT";
        double capital = (initialCapital != null && initialCapital > 0) ? initialCapital : defaultCapital;
        String resolvedOutputDir = (outputDirectory != null && !outputDirectory.isBlank()) ? outputDirectory : defaultOutputDir;
        double resolvedLatencyFactor = (latencyFactor != null && latencyFactor >= 0.0) ? latencyFactor : defaultLatencyFactor;

        List<CollectorMarketMetadata> markets = datasetService.discoverMarkets(sym, startDate, endDate);
        if (markets.isEmpty()) {
            throw new IllegalArgumentException("No market rows available for backtest for symbol " + sym);
        }

        Set<IndicatorType> reqIndicatorsSet = strategy.getRequiredIndicators() != null
                ? strategy.getRequiredIndicators()
                : Set.of();
        IndicatorType[] requiredIndicators = reqIndicatorsSet.toArray(new IndicatorType[0]);
        boolean hasIndicators = requiredIndicators.length > 0;
        boolean needBasis = reqIndicatorsSet.contains(IndicatorType.BASIS);
        boolean needObi = reqIndicatorsSet.contains(IndicatorType.ORDER_BOOK_IMBALANCE);
        boolean needMicroPrice = reqIndicatorsSet.contains(IndicatorType.MICRO_PRICE);

        Set<ExternalStreamType> requiredExternalStreams = strategy.getRequiredExternalStreams() != null
                ? strategy.getRequiredExternalStreams()
                : Set.of();

        PolymarketOrderBook orderBook = new PolymarketOrderBook();
        SimulatedMatchingEngine matchingEngine = new SimulatedMatchingEngine(orderBook, resolvedLatencyFactor);
        DynamicIndicatorEngine indicatorEngine = new DynamicIndicatorEngine(reqIndicatorsSet);

        List<TradeRecord> trades = new ArrayList<>();
        double currentBalance = capital;
        double brierSum = 0.0;
        int evaluatedCount = 0;

        try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer()) {
            matchingEngine.setEventScheduler(streamer::scheduleEvent);
            BacktestSimulationContext context = new BacktestSimulationContext(matchingEngine, indicatorEngine);
            strategy.init(context);

            for (CollectorMarketMetadata market : markets) {
                long tStart = market.tStart();
                long tStartMs = tStart * 1000L;
                TradeDirection actualOutcome = market.actualOutcome() != null ? market.actualOutcome() : TradeDirection.UP;

                orderBook.clear();
                orderBook.setMarketTokenIds(market.upTokenId(), market.downTokenId());
                context.resetForCandle();
                indicatorEngine.resetActiveCandle(market.twapOpen());

                if (hasIndicators) {
                    notifyIndicators(strategy, indicatorEngine, requiredIndicators, tStartMs);
                }

                streamer.clear();
                streamer.loadCandleEvents(market, datasetService, sym, requiredExternalStreams);

                while (streamer.hasNext()) {
                    BacktestEvent event = streamer.next();
                    long evTs = event.timestampMs();
                    context.setCurrentTimestampMs(evTs);

                    switch (event) {
                        case BacktestEvent.PolymarketClobEvent e -> {
                            orderBook.applyCollectorEvent(e);
                            if (evTs >= tStartMs) {
                                OrderBookQuote quote = orderBook.toOrderBookQuote(evTs);
                                strategy.onOrderBookQuote(quote);
                            }
                        }
                        case BacktestEvent.PolymarketCryptoPriceEvent e -> {
                            indicatorEngine.onPolymarketCryptoPrice(evTs, e.price());
                            if (hasIndicators) {
                                notifyIndicators(strategy, indicatorEngine, requiredIndicators, evTs);
                            }
                            strategy.onPolymarketCryptoPrice(evTs, e.symbol(), e.source(), e.price(), e.priceTimestampMs());
                        }
                        case BacktestEvent.PolymarketCryptoTwap60sEvent e -> {
                            indicatorEngine.onPolymarketCryptoTwap60s(evTs, e.twapPrice());
                            if (hasIndicators) {
                                notifyIndicators(strategy, indicatorEngine, requiredIndicators, evTs);
                            }
                            strategy.onPolymarketCryptoTwap60s(
                                    evTs, e.symbol(), e.source(), e.twapPrice(), e.windowSeconds(), e.twapTimestampMs());
                        }
                        case BacktestEvent.BinanceSpotTradeEvent e -> {
                            indicatorEngine.onSpotTrade(evTs, e.price(), e.quantity(), e.isBuyerMaker());
                            if (hasIndicators) {
                                notifyIndicators(strategy, indicatorEngine, requiredIndicators, evTs);
                            }
                            strategy.onBinanceSpotTrade(evTs, e.price(), e.quantity(), e.isBuyerMaker());
                        }
                        case BacktestEvent.BinanceFuturesTradeEvent e -> {
                            indicatorEngine.onFuturesTrade(evTs, e.price(), e.quantity(), e.isBuyerMaker());
                            if (needBasis) {
                                double val = indicatorEngine.getIndicatorValue(IndicatorType.BASIS);
                                if (!Double.isNaN(val)) {
                                    strategy.onIndicatorUpdate(IndicatorType.BASIS, val, evTs);
                                }
                            }
                            strategy.onBinanceFuturesTrade(evTs, e.price(), e.quantity(), e.isBuyerMaker());
                        }
                        case BacktestEvent.BinanceFuturesOrderBookEvent e -> {
                            indicatorEngine.onFuturesOrderBook(evTs, e.bestBid(), e.bestAsk(), e.depthBids(), e.depthAsks(), e.obi());
                            if (needObi) {
                                strategy.onIndicatorUpdate(IndicatorType.ORDER_BOOK_IMBALANCE,
                                        indicatorEngine.getIndicatorValue(IndicatorType.ORDER_BOOK_IMBALANCE), evTs);
                            }
                            if (needMicroPrice) {
                                strategy.onIndicatorUpdate(IndicatorType.MICRO_PRICE,
                                        indicatorEngine.getIndicatorValue(IndicatorType.MICRO_PRICE), evTs);
                            }
                            strategy.onBinanceFuturesOrderBook(evTs, e.bestBid(), e.bestAsk(), e.depthBids(), e.depthAsks(), e.obi());
                        }
                        case BacktestEvent.BinanceFuturesForceOrderEvent e ->
                                strategy.onBinanceFuturesForceOrder(evTs, e.side(), e.price(), e.averagePrice(), e.quantity());
                        case BacktestEvent.DeribitFuturesTradeEvent e ->
                                strategy.onDeribitFuturesTrade(
                                        evTs, e.instrumentName(), e.direction(), e.price(), e.amount(), e.markPrice(), e.indexPrice());
                        case BacktestEvent.TwapUpdateEvent e ->
                                strategy.onTwapUpdate(e.update());
                        case BacktestEvent.PendingOrderExecutionEvent e -> {
                            ExecutionReport report = matchingEngine.executePendingOrder(e);
                            if (report != null) {
                                strategy.onExecutionReport(report);
                            }
                        }
                        case BacktestEvent.CandleLifecycleEvent e -> {
                            if (e.type() == BacktestEvent.CandleLifecycleType.CANDLE_CLOSE) {
                                strategy.onCandleResolution(actualOutcome);

                                if (context.hasActiveTrade()) {
                                    TradeRecord trade = context.finalizeTrade(market, actualOutcome, currentBalance);
                                    if (trade != null) {
                                        trades.add(trade);
                                        currentBalance = trade.balanceAfterTrade();
                                    }
                                }

                                MarketCandle completedCandle = (e.completedCandle() != null)
                                        ? e.completedCandle()
                                        : indicatorEngine.buildCompletedCandle(market.tStart(), market.tEnd());
                                indicatorEngine.onMarketCandleCompleted(completedCandle);
                                strategy.onMarketCandleCompleted(completedCandle);
                                if (hasIndicators) {
                                    notifyIndicators(strategy, indicatorEngine, requiredIndicators, evTs);
                                }

                                double actualUp = (actualOutcome == TradeDirection.UP) ? 1.0 : 0.0;
                                double pModelUp = context.getEstimatedModelProbUp(market);
                                double diff = pModelUp - actualUp;
                                brierSum += diff * diff;
                                evaluatedCount++;
                            }
                        }
                    }
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
            return rawResult.withJsonFilePath(exportedPath.toAbsolutePath().toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to export backtest results to JSON: " + e.getMessage(), e);
        }
    }

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
