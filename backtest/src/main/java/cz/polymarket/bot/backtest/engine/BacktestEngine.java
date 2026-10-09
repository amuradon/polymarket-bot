package cz.polymarket.bot.backtest.engine;

import cz.polymarket.bot.backtest.cache.BacktestDataCacheService;
import cz.polymarket.bot.backtest.cache.RawMarketDataProcessor;
import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import cz.polymarket.bot.backtest.export.BacktestJsonExporter;
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

/**
 * Event-driven backtesting engine emulating live WebSocket and REST execution.
 * Dispatches multi-source chronological events (Binance Spot, Binance Futures, OrderBook, Polymarket L2),
 * dynamically updates required technical indicators on-the-fly, and executes orders
 * via a simulated matching engine with network latency against reconstructed Polymarket L2 order book.
 */
@ApplicationScoped
public class BacktestEngine {

    private final BacktestDataCacheService dataCacheService;
    private final RawMarketDataProcessor rawMarketDataProcessor;
    private final StrategyRegistry strategyRegistry;
    private final PerformanceMetricsCalculator metricsCalculator;
    private final BacktestJsonExporter jsonExporter;
    private final String defaultOutputDir;
    private final double defaultCapital;

    @Inject
    public BacktestEngine(
            BacktestDataCacheService dataCacheService,
            RawMarketDataProcessor rawMarketDataProcessor,
            StrategyRegistry strategyRegistry,
            PerformanceMetricsCalculator metricsCalculator,
            BacktestJsonExporter jsonExporter,
            @ConfigProperty(name = "polymarket.backtest.output-dir", defaultValue = "D:/Crypto/data/Polymarket/backtesting")
            String defaultOutputDir,
            @ConfigProperty(name = "polymarket.backtest.initial-capital", defaultValue = "10000.0")
            double defaultCapital) {
        if (dataCacheService == null) {
            throw new IllegalArgumentException("dataCacheService cannot be null");
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
        this.dataCacheService = dataCacheService;
        this.rawMarketDataProcessor = rawMarketDataProcessor;
        this.strategyRegistry = strategyRegistry;
        this.metricsCalculator = metricsCalculator;
        this.jsonExporter = jsonExporter;
        this.defaultOutputDir = defaultOutputDir;
        this.defaultCapital = defaultCapital;
    }

    public BacktestEngine(
            BacktestDataCacheService dataCacheService,
            StrategyRegistry strategyRegistry,
            PerformanceMetricsCalculator metricsCalculator,
            BacktestJsonExporter jsonExporter,
            String defaultOutputDir,
            double defaultCapital) {
        this(dataCacheService, null, strategyRegistry, metricsCalculator, jsonExporter, defaultOutputDir, defaultCapital);
    }

    public BacktestResult runBacktest(
            String strategyName,
            String symbol,
            String startDate,
            String endDate,
            String datasetPath,
            Double initialCapital,
            String outputDirectory) {

        if (strategyName == null || strategyName.isBlank()) {
            throw new IllegalArgumentException("Strategy name is mandatory");
        }

        TradingStrategy strategy = strategyRegistry.getStrategy(strategyName);
        strategy.reset();

        String sym = (symbol != null && !symbol.isBlank()) ? symbol.toUpperCase() : "BTCUSDT";
        double capital = (initialCapital != null && initialCapital > 0) ? initialCapital : defaultCapital;
        String resolvedOutputDir = (outputDirectory != null && !outputDirectory.isBlank()) ? outputDirectory : defaultOutputDir;

        List<BacktestMarketRow> rows = dataCacheService.loadMarketData(sym, startDate, endDate, datasetPath);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("No market rows available for backtest for symbol " + sym);
        }

        PolymarketOrderBook orderBook = new PolymarketOrderBook();
        SimulatedMatchingEngine matchingEngine = new SimulatedMatchingEngine(orderBook, 50L);
        DynamicIndicatorEngine indicatorEngine = new DynamicIndicatorEngine(strategy.getRequiredIndicators());
        ChronologicalEventStreamer streamer = new ChronologicalEventStreamer();

        matchingEngine.setEventScheduler(streamer::scheduleEvent);

        BacktestSimulationContext context = new BacktestSimulationContext(matchingEngine, indicatorEngine);
        strategy.init(context);

        List<TradeRecord> trades = new ArrayList<>();
        double currentBalance = capital;
        double brierSum = 0.0;
        int evaluatedCount = 0;

        for (BacktestMarketRow row : rows) {
            long tStart = row.tStart();
            TradeDirection actualOutcome = "UP".equalsIgnoreCase(row.actualOutcome()) ? TradeDirection.UP : TradeDirection.DOWN;

            orderBook.clear();
            context.resetForCandle(row, strategy);
            indicatorEngine.resetActiveCandle(tStart * 1000L, row.sOpen());
            for (IndicatorType type : strategy.getRequiredIndicators()) {
                double val = indicatorEngine.getIndicatorValue(type);
                if (!Double.isNaN(val)) {
                    strategy.onIndicatorUpdate(type, val, tStart * 1000L);
                }
            }

            streamer.clear();
            streamer.loadCandleEvents(row, rawMarketDataProcessor != null, rawMarketDataProcessor, sym);

            while (streamer.hasNext()) {
                BacktestEvent event = streamer.next();
                context.setCurrentTimestampMs(event.timestampMs());

                if (event instanceof BacktestEvent.BinanceSpotTradeEvent e) {
                    indicatorEngine.onSpotTrade(e.timestampMs(), e.price(), e.quantity(), e.isBuyerMaker());
                    for (IndicatorType type : strategy.getRequiredIndicators()) {
                        double val = indicatorEngine.getIndicatorValue(type);
                        if (!Double.isNaN(val)) {
                            strategy.onIndicatorUpdate(type, val, e.timestampMs());
                        }
                    }
                    strategy.onBinanceSpotTrade(e.timestampMs(), e.price(), e.quantity(), e.isBuyerMaker());
                } else if (event instanceof BacktestEvent.BinanceFuturesTradeEvent e) {
                    indicatorEngine.onFuturesTrade(e.timestampMs(), e.price(), e.quantity(), e.isBuyerMaker());
                    if (strategy.getRequiredIndicators().contains(IndicatorType.BASIS)) {
                        strategy.onIndicatorUpdate(IndicatorType.BASIS, indicatorEngine.getIndicatorValue(IndicatorType.BASIS), e.timestampMs());
                    }
                    strategy.onBinanceFuturesTrade(e.timestampMs(), e.price(), e.quantity(), e.isBuyerMaker());
                } else if (event instanceof BacktestEvent.BinanceFuturesOrderBookEvent e) {
                    indicatorEngine.onFuturesOrderBook(e.timestampMs(), e.bestBid(), e.bestAsk(), e.depthBids(), e.depthAsks(), e.obi());
                    for (IndicatorType type : strategy.getRequiredIndicators()) {
                        if (type == IndicatorType.ORDER_BOOK_IMBALANCE || type == IndicatorType.BINANCE_OBI || type == IndicatorType.MICRO_PRICE) {
                            strategy.onIndicatorUpdate(type, indicatorEngine.getIndicatorValue(type), e.timestampMs());
                        }
                    }
                    strategy.onBinanceFuturesOrderBook(e.timestampMs(), e.bestBid(), e.bestAsk(), e.depthBids(), e.depthAsks(), e.obi());
                } else if (event instanceof BacktestEvent.PolymarketL2UpdateEvent e) {
                    if ("snapshot".equalsIgnoreCase(e.eventType())) {
                        orderBook.applySnapshot(e.bidsJson(), e.asksJson(), e.timestampMs());
                    } else if ("delta".equalsIgnoreCase(e.eventType())) {
                        orderBook.applyDelta(e.side(), e.price(), e.size(), e.timestampMs());
                    } else if ("trade".equalsIgnoreCase(e.eventType())) {
                        orderBook.applyTrade(e.price(), e.size(), e.side(), e.timestampMs());
                    }
                    OrderBookQuote quote = orderBook.toOrderBookQuote(e.timestampMs());
                    strategy.onOrderBookQuote(quote);
                } else if (event instanceof BacktestEvent.PolymarketQuoteEvent e) {
                    orderBook.applyQuote(e.quote());
                    strategy.onOrderBookQuote(e.quote());
                } else if (event instanceof BacktestEvent.TwapUpdateEvent e) {
                    strategy.onTwapUpdate(e.update());
                } else if (event instanceof BacktestEvent.ExecutionReportEvent e) {
                    strategy.onExecutionReport(e.report());
                } else if (event instanceof BacktestEvent.CandleLifecycleEvent e) {
                    if (e.type() == BacktestEvent.CandleLifecycleType.CANDLE_CLOSE) {
                        strategy.onCandleResolution(actualOutcome);

                        if (context.hasActiveTrade()) {
                            TradeRecord trade = context.finalizeTrade(row, actualOutcome, currentBalance);
                            if (trade != null) {
                                trades.add(trade);
                                currentBalance = trade.balanceAfterTrade();
                            }
                        }

                        MarketCandle completedCandle = (e.completedCandle() != null) ? e.completedCandle() : row.toMarketCandle();
                        indicatorEngine.onMarketCandleCompleted(completedCandle);
                        strategy.onMarketCandleCompleted(completedCandle);
                        for (IndicatorType type : strategy.getRequiredIndicators()) {
                            double val = indicatorEngine.getIndicatorValue(type);
                            if (!Double.isNaN(val)) {
                                strategy.onIndicatorUpdate(type, val, e.timestampMs());
                            }
                        }

                        double actualUp = (actualOutcome == TradeDirection.UP) ? 1.0 : 0.0;
                        double pModelUp = context.getEstimatedModelProbUp(row);
                        brierSum += (pModelUp - actualUp) * (pModelUp - actualUp);
                        evaluatedCount++;
                    }
                }
            }
        }

        double avgBrier = evaluatedCount > 0 ? (brierSum / evaluatedCount) : 0.25;
        PerformanceMetrics metrics = metricsCalculator.calculate(trades, capital, avgBrier);

        String effectiveDatasetPath = (datasetPath != null && !datasetPath.isBlank()) ? datasetPath : ("Cache: " + sym);

        BacktestResult rawResult = new BacktestResult(
                strategy.getName(),
                effectiveDatasetPath,
                Instant.now(),
                rows.size(),
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
}
