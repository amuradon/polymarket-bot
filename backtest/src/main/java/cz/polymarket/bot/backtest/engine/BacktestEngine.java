package cz.polymarket.bot.backtest.engine;

import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import cz.polymarket.bot.backtest.data.ParquetDatasetLoader;
import cz.polymarket.bot.backtest.export.BacktestJsonExporter;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategy;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Backtest simulation engine coordinating dataset loading, strategy execution,
 * metrics calculation, and JSON export.
 */
@ApplicationScoped
public class BacktestEngine {

    private final ParquetDatasetLoader datasetLoader;
    private final StrategyRegistry strategyRegistry;
    private final PerformanceMetricsCalculator metricsCalculator;
    private final BacktestJsonExporter jsonExporter;
    private final String defaultDatasetPath;
    private final String defaultOutputDir;
    private final double defaultCapital;

    @Inject
    public BacktestEngine(
            ParquetDatasetLoader datasetLoader,
            StrategyRegistry strategyRegistry,
            PerformanceMetricsCalculator metricsCalculator,
            BacktestJsonExporter jsonExporter,
            @ConfigProperty(name = "polymarket.backtest.dataset-path", defaultValue = "D:/Polymarket/btc_nextCandle/unified_market_data.parquet")
            String defaultDatasetPath,
            @ConfigProperty(name = "polymarket.backtest.output-dir", defaultValue = "D:/Crypto/data/Polymarket/backtesting")
            String defaultOutputDir,
            @ConfigProperty(name = "polymarket.backtest.initial-capital", defaultValue = "10000.0")
            double defaultCapital) {
        if (datasetLoader == null) {
            throw new IllegalArgumentException("datasetLoader cannot be null");
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
        this.datasetLoader = datasetLoader;
        this.strategyRegistry = strategyRegistry;
        this.metricsCalculator = metricsCalculator;
        this.jsonExporter = jsonExporter;
        this.defaultDatasetPath = defaultDatasetPath;
        this.defaultOutputDir = defaultOutputDir;
        this.defaultCapital = defaultCapital;
    }

    public BacktestResult runBacktest(
            String strategyName,
            String datasetPath,
            Double initialCapital,
            String outputDirectory) {

        if (strategyName == null || strategyName.isBlank()) {
            throw new IllegalArgumentException("Strategy name is mandatory");
        }

        TradingStrategy strategy = strategyRegistry.getStrategy(strategyName);
        strategy.reset();

        String resolvedDataset = (datasetPath != null && !datasetPath.isBlank()) ? datasetPath : defaultDatasetPath;
        double capital = (initialCapital != null && initialCapital > 0) ? initialCapital : defaultCapital;
        String resolvedOutputDir = (outputDirectory != null && !outputDirectory.isBlank()) ? outputDirectory : defaultOutputDir;

        List<BacktestMarketRow> rows = datasetLoader.loadDataset(resolvedDataset);

        List<TradeRecord> trades = new ArrayList<>();
        double currentBalance = capital;
        double brierSum = 0.0;
        int evaluatedCount = 0;

        BacktestSimulationContext context = new BacktestSimulationContext();
        strategy.init(context);

        TWAPArbitrageStrategy twapStrategy = (strategy instanceof TWAPArbitrageStrategy s) ? s : null;

        for (BacktestMarketRow row : rows) {
            long tStart = row.tStart();
            long tEnd = row.tEnd();
            TradeDirection actualOutcome = "UP".equalsIgnoreCase(row.actualOutcome()) ? TradeDirection.UP : TradeDirection.DOWN;

            context.resetForCandle(row, strategy);

            // 1. Initial TWAP update at t=60s
            strategy.onTwapUpdate(new TwapUpdate(
                    cz.polymarket.bot.domain.Timeframe.FIFTEEN_MINUTES,
                    tStart,
                    tEnd,
                    BigDecimal.valueOf(row.twapOpen()),
                    new TwapPoint(tStart + 60, BigDecimal.valueOf(row.sOpen()), BigDecimal.valueOf(row.sOpen())),
                    true
            ));

            if (twapStrategy != null) {
                twapStrategy.setCurrentBasisBps(row.basisOpenBps());
                double distTwap = row.sClose() - row.twapOpen();
                twapStrategy.setDistTwapOverride(distTwap);
            }

            // 2. Order book quote at t=60s
            OrderBookQuote quote60 = row.toOrderBookQuote(60);
            strategy.onOrderBookQuote(quote60);

            // 3. Intra-candle exit evaluation using max/min contract prices
            if (context.hasActiveTrade()) {
                double pmMax = row.pmMaxPrice();
                double pmMin = row.pmMinPrice();
                OrderBookQuote maxQuote = new OrderBookQuote(
                        pmMax, pmMax,
                        Math.max(0.01, 1.0 - pmMin), Math.max(0.01, 1.0 - pmMin),
                        row.pmDepth1cUp(), row.pmDepth1cDown(),
                        pmMax, Math.max(0.01, 1.0 - pmMin),
                        (tStart + 300) * 1000L
                );
                strategy.onOrderBookQuote(maxQuote);
            }

            // 4. Candle resolution
            strategy.onCandleResolution(actualOutcome);

            // 5. Finalize trade if one occurred in this candle
            if (context.hasActiveTrade()) {
                TradeRecord trade = context.finalizeTrade(row, actualOutcome, currentBalance);
                if (trade != null) {
                    trades.add(trade);
                    currentBalance = trade.balanceAfterTrade();
                }
            }

            // 6. Complete market candle (history updated for indicators in next interval)
            strategy.onMarketCandleCompleted(row.toMarketCandle());

            // 7. Track Brier score
            double actualUp = (actualOutcome == TradeDirection.UP) ? 1.0 : 0.0;
            double pModelUp = context.getEstimatedModelProbUp(row);
            brierSum += (pModelUp - actualUp) * (pModelUp - actualUp);
            evaluatedCount++;
        }

        double avgBrier = evaluatedCount > 0 ? (brierSum / evaluatedCount) : 0.25;
        PerformanceMetrics metrics = metricsCalculator.calculate(trades, capital, avgBrier);

        BacktestResult rawResult = new BacktestResult(
                strategy.getName(),
                resolvedDataset,
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
