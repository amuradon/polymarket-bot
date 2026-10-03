package cz.polymarket.bot.backtest.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.data.ParquetDatasetLoader;
import cz.polymarket.bot.backtest.export.BacktestJsonExporter;
import cz.polymarket.bot.calculator.KellyPositionSizer;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.calculator.RealizedVolatilityCalculator;
import cz.polymarket.bot.calculator.VwapCalculator;
import cz.polymarket.bot.strategy.NextCandleProbabilityModel;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategy;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategyConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class BacktestEngineTest {

    private BacktestEngine backtestEngine;
    private static final String PARQUET_PATH = "D:/Polymarket/btc_nextCandle/unified_market_data.parquet";

    @BeforeEach
    void setUp() {
        ParquetDatasetLoader datasetLoader = new ParquetDatasetLoader();
        PerformanceMetricsCalculator metricsCalculator = new PerformanceMetricsCalculator();
        BacktestJsonExporter jsonExporter = new BacktestJsonExporter(new ObjectMapper(), "D:/Crypto/data/Polymarket/backtesting");

        TWAPArbitrageStrategyConfig config = TWAPArbitrageStrategyConfig.defaultIteration20();
        KellyPositionSizer positionSizer = new KellyPositionSizer(10000.0, 0.25, 50.0, 300.0);
        NextCandleProbabilityModel probabilityModel = new NextCandleProbabilityModel(positionSizer);

        TWAPArbitrageStrategy strategy = new TWAPArbitrageStrategy(
                new VwapCalculator(),
                new RealizedVolatilityCalculator(),
                probabilityModel,
                config
        );

        StrategyRegistry strategyRegistry = new StrategyRegistry(Map.of(
                strategy.getName(), strategy
        ));

        backtestEngine = new BacktestEngine(
                datasetLoader,
                strategyRegistry,
                metricsCalculator,
                jsonExporter,
                PARQUET_PATH,
                "D:/Crypto/data/Polymarket/backtesting",
                10000.0
        );
    }

    @Test
    @DisplayName("Should run backtest for TWAPArbitrageStrategy reproducing Iteration 20 metrics")
    void shouldRunBacktestReproducingIteration20(@TempDir Path tempDir) {
        if (!new File(PARQUET_PATH).exists()) {
            return;
        }

        BacktestResult result = backtestEngine.runBacktest(
                "TWAPArbitrageStrategy",
                PARQUET_PATH,
                10000.0,
                tempDir.toString()
        );

        assertThat(result).isNotNull();
        assertThat(result.strategyName()).isEqualTo("TWAPArbitrageStrategy");
        assertThat(result.totalMarkets()).isEqualTo(5100);

        // Verify metrics match Iteration 20 stats
        assertThat(result.trades()).hasSize(2801);
        assertThat(result.metrics().winningTrades()).isEqualTo(2377);
        assertThat(result.metrics().losingTrades()).isEqualTo(424);
        assertThat(result.metrics().winRatePct()).isCloseTo(84.86, within(0.1));
        assertThat(result.metrics().totalNetPnl()).isCloseTo(234636.17, within(50.0));
        assertThat(result.metrics().profitFactor()).isCloseTo(6.16, within(0.1));
        assertThat(result.metrics().brierScore()).isCloseTo(0.1631, within(0.01));

        // Verify JSON export
        assertThat(result.jsonFilePath()).isNotNull();
        Path jsonPath = Path.of(result.jsonFilePath());
        assertThat(Files.exists(jsonPath)).isTrue();
        assertThat(Files.exists(tempDir.resolve("latest.json"))).isTrue();
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when unknown strategy requested")
    void shouldThrowWhenUnknownStrategy() {
        assertThatThrownBy(() -> backtestEngine.runBacktest("UnknownStrategy", PARQUET_PATH, 10000.0, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown strategy");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when dataset file not found")
    void shouldThrowWhenDatasetNotFound() {
        assertThatThrownBy(() -> backtestEngine.runBacktest("TWAPArbitrageStrategy", "invalid/path.parquet", 10000.0, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
