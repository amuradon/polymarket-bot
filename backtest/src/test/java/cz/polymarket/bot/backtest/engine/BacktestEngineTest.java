package cz.polymarket.bot.backtest.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.cache.BacktestDataCacheService;
import cz.polymarket.bot.backtest.cache.BinaryMarketCacheService;
import cz.polymarket.bot.backtest.cache.CachedMarketRow;
import cz.polymarket.bot.backtest.cache.MissingRawDataRegistry;
import cz.polymarket.bot.backtest.cache.RawMarketDataProcessor;
import cz.polymarket.bot.backtest.data.ParquetDatasetLoader;
import cz.polymarket.bot.backtest.export.BacktestJsonExporter;
import cz.polymarket.bot.calculator.KellyPositionSizer;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.strategy.NextCandleProbabilityModel;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategy;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategyConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BacktestEngineTest {

    private BacktestEngine backtestEngine;
    private BinaryMarketCacheService cacheService;
    private static final long BASE_START = 1785542400L; // 2026-08-01 00:00:00 UTC

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        Path cacheDir = tempDir.resolve("cache");
        cacheService = new BinaryMarketCacheService(cacheDir.toString());
        MissingRawDataRegistry missingRegistry = new MissingRawDataRegistry("BTCUSDT");
        RawMarketDataProcessor processor = new RawMarketDataProcessor(
                cacheService, missingRegistry,
                tempDir.resolve("Binance").toString(),
                tempDir.resolve("Polymarket").toString()
        );
        BacktestDataCacheService dataCacheService = new BacktestDataCacheService(
                cacheService, processor, missingRegistry
        );

        // Pre-populate representative synthesized test dataset in binary cache
        List<CachedMarketRow> rows = List.of(
                // Interval 1: Baseline flat candle, no entry
                new CachedMarketRow(
                        BASE_START, BASE_START + 900L,
                        60000.0, 60050.0, 59950.0, 60000.0,
                        10.0, 600000.0, 0.0,
                        60000.0, 60050.0, 59950.0, 60000.0,
                        10.0, 600000.0, 0.0,
                        true,
                        0.50, 0.49, 0.50, 0.49, 0.50, 0.49, 0.50, 0.49,
                        0.50, 0.015, 0.50, 0.015,
                        0.52, 0.48, 300.0, 300.0
                ),
                // Interval 2: Bullish trend, distTwap = +120 USD > 80 USD, triggers BUY UP at 0.52, wins at 1.00
                new CachedMarketRow(
                        BASE_START + 900L, BASE_START + 1800L,
                        60000.0, 60200.0, 59980.0, 60150.0,
                        25.0, 1500000.0, 15.0,
                        60010.0, 60210.0, 59990.0, 60160.0,
                        30.0, 1800000.0, 18.0,
                        true,
                        0.52, 0.50, 0.52, 0.50, 0.55, 0.53, 0.60, 0.58,
                        0.52, 0.015, 0.48, 0.015,
                        0.75, 0.48, 300.0, 300.0
                ),
                // Interval 3: Bearish trend, distTwap = -120 USD < -80 USD, triggers BUY DOWN at 0.50, wins at 1.00
                new CachedMarketRow(
                        BASE_START + 1800L, BASE_START + 2700L,
                        60000.0, 60020.0, 59800.0, 59850.0,
                        30.0, 1800000.0, -20.0,
                        60005.0, 60025.0, 59790.0, 59840.0,
                        35.0, 2100000.0, -25.0,
                        false,
                        0.50, 0.48, 0.50, 0.48, 0.45, 0.43, 0.40, 0.38,
                        0.50, 0.015, 0.50, 0.015,
                        0.55, 0.35, 300.0, 300.0
                )
        );

        cacheService.writeMarketRowsCache("BTCUSDT", "2026-08", rows);
        cacheService.writeIndicatorCache("BTCUSDT", "twap", "2026-08", List.of("twap_open", "twap_close"), Map.of(
                BASE_START, Map.of("twap_open", 60000.0, "twap_close", 60000.0),
                BASE_START + 900L, Map.of("twap_open", 60000.0, "twap_close", 60150.0),
                BASE_START + 1800L, Map.of("twap_open", 60000.0, "twap_close", 59850.0)
        ));
        cacheService.writeIndicatorCache("BTCUSDT", "basis", "2026-08", List.of("basis_open_bps", "basis_close_bps"), Map.of(
                BASE_START, Map.of("basis_open_bps", 0.0, "basis_close_bps", 0.0),
                BASE_START + 900L, Map.of("basis_open_bps", 1.6, "basis_close_bps", 1.6),
                BASE_START + 1800L, Map.of("basis_open_bps", 0.8, "basis_close_bps", 0.8)
        ));

        PerformanceMetricsCalculator metricsCalculator = new PerformanceMetricsCalculator();
        BacktestJsonExporter jsonExporter = new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export").toString());

        TWAPArbitrageStrategyConfig config = TWAPArbitrageStrategyConfig.defaultIteration20();
        KellyPositionSizer positionSizer = new KellyPositionSizer(10000.0, 0.25, 50.0, 300.0);
        NextCandleProbabilityModel probabilityModel = new NextCandleProbabilityModel(positionSizer);

        TWAPArbitrageStrategy strategy = new TWAPArbitrageStrategy(probabilityModel, config);

        StrategyRegistry strategyRegistry = new StrategyRegistry(Map.of(
                strategy.getName(), strategy
        ));

        backtestEngine = new BacktestEngine(
                dataCacheService,
                processor,
                strategyRegistry,
                metricsCalculator,
                jsonExporter,
                tempDir.resolve("export").toString(),
                10000.0
        );
    }

    @Test
    @DisplayName("Should run fast backtest over synthesized representative dataset")
    void shouldRunBacktestOverRepresentativeSample(@TempDir Path tempDir) {
        BacktestResult result = backtestEngine.runBacktest(
                "TWAPArbitrageStrategy",
                "BTCUSDT",
                "2026-08-01",
                "2026-08-01",
                10000.0,
                tempDir.toString()
        );

        assertThat(result).isNotNull();
        assertThat(result.strategyName()).isEqualTo("TWAPArbitrageStrategy");
        assertThat(result.totalMarkets()).isEqualTo(3);

        // Verify trades were simulated with realistic matching and latency
        assertThat(result.trades()).isNotEmpty();
        assertThat(result.metrics().totalTrades()).isEqualTo(result.trades().size());
        assertThat(result.metrics().totalNetPnl()).isGreaterThan(0.0);

        // Verify JSON export was produced
        assertThat(result.jsonFilePath()).isNotNull();
        Path jsonPath = Path.of(result.jsonFilePath());
        assertThat(Files.exists(jsonPath)).isTrue();
        assertThat(Files.exists(tempDir.resolve("latest.json"))).isTrue();
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when unknown strategy requested")
    void shouldThrowWhenUnknownStrategy() {
        assertThatThrownBy(() -> backtestEngine.runBacktest("UnknownStrategy", "BTCUSDT", null, null, 10000.0, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown strategy");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when no market rows available")
    void shouldThrowWhenNoMarketRowsAvailable() {
        assertThatThrownBy(() -> backtestEngine.runBacktest("TWAPArbitrageStrategy", "UNKNOWN_SYMBOL", null, null, 10000.0, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No market rows available");
    }
}
