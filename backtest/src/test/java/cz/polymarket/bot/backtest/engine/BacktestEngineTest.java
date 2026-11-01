package cz.polymarket.bot.backtest.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.cache.BacktestDataCacheService;
import cz.polymarket.bot.backtest.cache.BinaryMarketCacheService;
import cz.polymarket.bot.backtest.cache.CachedMarketRow;
import cz.polymarket.bot.backtest.cache.MissingRawDataRegistry;
import cz.polymarket.bot.backtest.cache.RawMarketDataProcessor;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BacktestEngineTest {

    private BacktestEngine backtestEngine;
    private static final long BASE_START = 1785542400L; // 2026-08-01 00:00:00 UTC

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        Path cacheDir = tempDir.resolve("cache");
        BinaryMarketCacheService cacheService = new BinaryMarketCacheService(cacheDir.toString());
        MissingRawDataRegistry missingRegistry = new MissingRawDataRegistry("BTCUSDT");
        RawMarketDataProcessor processor = new RawMarketDataProcessor(
                cacheService, missingRegistry,
                tempDir.resolve("Binance").toString(),
                tempDir.resolve("Polymarket").toString()
        );
        BacktestDataCacheService dataCacheService = new BacktestDataCacheService(
                cacheService, processor
        );

        long c1Start = BASE_START;
        long c2Start = BASE_START + 900L;
        long c3Start = BASE_START + 1800L;

        // Create deterministic synthetic raw Parquet files for Spot, Futures, and Polymarket L2
        Path spotDir = processor.getBinanceSpotAggTradesDir("BTCUSDT");
        Path futDir = processor.getBinanceFuturesAggTradesDir("BTCUSDT");
        Path pmDir = processor.getPolymarketDir("BTCUSDT");
        Files.createDirectories(spotDir);
        Files.createDirectories(futDir);
        Files.createDirectories(pmDir);

        Path spotFile = spotDir.resolve("BTCUSDT-aggTrades-2026-08-01.parquet");
        Path futFile = futDir.resolve("BTCUSDT-aggTrades-2026-08-01.parquet");
        Path pmFile1 = pmDir.resolve(c1Start + ".parquet");
        Path pmFile2 = pmDir.resolve(c2Start + ".parquet");
        Path pmFile3 = pmDir.resolve(c3Start + ".parquet");

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    // Candle 1: flat around 60000
                    "(" + (c1Start * 1000L + 100L) + "::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, false::BOOLEAN), " +
                    "(" + ((c1Start + 60L) * 1000L) + "::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, true::BOOLEAN), " +
                    // Candle 2: bullish jump to 60150 at 60s
                    "(" + (c2Start * 1000L + 100L) + "::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, false::BOOLEAN), " +
                    "(" + ((c2Start + 60L) * 1000L) + "::BIGINT, 60150.0::DOUBLE, 15.0::DOUBLE, false::BOOLEAN), " +
                    // Candle 3: bearish drop to 59850 at 60s
                    "(" + (c3Start * 1000L + 100L) + "::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, true::BOOLEAN), " +
                    "(" + ((c3Start + 60L) * 1000L) + "::BIGINT, 59850.0::DOUBLE, 20.0::DOUBLE, true::BOOLEAN)" +
                    ") AS t(transact_time, price, quantity, is_buyer_maker)) TO '" + normSpot + "' (FORMAT PARQUET)");

            String normFut = futFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L + 100L) + "::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, false::BOOLEAN), " +
                    "(" + ((c2Start + 60L) * 1000L) + "::BIGINT, 60160.0::DOUBLE, 18.0::DOUBLE, false::BOOLEAN), " +
                    "(" + ((c3Start + 60L) * 1000L) + "::BIGINT, 59840.0::DOUBLE, 25.0::DOUBLE, true::BOOLEAN)" +
                    ") AS t(transact_time, price, quantity, is_buyer_maker)) TO '" + normFut + "' (FORMAT PARQUET)");

            String normPm1 = pmFile1.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "('snapshot'::VARCHAR, " + ((c1Start + 60L) * 1000L + 10L) + "::BIGINT, 0.50::DOUBLE, 0.0::DOUBLE, 'BUY'::VARCHAR, '[{\"price\":\"0.49\",\"size\":\"300.0\"}]'::VARCHAR, '[{\"price\":\"0.50\",\"size\":\"300.0\"}]'::VARCHAR)" +
                    ") AS t(event_type, t, price, size, side, bids, asks)) TO '" + normPm1 + "' (FORMAT PARQUET)");

            String normPm2 = pmFile2.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "('snapshot'::VARCHAR, " + ((c2Start + 60L) * 1000L + 10L) + "::BIGINT, 0.52::DOUBLE, 0.0::DOUBLE, 'BUY'::VARCHAR, '[{\"price\":\"0.50\",\"size\":\"300.0\"}]'::VARCHAR, '[{\"price\":\"0.52\",\"size\":\"300.0\"}]'::VARCHAR)" +
                    ") AS t(event_type, t, price, size, side, bids, asks)) TO '" + normPm2 + "' (FORMAT PARQUET)");

            String normPm3 = pmFile3.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "('snapshot'::VARCHAR, " + ((c3Start + 60L) * 1000L + 10L) + "::BIGINT, 0.50::DOUBLE, 0.0::DOUBLE, 'BUY'::VARCHAR, '[{\"price\":\"0.50\",\"size\":\"300.0\"}]'::VARCHAR, '[{\"price\":\"0.52\",\"size\":\"300.0\"}]'::VARCHAR)" +
                    ") AS t(event_type, t, price, size, side, bids, asks)) TO '" + normPm3 + "' (FORMAT PARQUET)");
        }

        // Pre-populate binary cache rows matching the raw files
        List<CachedMarketRow> rows = List.of(
                new CachedMarketRow(
                        c1Start, c1Start + 900L,
                        60000.0, 60050.0, 59950.0, 60000.0,
                        10.0, 600000.0, 0.0,
                        60000.0, 60050.0, 59950.0, 60000.0,
                        10.0, 600000.0, 0.0,
                        true,
                        0.50, 0.49, 0.50, 0.49, 0.50, 0.49, 0.50, 0.49,
                        0.50, 0.015, 0.50, 0.015,
                        0.52, 0.48, 300.0, 300.0
                ),
                new CachedMarketRow(
                        c2Start, c2Start + 900L,
                        60000.0, 60200.0, 59980.0, 60150.0,
                        25.0, 1500000.0, 15.0,
                        60010.0, 60210.0, 59990.0, 60160.0,
                        30.0, 1800000.0, 18.0,
                        true,
                        0.52, 0.50, 0.52, 0.50, 0.55, 0.53, 0.60, 0.58,
                        0.52, 0.015, 0.48, 0.015,
                        0.75, 0.48, 300.0, 300.0
                ),
                new CachedMarketRow(
                        c3Start, c3Start + 900L,
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
                c1Start, Map.of("twap_open", 60000.0, "twap_close", 60000.0),
                c2Start, Map.of("twap_open", 60000.0, "twap_close", 60150.0),
                c3Start, Map.of("twap_open", 60000.0, "twap_close", 59850.0)
        ));
        cacheService.writeIndicatorCache("BTCUSDT", "basis", "2026-08", List.of("basis_open_bps", "basis_close_bps"), Map.of(
                c1Start, Map.of("basis_open_bps", 0.0, "basis_close_bps", 0.0),
                c2Start, Map.of("basis_open_bps", 1.6, "basis_close_bps", 1.6),
                c3Start, Map.of("basis_open_bps", 0.8, "basis_close_bps", 0.8)
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
    @DisplayName("Should run backtest over raw Parquet event files without synthetic fallback events")
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

        // Verify trades were simulated with realistic matching and latency against raw book snapshots
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
