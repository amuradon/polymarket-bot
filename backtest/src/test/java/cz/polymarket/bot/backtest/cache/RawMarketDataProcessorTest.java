package cz.polymarket.bot.backtest.cache;

import cz.polymarket.bot.backtest.data.BacktestMarketRow;
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

class RawMarketDataProcessorTest {

    @TempDir
    Path tempDir;

    private BinaryMarketCacheService cacheService;
    private MissingRawDataRegistry missingRegistry;
    private RawMarketDataProcessor processor;

    @BeforeEach
    void setUp() {
        Path cacheDir = tempDir.resolve("cache");
        Path binanceDir = tempDir.resolve("Binance");
        Path polymarketDir = tempDir.resolve("Polymarket");

        cacheService = new BinaryMarketCacheService(cacheDir.toString());
        missingRegistry = new MissingRawDataRegistry("BTCUSDT");
        processor = new RawMarketDataProcessor(cacheService, missingRegistry, binanceDir.toString(), polymarketDir.toString());
    }

    @Test
    @DisplayName("Should calculate technical indicators: VWAP, CVD, and Volatility")
    void shouldCalculateTechnicalIndicators() {
        long tStart = 1790812800L;
        CachedMarketRow row1 = new CachedMarketRow(
                tStart, tStart + 900,
                83000.0, 83500.0, 82900.0, 83400.0, 10.0, 832000.0, 5.0,
                83010.0, 83510.0, 82910.0, 83410.0, 20.0, 1664000.0, 8.0,
                true,
                0.50, 0.49, 0.52, 0.51, 0.55, 0.54, 0.58, 0.57,
                0.51, 0.015, 0.49, 0.015,
                0.60, 0.45, 300.0, 300.0
        );

        CachedMarketRow row2 = new CachedMarketRow(
                tStart + 900, tStart + 1800,
                83400.0, 83800.0, 83300.0, 83700.0, 15.0, 1254000.0, -3.0,
                83410.0, 83810.0, 83310.0, 83710.0, 25.0, 2090000.0, -5.0,
                true,
                0.53, 0.52, 0.56, 0.55, 0.60, 0.59, 0.65, 0.64,
                0.54, 0.016, 0.47, 0.016,
                0.70, 0.50, 400.0, 350.0
        );

        List<CachedMarketRow> rows = List.of(row1, row2);
        Map<String, Map<Long, Map<String, Double>>> indicators = processor.computeIndicators(rows);

        assertThat(indicators).containsKey("vwap");
        assertThat(indicators).containsKey("cvd");
        assertThat(indicators).containsKey("volatility");

        Map<String, Double> vwapRow1 = indicators.get("vwap").get(tStart);
        assertThat(vwapRow1).isNotNull();
        assertThat(vwapRow1.get("vwap")).isPositive();

        Map<String, Double> cvdRow2 = indicators.get("cvd").get(tStart + 900);
        assertThat(cvdRow2).isNotNull();
        assertThat(cvdRow2.get("cvd_15m")).isEqualTo(-3.0);
    }

    @Test
    @DisplayName("Should import BacktestMarketRows into cache and persist separate indicators")
    void shouldImportRowsIntoCache() {
        long tStart = 1790812800L;
        BacktestMarketRow row = new BacktestMarketRow(
                tStart, tStart + 900, "2026-10-01 00:00:00", "2026-10-01",
                83500.0, 83600.0, "UP",
                83500.0, 83650.0, 83450.0, 83600.0, 10.0, 835500.0, 4.0,
                83510.0, 83610.0, 15.0, 1253000.0, 6.0,
                1.2, 1.2,
                0.50, 0.49, 0.52, 0.51, 0.55, 0.54, 0.58, 0.57,
                0.51, 0.015, 0.49, 0.015,
                0.60, 0.45, 300.0, 300.0
        );

        processor.importMarketRows("BTCUSDT", "2026-10", List.of(row));

        assertThat(cacheService.hasMarketRowsCache("BTCUSDT", "2026-10")).isTrue();
        assertThat(cacheService.hasIndicatorCache("BTCUSDT", "twap", "2026-10")).isTrue();
        assertThat(cacheService.hasIndicatorCache("BTCUSDT", "basis", "2026-10")).isTrue();
        assertThat(cacheService.hasIndicatorCache("BTCUSDT", "vwap", "2026-10")).isTrue();
        assertThat(cacheService.hasIndicatorCache("BTCUSDT", "cvd", "2026-10")).isTrue();
        assertThat(cacheService.hasIndicatorCache("BTCUSDT", "volatility", "2026-10")).isTrue();

        List<CachedMarketRow> cachedRows = cacheService.readMarketRowsCache("BTCUSDT", "2026-10");
        assertThat(cachedRows).hasSize(1);
        assertThat(cachedRows.get(0).tStart()).isEqualTo(tStart);

        Map<Long, Map<String, Double>> twapCache = cacheService.readIndicatorCache("BTCUSDT", "twap", "2026-10");
        assertThat(twapCache).containsKey(tStart);
        assertThat(twapCache.get(tStart).get("twap_open")).isEqualTo(83500.0);
        assertThat(twapCache.get(tStart).get("twap_close")).isEqualTo(83600.0);
    }

    @Test
    @DisplayName("Should process raw Spot and Futures Parquet files for a month, write cached rows, and record missing files")
    void shouldProcessRawParquetFilesForMonth() throws Exception {
        String symbol = "BTCUSDT";
        String month = "2026-08";
        String dateStr = "2026-08-01";

        Path spotDir = processor.getBinanceSpotAggTradesDir(symbol);
        Path futDir = processor.getBinanceFuturesAggTradesDir(symbol);
        Files.createDirectories(spotDir);
        Files.createDirectories(futDir);

        Path spotParquet = spotDir.resolve(symbol + "-aggTrades-" + dateStr + ".parquet");
        Path futParquet = futDir.resolve(symbol + "-aggTrades-" + dateStr + ".parquet");

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            String normSpot = spotParquet.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(1785542400100000::BIGINT, 65000.50::DOUBLE, 0.015::DOUBLE, true::BOOLEAN), " +
                    "(1785542400200000::BIGINT, 65001.00::DOUBLE, 0.020::DOUBLE, false::BOOLEAN)" +
                    ") AS t(transact_time, price, quantity, is_buyer_maker)) TO '" + normSpot + "' (FORMAT PARQUET)");

            String normFut = futParquet.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(1785542400123::BIGINT, 65010.0::DOUBLE, 0.05::DOUBLE, true::BOOLEAN), " +
                    "(1785542400234::BIGINT, 65012.0::DOUBLE, 0.10::DOUBLE, false::BOOLEAN)" +
                    ") AS t(transact_time, price, quantity, is_buyer_maker)) TO '" + normFut + "' (FORMAT PARQUET)");
        }

        processor.processRawDataForMonth(symbol, month);

        assertThat(cacheService.hasMarketRowsCache(symbol, month)).isTrue();
        List<CachedMarketRow> rows = cacheService.readMarketRowsCache(symbol, month);
        assertThat(rows).isNotEmpty();
        assertThat(missingRegistry.isPolymarketMissing(1785542400L)).isTrue();
        assertThat(missingRegistry.isFuturesOrderBookMissing("2026-08-01-00")).isTrue();
    }
}
