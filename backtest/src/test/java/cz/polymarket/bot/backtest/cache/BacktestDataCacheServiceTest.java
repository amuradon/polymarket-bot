package cz.polymarket.bot.backtest.cache;

import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import cz.polymarket.bot.backtest.data.ParquetDatasetLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BacktestDataCacheServiceTest {

    @TempDir
    Path tempDir;

    private BinaryMarketCacheService cacheService;
    private MissingRawDataRegistry missingRegistry;
    private RawMarketDataProcessor processor;
    private ParquetDatasetLoader parquetDatasetLoader;
    private BacktestDataCacheService dataCacheService;

    @BeforeEach
    void setUp() {
        Path cacheDir = tempDir.resolve("cache");
        Path binanceDir = tempDir.resolve("Binance");
        Path polymarketDir = tempDir.resolve("Polymarket");

        cacheService = new BinaryMarketCacheService(cacheDir.toString());
        missingRegistry = new MissingRawDataRegistry("BTCUSDT");
        processor = new RawMarketDataProcessor(cacheService, missingRegistry, binanceDir.toString(), polymarketDir.toString());
        parquetDatasetLoader = new ParquetDatasetLoader();
        dataCacheService = new BacktestDataCacheService(cacheService, processor, parquetDatasetLoader, missingRegistry);
    }

    @Test
    @DisplayName("Should serve market rows directly from memory-mapped cache on cache hit")
    void shouldServeFromMemoryMappedCacheOnHit() {
        String symbol = "BTCUSDT";
        String month = "2026-10";
        long tStart = 1790812800L;

        BacktestMarketRow sampleRow = new BacktestMarketRow(
                tStart, tStart + 900, "2026-10-01 00:00:00", "2026-10-01",
                83500.0, 83600.0, "UP",
                83500.0, 83650.0, 83450.0, 83600.0, 10.0, 835500.0, 4.0,
                83510.0, 83610.0, 15.0, 1253000.0, 6.0,
                1.2, 1.2,
                0.50, 0.49, 0.52, 0.51, 0.55, 0.54, 0.58, 0.57,
                0.51, 0.015, 0.49, 0.015,
                0.60, 0.45, 300.0, 300.0
        );

        // Populate cache
        processor.importMarketRows(symbol, month, List.of(sampleRow));

        // Load via service
        List<BacktestMarketRow> loaded = dataCacheService.loadMarketData(symbol, "2026-10-01", "2026-10-01", null);

        assertThat(loaded).hasSize(1);
        BacktestMarketRow r = loaded.get(0);
        assertThat(r.tStart()).isEqualTo(tStart);
        assertThat(r.sOpen()).isEqualTo(83500.0);
        assertThat(r.twapOpen()).isEqualTo(83500.0);
        assertThat(r.twapClose()).isEqualTo(83600.0);
        assertThat(r.actualOutcome()).isEqualTo("UP");
        assertThat(r.basisOpenBps()).isEqualTo(1.2);
    }
}
