package cz.polymarket.bot.backtest.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.TradeDirection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BacktestJsonExporterTest {

    private BacktestJsonExporter exporter;

    @BeforeEach
    void setUp() {
        exporter = new BacktestJsonExporter(new ObjectMapper(), "D:/Crypto/data/Polymarket/backtesting");
    }

    @Test
    @DisplayName("Should export backtest results and individual trades to JSON file in specified directory")
    void shouldExportBacktestResultsToJson(@TempDir Path tempDir) throws IOException {
        TradeRecord trade = new TradeRecord(
                "2026-08-07 00:00:00+00:00",
                1786060800L,
                TradeDirection.UP,
                0.52,
                1.00,
                100.0,
                192.30,
                3.40,
                88.90,
                "Resolution (TWAP 60s)",
                0.66,
                0.52,
                0.14,
                true,
                10088.90
        );

        PerformanceMetrics metrics = new PerformanceMetrics(
                1, 1, 0, 100.0,
                88.90, 3.40, 92.30, 88.90, 0.0,
                Double.POSITIVE_INFINITY, 0.0, 0.0, 88.90,
                10.5, 12.0, 0.12, 10000.0, 10088.90
        );

        BacktestResult result = new BacktestResult(
                "TWAPArbitrageStrategy",
                "D:/Polymarket/btc_nextCandle/unified_market_data.parquet",
                Instant.parse("2026-10-03T10:00:00Z"),
                5100,
                metrics,
                List.of(trade),
                null
        );

        Path exportedFile = exporter.export(result, tempDir.toString());

        assertThat(exportedFile).isNotNull();
        assertThat(Files.exists(exportedFile)).isTrue();
        assertThat(exportedFile.getFileName().toString())
                .startsWith("backtest_TWAPArbitrageStrategy_")
                .endsWith(".json");

        String content = Files.readString(exportedFile);
        assertThat(content).contains("TWAPArbitrageStrategy");
        assertThat(content).contains("88.9");
        assertThat(content).contains("Resolution (TWAP 60s)");

        // Verify latest.json was also created
        Path latestFile = tempDir.resolve("latest.json");
        assertThat(Files.exists(latestFile)).isTrue();
    }
}
