package cz.polymarket.bot.paper.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.TradeDirection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PaperTradeRepositoryTest {

    @TempDir
    Path tempDir;

    private ObjectMapper objectMapper;
    private PerformanceMetricsCalculator metricsCalculator;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        metricsCalculator = new PerformanceMetricsCalculator();
    }

    @Test
    void shouldInitializeEmptyWhenNoExistingFiles() {
        PaperTradeRepository repository = new PaperTradeRepository(
                tempDir.toString(),
                10000.0,
                objectMapper,
                metricsCalculator
        );

        assertThat(repository.getTrades()).isEmpty();
        assertThat(repository.getCurrentBalance()).isEqualTo(10000.0);
        assertThat(repository.getInitialCapital()).isEqualTo(10000.0);
        assertThat(repository.getMetrics().totalTrades()).isEqualTo(0);
    }

    @Test
    void shouldAppendTradesAndPersistToJsonLinesAndMetricsSnapshot() throws IOException {
        PaperTradeRepository repository = new PaperTradeRepository(
                tempDir.toString(),
                10000.0,
                objectMapper,
                metricsCalculator
        );

        TradeRecord trade1 = new TradeRecord(
                "2026-10-03T12:00:00Z",
                1791038400L,
                TradeDirection.UP,
                0.55,
                0.70,
                550.0,
                1000.0,
                10.0,
                140.0,
                "Take Profit (0.70)",
                0.75,
                0.55,
                0.20,
                true,
                10140.0
        );

        repository.recordTrade(trade1);

        assertThat(repository.getTrades()).hasSize(1);
        assertThat(repository.getCurrentBalance()).isEqualTo(10140.0);
        assertThat(repository.getMetrics().totalTrades()).isEqualTo(1);
        assertThat(repository.getMetrics().winningTrades()).isEqualTo(1);
        assertThat(repository.getMetrics().winRatePct()).isEqualTo(100.0);
        assertThat(repository.getMetrics().totalNetPnl()).isEqualTo(140.0);

        Path jsonlPath = tempDir.resolve("trades.jsonl");
        assertThat(Files.exists(jsonlPath)).isTrue();
        List<String> lines = Files.readAllLines(jsonlPath);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).contains("2026-10-03T12:00:00Z").contains("Take Profit (0.70)");

        Path metricsPath = tempDir.resolve("metrics.json");
        assertThat(Files.exists(metricsPath)).isTrue();
        assertThat(Files.readString(metricsPath)).contains("\"totalTrades\":1").contains("\"winRatePct\":100.0");
    }

    @Test
    void shouldSurviveRestartAndReplayHistoricalTrades() {
        PaperTradeRepository repository1 = new PaperTradeRepository(
                tempDir.toString(),
                10000.0,
                objectMapper,
                metricsCalculator
        );

        TradeRecord trade1 = new TradeRecord(
                "2026-10-03T12:00:00Z",
                1791038400L,
                TradeDirection.UP,
                0.55,
                0.70,
                550.0,
                1000.0,
                10.0,
                140.0,
                "Take Profit (0.70)",
                0.75,
                0.55,
                0.20,
                true,
                10140.0
        );

        TradeRecord trade2 = new TradeRecord(
                "2026-10-03T12:15:00Z",
                1791039300L,
                TradeDirection.DOWN,
                0.60,
                0.00,
                600.0,
                1000.0,
                10.0,
                -610.0,
                "Resolution (TWAP 60s)",
                0.70,
                0.60,
                0.10,
                false,
                9530.0
        );

        repository1.recordTrade(trade1);
        repository1.recordTrade(trade2);

        assertThat(repository1.getTrades()).hasSize(2);
        assertThat(repository1.getCurrentBalance()).isEqualTo(9530.0);

        // Simulate crash / restart / upgrade with a new repository instance pointing to the same data dir
        PaperTradeRepository repository2 = new PaperTradeRepository(
                tempDir.toString(),
                10000.0,
                objectMapper,
                metricsCalculator
        );

        assertThat(repository2.getTrades()).hasSize(2);
        assertThat(repository2.getCurrentBalance()).isEqualTo(9530.0);
        assertThat(repository2.getMetrics().totalTrades()).isEqualTo(2);
        assertThat(repository2.getMetrics().winningTrades()).isEqualTo(1);
        assertThat(repository2.getMetrics().losingTrades()).isEqualTo(1);
        assertThat(repository2.getMetrics().winRatePct()).isEqualTo(50.0);
        assertThat(repository2.getMetrics().totalNetPnl()).isEqualTo(-470.0);
    }
}
