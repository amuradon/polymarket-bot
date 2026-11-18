package cz.polymarket.bot.storage;

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

/**
 * Unit tests for {@link PaperTradeRepository} verifying JSONL append durability, metrics snapshots, and restart replay.
 */
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
    void shouldAppendTradesPersistSnapshotsAndSurviveRestart() throws IOException {
        PaperTradeRepository repository1 = new PaperTradeRepository(
                tempDir.toString(),
                10000.0,
                objectMapper,
                metricsCalculator
        );
        assertThat(repository1.getTrades()).isEmpty();
        assertThat(repository1.getCurrentBalance()).isEqualTo(10000.0);

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

        List<String> lines = Files.readAllLines(tempDir.resolve("trades.jsonl"));
        assertThat(lines).hasSize(2);
        assertThat(Files.exists(tempDir.resolve("metrics.json"))).isTrue();

        PaperTradeRepository repository2 = new PaperTradeRepository(
                tempDir.toString(),
                10000.0,
                objectMapper,
                metricsCalculator
        );
        assertThat(repository2.getTrades()).hasSize(2);
        assertThat(repository2.getCurrentBalance()).isEqualTo(9530.0);
        assertThat(repository2.getMetrics().winRatePct()).isEqualTo(50.0);
    }
}
