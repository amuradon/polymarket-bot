package cz.polymarket.bot.live.storage;

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

class LiveTradeRepositoryTest {

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
    void shouldInitializeEmptyAndSyncAccountBalanceFromAccount() {
        LiveTradeRepository repository = new LiveTradeRepository(
                tempDir.toString(),
                objectMapper,
                metricsCalculator
        );

        assertThat(repository.getTrades()).isEmpty();
        assertThat(repository.getCurrentBalance()).isEqualTo(0.0);
        assertThat(repository.getInitialCapital()).isEqualTo(0.0);
        assertThat(repository.getMetrics().totalTrades()).isEqualTo(0);

        repository.syncAccountBalance(2328.0);
        assertThat(repository.getCurrentBalance()).isEqualTo(2328.0);
        assertThat(repository.getInitialCapital()).isEqualTo(2328.0);
        assertThat(repository.getMetrics().initialCapital()).isEqualTo(2328.0);
    }

    @Test
    void shouldAppendTradesAndPersistToJsonLinesAndMetricsSnapshot() throws IOException {
        LiveTradeRepository repository = new LiveTradeRepository(
                tempDir.toString(),
                objectMapper,
                metricsCalculator
        );
        repository.syncAccountBalance(1000.0);

        TradeRecord trade1 = new TradeRecord(
                "2026-11-04T12:00:00Z",
                1793793600L,
                TradeDirection.UP,
                0.55,
                1.00,
                55.0,
                100.0,
                0.0,
                45.0,
                "Resolution (TWAP 60s)",
                0.68,
                0.55,
                0.13,
                true,
                1045.0
        );

        repository.recordTrade(trade1);

        assertThat(repository.getTrades()).hasSize(1);
        assertThat(repository.getCurrentBalance()).isEqualTo(1045.0);
        assertThat(repository.getMetrics().totalTrades()).isEqualTo(1);
        assertThat(repository.getMetrics().winningTrades()).isEqualTo(1);
        assertThat(repository.getMetrics().winRatePct()).isEqualTo(100.0);
        assertThat(repository.getMetrics().totalNetPnl()).isEqualTo(45.0);

        Path jsonlPath = tempDir.resolve("trades.jsonl");
        assertThat(Files.exists(jsonlPath)).isTrue();
        List<String> lines = Files.readAllLines(jsonlPath);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).contains("2026-11-04T12:00:00Z").contains("Resolution (TWAP 60s)");

        Path metricsPath = tempDir.resolve("metrics.json");
        assertThat(Files.exists(metricsPath)).isTrue();
        assertThat(Files.readString(metricsPath)).contains("\"totalTrades\":1").contains("\"winRatePct\":100.0");
    }

    @Test
    void shouldDebitOpenPositionCostAndRecoverFromStaleNegativeBalanceOnAccountSync() {
        LiveTradeRepository repository1 = new LiveTradeRepository(
                tempDir.toString(),
                objectMapper,
                metricsCalculator
        );
        repository1.syncAccountBalance(1000.0);
        repository1.debitOpenPositionCost(56.73);
        assertThat(repository1.getCurrentBalance()).isEqualTo(943.27);

        TradeRecord trade1 = new TradeRecord(
                "2026-11-04T12:00:00Z",
                1793793600L,
                TradeDirection.UP,
                0.50,
                0.00,
                1042.92,
                2085.84,
                0.0,
                -1042.92,
                "Resolution (TWAP 60s)",
                0.65,
                0.50,
                0.15,
                false,
                -42.92
        );
        repository1.recordTrade(trade1);

        LiveTradeRepository repository2 = new LiveTradeRepository(
                tempDir.toString(),
                objectMapper,
                metricsCalculator
        );

        assertThat(repository2.getTrades()).hasSize(1);
        assertThat(repository2.getCurrentBalance()).isEqualTo(-42.92);

        // Syncing from real Polymarket account collateral immediately restores the true positive balance
        repository2.syncAccountBalance(2328.0);
        assertThat(repository2.getCurrentBalance()).isEqualTo(2328.0);

        repository2.reset();
        assertThat(repository2.getTrades()).isEmpty();
        assertThat(repository2.getCurrentBalance()).isEqualTo(2328.0);
        assertThat(repository2.getInitialCapital()).isEqualTo(2328.0);
    }
}
