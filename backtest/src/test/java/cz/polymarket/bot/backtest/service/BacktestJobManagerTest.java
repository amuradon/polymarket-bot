package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.BacktestJob;
import cz.polymarket.bot.backtest.data.BacktestJobStatus;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class BacktestJobManagerTest {

    private BacktestEngine backtestEngine;
    private ExecutorService executorService;
    private BacktestJobManager jobManager;

    @BeforeEach
    void setUp() {
        backtestEngine = Mockito.mock(BacktestEngine.class);
        executorService = Executors.newSingleThreadExecutor();
        jobManager = new BacktestJobManager(backtestEngine, executorService);
    }

    @AfterEach
    void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Should submit job, track execution asynchronously, and reach COMPLETED status")
    void shouldSubmitJobAndComplete() {
        BacktestRunRequest request = new BacktestRunRequest(
                "TWAPArbitrageStrategy",
                "BTCUSDT",
                "2026-08-01",
                "2026-08-05",
                10000.0,
                "D:/Crypto/data/Polymarket/backtesting"
        );

        BacktestResult mockResult = new BacktestResult(
                "TWAPArbitrageStrategy",
                "BTCUSDT",
                Instant.now(),
                100,
                PerformanceMetrics.empty(10000.0),
                List.of(),
                "D:/Crypto/data/Polymarket/backtesting/result.json"
        );

        when(backtestEngine.runBacktest(
                eq("TWAPArbitrageStrategy"),
                eq("BTCUSDT"),
                eq("2026-08-01"),
                eq("2026-08-05"),
                eq(10000.0),
                eq("D:/Crypto/data/Polymarket/backtesting")
        )).thenReturn(mockResult);

        BacktestJob job = jobManager.submitJob(request);

        assertThat(job).isNotNull();
        assertThat(job.jobId()).isNotBlank();
        assertThat(job.request()).isEqualTo(request);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Optional<BacktestJob> queried = jobManager.getJob(job.jobId());
            assertThat(queried).isPresent();
            assertThat(queried.get().status()).isEqualTo(BacktestJobStatus.COMPLETED);
            assertThat(queried.get().result()).isNotNull();
            assertThat(queried.get().result().jsonFilePath()).isEqualTo("D:/Crypto/data/Polymarket/backtesting/result.json");
            assertThat(queried.get().completedAt()).isNotNull();
            assertThat(queried.get().error()).isNull();
        });
    }

    @Test
    @DisplayName("Should mark job as FAILED when BacktestEngine throws exception")
    void shouldHandleJobFailure() {
        BacktestRunRequest request = new BacktestRunRequest(
                "TWAPArbitrageStrategy",
                "BTCUSDT",
                "2026-08-01",
                "2026-08-05",
                10000.0,
                null
        );

        when(backtestEngine.runBacktest(any(), any(), any(), any(), any(), any()))
                .thenThrow(new IllegalArgumentException("No market data rows found for symbol BTCUSDT"));

        BacktestJob job = jobManager.submitJob(request);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Optional<BacktestJob> queried = jobManager.getJob(job.jobId());
            assertThat(queried).isPresent();
            assertThat(queried.get().status()).isEqualTo(BacktestJobStatus.FAILED);
            assertThat(queried.get().error()).contains("No market data rows found for symbol BTCUSDT");
            assertThat(queried.get().completedAt()).isNotNull();
            assertThat(queried.get().result()).isNull();
        });
    }

    @Test
    @DisplayName("Should return empty optional for non-existent job ID")
    void shouldReturnEmptyForUnknownJob() {
        Optional<BacktestJob> queried = jobManager.getJob("non-existent-uuid");
        assertThat(queried).isEmpty();
    }
}
