package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.BacktestJob;
import cz.polymarket.bot.backtest.data.BacktestJobStatus;
import cz.polymarket.bot.backtest.data.BacktestProgressListener;
import cz.polymarket.bot.backtest.data.BacktestRunConfig;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.engine.BacktestCancelledException;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    @DisplayName("Should submit job, track progress asynchronously, and reach COMPLETED status")
    void shouldSubmitJobAndComplete() {
        BacktestRunRequest request = new BacktestRunRequest(
                "Utter Test",
                "UtterParameciumStrategy",
                "BTCUSDT",
                "2026-08-01",
                "2026-08-05",
                false,
                10000.0,
                1.5,
                100L,
                50L,
                50L,
                null,
                false,
                false
        );

        when(backtestEngine.runBacktest(any(), eq(request), any())).thenAnswer(inv -> {
            String runId = inv.getArgument(0);
            BacktestProgressListener listener = inv.getArgument(2);
            listener.onDiscovered(2, 10);
            listener.onDayStarted("2026-08-01");
            listener.onDayCompleted(1, 5, "2026-08-01");
            listener.onDayCompleted(2, 10, "2026-08-05");
            return new BacktestResult(
                    runId,
                    "Utter Test",
                    "UtterParameciumStrategy",
                    "BTCUSDT",
                    Instant.now(),
                    42L,
                    10,
                    new BacktestRunConfig("UtterParameciumStrategy", "BTCUSDT", "2026-08-01", "2026-08-05",
                            10000.0, 1.5, 100L, 50L, 50L, Map.of(), Map.of()),
                    PerformanceMetrics.empty(10000.0),
                    List.of(),
                    List.of(),
                    false,
                    0
            );
        });

        BacktestJob job = jobManager.submitJob(request);

        assertThat(job).isNotNull();
        assertThat(job.jobId()).isNotBlank();
        assertThat(job.runId()).isNotBlank();
        assertThat(job.request()).isEqualTo(request);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Optional<BacktestJob> queried = jobManager.getJob(job.jobId());
            assertThat(queried).isPresent();
            assertThat(queried.get().status()).isEqualTo(BacktestJobStatus.COMPLETED);
            assertThat(queried.get().totalDays()).isEqualTo(2);
            assertThat(queried.get().completedDays()).isEqualTo(2);
            assertThat(queried.get().totalMarkets()).isEqualTo(10);
            assertThat(queried.get().completedMarkets()).isEqualTo(10);
            assertThat(queried.get().progressPct()).isEqualTo(100.0);
            assertThat(queried.get().result()).isNotNull();
            assertThat(queried.get().completedAt()).isNotNull();
            assertThat(queried.get().error()).isNull();
        });
        assertThat(jobManager.listJobs()).hasSize(1);
    }

    @Test
    @DisplayName("Should mark job as FAILED when BacktestEngine throws exception")
    void shouldHandleJobFailure() {
        BacktestRunRequest request = new BacktestRunRequest(
                null, "UtterParameciumStrategy", "BTCUSDT", "2026-08-01", "2026-08-05",
                false, 10000.0, 1.0, null, null, null, null, false, false
        );

        when(backtestEngine.runBacktest(any(), any(), any()))
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
    @DisplayName("Should cancel queued and active jobs cleanly")
    void shouldCancelQueuedAndActiveJobs() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);

        BacktestRunRequest req1 = new BacktestRunRequest(
                "First", "UtterParameciumStrategy", "BTCUSDT", "2026-08-01", "2026-08-01",
                false, 10000.0, 1.0, null, null, null, null, false, false);
        BacktestRunRequest req2 = new BacktestRunRequest(
                "Queued Second", "UtterParameciumStrategy", "BTCUSDT", "2026-08-01", "2026-08-01",
                false, 10000.0, 1.0, null, null, null, null, false, false);

        when(backtestEngine.runBacktest(any(), eq(req1), any())).thenAnswer(inv -> {
            BacktestProgressListener listener = inv.getArgument(2);
            firstStarted.countDown();
            releaseFirst.await(5, TimeUnit.SECONDS);
            if (listener.isCancelled()) {
                throw new BacktestCancelledException("Cancelled");
            }
            return null;
        });

        BacktestJob job1 = jobManager.submitJob(req1);
        assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
        BacktestJob job2 = jobManager.submitJob(req2);

        // Cancel queued job2 before job1 releases
        Optional<BacktestJob> cancelled2 = jobManager.cancelJob(job2.jobId());
        assertThat(cancelled2).isPresent();
        assertThat(cancelled2.get().status()).isEqualTo(BacktestJobStatus.CANCELLED);

        // Request cooperative cancellation of active job1
        jobManager.cancelJob(job1.jobId());
        releaseFirst.countDown();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(jobManager.getJob(job1.jobId()).orElseThrow().status()).isEqualTo(BacktestJobStatus.CANCELLED));
    }

    @Test
    @DisplayName("Should return empty optional for non-existent job ID")
    void shouldReturnEmptyForUnknownJob() {
        Optional<BacktestJob> queried = jobManager.getJob("non-existent-uuid");
        assertThat(queried).isEmpty();
        assertThat(jobManager.cancelJob("non-existent-uuid")).isEmpty();
    }
}
