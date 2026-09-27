package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.DataType;
import cz.polymarket.bot.backtest.data.DownloadJob;
import cz.polymarket.bot.backtest.data.DownloadJobStatus;
import cz.polymarket.bot.backtest.data.DownloadRequest;
import cz.polymarket.bot.backtest.data.DownloadResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class DataDownloadJobManagerTest {

    private BinanceAggTradesDownloader aggTradesDownloader;
    private BinanceOrderBookDownloader orderBookDownloader;
    private ServerHaltTracker serverHaltTracker;
    private DataDownloadJobManager jobManager;

    @BeforeEach
    void setUp() {
        aggTradesDownloader = Mockito.mock(BinanceAggTradesDownloader.class);
        orderBookDownloader = Mockito.mock(BinanceOrderBookDownloader.class);
        serverHaltTracker = new ServerHaltTracker();
        jobManager = new DataDownloadJobManager(
                aggTradesDownloader,
                orderBookDownloader,
                Executors.newSingleThreadExecutor(),
                serverHaltTracker
        );
    }

    @Test
    void shouldSubmitAndCompleteJobForAllDataTypes() {
        String symbol = "BTCUSDT";
        LocalDate start = LocalDate.of(2026, 8, 1);
        LocalDate end = LocalDate.of(2026, 8, 2);

        when(aggTradesDownloader.downloadSpotTrades(eq(symbol), eq(start), eq(end)))
                .thenReturn(new DownloadResult(2, 0, 0, List.of()));
        when(aggTradesDownloader.downloadFuturesTrades(eq(symbol), eq(start), eq(end)))
                .thenReturn(new DownloadResult(2, 0, 0, List.of()));
        when(orderBookDownloader.downloadOrderBook(eq(symbol), eq(start), eq(end)))
                .thenReturn(new DownloadResult(48, 0, 0, List.of()));

        DownloadRequest request = DownloadRequest.of("BTCUSDT", "2026-08-01", "2026-08-02", null);
        DownloadJob job = jobManager.submitJob(request);

        assertThat(job).isNotNull();
        assertThat(job.jobId()).isNotBlank();

        await().untilAsserted(() -> assertThat(job.status()).isEqualTo(DownloadJobStatus.COMPLETED));

        assertThat(job.downloadedFiles()).isEqualTo(52); // 2 + 2 + 48
        assertThat(job.skippedFiles()).isEqualTo(0);
        assertThat(job.failedFiles()).isEqualTo(0);
        assertThat(job.completedAt()).isNotNull();

        assertThat(jobManager.getJob(job.jobId())).contains(job);
    }

    @Test
    void shouldContinueWithOtherServerWhenOneServerHaltedOnHttp429() {
        String symbol = "BTCUSDT";
        LocalDate start = LocalDate.of(2026, 8, 1);
        LocalDate end = LocalDate.of(2026, 8, 1);

        // OrderBook server returns 429: 1 failed, 23 skipped
        when(orderBookDownloader.downloadOrderBook(eq(symbol), eq(start), eq(end)))
                .thenReturn(new DownloadResult(0, 23, 1, List.of("HTTP 429 Too Many Requests: Rate limit exceeded")));

        // Spot trades from other server (Binance vision) succeeds: 1 downloaded
        when(aggTradesDownloader.downloadSpotTrades(eq(symbol), eq(start), eq(end)))
                .thenReturn(new DownloadResult(1, 0, 0, List.of()));

        DownloadRequest request = DownloadRequest.of(
                "BTCUSDT",
                "2026-08-01",
                "2026-08-01",
                Set.of(DataType.SPOT_TRADES, DataType.ORDER_BOOK)
        );
        DownloadJob job = jobManager.submitJob(request);

        await().untilAsserted(() -> assertThat(job.status()).isEqualTo(DownloadJobStatus.COMPLETED));

        // Both servers executed: orderbook was rate-limited and skipped, spot trades succeeded
        assertThat(job.downloadedFiles()).isEqualTo(1);
        assertThat(job.skippedFiles()).isEqualTo(23);
        assertThat(job.failedFiles()).isEqualTo(1);
        assertThat(job.errors()).hasSize(1);
        assertThat(job.errors().get(0)).contains("HTTP 429 Too Many Requests");
    }

    @Test
    void shouldHandleFailuresGracefully() {
        String symbol = "BTCUSDT";
        LocalDate date = LocalDate.of(2026, 8, 1);

        when(aggTradesDownloader.downloadSpotTrades(any(), any(), any()))
                .thenThrow(new RuntimeException("Simulated network outage"));

        DownloadRequest request = DownloadRequest.of(
                "BTCUSDT",
                "2026-08-01",
                "2026-08-01",
                Set.of(DataType.SPOT_TRADES)
        );
        DownloadJob job = jobManager.submitJob(request);

        await().untilAsserted(() -> assertThat(job.status()).isEqualTo(DownloadJobStatus.FAILED));
        assertThat(job.errors()).isNotEmpty();
        assertThat(job.errors().get(0)).contains("Simulated network outage");
    }
}
