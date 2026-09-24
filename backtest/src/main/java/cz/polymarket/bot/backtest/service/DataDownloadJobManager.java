package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.DataType;
import cz.polymarket.bot.backtest.data.DownloadJob;
import cz.polymarket.bot.backtest.data.DownloadJobStatus;
import cz.polymarket.bot.backtest.data.DownloadRequest;
import cz.polymarket.bot.backtest.data.DownloadResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

@ApplicationScoped
public class DataDownloadJobManager {

    private static final Logger LOG = Logger.getLogger(DataDownloadJobManager.class);

    private final BinanceAggTradesDownloader aggTradesDownloader;
    private final BinanceOrderBookDownloader orderBookDownloader;
    private final ExecutorService executorService;
    private final Map<String, DownloadJob> jobs = new ConcurrentHashMap<>();

    @Inject
    public DataDownloadJobManager(
            BinanceAggTradesDownloader aggTradesDownloader,
            BinanceOrderBookDownloader orderBookDownloader,
            ExecutorService executorService
    ) {
        this.aggTradesDownloader = aggTradesDownloader;
        this.orderBookDownloader = orderBookDownloader;
        this.executorService = executorService;
    }

    public DownloadJob submitJob(DownloadRequest request) {
        DownloadJob job = new DownloadJob(request);
        jobs.put(job.jobId(), job);
        executorService.submit(() -> executeJob(job));
        return job;
    }

    public Optional<DownloadJob> getJob(String jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    public List<DownloadJob> getAllJobs() {
        return List.copyOf(jobs.values());
    }

    private void executeJob(DownloadJob job) {
        job.setStatus(DownloadJobStatus.IN_PROGRESS);
        DownloadRequest request = job.request();
        LOG.infof("Starting background download job %s for %s (%s to %s)",
                job.jobId(), request.symbol(), request.startDate(), request.endDate());

        try {
            for (DataType type : request.dataTypes()) {
                switch (type) {
                    case SPOT_TRADES -> {
                        LOG.infof("Job %s: Downloading spot trades for %s", job.jobId(), request.symbol());
                        DownloadResult result = aggTradesDownloader.downloadSpotTrades(
                                request.symbol(), request.startDate(), request.endDate()
                        );
                        job.applyResult(result);
                    }
                    case FUTURES_TRADES -> {
                        LOG.infof("Job %s: Downloading futures trades for %s", job.jobId(), request.symbol());
                        DownloadResult result = aggTradesDownloader.downloadFuturesTrades(
                                request.symbol(), request.startDate(), request.endDate()
                        );
                        job.applyResult(result);
                    }
                    case ORDER_BOOK -> {
                        LOG.infof("Job %s: Downloading futures orderbook for %s", job.jobId(), request.symbol());
                        DownloadResult result = orderBookDownloader.downloadOrderBook(
                                request.symbol(), request.startDate(), request.endDate()
                        );
                        job.applyResult(result);
                    }
                }
            }

            job.setStatus(DownloadJobStatus.COMPLETED);
            LOG.infof("Completed background download job %s: downloaded=%d, skipped=%d, failed=%d",
                    job.jobId(), job.downloadedFiles(), job.skippedFiles(), job.failedFiles());

        } catch (Throwable t) {
            String err = "Job " + job.jobId() + " encountered fatal error: " + t.getMessage();
            LOG.error(err, t);
            job.addError(err);
            job.setStatus(DownloadJobStatus.FAILED);
        }
    }
}
