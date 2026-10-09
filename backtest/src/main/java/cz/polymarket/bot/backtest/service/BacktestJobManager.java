package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.BacktestJob;
import cz.polymarket.bot.backtest.data.BacktestJobStatus;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Service orchestrating asynchronous execution and lifecycle tracking of backtest simulation jobs.
 */
@ApplicationScoped
public class BacktestJobManager {

    private static final Logger LOG = Logger.getLogger(BacktestJobManager.class);

    private final BacktestEngine backtestEngine;
    private final ExecutorService executorService;
    private final Map<String, BacktestJob> jobs = new ConcurrentHashMap<>();

    /**
     * Constructs a new {@link BacktestJobManager} with injected execution dependencies.
     *
     * @param backtestEngine backtest simulation engine
     * @param executorService managed executor service for background thread execution
     */
    @Inject
    public BacktestJobManager(BacktestEngine backtestEngine, ExecutorService executorService) {
        this.backtestEngine = Objects.requireNonNull(backtestEngine, "backtestEngine must not be null");
        this.executorService = Objects.requireNonNull(executorService, "executorService must not be null");
    }

    /**
     * Submits a backtest run request for background execution and registers the job.
     *
     * @param request parameter payload for backtest execution
     * @return initialized backtest job in QUEUED status
     */
    public BacktestJob submitJob(BacktestRunRequest request) {
        BacktestJob job = new BacktestJob(request);
        jobs.put(job.jobId(), job);
        executorService.submit(() -> executeJob(job));
        return job;
    }

    /**
     * Retrieves a backtest job by its identifier.
     *
     * @param jobId UUID string identifying the job
     * @return optional containing the job if found, or empty if absent
     */
    public Optional<BacktestJob> getJob(String jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    /**
     * Returns an unmodifiable list of all registered backtest jobs.
     *
     * @return list of backtest jobs
     */
    public List<BacktestJob> getAllJobs() {
        return List.copyOf(jobs.values());
    }

    private void executeJob(BacktestJob job) {
        job.setStatus(BacktestJobStatus.IN_PROGRESS);
        BacktestRunRequest request = job.request();
        LOG.infof("Starting background backtest job %s for strategy %s on %s",
                job.jobId(), request.strategyName(), request.symbol());

        try {
            BacktestResult result = backtestEngine.runBacktest(
                    request.strategyName(),
                    request.symbol(),
                    request.startDate(),
                    request.endDate(),
                    request.initialCapital(),
                    request.outputDirectory()
            );
            job.setResult(result);
            job.setCompletedAt(Instant.now());
            job.setStatus(BacktestJobStatus.COMPLETED);
            LOG.infof("Completed background backtest job %s. Result JSON: %s",
                    job.jobId(), result.jsonFilePath());
        } catch (Throwable t) {
            String err = "Backtest job " + job.jobId() + " failed: " + t.getMessage();
            LOG.error(err, t);
            job.setError(t.getMessage() != null ? t.getMessage() : t.toString());
            job.setCompletedAt(Instant.now());
            job.setStatus(BacktestJobStatus.FAILED);
        }
    }
}
