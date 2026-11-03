package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.BacktestJob;
import cz.polymarket.bot.backtest.data.BacktestJobStatus;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Service orchestrating asynchronous execution and lifecycle tracking of backtest simulation jobs.
 */
@ApplicationScoped
public class BacktestJobManager {

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
        if (backtestEngine == null) {
            Log.fatal("Fatal initialization error in BacktestJobManager: backtestEngine must not be null");
            throw new NullPointerException("backtestEngine must not be null");
        }
        if (executorService == null) {
            Log.fatal("Fatal initialization error in BacktestJobManager: executorService must not be null");
            throw new NullPointerException("executorService must not be null");
        }
        this.backtestEngine = backtestEngine;
        this.executorService = executorService;
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
        if (Log.isInfoEnabled()) {
            Log.infof("Queued background backtest job %s for strategy=%s, symbol=%s",
                    job.jobId(), request.strategyName(), request.symbol());
        }
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

    private void executeJob(BacktestJob job) {
        job.setStatus(BacktestJobStatus.IN_PROGRESS);
        BacktestRunRequest request = job.request();
        if (Log.isInfoEnabled()) {
            Log.infof("Starting background backtest job %s for strategy %s on %s",
                    job.jobId(), request.strategyName(), request.symbol());
        }

        try {
            BacktestResult result = backtestEngine.runBacktest(
                    request.strategyName(),
                    request.symbol(),
                    request.startDate(),
                    request.endDate(),
                    request.initialCapital(),
                    request.outputDirectory(),
                    request.latencyFactor()
            );
            job.setResult(result);
            job.setCompletedAt(Instant.now());
            job.setStatus(BacktestJobStatus.COMPLETED);
            if (Log.isInfoEnabled()) {
                Log.infof("Completed background backtest job %s: totalMarkets=%d, trades=%d, resultJson=%s",
                        job.jobId(),
                        result.totalMarkets(),
                        result.trades() != null ? result.trades().size() : 0,
                        result.jsonFilePath());
            }
        } catch (Error fatalErr) {
            Log.fatalf(fatalErr, "Fatal error in backtesting engine during job %s: %s",
                    job.jobId(), fatalErr.getMessage());
            job.setError(fatalErr.getMessage() != null ? fatalErr.getMessage() : fatalErr.toString());
            job.setCompletedAt(Instant.now());
            job.setStatus(BacktestJobStatus.FAILED);
        } catch (Throwable t) {
            Log.errorf(t, "Backtest job %s failed: %s", job.jobId(), t.getMessage());
            job.setError(t.getMessage() != null ? t.getMessage() : t.toString());
            job.setCompletedAt(Instant.now());
            job.setStatus(BacktestJobStatus.FAILED);
        }
    }
}
