package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.BacktestJob;
import cz.polymarket.bot.backtest.data.BacktestJobStatus;
import cz.polymarket.bot.backtest.data.BacktestProgressListener;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.engine.BacktestCancelledException;
import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import io.quarkus.logging.Log;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Service orchestrating sequential queued execution, live progress tracking, and cooperative cancellation
 * of asynchronous backtest simulation jobs.
 */
@ApplicationScoped
public class BacktestJobManager {

    private final BacktestEngine backtestEngine;
    private final ExecutorService queueExecutor;
    private final boolean ownsExecutor;
    private final Map<String, BacktestJob> jobs = new ConcurrentHashMap<>();

    /**
     * CDI constructor creating a dedicated sequential background queue executor.
     *
     * @param backtestEngine backtest simulation engine
     */
    @Inject
    public BacktestJobManager(BacktestEngine backtestEngine) {
        this(backtestEngine, Executors.newSingleThreadExecutor(runnable -> {
            Thread t = new Thread(runnable, "backtest-job-queue");
            t.setDaemon(true);
            return t;
        }), true);
    }

    /**
     * Test-friendly constructor accepting an externally managed executor service.
     *
     * @param backtestEngine backtest simulation engine
     * @param queueExecutor executor service running queued jobs
     */
    public BacktestJobManager(BacktestEngine backtestEngine, ExecutorService queueExecutor) {
        this(backtestEngine, queueExecutor, false);
    }

    private BacktestJobManager(BacktestEngine backtestEngine, ExecutorService queueExecutor, boolean ownsExecutor) {
        if (backtestEngine == null) {
            Log.fatal("Fatal initialization error in BacktestJobManager: backtestEngine must not be null");
            throw new NullPointerException("backtestEngine must not be null");
        }
        if (queueExecutor == null) {
            Log.fatal("Fatal initialization error in BacktestJobManager: queueExecutor must not be null");
            throw new NullPointerException("queueExecutor must not be null");
        }
        this.backtestEngine = backtestEngine;
        this.queueExecutor = queueExecutor;
        this.ownsExecutor = ownsExecutor;
    }

    /**
     * Shuts down the dedicated sequential job queue executor if owned by this bean.
     */
    @PreDestroy
    public void shutdown() {
        if (ownsExecutor) {
            queueExecutor.shutdownNow();
        }
    }

    /**
     * Submits a backtest run request onto the sequential execution queue and registers the job.
     *
     * @param request parameter payload for backtest execution
     * @return initialized backtest job in {@link BacktestJobStatus#QUEUED} status
     */
    public BacktestJob submitJob(BacktestRunRequest request) {
        BacktestJob job = new BacktestJob(request);
        jobs.put(job.jobId(), job);
        if (Log.isInfoEnabled()) {
            Log.infof("Queued background backtest job %s (runId=%s) for strategy=%s, symbol=%s",
                    job.jobId(), job.runId(), request.strategyName(), request.symbol());
        }
        Future<?> future = queueExecutor.submit(() -> executeJob(job));
        job.setFuture(future);
        return job;
    }

    /**
     * Retrieves a backtest job by its identifier.
     *
     * @param jobId UUID string identifying the job
     * @return optional containing the job if found, or empty if absent
     */
    public Optional<BacktestJob> getJob(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(jobs.get(jobId.trim()));
    }

    /**
     * Lists all tracked jobs ordered newest first.
     *
     * @return list of {@link BacktestJob}
     */
    public List<BacktestJob> listJobs() {
        List<BacktestJob> list = new ArrayList<>(jobs.values());
        list.sort(Comparator.comparing(BacktestJob::createdAt).reversed());
        return List.copyOf(list);
    }

    /**
     * Requests cancellation of a queued or active backtest job.
     *
     * @param jobId UUID string identifying the job
     * @return optional updated {@link BacktestJob}, or empty if not found
     */
    public Optional<BacktestJob> cancelJob(String jobId) {
        BacktestJob job = jobs.get(jobId != null ? jobId.trim() : "");
        if (job == null) {
            return Optional.empty();
        }
        BacktestJobStatus status = job.status();
        if (status == BacktestJobStatus.COMPLETED
                || status == BacktestJobStatus.FAILED
                || status == BacktestJobStatus.CANCELLED) {
            return Optional.of(job);
        }

        job.markCancelled();
        Future<?> future = job.future();
        if (status == BacktestJobStatus.QUEUED && future != null && future.cancel(false)) {
            job.setStatus(BacktestJobStatus.CANCELLED);
            job.setCompletedAt(Instant.now());
            if (Log.isInfoEnabled()) {
                Log.infof("Cancelled queued backtest job %s", job.jobId());
            }
        } else {
            if (Log.isInfoEnabled()) {
                Log.infof("Requested cooperative cancellation for active backtest job %s", job.jobId());
            }
        }
        return Optional.of(job);
    }

    private void executeJob(BacktestJob job) {
        if (job.isCancelled()) {
            job.setStatus(BacktestJobStatus.CANCELLED);
            job.setCompletedAt(Instant.now());
            return;
        }
        job.setStatus(BacktestJobStatus.IN_PROGRESS);
        BacktestRunRequest request = job.request();
        if (Log.isInfoEnabled()) {
            Log.infof("Starting background backtest job %s for strategy %s on %s",
                    job.jobId(), request.strategyName(), request.symbol());
        }

        BacktestProgressListener progressListener = new BacktestProgressListener() {
            @Override
            public void onDiscovered(int totalDays, int totalMarkets) {
                job.setTotalDays(totalDays);
                job.setTotalMarkets(totalMarkets);
            }

            @Override
            public void onDayStarted(String dateUtc) {
                job.setCurrentDate(dateUtc);
            }

            @Override
            public void onDayCompleted(int completedDays, int completedMarkets, String dateUtc) {
                job.setCompletedDays(completedDays);
                job.setCompletedMarkets(completedMarkets);
                job.setCurrentDate(dateUtc);
            }

            @Override
            public boolean isCancelled() {
                return job.isCancelled();
            }
        };

        try {
            BacktestResult result = backtestEngine.runBacktest(job.runId(), request, progressListener);
            job.setResult(result);
            job.setLabel(result.label());
            job.setDurationMs(result.durationMs());
            job.setCompletedAt(Instant.now());
            job.setStatus(BacktestJobStatus.COMPLETED);
            if (Log.isInfoEnabled()) {
                Log.infof("Completed background backtest job %s (runId=%s): totalMarkets=%d, trades=%d",
                        job.jobId(),
                        result.runId(),
                        result.totalMarkets(),
                        result.trades() != null ? result.trades().size() : 0);
            }
        } catch (BacktestCancelledException cancelledEx) {
            job.setCompletedAt(Instant.now());
            job.setStatus(BacktestJobStatus.CANCELLED);
            if (Log.isInfoEnabled()) {
                Log.infof("Backtest job %s cancelled: %s", job.jobId(), cancelledEx.getMessage());
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
