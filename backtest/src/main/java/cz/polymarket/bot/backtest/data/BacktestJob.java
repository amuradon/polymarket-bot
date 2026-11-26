package cz.polymarket.bot.backtest.data;

import cz.polymarket.bot.backtest.engine.BacktestResult;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encapsulates the runtime state, progress tracking, cancellation flag, and execution outcome
 * of an asynchronous backtesting job.
 */
public class BacktestJob {

    private final String jobId;
    private volatile String runId;
    private final BacktestRunRequest request;
    private final Instant createdAt;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private volatile String label;
    private volatile boolean mergedIntoExisting;
    private volatile int skippedDays;
    private volatile BacktestJobStatus status;
    private volatile Instant completedAt;
    private volatile BacktestResult result;
    private volatile String error;
    private volatile int totalDays;
    private volatile int completedDays;
    private volatile int totalMarkets;
    private volatile int completedMarkets;
    private volatile String currentDate;
    private volatile long durationMs;
    private volatile Future<?> future;

    /**
     * Creates a new backtest job in {@link BacktestJobStatus#QUEUED} status with random job and run UUIDs.
     *
     * @param request the validated backtest run request
     */
    public BacktestJob(BacktestRunRequest request) {
        this.jobId = UUID.randomUUID().toString();
        this.runId = UUID.randomUUID().toString();
        this.request = Objects.requireNonNull(request, "request must not be null");
        this.createdAt = Instant.now();
        this.status = BacktestJobStatus.QUEUED;
        this.label = (request.label() != null && !request.label().isBlank())
                ? request.label().trim()
                : null;
    }

    /**
     * Returns the unique identifier of the job.
     *
     * @return job identifier string
     */
    public String jobId() {
        return jobId;
    }

    /**
     * Returns the persistent run identifier associated with this job.
     *
     * @return run identifier string
     */
    public String runId() {
        return runId;
    }

    /**
     * Updates the resolved persistent run identifier (e.g., when merging into an existing run).
     *
     * @param runId resolved run identifier
     */
    public void setRunId(String runId) {
        this.runId = runId;
    }

    /**
     * Returns whether this job merged into an existing run with matching configuration.
     *
     * @return {@code true} when merged into an existing run
     */
    public boolean mergedIntoExisting() {
        return mergedIntoExisting;
    }

    /**
     * Sets whether this job merged into an existing run.
     *
     * @param mergedIntoExisting merge flag
     */
    public void setMergedIntoExisting(boolean mergedIntoExisting) {
        this.mergedIntoExisting = mergedIntoExisting;
    }

    /**
     * Returns count of UTC days skipped because they were already present in the target run.
     *
     * @return skipped day count
     */
    public int skippedDays() {
        return skippedDays;
    }

    /**
     * Sets count of skipped UTC days.
     *
     * @param skippedDays skipped day count
     */
    public void setSkippedDays(int skippedDays) {
        this.skippedDays = skippedDays;
    }

    /**
     * Returns the parameters request associated with this job.
     *
     * @return backtest run request
     */
    public BacktestRunRequest request() {
        return request;
    }

    /**
     * Returns the timestamp when the job was initially created and queued.
     *
     * @return creation instant
     */
    public Instant createdAt() {
        return createdAt;
    }

    /**
     * Returns the resolved or user-supplied label for this job.
     *
     * @return label string or null if not yet resolved
     */
    public String label() {
        return label;
    }

    /**
     * Updates the human-readable label for this job.
     *
     * @param label label string
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * Returns the current lifecycle status of the job.
     *
     * @return backtest job status
     */
    public BacktestJobStatus status() {
        return status;
    }

    /**
     * Updates the lifecycle status of the job.
     *
     * @param status new status
     */
    public void setStatus(BacktestJobStatus status) {
        this.status = status;
    }

    /**
     * Returns the timestamp when the job finished execution, or null if still pending.
     *
     * @return completion instant or null
     */
    public Instant completedAt() {
        return completedAt;
    }

    /**
     * Sets the timestamp when the job finished execution.
     *
     * @param completedAt completion instant
     */
    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    /**
     * Returns the backtest simulation result if execution completed successfully, or null.
     *
     * @return backtest result or null
     */
    public BacktestResult result() {
        return result;
    }

    /**
     * Sets the backtest simulation result upon successful completion.
     *
     * @param result backtest result object
     */
    public void setResult(BacktestResult result) {
        this.result = result;
    }

    /**
     * Returns the error message if execution failed, or null.
     *
     * @return error message string or null
     */
    public String error() {
        return error;
    }

    /**
     * Sets the error message when job execution fails.
     *
     * @param error descriptive error message
     */
    public void setError(String error) {
        this.error = error;
    }

    /**
     * Returns total UTC days discovered for this job.
     *
     * @return total day count
     */
    public int totalDays() {
        return totalDays;
    }

    /**
     * Sets total UTC days discovered for this job.
     *
     * @param totalDays total day count
     */
    public void setTotalDays(int totalDays) {
        this.totalDays = totalDays;
    }

    /**
     * Returns count of UTC days completed so far.
     *
     * @return completed day count
     */
    public int completedDays() {
        return completedDays;
    }

    /**
     * Sets count of UTC days completed so far.
     *
     * @param completedDays completed day count
     */
    public void setCompletedDays(int completedDays) {
        this.completedDays = completedDays;
    }

    /**
     * Returns total candidate markets discovered for this job.
     *
     * @return total market count
     */
    public int totalMarkets() {
        return totalMarkets;
    }

    /**
     * Sets total candidate markets discovered for this job.
     *
     * @param totalMarkets total market count
     */
    public void setTotalMarkets(int totalMarkets) {
        this.totalMarkets = totalMarkets;
    }

    /**
     * Returns count of markets processed so far.
     *
     * @return completed market count
     */
    public int completedMarkets() {
        return completedMarkets;
    }

    /**
     * Sets count of markets processed so far.
     *
     * @param completedMarkets completed market count
     */
    public void setCompletedMarkets(int completedMarkets) {
        this.completedMarkets = completedMarkets;
    }

    /**
     * Returns the UTC date currently being processed, or null if not started.
     *
     * @return current UTC date string
     */
    public String currentDate() {
        return currentDate;
    }

    /**
     * Sets the UTC date currently being processed.
     *
     * @param currentDate UTC date string
     */
    public void setCurrentDate(String currentDate) {
        this.currentDate = currentDate;
    }

    /**
     * Returns total execution duration in milliseconds once completed.
     *
     * @return duration in milliseconds
     */
    public long durationMs() {
        return durationMs;
    }

    /**
     * Sets total execution duration in milliseconds.
     *
     * @param durationMs duration in milliseconds
     */
    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    /**
     * Marks this job as cancelled and returns whether cancellation was newly requested.
     *
     * @return {@code true} if cancellation transitioned from false to true
     */
    public boolean markCancelled() {
        return cancelled.compareAndSet(false, true);
    }

    /**
     * Returns whether cancellation has been requested for this job.
     *
     * @return {@code true} if cancelled
     */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Stores the scheduled executor future associated with this job.
     *
     * @param future scheduled future
     */
    public void setFuture(Future<?> future) {
        this.future = future;
    }

    /**
     * Returns the scheduled executor future associated with this job, or null.
     *
     * @return scheduled future or null
     */
    public Future<?> future() {
        return future;
    }

    /**
     * Computes completion percentage in {@code [0.0, 100.0]} based on processed markets.
     *
     * @return progress percentage
     */
    public double progressPct() {
        if (status == BacktestJobStatus.COMPLETED) {
            return 100.0;
        }
        if (totalMarkets <= 0) {
            return 0.0;
        }
        return Math.min(100.0, Math.max(0.0, (completedMarkets * 100.0) / totalMarkets));
    }
}
