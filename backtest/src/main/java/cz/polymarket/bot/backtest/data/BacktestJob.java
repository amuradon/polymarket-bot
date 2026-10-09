package cz.polymarket.bot.backtest.data;

import cz.polymarket.bot.backtest.engine.BacktestResult;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates the runtime state and execution outcome of an asynchronous backtesting job.
 */
public class BacktestJob {

    private final String jobId;
    private final BacktestRunRequest request;
    private final Instant createdAt;
    private volatile BacktestJobStatus status;
    private volatile Instant completedAt;
    private volatile BacktestResult result;
    private volatile String error;

    /**
     * Creates a new backtest job in QUEUED status with a random UUID.
     *
     * @param request the validated backtest run request
     */
    public BacktestJob(BacktestRunRequest request) {
        this(UUID.randomUUID().toString(), request);
    }

    /**
     * Creates a new backtest job with an explicit identifier.
     *
     * @param jobId unique job identifier
     * @param request the validated backtest run request
     */
    public BacktestJob(String jobId, BacktestRunRequest request) {
        this.jobId = Objects.requireNonNull(jobId, "jobId must not be null");
        this.request = Objects.requireNonNull(request, "request must not be null");
        this.createdAt = Instant.now();
        this.status = BacktestJobStatus.QUEUED;
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
}
