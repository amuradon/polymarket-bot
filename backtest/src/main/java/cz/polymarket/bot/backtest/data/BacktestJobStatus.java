package cz.polymarket.bot.backtest.data;

/**
 * Lifecycle states of an asynchronous backtest simulation job.
 */
public enum BacktestJobStatus {

    /**
     * Job has been submitted and queued for background execution.
     */
    QUEUED,

    /**
     * Job is actively executing against historical market data.
     */
    IN_PROGRESS,

    /**
     * Job has successfully finished and results are available.
     */
    COMPLETED,

    /**
     * Job execution failed with an unrecoverable error.
     */
    FAILED
}
