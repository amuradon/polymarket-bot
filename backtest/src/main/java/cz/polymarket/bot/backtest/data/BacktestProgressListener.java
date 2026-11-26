package cz.polymarket.bot.backtest.data;

/**
 * Callback listener receiving live backtest execution progress and cooperative cancellation signals.
 */
public interface BacktestProgressListener {

    /**
     * No-op listener used when progress tracking or cancellation is not needed.
     */
    BacktestProgressListener NOOP = new BacktestProgressListener() {};

    /**
     * Invoked once all target markets and UTC days have been discovered for the simulation run.
     *
     * @param totalDays number of distinct UTC days to process
     * @param totalMarkets total number of candidate 15-minute markets
     */
    default void onDiscovered(int totalDays, int totalMarkets) {}

    /**
     * Invoked right before processing a UTC day begins.
     *
     * @param dateUtc UTC date string ({@code yyyy-MM-dd})
     */
    default void onDayStarted(String dateUtc) {}

    /**
     * Invoked once all markets for a UTC day have been deterministically reduced.
     *
     * @param completedDays cumulative count of completed UTC days
     * @param completedMarkets cumulative count of processed markets
     * @param dateUtc completed UTC date string ({@code yyyy-MM-dd})
     */
    default void onDayCompleted(int completedDays, int completedMarkets, String dateUtc) {}

    /**
     * Checks whether cancellation has been requested for the active simulation run.
     *
     * @return {@code true} if the simulation should abort cooperatively
     */
    default boolean isCancelled() {
        return false;
    }
}
