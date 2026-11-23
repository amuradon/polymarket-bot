package cz.polymarket.bot.backtest.indicator;

import cz.polymarket.bot.backtest.stream.BacktestEvent;

/**
 * Read-only columnar holder for a single UTC day's precomputed indicator values.
 * <p>
 * Stores primitive timestamp and value arrays in strictly ascending chronological order
 * so parallel 15-minute market workers can locate their {@code [tStartMs, tEndMs]} slice
 * in {@code O(log N)} via binary search without per-candle database queries.
 */
public interface DailyIndicatorTrack {

    /**
     * Returns the canonical name of the indicator that produced this track.
     *
     * @return indicator name
     */
    String indicatorName();

    /**
     * Returns the total number of indicator events in this UTC day track.
     *
     * @return event count
     */
    int size();

    /**
     * Returns whether this daily track contains zero events.
     *
     * @return {@code true} if empty
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the simulated arrival timestamp in milliseconds UTC at the specified index.
     *
     * @param index row index ({@code 0 <= index < size()})
     * @return timestamp in milliseconds UTC
     */
    long timestampMs(int index);

    /**
     * Constructs or retrieves the {@link BacktestEvent} at the specified index.
     *
     * @param index row index ({@code 0 <= index < size()})
     * @return chronological {@link BacktestEvent}
     */
    BacktestEvent eventAt(int index);

    /**
     * Finds the first index whose {@link #timestampMs(int)} is greater than or equal to {@code targetTsMs}
     * using {@code O(log N)} binary search, or {@link #size()} if all events occur before {@code targetTsMs}.
     *
     * @param targetTsMs inclusive lower-bound timestamp in milliseconds UTC
     * @return first index {@code >= targetTsMs}, or {@link #size()}
     */
    default int findFirstIndexAtOrAfter(long targetTsMs) {
        int low = 0;
        int high = size() - 1;
        int result = size();
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (timestampMs(mid) >= targetTsMs) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return result;
    }

    /**
     * Finds the first index whose {@link #timestampMs(int)} is strictly greater than {@code targetTsMs}
     * using {@code O(log N)} binary search, or {@link #size()} if all events occur at or before {@code targetTsMs}.
     *
     * @param targetTsMs inclusive upper-bound timestamp in milliseconds UTC
     * @return first index {@code > targetTsMs}, or {@link #size()}
     */
    default int findFirstIndexAfter(long targetTsMs) {
        int low = 0;
        int high = size() - 1;
        int result = size();
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (timestampMs(mid) > targetTsMs) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return result;
    }
}
