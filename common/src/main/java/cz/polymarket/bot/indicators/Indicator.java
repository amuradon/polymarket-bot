package cz.polymarket.bot.indicators;

import java.util.Map;

/**
 * Common contract implemented by real-time and historical market indicators.
 * <p>
 * Provides a canonical indicator identifier, a deterministic map of configuration parameters
 * used by the backtesting engine to compute repeatable cache hashes and persist indicator metadata,
 * and a typed listener lifecycle for hot-path dispatch and inactive-indicator short-circuiting.
 *
 * @param <L> listener type notified when this indicator emits updates
 */
public interface Indicator<L> {

    /**
     * Returns the canonical name of the indicator used as its cache directory identifier
     * (e.g. {@code "SpotTwapIndicator"}).
     *
     * @return non-blank indicator name
     */
    default String getName() {
        return getClass().getSimpleName();
    }

    /**
     * Returns an immutable, deterministically ordered map of all algorithm parameters
     * governing this indicator's output values.
     *
     * @return deterministically ordered parameter map
     */
    Map<String, Object> getConfigurationParameters();

    /**
     * Resets all internal state of the indicator.
     */
    void reset();

    /**
     * Registers a listener to receive indicator updates.
     *
     * @param listener listener to register
     */
    void addListener(L listener);

    /**
     * Unregisters a previously registered listener.
     *
     * @param listener listener to remove
     */
    void removeListener(L listener);

    /**
     * Removes all registered listeners.
     */
    void clearListeners();

    /**
     * Returns {@code true} if at least one listener is currently registered on this indicator.
     *
     * @return {@code true} when at least one listener is registered
     */
    boolean hasListeners();

    /**
     * Dispatches the latest calculated indicator state to all registered listeners without heap allocation.
     *
     * @param timestampMs effective timestamp in milliseconds UTC
     * @param symbol      lowercase USD crypto symbol (e.g. {@code "btcusd"})
     */
    void notifyListeners(long timestampMs, String symbol);
}
