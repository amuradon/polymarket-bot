package cz.polymarket.bot.indicators;

import java.util.Map;

/**
 * Common contract implemented by real-time and historical market indicators.
 * <p>
 * Provides a canonical indicator identifier and a deterministic map of configuration parameters
 * used by the backtesting engine to compute repeatable cache hashes and persist indicator metadata.
 */
public interface Indicator {

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
}
