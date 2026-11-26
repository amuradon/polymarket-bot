package cz.polymarket.bot.backtest.data;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.List;

/**
 * Metadata descriptor for a registered trading strategy and its configurable parameters.
 *
 * @param name unique registered strategy name
 * @param timeframe candle timeframe code (e.g. {@code "15m"})
 * @param parameters ordered list of configurable parameter descriptors
 */
@Schema(name = "StrategyMetadata", description = "Descriptor of a registered strategy and its parameters.")
public record StrategyMetadata(
        String name,
        String timeframe,
        List<StrategyParameterDescriptor> parameters
) {

    /**
     * Metadata descriptor for a single strategy configuration parameter.
     *
     * @param name Java parameter name used in {@code strategyParams} overrides
     * @param propertyName MicroProfile config property key
     * @param type normalized primitive/value type ({@code int}, {@code long}, {@code double}, {@code boolean}, {@code string})
     * @param defaultValue active default value
     * @param keyParam whether this parameter is surfaced as a primary form input in the UI
     * @param group logical UI grouping label
     */
    @Schema(name = "StrategyParameterDescriptor", description = "Metadata for a single strategy parameter.")
    public record StrategyParameterDescriptor(
            String name,
            String propertyName,
            String type,
            Object defaultValue,
            boolean keyParam,
            String group
    ) {
    }
}
