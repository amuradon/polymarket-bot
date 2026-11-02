package cz.polymarket.bot.strategy;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Registry resolving available trading strategies by name.
 * Provides unified resolution across Backtesting, Paper, and Live trading modules.
 */
@ApplicationScoped
public class StrategyRegistry {

    private final Map<String, TradingStrategy> strategies;
    private final Map<String, TradingStrategy> lowercaseIndex;

    /**
     * CDI constructor dynamically injecting all discovered {@link TradingStrategy} beans.
     *
     * @param strategyInstances iterable instance of discovered strategy CDI beans
     */
    @Inject
    public StrategyRegistry(Instance<TradingStrategy> strategyInstances) {
        Map<String, TradingStrategy> map = new HashMap<>();
        Map<String, TradingStrategy> lowerMap = new HashMap<>();
        if (strategyInstances != null) {
            for (TradingStrategy strategy : strategyInstances) {
                registerStrategy(map, lowerMap, strategy.getName(), strategy);
            }
        }
        this.strategies = Collections.unmodifiableMap(map);
        this.lowercaseIndex = Collections.unmodifiableMap(lowerMap);
    }

    /**
     * Programmatic constructor registering an explicit map of trading strategies.
     *
     * @param strategies map of strategy names to strategy implementations
     */
    public StrategyRegistry(Map<String, TradingStrategy> strategies) {
        Map<String, TradingStrategy> map = new HashMap<>();
        Map<String, TradingStrategy> lowerMap = new HashMap<>();
        if (strategies != null) {
            for (Map.Entry<String, TradingStrategy> entry : strategies.entrySet()) {
                registerStrategy(map, lowerMap, entry.getKey(), entry.getValue());
            }
        }
        this.strategies = Collections.unmodifiableMap(map);
        this.lowercaseIndex = Collections.unmodifiableMap(lowerMap);
    }

    private static void registerStrategy(
            Map<String, TradingStrategy> map,
            Map<String, TradingStrategy> lowerMap,
            String name,
            TradingStrategy strategy) {
        if (name == null || strategy == null) {
            return;
        }
        map.put(name, strategy);
        String lower = name.toLowerCase(Locale.ROOT);
        lowerMap.put(lower, strategy);
        String normalized = normalizeKey(lower);
        lowerMap.putIfAbsent(normalized, strategy);
        if (normalized.endsWith("strategy") && normalized.length() > "strategy".length()) {
            lowerMap.putIfAbsent(normalized.substring(0, normalized.length() - "strategy".length()), strategy);
        }
    }

    private static String normalizeKey(String key) {
        return key.replace("_", "").replace("-", "").trim().toLowerCase(Locale.ROOT);
    }

    /**
     * Resolves a trading strategy by its case-insensitive or normalized name.
     *
     * @param name name of the strategy to find (e.g. "TWAPArbitrageStrategy" or "utter_paramecium")
     * @return matching TradingStrategy instance
     * @throws IllegalArgumentException if name is blank or strategy is not registered
     */
    public TradingStrategy getStrategy(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Strategy name cannot be null or blank");
        }
        TradingStrategy strategy = strategies.get(name);
        if (strategy == null) {
            String lower = name.trim().toLowerCase(Locale.ROOT);
            strategy = lowercaseIndex.get(lower);
            if (strategy == null) {
                strategy = lowercaseIndex.get(normalizeKey(lower));
            }
        }
        if (strategy == null) {
            throw new IllegalArgumentException("Unknown strategy: '" + name + "'. Available strategies: " + strategies.keySet());
        }
        return strategy;
    }

    /**
     * Returns the set of all registered strategy names.
     *
     * @return unmodifiable set of available strategy names
     */
    public Set<String> getAvailableStrategies() {
        return strategies.keySet();
    }
}
