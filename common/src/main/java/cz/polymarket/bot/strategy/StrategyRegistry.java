package cz.polymarket.bot.strategy;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.util.Collections;
import java.util.HashMap;
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

    @Inject
    public StrategyRegistry(Instance<TradingStrategy> strategyInstances) {
        Map<String, TradingStrategy> map = new HashMap<>();
        Map<String, TradingStrategy> lowerMap = new HashMap<>();
        if (strategyInstances != null) {
            for (TradingStrategy strategy : strategyInstances) {
                map.put(strategy.getName(), strategy);
                lowerMap.put(strategy.getName().toLowerCase(), strategy);
            }
        }
        this.strategies = Collections.unmodifiableMap(map);
        this.lowercaseIndex = Collections.unmodifiableMap(lowerMap);
    }

    public StrategyRegistry(Map<String, TradingStrategy> strategies) {
        Map<String, TradingStrategy> map = new HashMap<>();
        Map<String, TradingStrategy> lowerMap = new HashMap<>();
        if (strategies != null) {
            for (Map.Entry<String, TradingStrategy> entry : strategies.entrySet()) {
                map.put(entry.getKey(), entry.getValue());
                lowerMap.put(entry.getKey().toLowerCase(), entry.getValue());
            }
        }
        this.strategies = Collections.unmodifiableMap(map);
        this.lowercaseIndex = Collections.unmodifiableMap(lowerMap);
    }

    public TradingStrategy getStrategy(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Strategy name cannot be null or blank");
        }
        TradingStrategy strategy = strategies.get(name);
        if (strategy == null) {
            strategy = lowercaseIndex.get(name.trim().toLowerCase());
        }
        if (strategy == null) {
            throw new IllegalArgumentException("Unknown strategy: '" + name + "'. Available strategies: " + strategies.keySet());
        }
        return strategy;
    }

    public Set<String> getAvailableStrategies() {
        return strategies.keySet();
    }
}
