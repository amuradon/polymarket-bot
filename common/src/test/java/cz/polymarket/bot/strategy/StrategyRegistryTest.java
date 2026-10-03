package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.TwapUpdate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StrategyRegistryTest {

    static class DummyStrategy implements TradingStrategy {
        private final String name;

        DummyStrategy(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void init(StrategyContext context) {}

        @Override
        public void onTwapUpdate(TwapUpdate update) {}

        @Override
        public void onExecutionReport(ExecutionReport report) {}
    }

    @Test
    @DisplayName("Should retrieve strategy by exact or case-insensitive name")
    void shouldFindStrategyByName() {
        TradingStrategy s1 = new DummyStrategy("TWAPArbitrageStrategy");
        TradingStrategy s2 = new DummyStrategy("MeanReversionStrategy");

        StrategyRegistry registry = new StrategyRegistry(Map.of(
                s1.getName(), s1,
                s2.getName(), s2
        ));

        assertThat(registry.getStrategy("TWAPArbitrageStrategy")).isSameAs(s1);
        assertThat(registry.getStrategy("twaparbitragestrategy")).isSameAs(s1);
        assertThat(registry.getStrategy("MeanReversionStrategy")).isSameAs(s2);
        assertThat(registry.getAvailableStrategies()).containsExactlyInAnyOrder("TWAPArbitrageStrategy", "MeanReversionStrategy");
    }

    @Test
    @DisplayName("Should throw when unknown strategy requested")
    void shouldThrowWhenUnknownStrategy() {
        StrategyRegistry registry = new StrategyRegistry(Map.of());

        assertThatThrownBy(() -> registry.getStrategy("NonExistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown strategy");
    }

    @Test
    @DisplayName("Should throw when strategy name is blank")
    void shouldThrowWhenStrategyNameIsBlank() {
        StrategyRegistry registry = new StrategyRegistry(Map.of());

        assertThatThrownBy(() -> registry.getStrategy(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null or blank");
    }
}
