package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.StrategyMetadata;
import cz.polymarket.bot.backtest.service.StrategyParameterService.ConfiguredStrategy;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.OraclePulseLockStrategy;
import cz.polymarket.bot.strategy.OraclePulseLockStrategyConfig;
import cz.polymarket.bot.strategy.PricingModelStrategy;
import cz.polymarket.bot.strategy.PricingModelStrategyConfig;
import cz.polymarket.bot.strategy.RoundMainstreamStrategy;
import cz.polymarket.bot.strategy.RoundMainstreamStrategyConfig;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradingStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategyConfig;
import cz.polymarket.bot.strategy.UtterParameciumStrategyV0;
import cz.polymarket.bot.strategy.UtterParameciumStrategyV1;
import cz.polymarket.bot.strategy.UtterParameciumStrategyV2;
import cz.polymarket.bot.strategy.UtterParameciumStrategyV3;
import cz.polymarket.bot.strategy.UtterParameciumStrategyV4;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StrategyParameterServiceTest {

    private StrategyParameterService service;

    @BeforeEach
    void setUp() {
        List<TradingStrategy> allStrategies = List.of(
                new OraclePulseLockStrategy(OraclePulseLockStrategyConfig.defaults()),
                new PricingModelStrategy(PricingModelStrategyConfig.defaults()),
                new RoundMainstreamStrategy(RoundMainstreamStrategyConfig.defaultModelV1()),
                new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1()),
                new UtterParameciumStrategyV0(UtterParameciumStrategyV0.Config.defaultConfig()),
                new UtterParameciumStrategyV1(UtterParameciumStrategyV1.Config.defaultConfig()),
                new UtterParameciumStrategyV2(UtterParameciumStrategyV2.Config.defaultConfig()),
                new UtterParameciumStrategyV3(UtterParameciumStrategyV3.Config.defaultConfig()),
                new UtterParameciumStrategyV4(UtterParameciumStrategyV4.Config.defaultConfig())
        );
        Map<String, TradingStrategy> map = new java.util.LinkedHashMap<>();
        for (TradingStrategy s : allStrategies) {
            map.put(s.getName(), s);
        }
        service = new StrategyParameterService(new StrategyRegistry(map));
    }

    @Test
    @DisplayName("Should introspect all 9 registered strategies with parameters and curated key parameters")
    void shouldIntrospectAllNineStrategies() {
        List<StrategyMetadata> list = service.listStrategies();
        assertThat(list).hasSize(9);

        for (StrategyMetadata meta : list) {
            assertThat(meta.parameters()).isNotEmpty();
            assertThat(meta.parameters().stream().filter(StrategyMetadata.StrategyParameterDescriptor::keyParam).count())
                    .isGreaterThanOrEqualTo(5);
        }

        StrategyMetadata oracleMeta = service.getStrategyMetadata("OraclePulseLockStrategy");
        assertThat(oracleMeta.parameters())
                .anySatisfy(p -> {
                    assertThat(p.name()).isEqualTo("timeframeCode");
                    assertThat(p.defaultValue()).isEqualTo("15m");
                });

        StrategyMetadata pricingMeta = service.getStrategyMetadata("PricingModelStrategy");
        assertThat(pricingMeta.parameters())
                .anySatisfy(p -> {
                    assertThat(p.name()).isEqualTo("timeframeLabel");
                    assertThat(p.defaultValue()).isEqualTo("15m");
                });
    }

    @Test
    @DisplayName("Should instantiate custom-configured strategies with typed overrides across all 9 strategy implementations")
    void shouldInstantiateConfiguredStrategiesWithOverrides() {
        ConfiguredStrategy oracle = service.instantiateStrategy("OraclePulseLockStrategy", Map.of(
                "minTauMs", 3000,
                "maxRiskPerMarketUsd", 500,
                "timeframeCode", "15m"
        ));
        assertThat(oracle.strategy()).isInstanceOf(OraclePulseLockStrategy.class);
        assertThat(((OraclePulseLockStrategy) oracle.strategy()).getConfig().minTauMs()).isEqualTo(3000L);
        assertThat(((OraclePulseLockStrategy) oracle.strategy()).getConfig().maxRiskPerMarketUsd()).isEqualTo(500.0);
        assertThat(oracle.appliedOverrides())
                .containsEntry("minTauMs", 3000L)
                .containsEntry("maxRiskPerMarketUsd", 500.0)
                .doesNotContainKey("timeframeCode");

        ConfiguredStrategy v4 = service.instantiateStrategy("UtterParameciumStrategyV4", Map.of(
                "allowDirectionFlip", true,
                "minEdgeMc", 35
        ));
        assertThat(v4.strategy()).isInstanceOf(UtterParameciumStrategyV4.class);
        assertThat(((UtterParameciumStrategyV4) v4.strategy()).getConfig().allowDirectionFlip()).isTrue();
        assertThat(((UtterParameciumStrategyV4) v4.strategy()).getConfig().minEdgeMc()).isEqualTo(35);

        ConfiguredStrategy pricing = service.instantiateStrategy("PricingModelStrategy", Map.of(
                "minNetEdgeMc", 55
        ));
        assertThat(((PricingModelStrategy) pricing.strategy()).getConfig().minNetEdgeMc()).isEqualTo(55);

        ConfiguredStrategy round = service.instantiateStrategy("RoundMainstreamStrategy", Map.of(
                "minEffectiveSweepShares", 2500
        ));
        assertThat(((RoundMainstreamStrategy) round.strategy()).getConfig().minEffectiveSweepShares()).isEqualTo(2500);
    }

    @Test
    @DisplayName("Should fail fast on unknown strategy parameter or violated config invariant")
    void shouldFailFastOnInvalidParameterOrInvariant() {
        assertThatThrownBy(() -> service.instantiateStrategy("OraclePulseLockStrategy", Map.of("nonExistentParam", 42)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown strategy parameter");

        assertThatThrownBy(() -> service.instantiateStrategy("OraclePulseLockStrategy", Map.of("minTauMs", "not-a-number")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expects a long integer");

        // Violates minTauMs >= 500 invariant in OraclePulseLockStrategyConfig
        assertThatThrownBy(() -> service.instantiateStrategy("OraclePulseLockStrategy", Map.of("minTauMs", 100)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Tau window");
    }

    @Test
    @DisplayName("Should handle synthetic strategies without config bean cleanly")
    void shouldHandleParameterlessStrategy() {
        TradingStrategy plain = new TradingStrategy() {
            @Override
            public String getName() {
                return "PlainSynthetic";
            }

            @Override
            public void init(StrategyContext context) {}

            @Override
            public void onExecutionReport(ExecutionReport report) {}
        };
        StrategyParameterService local = new StrategyParameterService(new StrategyRegistry(Map.of("PlainSynthetic", plain)));
        assertThat(local.getStrategyMetadata("PlainSynthetic").parameters()).isEmpty();
        assertThat(local.instantiateStrategy("PlainSynthetic", null).strategy()).isSameAs(plain);
        assertThatThrownBy(() -> local.instantiateStrategy("PlainSynthetic", Map.of("foo", 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not expose configurable parameters");
    }
}
