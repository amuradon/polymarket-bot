package cz.polymarket.bot.strategy;

import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests verifying historical snapshots ({@link UtterParameciumStrategyV0},
 * {@link UtterParameciumStrategyV1}, {@link UtterParameciumStrategyV2},
 * {@link UtterParameciumStrategyV3}, and {@link UtterParameciumStrategyV4}) using deterministic synthetic fixtures.
 */
class UtterParameciumStrategyVersionsTest {

    private static final long CANDLE_START_SEC = 1_785_542_400L;
    private static final long CANDLE_END_SEC = CANDLE_START_SEC + 900L;
    private static final long CANDLE_START_MS = CANDLE_START_SEC * 1_000L;
    private static final long CANDLE_END_MS = CANDLE_END_SEC * 1_000L;

    @Test
    @DisplayName("Should expose isolated default configs, names, timeframes, and independent instances across V0..V4")
    void shouldExposeIsolatedDefaultConfigsAndMetadata() {
        UtterParameciumStrategyV0 v0 = new UtterParameciumStrategyV0(UtterParameciumStrategyV0.Config.defaultConfig());
        UtterParameciumStrategyV1 v1 = new UtterParameciumStrategyV1(UtterParameciumStrategyV1.Config.defaultConfig());
        UtterParameciumStrategyV2 v2 = new UtterParameciumStrategyV2(UtterParameciumStrategyV2.Config.defaultConfig());
        UtterParameciumStrategyV3 v3 = new UtterParameciumStrategyV3(UtterParameciumStrategyV3.Config.defaultConfig());
        UtterParameciumStrategyV4 v4 = new UtterParameciumStrategyV4(UtterParameciumStrategyV4.Config.defaultConfig());

        assertThat(v0.getName()).isEqualTo("UtterParameciumStrategyV0");
        assertThat(v1.getName()).isEqualTo("UtterParameciumStrategyV1");
        assertThat(v2.getName()).isEqualTo("UtterParameciumStrategyV2");
        assertThat(v3.getName()).isEqualTo("UtterParameciumStrategyV3");
        assertThat(v4.getName()).isEqualTo("UtterParameciumStrategyV4");

        assertThat(v0.getTimeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(v1.getTimeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(v2.getTimeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(v3.getTimeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(v4.getTimeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);

        assertThat(v0.getConfig().maxTauMs()).isEqualTo(61_000L);
        assertThat(v0.getConfig().cooldownMs()).isEqualTo(250L);
        assertThat(v0.getConfig().maxEntryPriceMc()).isEqualTo(970);
        assertThat(v0.getConfig().defaultSigma1sCents()).isEqualTo(150L);
        assertThat(v0.getConfig().minSigma1sCents()).isEqualTo(80L);
        assertThat(v0.getRequiredClobLookbackMs()).isEqualTo(76_000L);

        assertThat(v1.getConfig().maxTauMs()).isEqualTo(61_000L);
        assertThat(v1.getConfig().cooldownMs()).isEqualTo(400L);
        assertThat(v1.getConfig().maxEntryPriceMc()).isEqualTo(880);
        assertThat(v1.getConfig().maxModelProbMc()).isEqualTo(975);
        assertThat(v1.getConfig().earlyInventoryCapBps()).isEqualTo(5_500);
        assertThat(v1.getConfig().fullInventoryTauMs()).isEqualTo(35_000L);
        assertThat(v1.getConfig().allowDirectionFlip()).isFalse();
        assertThat(v1.getRequiredClobLookbackMs()).isEqualTo(76_000L);

        assertThat(v2.getConfig().maxTauMs()).isEqualTo(60_000L);
        assertThat(v2.getConfig().cooldownMs()).isEqualTo(350L);
        assertThat(v2.getConfig().maxEntryPriceMc()).isEqualTo(840);
        assertThat(v2.getConfig().earlyInventoryCapBps()).isEqualTo(5_000);
        assertThat(v2.getConfig().fullInventoryTauMs()).isEqualTo(36_000L);
        assertThat(v2.getConfig().calcLeadWeightBps()).isEqualTo(6_000);
        assertThat(v2.getConfig().calcAdverseLeadBlockCents()).isEqualTo(850L);
        assertThat(v2.getConfig().calcAdverseDrift4sBlockCents()).isEqualTo(1_250L);
        assertThat(v2.getRequiredClobLookbackMs()).isEqualTo(77_000L);

        assertThat(v3.getConfig().maxTauMs()).isEqualTo(60_000L);
        assertThat(v3.getConfig().cooldownMs()).isEqualTo(350L);
        assertThat(v3.getConfig().maxEntryPriceMc()).isEqualTo(840);
        assertThat(v3.getConfig().calcLeadWeightBps()).isEqualTo(7_000);
        assertThat(v3.getConfig().calcAdverseLeadBlockCents()).isEqualTo(650L);
        assertThat(v3.getConfig().calcAdverseDrift4sBlockCents()).isEqualTo(950L);
        assertThat(v3.getRequiredClobLookbackMs()).isEqualTo(77_000L);

        assertThat(v4.getConfig().maxTauMs()).isEqualTo(64_000L);
        assertThat(v4.getConfig().cooldownMs()).isEqualTo(300L);
        assertThat(v4.getConfig().maxEntryPriceMc()).isEqualTo(920);
        assertThat(v4.getConfig().maxModelProbMc()).isEqualTo(985);
        assertThat(v4.getConfig().maxOrderShares()).isEqualTo(45);
        assertThat(v4.getConfig().edgeScalingFactor()).isEqualTo(90);
        assertThat(v4.getRequiredClobLookbackMs()).isEqualTo(81_000L);

        assertThat(v0.createInstance()).isNotSameAs(v0).isInstanceOf(UtterParameciumStrategyV0.class);
        assertThat(v1.createInstance()).isNotSameAs(v1).isInstanceOf(UtterParameciumStrategyV1.class);
        assertThat(v2.createInstance()).isNotSameAs(v2).isInstanceOf(UtterParameciumStrategyV2.class);
        assertThat(v3.createInstance()).isNotSameAs(v3).isInstanceOf(UtterParameciumStrategyV3.class);
        assertThat(v4.createInstance()).isNotSameAs(v4).isInstanceOf(UtterParameciumStrategyV4.class);
    }

    @Test
    @DisplayName("Should coexist in StrategyRegistry alongside UtterParameciumStrategy without alias collisions")
    void shouldCoexistInStrategyRegistryWithoutCollisions() {
        UtterParameciumStrategy main = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        UtterParameciumStrategyV0 v0 = new UtterParameciumStrategyV0(UtterParameciumStrategyV0.Config.defaultConfig());
        UtterParameciumStrategyV1 v1 = new UtterParameciumStrategyV1(UtterParameciumStrategyV1.Config.defaultConfig());
        UtterParameciumStrategyV2 v2 = new UtterParameciumStrategyV2(UtterParameciumStrategyV2.Config.defaultConfig());
        UtterParameciumStrategyV3 v3 = new UtterParameciumStrategyV3(UtterParameciumStrategyV3.Config.defaultConfig());
        UtterParameciumStrategyV4 v4 = new UtterParameciumStrategyV4(UtterParameciumStrategyV4.Config.defaultConfig());

        StrategyRegistry registry = new StrategyRegistry(Map.of(
                main.getName(), main,
                v0.getName(), v0,
                v1.getName(), v1,
                v2.getName(), v2,
                v3.getName(), v3,
                v4.getName(), v4
        ));

        assertThat(registry.getStrategy("UtterParameciumStrategy")).isSameAs(main);
        assertThat(registry.getStrategy("utter-paramecium")).isSameAs(main);
        assertThat(registry.getStrategy("UtterParameciumStrategyV0")).isSameAs(v0);
        assertThat(registry.getStrategy("utterparameciumstrategyv0")).isSameAs(v0);
        assertThat(registry.getStrategy("UtterParameciumStrategyV1")).isSameAs(v1);
        assertThat(registry.getStrategy("UtterParameciumStrategyV2")).isSameAs(v2);
        assertThat(registry.getStrategy("UtterParameciumStrategyV3")).isSameAs(v3);
        assertThat(registry.getStrategy("UtterParameciumStrategyV4")).isSameAs(v4);
    }

    @Test
    @DisplayName("Should emit BUY BTC-UP across all five historical versions on a clean synthetic settlement cushion")
    void shouldEmitSignalsAcrossAllVersionsOnCleanSyntheticSetup() {
        List<TradingStrategy> versions = List.of(
                new UtterParameciumStrategyV0(UtterParameciumStrategyV0.Config.defaultConfig()),
                new UtterParameciumStrategyV1(UtterParameciumStrategyV1.Config.defaultConfig()),
                new UtterParameciumStrategyV2(UtterParameciumStrategyV2.Config.defaultConfig()),
                new UtterParameciumStrategyV3(UtterParameciumStrategyV3.Config.defaultConfig()),
                new UtterParameciumStrategyV4(UtterParameciumStrategyV4.Config.defaultConfig())
        );

        for (TradingStrategy strategy : versions) {
            List<OrderCommand> orders = new ArrayList<>();
            strategy.init(createSyntheticContext(orders));
            strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

            long windowStartMs = CANDLE_END_MS - 60_000L;
            for (int sec = 0; sec <= 30; sec++) {
                long ts = windowStartMs + sec * 1_000L;
                strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_025.00, ts);
            }
            long evalTs = CANDLE_END_MS - 30_000L;
            strategy.onOrderBookQuote(new OrderBookQuote(0.62, 0.60, 0.40, 0.38, 500.0, 500.0, 0.62, 0.40, evalTs));

            assertThat(orders)
                    .as("Expected BUY BTC-UP signal from %s", strategy.getName())
                    .hasSize(1);
            assertThat(orders.getFirst().token()).isEqualTo("BTC-UP");
            assertThat(orders.getFirst().side()).isEqualTo("BUY");
        }
    }

    @Test
    @DisplayName("Should reflect evolutionary risk gates from V0 through V3 on synthetic extreme-price and spot-lead scenarios")
    void shouldReflectEvolutionaryRiskGatesAcrossVersions() {
        // 1. Extreme ask price ($0.92): V0 allows (< $0.97), V1/V2/V3 block ($0.88 / $0.84 ceilings)
        List<OrderCommand> v0HighAskOrders = runSingleQuoteScenario(
                new UtterParameciumStrategyV0(UtterParameciumStrategyV0.Config.defaultConfig()),
                90_080.00,
                0.92,
                0.90,
                0.0
        );
        List<OrderCommand> v1HighAskOrders = runSingleQuoteScenario(
                new UtterParameciumStrategyV1(UtterParameciumStrategyV1.Config.defaultConfig()),
                90_080.00,
                0.92,
                0.90,
                0.0
        );
        assertThat(v0HighAskOrders).hasSize(1);
        assertThat(v1HighAskOrders).isEmpty();

        // 2. Adverse multi-exchange spot lead (-$7.50 below aligned Chainlink spot at tau = 45s):
        // V1 ignores calcSpot, V2 haircuts cushion (block at -$8.50), V3 blocks (>= -$6.50)
        List<OrderCommand> v1LeadOrders = runSingleQuoteScenario(
                new UtterParameciumStrategyV1(UtterParameciumStrategyV1.Config.defaultConfig()),
                90_022.00,
                0.60,
                0.58,
                -7.50
        );
        List<OrderCommand> v2LeadOrders = runSingleQuoteScenario(
                new UtterParameciumStrategyV2(UtterParameciumStrategyV2.Config.defaultConfig()),
                90_022.00,
                0.60,
                0.58,
                -7.50
        );
        List<OrderCommand> v3LeadOrders = runSingleQuoteScenario(
                new UtterParameciumStrategyV3(UtterParameciumStrategyV3.Config.defaultConfig()),
                90_022.00,
                0.60,
                0.58,
                -7.50
        );
        assertThat(v1LeadOrders).hasSize(1);
        assertThat(v2LeadOrders).hasSize(1);
        assertThat(v3LeadOrders).isEmpty();
    }

    private static List<OrderCommand> runSingleQuoteScenario(
            TradingStrategy strategy,
            double chainlinkSpot,
            double askUp,
            double bidUp,
            double calcSpotDeltaUsd) {
        List<OrderCommand> orders = new ArrayList<>();
        strategy.init(createSyntheticContext(orders));
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int sec = -5; sec <= 15; sec++) {
            long ts = windowStartMs + sec * 1_000L;
            double calcPrice = (sec == 15) ? (chainlinkSpot + calcSpotDeltaUsd) : chainlinkSpot;
            strategy.onCalculatedSpotTwap(ts, "BTCUSDT", calcPrice, calcPrice);
            if (sec < 15) {
                strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", chainlinkSpot, ts);
            }
        }
        long evalTs = windowStartMs + 15_000L;
        strategy.onPolymarketCryptoPrice(evalTs, "btcusd", "chainlink", chainlinkSpot, evalTs);
        strategy.onOrderBookQuote(new OrderBookQuote(
                askUp, bidUp, 1.0 - bidUp, 1.0 - askUp, 500.0, 500.0, askUp, 1.0 - bidUp, evalTs
        ));
        for (OrderCommand cmd : orders) {
            strategy.onExecutionReport(new ExecutionReport(
                    cmd.clientOrderId(), "ex-1", "FILLED", cmd.price(), cmd.size(), evalTs, null
            ));
        }
        return orders;
    }

    private static StrategyContext createSyntheticContext(List<OrderCommand> sink) {
        ExecutionRouter router = new ExecutionRouter() {
            @Override
            public void submitOrder(OrderCommand command) {
                sink.add(command);
            }

            @Override
            public void cancelOrder(String clientOrderId) {
            }
        };
        HourlyPriceCache cache = new HourlyPriceCache(3_600L);
        return new StrategyContext() {
            @Override
            public ExecutionRouter getExecutionRouter() {
                return router;
            }

            @Override
            public HourlyPriceCache getPriceCache() {
                return cache;
            }
        };
    }
}
