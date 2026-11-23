package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Deterministic synthetic unit tests for {@link PricingModelStrategy} and {@link PricingModelStrategyConfig}.
 */
class PricingModelStrategyTest {

    private static final long CANDLE_START_SEC = 1_794_131_100L;
    private static final long CANDLE_END_SEC = CANDLE_START_SEC + 900L;
    private static final long CANDLE_START_MS = CANDLE_START_SEC * 1_000L;
    private static final long CANDLE_END_MS = CANDLE_END_SEC * 1_000L;

    private PricingModelStrategyConfig config;
    private PricingModelStrategy strategy;
    private List<OrderCommand> submittedOrders;
    private int balanceRefreshCount;

    @BeforeEach
    void setUp() {
        config = PricingModelStrategyConfig.defaults();
        strategy = new PricingModelStrategy(config);
        submittedOrders = new ArrayList<>();
        balanceRefreshCount = 0;

        ExecutionRouter router = new ExecutionRouter() {
            @Override
            public void submitOrder(OrderCommand command) {
                submittedOrders.add(command);
            }

            @Override
            public void cancelOrder(String clientOrderId) {
            }
        };

        StrategyContext testContext = new StrategyContext() {
            @Override
            public ExecutionRouter getExecutionRouter() {
                return router;
            }

            @Override
            public void refreshAccountBalance() {
                balanceRefreshCount++;
            }
        };

        strategy.init(testContext);
    }

    @Test
    @DisplayName("Should enforce fail-fast configuration invariants and unwrap CDI proxies")
    void shouldValidateConfigAndUnwrapProxy() {
        assertThat(strategy.getConfig()).isSameAs(config);
        assertThat(strategy.getName()).isEqualTo("PricingModelStrategy");
        assertThat(strategy.getTimeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(strategy.getRequiredClobLookbackMs()).isEqualTo(370_000L);
        assertThat(strategy.effectiveMaxOpenPriceMc(240_000L)).isEqualTo(680);
        assertThat(strategy.effectiveMaxOpenPriceMc(40_000L)).isEqualTo(920);
        assertThat(strategy.effectiveMaxOpenPriceMc(112_500L)).isEqualTo(800);

        assertThatThrownBy(() -> PricingModelStrategyConfig.unwrap(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strategy.init(null))
                .isInstanceOf(IllegalArgumentException.class);

        // Invalid tau bounds
        assertThatThrownBy(() -> new PricingModelStrategyConfig(
                "15m", 500_000L, 100_000L, 3_000L, 2_500L, 75L,
                0.25, 30.0, 180.0, 0.55, 0.45, 30.0, 60, 0.70, 15,
                250, 680, 920, 180_000L, 25, 25, 40, 25, 220, 350, 8,
                1_250L, 2_750L, 25, 10, 700, 25, 150, 2, 200,
                "pm-btc-15m", "BTC-UP", "BTC-DOWN"))
                .isInstanceOf(IllegalArgumentException.class);

        class ProxiedConfig extends PricingModelStrategyConfig {
            ProxiedConfig(PricingModelStrategyConfig d) {
                super(d.timeframe().getCode(), d.minTauMs(), d.maxTauMs(), d.cooldownMs(), d.maxQuoteAgeMs(),
                        d.spotDelayOffsetMs(), d.basisEmaAlpha(), d.volFastTauSec(), d.volSlowTauSec(),
                        d.volFastWeight(), d.minSigma1sBps(), d.maxSingleReturnBps(), d.maxCandleRangeBps(),
                        d.bridgeFloorBps(), d.minWarmupSamples(), d.minOpenPriceMc(), d.maxEarlyOpenPriceMc(),
                        d.maxOpenPriceMc(), d.earlyPriceCapTauMs(), d.maxSpreadMc(), d.minDepthShares(),
                        d.minNetEdgeMc(), d.minFillNetEdgeMc(), d.minLeadGapCentiBps(),
                        d.strongLeadThresholdCentiBps(), d.strongLeadEdgeBonusMc(),
                        d.modelJumpWindowMs(), d.secondaryJumpWindowMs(), d.minModelJumpMc(),
                        d.maxSlippageMc(), d.takerFeeRateBps(), d.minOrderShares(), d.maxOrderShares(),
                        d.maxOrdersPerMarket(), d.maxInventoryShares(),
                        d.defaultMarketId(), d.upToken(), d.downToken());
            }
        }

        PricingModelStrategy unwrappedStrategy = new PricingModelStrategy(new ProxiedConfig(config));
        assertThat(unwrappedStrategy.getConfig().getClass()).isEqualTo(PricingModelStrategyConfig.class);
        assertThat(unwrappedStrategy.createInstance()).isNotSameAs(unwrappedStrategy);
    }

    @Test
    @DisplayName("Should satisfy Student-t df=4 symmetry, monotonicity, and parabolic taker fee bounds")
    void shouldComputeExactStudentT4AndParabolicFee() {
        assertThat(PricingModelStrategy.studentT4ProbMc(0.0)).isEqualTo(500);
        int pPos = PricingModelStrategy.studentT4ProbMc(1.5);
        int pNeg = PricingModelStrategy.studentT4ProbMc(-1.5);
        assertThat(pPos + pNeg).isEqualTo(1_000);
        assertThat(pPos).isBetween(880, 920);
        assertThat(PricingModelStrategy.takerFeeMc(500, 0.07)).isEqualTo(18);
        assertThat(PricingModelStrategy.takerFeeMc(900, 0.07)).isEqualTo(7);
    }

    @Test
    @DisplayName("Should reject static mispricing, forbid averaging down, and cap at 2 clips per market")
    void shouldRejectStaticMispricingForbidAveragingDownAndCapTwoClips() {
        double basePrice = 80_000.0;
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", basePrice, 60, CANDLE_START_MS);

        long t0 = CANDLE_END_MS - 180_000L;
        for (int s = 0; s <= 25; s++) {
            long ts = t0 + s * 1_000L;
            double px = basePrice + ((s % 2 == 0) ? 2.0 : -2.0);
            strategy.onSpotTwap(ts, "btcusd", px, basePrice);
            strategy.onPolymarketCryptoPrice(ts + 200L, "btcusd", "chainlink", px, ts - 1_000L);
        }

        // 1. Static mispricing without active lead-gap -> NO ORDER
        long staticTs = t0 + 30_000L;
        strategy.onSpotTwap(staticTs - 3_000L, "btcusd", basePrice + 35.0, basePrice + 10.0);
        strategy.onSpotTwap(staticTs, "btcusd", basePrice + 35.0, basePrice + 15.0);
        strategy.onPolymarketCryptoPrice(staticTs + 100L, "btcusd", "chainlink", basePrice + 35.0, staticTs - 1_000L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.52, 0.51, 0.49, 0.48, 200.0, 200.0, 0.52, 0.49, staticTs + 150L));
        assertThat(submittedOrders).isEmpty();

        // 2. Clip #1 on active impulse at ask = 0.52
        long jump1Ts = staticTs + 500L;
        strategy.onSpotTwap(jump1Ts, "btcusd", basePrice + 62.0, basePrice + 20.0);
        assertThat(submittedOrders).hasSize(1);
        OrderCommand c1 = submittedOrders.getFirst();
        assertThat(c1.price()).isEqualByComparingTo(new BigDecimal("0.530"));
        assertThat(balanceRefreshCount).isEqualTo(1);
        strategy.onExecutionReport(new ExecutionReport(
                c1.clientOrderId(), "ex-1", "FILLED", new BigDecimal("0.520"), new BigDecimal("50"), jump1Ts + 16L, "Filled"));

        // 3. Averaging-down attempt at lower ask = 0.44 (< 0.52 - 0.01) must be REJECTED
        long avgDownTs = jump1Ts + 4_000L;
        strategy.onPolymarketCryptoPrice(avgDownTs - 200L, "btcusd", "chainlink", basePrice + 10.0, avgDownTs - 1_400L);
        strategy.onSpotTwap(avgDownTs - 1_500L, "btcusd", basePrice + 10.0, basePrice + 10.0);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.44, 0.43, 0.57, 0.56, 200.0, 200.0, 0.44, 0.57, avgDownTs - 50L));
        strategy.onSpotTwap(avgDownTs, "btcusd", basePrice + 42.0, basePrice + 15.0);
        assertThat(submittedOrders).hasSize(1);

        // 4. Continuation Clip #2 at higher ask = 0.58 (>= 0.52 - 0.01) is ACCEPTED
        long jump2Ts = avgDownTs + 4_000L;
        strategy.onPolymarketCryptoPrice(jump2Ts - 200L, "btcusd", "chainlink", basePrice + 42.0, jump2Ts - 1_400L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.58, 0.57, 0.43, 0.42, 200.0, 200.0, 0.58, 0.43, jump2Ts - 50L));
        strategy.onSpotTwap(jump2Ts, "btcusd", basePrice + 85.0, basePrice + 25.0);
        assertThat(submittedOrders).hasSize(2);
        OrderCommand c2 = submittedOrders.get(1);
        strategy.onExecutionReport(new ExecutionReport(
                c2.clientOrderId(), "ex-2", "FILLED", new BigDecimal("0.580"), new BigDecimal("50"), jump2Ts + 16L, "Filled"));

        // 5. Clip #3 must be BLOCKED by maxOrdersPerMarket = 2 even when inventory remains
        long jump3Ts = jump2Ts + 4_000L;
        strategy.onPolymarketCryptoPrice(jump3Ts - 200L, "btcusd", "chainlink", basePrice + 85.0, jump3Ts - 1_400L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.62, 0.61, 0.39, 0.38, 200.0, 200.0, 0.62, 0.39, jump3Ts - 50L));
        strategy.onSpotTwap(jump3Ts, "btcusd", basePrice + 135.0, basePrice + 35.0);
        assertThat(submittedOrders).hasSize(2);
    }

    @Test
    @DisplayName("Should block chaotic >60bps candles pre-final-minute and enforce early max ask cap 0.68")
    void shouldEnforceEarlyPriceCapAndCandleChaosGuard() {
        double strike = 80_000.0;
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", strike, 60, CANDLE_START_MS);

        long t0 = CANDLE_END_MS - 240_000L;
        for (int s = 0; s <= 20; s++) {
            long ts = t0 + s * 1_000L;
            double px = strike + ((s % 2 == 0) ? 2.0 : -2.0);
            strategy.onSpotTwap(ts, "btcusd", px, strike);
            strategy.onPolymarketCryptoPrice(ts + 200L, "btcusd", "chainlink", px, ts - 1_000L);
        }

        // At tau = 215s (>= 180s), ask = 0.74 exceeds maxEarlyOpenPriceMc (0.68) -> REJECTED
        long earlyTs = t0 + 25_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.74, 0.73, 0.27, 0.26, 300.0, 300.0, 0.74, 0.27, earlyTs - 50L));
        strategy.onSpotTwap(earlyTs, "btcusd", strike + 70.0, strike + 10.0);
        assertThat(submittedOrders).isEmpty();

        // Simulate extreme >60 bps intra-candle swing (+260 / -260 USD = 65 bps) after resting quote expires,
        // then verify that subsequent pre-final-minute entry is blocked by maxCandleRangeBps even at ask = 0.50!
        long swingStartTs = earlyTs + 4_000L;
        strategy.onSpotTwap(swingStartTs, "btcusd", strike - 260.0, strike);
        strategy.onSpotTwap(swingStartTs + 1_000L, "btcusd", strike + 260.0, strike);
        long chaosTs = swingStartTs + 4_000L;
        strategy.onPolymarketCryptoPrice(chaosTs - 200L, "btcusd", "chainlink", strike + 10.0, chaosTs - 1_400L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.50, 0.49, 0.51, 0.50, 300.0, 300.0, 0.50, 0.51, chaosTs - 50L));
        strategy.onSpotTwap(chaosTs, "btcusd", strike + 120.0, strike);
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should capture 2.5s smooth impulses via secondary jump window and compensate venue basis")
    void shouldCaptureSecondaryJumpWindowAndAnchorVenueBasis() {
        double strike = 80_000.0;
        double venueBias = 40.0;
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", strike, 60, CANDLE_START_MS);

        long t0 = CANDLE_END_MS - 95_000L;
        for (int s = 0; s <= 40; s++) {
            long ts = t0 + s * 1_000L;
            double clPrice = strike + ((s % 2 == 0) ? 1.5 : -1.5);
            strategy.onSpotTwap(ts, "btcusd", clPrice + venueBias, strike + venueBias);
            strategy.onPolymarketCryptoPrice(ts + 1_200L, "btcusd", "chainlink", clPrice, ts);
        }

        // Smooth 2.4-second DOWN move in two steps where the last 1.25s step alone is small (< 25 mc),
        // but cumulative 2.75s jump is > 40 mc!
        long step1Ts = CANDLE_END_MS - 32_400L;
        strategy.onPolymarketCryptoPrice(step1Ts - 100L, "btcusd", "chainlink", strike, step1Ts - 1_300L);
        strategy.onSpotTwap(step1Ts, "btcusd", strike + venueBias - 24.0, strike + venueBias);

        long step2Ts = CANDLE_END_MS - 30_000L; // +2,400 ms after base!
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.30, 0.29, 0.71, 0.70, 300.0, 300.0, 0.30, 0.71, step2Ts - 40L));
        strategy.onSpotTwap(step2Ts, "btcusd", strike + venueBias - 26.0, strike + venueBias);

        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().token()).isEqualTo("BTC-DOWN");
    }
}
