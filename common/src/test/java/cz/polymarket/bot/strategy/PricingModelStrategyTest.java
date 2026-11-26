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
        assertThat(strategy.getRequiredClobLookbackMs()).isEqualTo(430_000L);
        assertThat(config.maxJumpHazardBps()).isEqualTo(750);

        assertThatThrownBy(() -> PricingModelStrategyConfig.unwrap(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strategy.init(null))
                .isInstanceOf(IllegalArgumentException.class);

        // Invalid tau bounds
        assertThatThrownBy(() -> new PricingModelStrategyConfig(
                "15m", 500_000L, 100_000L, 3_000L, 2_500L, 75L,
                0.25, 30.0, 180.0, 0.55, 0.58, 30.0, 45, 0.85, 0.70, 750, 15,
                250, 920, 25, 25, 40, 25, 220,
                1_250L, 25, 10, 700, 25, 150, 2, 200,
                "pm-btc-15m", "BTC-UP", "BTC-DOWN"))
                .isInstanceOf(IllegalArgumentException.class);

        // Invalid jump hazard (> 3000 bps)
        assertThatThrownBy(() -> new PricingModelStrategyConfig(
                "15m", 8_000L, 420_000L, 3_000L, 2_500L, 75L,
                0.25, 30.0, 180.0, 0.55, 0.58, 30.0, 45, 0.85, 0.70, 4_000, 15,
                250, 920, 25, 25, 40, 25, 220,
                1_250L, 25, 10, 700, 25, 150, 2, 200,
                "pm-btc-15m", "BTC-UP", "BTC-DOWN"))
                .isInstanceOf(IllegalArgumentException.class);

        class ProxiedConfig extends PricingModelStrategyConfig {
            ProxiedConfig(PricingModelStrategyConfig d) {
                super(d.timeframe().getCode(), d.minTauMs(), d.maxTauMs(), d.cooldownMs(), d.maxQuoteAgeMs(),
                        d.spotDelayOffsetMs(), d.basisEmaAlpha(), d.volFastTauSec(), d.volSlowTauSec(),
                        d.volFastWeight(), d.minSigma1sBps(), d.maxSingleReturnBps(),
                        d.maxWhipsawRangeBps(), d.parkinsonScale(), d.bridgeFloorBps(), d.maxJumpHazardBps(),
                        d.minWarmupSamples(), d.minOpenPriceMc(), d.maxOpenPriceMc(),
                        d.maxSpreadMc(), d.minDepthShares(), d.minNetEdgeMc(), d.minFillNetEdgeMc(),
                        d.minLeadGapCentiBps(), d.modelJumpWindowMs(), d.minModelJumpMc(),
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
    @DisplayName("Should satisfy Student-t df=4 symmetry, Bayesian jump shrinkage, and parabolic taker fee bounds")
    void shouldComputeExactStudentT4BayesianMixtureAndParabolicFee() {
        assertThat(PricingModelStrategy.studentT4ProbMc(0.0)).isEqualTo(500);
        int pPos = PricingModelStrategy.studentT4ProbMc(1.5);
        int pNeg = PricingModelStrategy.studentT4ProbMc(-1.5);
        assertThat(pPos + pNeg).isEqualTo(1_000);
        assertThat(pPos).isBetween(880, 920);

        // Bayesian jump mixture: zero shrinkage inside tau <= 45s, symmetric 7.5% shrinkage at tau >= 300s
        assertThat(PricingModelStrategy.bayesianJumpMixtureProbMc(900, 30_000L, 0.075)).isEqualTo(900);
        int shrunkUp = PricingModelStrategy.bayesianJumpMixtureProbMc(900, 350_000L, 0.075);
        int shrunkDown = PricingModelStrategy.bayesianJumpMixtureProbMc(100, 350_000L, 0.075);
        assertThat(shrunkUp).isEqualTo(870);
        assertThat(shrunkUp + shrunkDown).isEqualTo(1_000);

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
        strategy.onSpotTwap(staticTs - 3_000L, "btcusd", basePrice + 40.0, basePrice + 10.0);
        strategy.onSpotTwap(staticTs, "btcusd", basePrice + 40.0, basePrice + 15.0);
        strategy.onPolymarketCryptoPrice(staticTs + 100L, "btcusd", "chainlink", basePrice + 40.0, staticTs - 1_000L);
        strategy.onOrderBookQuote(new OrderBookQuote(0.52, 0.51, 0.49, 0.48, 200.0, 200.0, staticTs + 150L));
        assertThat(submittedOrders).isEmpty();

        // 2. Clip #1 on active impulse at ask = 0.52
        long jump1Ts = staticTs + 500L;
        strategy.onSpotTwap(jump1Ts, "btcusd", basePrice + 72.0, basePrice + 20.0);
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
        strategy.onOrderBookQuote(new OrderBookQuote(0.44, 0.43, 0.57, 0.56, 200.0, 200.0, avgDownTs - 50L));
        strategy.onSpotTwap(avgDownTs, "btcusd", basePrice + 52.0, basePrice + 15.0);
        assertThat(submittedOrders).hasSize(1);

        // 4. Continuation Clip #2 at higher ask = 0.58 (>= 0.52 - 0.01) is ACCEPTED
        long jump2Ts = avgDownTs + 4_000L;
        strategy.onPolymarketCryptoPrice(jump2Ts - 200L, "btcusd", "chainlink", basePrice + 52.0, jump2Ts - 1_400L);
        strategy.onOrderBookQuote(new OrderBookQuote(0.58, 0.57, 0.43, 0.42, 200.0, 200.0, jump2Ts - 50L));
        strategy.onSpotTwap(jump2Ts, "btcusd", basePrice + 105.0, basePrice + 25.0);
        assertThat(submittedOrders).hasSize(2);
        OrderCommand c2 = submittedOrders.get(1);
        strategy.onExecutionReport(new ExecutionReport(
                c2.clientOrderId(), "ex-2", "FILLED", new BigDecimal("0.580"), new BigDecimal("50"), jump2Ts + 16L, "Filled"));

        // 5. Clip #3 must be BLOCKED by maxOrdersPerMarket = 2 even when inventory remains
        long jump3Ts = jump2Ts + 4_000L;
        strategy.onPolymarketCryptoPrice(jump3Ts - 200L, "btcusd", "chainlink", basePrice + 105.0, jump3Ts - 1_400L);
        strategy.onOrderBookQuote(new OrderBookQuote(0.62, 0.61, 0.39, 0.38, 200.0, 200.0, jump3Ts - 50L));
        strategy.onSpotTwap(jump3Ts, "btcusd", basePrice + 160.0, basePrice + 35.0);
        assertThat(submittedOrders).hasSize(2);
    }

    @Test
    @DisplayName("Should allow clean >70bps directional trend while blocking >45bps two-way whipsaws")
    void shouldAllowCleanDirectionalBreakoutAndBlockTrueWhipsaw() {
        double strike = 80_000.0;
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", strike, 60, CANDLE_START_MS);

        long t0 = CANDLE_END_MS - 415_000L;
        for (int s = 0; s <= 18; s++) {
            long ts = t0 + s * 1_000L;
            double px = strike + ((s % 2 == 0) ? 20.0 : 0.0);
            strategy.onSpotTwap(ts, "btcusd", px, strike + 10.0);
            strategy.onPolymarketCryptoPrice(ts + 200L, "btcusd", "chainlink", px, ts - 1_000L);
        }
        for (int s = 19; s <= 34; s++) {
            long ts = t0 + s * 1_000L;
            double px = strike + ((s - 18) * 28.0);
            strategy.onSpotTwap(ts, "btcusd", px, strike + 200.0);
            strategy.onPolymarketCryptoPrice(ts + 200L, "btcusd", "chainlink", px, ts - 1_000L);
        }

        long breakoutTs = t0 + 34_500L; // sub-second burst at tau = 380.5s, clean +70 bps trend
        strategy.onOrderBookQuote(new OrderBookQuote(0.76, 0.75, 0.25, 0.24, 300.0, 300.0, breakoutTs - 50L));
        strategy.onSpotTwap(breakoutTs, "btcusd", strike + 560.0, strike + 300.0);
        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().token()).isEqualTo("BTC-UP");

        // Reset and verify >45 bps two-way V-whipsaw is blocked
        submittedOrders.clear();
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", strike, 60, CANDLE_START_MS);

        for (int s = 0; s <= 20; s++) {
            long ts = t0 + s * 1_000L;
            double px = strike + ((s % 2 == 0) ? 2.0 : -2.0);
            strategy.onSpotTwap(ts, "btcusd", px, strike);
            strategy.onPolymarketCryptoPrice(ts + 200L, "btcusd", "chainlink", px, ts - 1_000L);
        }
        long whipStartTs = t0 + 25_000L;
        strategy.onSpotTwap(whipStartTs, "btcusd", strike - 240.0, strike);
        strategy.onSpotTwap(whipStartTs + 1_000L, "btcusd", strike + 240.0, strike);
        strategy.onSpotTwap(whipStartTs + 2_000L, "btcusd", strike + 15.0, strike);
        strategy.onPolymarketCryptoPrice(whipStartTs + 3_500L, "btcusd", "chainlink", strike + 15.0, whipStartTs + 2_300L);

        long whipEvalTs = whipStartTs + 4_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(0.50, 0.49, 0.51, 0.50, 300.0, 300.0, whipEvalTs - 50L));
        strategy.onSpotTwap(whipEvalTs, "btcusd", strike + 60.0, strike);
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should apply exact Fractional Kelly c*(1-c) variance scaling and anchor venue basis")
    void shouldScaleSharesByFractionalKellyVarianceAndAnchorVenueBasis() {
        double strike = 80_000.0;
        double venueBias = 40.0;

        // 1. Neutral ATM entry (ask = 0.50, maximum binary variance c*(1-c) = 0.25) with modest +46 mc net edge
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", strike, 60, CANDLE_START_MS);
        long t0 = CANDLE_END_MS - 180_000L;
        for (int s = 0; s <= 25; s++) {
            long ts = t0 + s * 1_000L;
            double clPrice = strike + ((s % 2 == 0) ? 1.5 : -1.5);
            strategy.onSpotTwap(ts, "btcusd", clPrice + venueBias, strike + venueBias);
            strategy.onPolymarketCryptoPrice(ts + 1_200L, "btcusd", "chainlink", clPrice, ts);
        }
        long atmTs = t0 + 26_500L;
        strategy.onOrderBookQuote(new OrderBookQuote(0.50, 0.49, 0.51, 0.50, 400.0, 400.0, atmTs - 40L));
        strategy.onSpotTwap(atmTs, "btcusd", strike + venueBias + 23.0, strike + venueBias + 5.0);
        assertThat(submittedOrders).hasSize(1);
        int atmShares = submittedOrders.getFirst().size().intValue();
        assertThat(atmShares).isBetween(50, 80);

        // 2. Lower-variance near-expiry entry (ask = 0.86 at tau = 30s, c*(1-c) = 0.114) scales up Kelly conviction
        submittedOrders.clear();
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", strike, 60, CANDLE_START_MS);
        for (int s = 0; s <= 25; s++) {
            long ts = (CANDLE_END_MS - 60_000L) + s * 1_000L;
            double clPrice = strike - 6.0 + ((s % 2 == 0) ? 1.0 : -1.0);
            strategy.onSpotTwap(ts, "btcusd", clPrice + venueBias, strike + venueBias - 6.0);
            strategy.onPolymarketCryptoPrice(ts + 1_200L, "btcusd", "chainlink", clPrice, ts);
        }
        long lateTs = CANDLE_END_MS - 30_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(0.15, 0.14, 0.86, 0.85, 400.0, 400.0, lateTs - 40L));
        strategy.onSpotTwap(lateTs, "btcusd", strike + venueBias - 62.0, strike + venueBias - 32.0);
        assertThat(submittedOrders).hasSize(1);
        int kellyLowVarShares = submittedOrders.getFirst().size().intValue();
        assertThat(kellyLowVarShares).isGreaterThan(atmShares);
    }

    @Test
    @DisplayName("Should measure 1.25s impulse on unshrunk diffusive probability while pricing edge on Bayesian mixture")
    void shouldDecoupleUnshrunkImpulseFromBayesianEdgeValuation() {
        double strike = 80_000.0;
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", strike, 60, CANDLE_START_MS);

        long t0 = CANDLE_END_MS - 390_000L;
        for (int s = 0; s <= 30; s++) {
            long ts = t0 + s * 1_000L;
            double px = strike + s * 4.5;
            strategy.onSpotTwap(ts, "btcusd", px, strike + s * 2.0);
            if (s < 30) {
                strategy.onPolymarketCryptoPrice(ts + 200L, "btcusd", "chainlink", px, ts - 1_000L);
            }
        }
        // PolyBolt at preTs reflects lagged s = 29 ($80,130.5): pre-impulse rawT4 = 791 mc, Bayesian shrunk = 769 mc
        long preTs = t0 + 30_000L;
        strategy.onPolymarketCryptoPrice(preTs + 100L, "btcusd", "chainlink", strike + 130.5, preTs - 1_000L);

        // Sub-second impulse to spot = strike + 148.0: rawT4 = 817 mc (raw jump +26 mc >= 25), Bayesian shrunk = 793 mc (shrunk jump +24 mc)
        long evalTs = preTs + 500L;

        // 1. Overpriced ask = 0.76 (cost ~773 mc) has +44 mc raw edge, but only +20 mc Bayesian edge (< 40 mc) -> REJECTED
        strategy.onOrderBookQuote(new OrderBookQuote(0.76, 0.75, 0.25, 0.24, 300.0, 300.0, evalTs - 40L));
        strategy.onSpotTwap(evalTs, "btcusd", strike + 148.0, strike + 65.0);
        assertThat(submittedOrders).isEmpty();

        // 2. Fair ask = 0.72 (cost ~735 mc, Bayesian edge +58 mc) passes because unshrunk jump is +26 mc (>= 25 mc)
        strategy.onOrderBookQuote(new OrderBookQuote(0.72, 0.71, 0.29, 0.28, 300.0, 300.0, evalTs + 20L));
        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().token()).isEqualTo("BTC-UP");
    }
}
