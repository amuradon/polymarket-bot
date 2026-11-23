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
        assertThat(strategy.getRequiredClobLookbackMs()).isEqualTo(490_000L);

        assertThatThrownBy(() -> PricingModelStrategyConfig.unwrap(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strategy.init(null))
                .isInstanceOf(IllegalArgumentException.class);

        // Invalid tau bounds
        assertThatThrownBy(() -> new PricingModelStrategyConfig(
                "15m", 500_000L, 100_000L, 3_000L, 2_500L, 75L,
                0.25, 30.0, 180.0, 0.55, 0.45, 30.0, 0.70, 15,
                250, 920, 25, 25, 40, 25, 220, 1_250L, 25, 10, 700,
                25, 150, 250, "pm-btc-15m", "BTC-UP", "BTC-DOWN"))
                .isInstanceOf(IllegalArgumentException.class);

        class ProxiedConfig extends PricingModelStrategyConfig {
            ProxiedConfig(PricingModelStrategyConfig d) {
                super(d.timeframe().getCode(), d.minTauMs(), d.maxTauMs(), d.cooldownMs(), d.maxQuoteAgeMs(),
                        d.spotDelayOffsetMs(), d.basisEmaAlpha(), d.volFastTauSec(), d.volSlowTauSec(),
                        d.volFastWeight(), d.minSigma1sBps(), d.maxSingleReturnBps(), d.bridgeFloorBps(),
                        d.minWarmupSamples(), d.minOpenPriceMc(), d.maxOpenPriceMc(), d.maxSpreadMc(),
                        d.minDepthShares(), d.minNetEdgeMc(), d.minFillNetEdgeMc(), d.minLeadGapCentiBps(),
                        d.modelJumpWindowMs(), d.minModelJumpMc(), d.maxSlippageMc(), d.takerFeeRateBps(),
                        d.minOrderShares(), d.maxOrderShares(), d.maxInventoryShares(),
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
        assertThat(PricingModelStrategy.takerFeeMc(500, 0.07)).isEqualTo(18); // ceil(17.5) = 18 mc
        assertThat(PricingModelStrategy.takerFeeMc(900, 0.07)).isEqualTo(7);  // ceil(6.3) = 7 mc
    }

    @Test
    @DisplayName("Should reject static mispricing without active oracle lead-gap and trigger immediately on spot impulse")
    void shouldRejectStaticMispricingAndTriggerOnActiveDesynchronization() {
        double basePrice = 80_000.0;
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", basePrice, 60, CANDLE_START_MS);

        // Warm up 25 seconds of calm oscillations inside the active trading window (tau = 180s -> 155s)
        long t0 = CANDLE_END_MS - 180_000L;
        for (int s = 0; s <= 25; s++) {
            long ts = t0 + s * 1_000L;
            double px = basePrice + ((s % 2 == 0) ? 2.0 : -2.0);
            strategy.onSpotTwap(ts, "btcusd", px, basePrice);
            strategy.onPolymarketCryptoPrice(ts + 200L, "btcusd", "chainlink", px, ts - 1_000L);
        }

        // 1. Static mispricing: underlying sits higher (+30 USD), BUT PolyBolt already caught up (LeadGap = 0 bps!)
        long staticTs = t0 + 30_000L;
        strategy.onSpotTwap(staticTs - 2_000L, "btcusd", basePrice + 35.0, basePrice + 10.0);
        strategy.onSpotTwap(staticTs, "btcusd", basePrice + 35.0, basePrice + 15.0);
        strategy.onPolymarketCryptoPrice(staticTs + 100L, "btcusd", "chainlink", basePrice + 35.0, staticTs - 1_000L);

        // Cheap UP ask = 0.52 even though fair prob > 0.70, but zero lead-gap and zero fresh model jump -> NO ORDER
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.52, 0.51, 0.49, 0.48, 200.0, 200.0, 0.52, 0.49, staticTs + 150L));
        assertThat(submittedOrders).isEmpty();

        // 2. Active Latency Desynchronization: resting CLOB quote sits at 0.52 while SpotTwapIndicator jumps +32 bps (+25.6 USD)
        // before PolyBolt publishes the new tick -> triggers immediately inside onSpotTwap!
        long jumpTs = staticTs + 500L;
        strategy.onSpotTwap(jumpTs, "btcusd", basePrice + 62.0, basePrice + 20.0);

        assertThat(submittedOrders).hasSize(1);
        OrderCommand order = submittedOrders.getFirst();
        assertThat(order.side()).isEqualTo("BUY");
        assertThat(order.token()).isEqualTo("BTC-UP");
        assertThat(order.price()).isEqualByComparingTo(new BigDecimal("0.530")); // ask 0.520 + 0.010 slippage
        assertThat(order.size().intValue()).isBetween(config.minOrderShares(), 100);
        assertThat(balanceRefreshCount).isEqualTo(1);

        // Complete fill and verify single-direction lock prevents opposite-side whipsaw in same market
        strategy.onExecutionReport(new ExecutionReport(
                order.clientOrderId(), "ex-1", "FILLED",
                new BigDecimal("0.520"), order.size(), jumpTs + 16L, "Filled"));

        long revTs = jumpTs + 4_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.54, 0.46, 0.45, 200.0, 200.0, 0.55, 0.46, revTs - 50L));
        strategy.onSpotTwap(revTs, "btcusd", basePrice - 80.0, basePrice);
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should compensate venue basis offset and integrate locked-in 60s TWAP in final minute")
    void shouldAnchorVenueBasisAndIntegrateFinalMinuteTwap() {
        double strike = 80_000.0;
        double venueBias = 40.0; // SpotTwapIndicator systematically +$40 (+5 bps) above Chainlink!
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS + 200L, "btcusd", "chainlink", strike, 60, CANDLE_START_MS);

        long t0 = CANDLE_END_MS - 95_000L;
        for (int s = 0; s <= 40; s++) {
            long ts = t0 + s * 1_000L;
            double clPrice = strike + ((s % 2 == 0) ? 1.5 : -1.5);
            strategy.onSpotTwap(ts, "btcusd", clPrice + venueBias, strike + venueBias);
            strategy.onPolymarketCryptoPrice(ts + 1_200L, "btcusd", "chainlink", clPrice, ts);
        }

        // At tau = 53s, true Chainlink is right at strike (80,000), while raw SpotTwap is 80,040 (+5 bps).
        // Without basis anchoring, strategy would hallucinate UP edge; with basis anchoring, sHat ~= 80,000 -> NO ORDER!
        long checkTs = CANDLE_END_MS - 53_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.50, 0.49, 0.51, 0.50, 300.0, 300.0, 0.50, 0.51, checkTs));
        assertThat(submittedOrders).isEmpty();

        // Now genuine DOWN impulse (-30 bps = -$24) happens at tau = 30s while CLOB DOWN ask is stale at 0.51 -> triggers DOWN!
        long downJumpTs = CANDLE_END_MS - 30_000L;
        strategy.onPolymarketCryptoPrice(downJumpTs - 200L, "btcusd", "chainlink", strike, downJumpTs - 1_400L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.50, 0.49, 0.51, 0.50, 300.0, 300.0, 0.50, 0.51, downJumpTs - 50L));
        strategy.onSpotTwap(downJumpTs, "btcusd", strike + venueBias - 28.0, strike + venueBias);

        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().token()).isEqualTo("BTC-DOWN");
    }
}
