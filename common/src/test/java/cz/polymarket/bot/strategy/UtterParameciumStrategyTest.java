package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.OrderBookQuote;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Deterministic synthetic unit tests for {@link UtterParameciumStrategy} and {@link UtterParameciumStrategyConfig}.
 */
class UtterParameciumStrategyTest {

    private static final long CANDLE_START_SEC = 1_785_542_400L;
    private static final long CANDLE_END_SEC = CANDLE_START_SEC + 900L;
    private static final long CANDLE_START_MS = CANDLE_START_SEC * 1000L;
    private static final long CANDLE_END_MS = CANDLE_END_SEC * 1000L;

    private UtterParameciumStrategyConfig config;
    private UtterParameciumStrategy strategy;
    private List<OrderCommand> submittedOrders;
    private int balanceRefreshCount;
    private StrategyContext testContext;

    @BeforeEach
    void setUp() {
        config = UtterParameciumStrategyConfig.defaultModelV1();
        strategy = new UtterParameciumStrategy(config);
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

        testContext = new StrategyContext() {
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
    @DisplayName("Should validate heavyTailCdfMc conservatism, TWAP_REM_WEIGHT_BPS, and sub-cent tick rounding helpers")
    void shouldValidateHeavyTailCdfTwapWeightsAndTickRounding() {
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS).hasSize(62);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[0]).isZero();
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[5]).isEqualTo(1236);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[30]).isEqualTo(16730);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[60]).isEqualTo(47730);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[61]).isEqualTo(48937);

        assertThat(UtterParameciumStrategy.heavyTailCdfMc(0L)).isEqualTo(500);
        assertThat(UtterParameciumStrategy.heavyTailCdfMc(55L) + UtterParameciumStrategy.heavyTailCdfMc(-55L)).isEqualTo(1000);
        assertThat(UtterParameciumStrategy.heavyTailCdfMc(230L) + UtterParameciumStrategy.heavyTailCdfMc(-230L)).isEqualTo(1000);

        // Heavy-tail calibration must avoid Gaussian overconfidence in [1.8, 4.0] sigma while increasing late lock-in
        assertThat(UtterParameciumStrategy.heavyTailCdfMc(180L)).isBetween(895, 930);
        assertThat(UtterParameciumStrategy.heavyTailCdfMc(230L)).isBetween(925, 960);
        assertThat(UtterParameciumStrategy.heavyTailCdfMc(280L)).isBetween(945, 975);
        assertThat(UtterParameciumStrategy.heavyTailCdfMc(380L)).isBetween(968, 990);
        assertThat(UtterParameciumStrategy.modelProbCdfMc(250L, 12_000L))
                .isGreaterThan(UtterParameciumStrategy.modelProbCdfMc(250L, 50_000L));
        assertThat(UtterParameciumStrategy.heavyTailCdfMc(1200L)).isEqualTo(998);
        assertThat(UtterParameciumStrategy.heavyTailCdfMc(-1200L)).isEqualTo(2);

        assertThat(UtterParameciumStrategy.fromMcFloorTick(955)).isEqualTo(950);
        assertThat(UtterParameciumStrategy.fromMcFloorTick(640)).isEqualTo(640);
        assertThat(UtterParameciumStrategy.fromMcCeilTick(965)).isEqualTo(970);
        assertThat(UtterParameciumStrategy.fromMcCeilTick(650)).isEqualTo(650);
        assertThat(UtterParameciumStrategy.fromMcCeilTick(995)).isEqualTo(1000);

        assertThatThrownBy(() -> UtterParameciumStrategy.fromMcFloorTick(1001))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> UtterParameciumStrategy.fromMcCeilTick(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should skip market with WARN when explicit 60s TWAP target price is missing")
    void shouldSkipMarketWhenTargetPriceIsMissing() {
        assertThat(strategy.getTargetPriceCents()).isZero();

        long tau30Ms = CANDLE_END_MS - 30_000L;
        strategy.onPolymarketCryptoPrice(tau30Ms, "btcusd", "chainlink", 90_050.00, tau30Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tau30Ms
        ));

        assertThat(strategy.isMissingTargetPriceWarned()).isTrue();
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should ignore prior-second TWAP packet, lock candle-start target, and hand off boundary TWAP to next market")
    void shouldIgnorePriorSecondTwapPacketAndLockStartAndBoundaryTargetPrice() {
        strategy.onPolymarketCryptoTwap60s(
                CANDLE_START_MS + 70L, "btcusd", "chainlink", 89_998.00, 60, CANDLE_START_MS - 1_000L
        );
        assertThat(strategy.getTargetPriceCents()).isZero();

        strategy.onPolymarketCryptoTwap60s(
                CANDLE_START_MS + 1_100L, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS
        );
        assertThat(strategy.getTargetPriceCents()).isEqualTo(9_000_000L);

        strategy.onPolymarketCryptoTwap60s(
                CANDLE_START_MS + 2_100L, "btcusd", "chainlink", 90_005.00, 60, CANDLE_START_MS + 1_000L
        );
        assertThat(strategy.getTargetPriceCents()).isEqualTo(9_000_000L);

        // Boundary packet at exact CANDLE_END_MS hands off target price to next 15m market
        strategy.onPolymarketCryptoTwap60s(
                CANDLE_END_MS + 50L, "btcusd", "chainlink", 90_123.45, 60, CANDLE_END_MS
        );
        strategy.onPolymarketCryptoPrice(CANDLE_END_MS + 1_000L, "btcusd", "chainlink", 90_125.00, CANDLE_END_MS + 1_000L);
        assertThat(strategy.getTargetPriceCents()).isEqualTo(9_012_345L);
    }

    @Test
    @DisplayName("Should snipe stale ask immediately on de-biased onCalculatedSpotTwap breakout before Chainlink updates")
    void shouldFuseDeBiasedCalculatedSpotTwapLeadBeforeChainlinkUpdates() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // Up to tau = 46s, Chainlink oscillates around -$2.25 ($89,997.75) while CalculatedSpot has -$2.00 basis ($89,995.75)
        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int s = 0; s <= 14; s++) {
            long ts = windowStartMs + (s * 1000L);
            double osc = (s % 2 == 0) ? 0.75 : -0.75;
            strategy.onCalculatedSpotTwap(ts - 200L, "btcusd", 89_995.00 + osc, 89_995.00);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_997.00 + osc, ts);
        }

        long tau46Ms = CANDLE_END_MS - 46_000L;
        // Resting CLOB quote prices UP cheaply at $0.46 while DOWN is fairly priced at $0.72 (no trade before breakout)
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.46, 0.44, 0.72, 0.70, 100.0, 100.0, 0.46, 0.72, tau46Ms + 100L
        ));
        assertThat(submittedOrders).isEmpty();

        // At tau = 45.6s (400ms after Chainlink tick), direct multi-exchange spot surges +$20.00 ($89,995.75 -> $90,015.75)!
        // De-biased spot = $89,997.75 + 0.85 * $20 = $90,014.75 -> projected TWAP jumps solidly above $90,000 strike
        // BEFORE Chainlink publishes its next 1s tick!
        long breakoutTs = tau46Ms + 400L;
        strategy.onCalculatedSpotTwap(breakoutTs, "btcusd", 90_015.75, 89_996.00);

        assertThat(submittedOrders).hasSize(1);
        OrderCommand snipeOrder = submittedOrders.getFirst();
        assertThat(snipeOrder.token()).isEqualTo("BTC-UP");
        assertThat(snipeOrder.side()).isEqualTo("BUY");
        assertThat(snipeOrder.price()).isBetween(new BigDecimal("0.460"), new BigDecimal("0.490"));
        assertThat(balanceRefreshCount).isEqualTo(1);
    }

    @Test
    @DisplayName("Should emit UP signal in late TWAP lock-in when spot dips below strike but locked TWAP dominates")
    void shouldEmitUpSignalWhenSpotDropsBelowStrikeButProjectedTwapRemainsAboveStrike() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int s = 0; s < 40; s++) {
            long ts = windowStartMs + (s * 1000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_025.00, ts);
        }

        long tau20Ms = CANDLE_END_MS - 20_000L;
        strategy.onPolymarketCryptoPrice(tau20Ms, "btcusd", "chainlink", 89_995.00, tau20Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tau20Ms
        ));

        assertThat(submittedOrders).hasSize(1);
        OrderCommand order = submittedOrders.getFirst();
        assertThat(order.token()).isEqualTo("BTC-UP");
        assertThat(order.side()).isEqualTo("BUY");
        assertThat(order.price()).isEqualByComparingTo("0.680");
        assertThat(order.size().intValue()).isBetween(5, 80);
    }

    @Test
    @DisplayName("Should capture cheap underdog dislocations (< $0.40) and wide volatility spreads ($0.20) in low-vol regimes")
    void shouldCaptureCheapDislocationAndWideSpreadInLowVolRegime() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // Quiet low-volatility market ($0.65/s MAD floor): BTC holds +$15.50 above strike (> 3.0 sigma)
        long preWindowStartMs = CANDLE_END_MS - 65_000L;
        for (int s = 0; s <= 20; s++) {
            long ts = preWindowStartMs + (s * 1000L);
            double p = 90_015.50 + ((s % 2 == 0) ? 0.25 : -0.25);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", p, ts);
        }

        // CLOB makers widen spread (bid=0.14, ask=0.32 -> spread=180 mc) offering cheap $0.32 UP ask
        long tau45Ms = CANDLE_END_MS - 45_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.32, 0.14, 0.86, 0.68, 200.0, 200.0, 0.32, 0.86, tau45Ms
        ));

        assertThat(submittedOrders).hasSize(1);
        OrderCommand order = submittedOrders.getFirst();
        assertThat(order.token()).isEqualTo("BTC-UP");
        assertThat(order.size().intValue()).isGreaterThanOrEqualTo(15);
    }

    @Test
    @DisplayName("Should preserve edge-preserving limit price tolerance up to maxSlippageMc for cross-region latency")
    void shouldComputeEdgePreservingLimitPriceCappedByMinEdgeAndMaxSlippage() {
        assertThat(config.maxSlippageMc()).isEqualTo(30);

        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // At tau = 55s with defaultSigma1s = 150c -> sigmaRem = 627c.
        // Spot = $90,004.35 (+435c) -> z = 69 centi-sigma -> modelProbCdfMc(69, 55s) = 746 mc.
        // At askUp = $0.69 (690 mc): fee = 15 mc, edge = 746 - 690 - 15 = 41 mc >= minEdge (22 mc).
        // At 700 mc: fee = 15 mc, edge = 31 mc >= 22 mc.
        // At 710 mc: fee = 14 mc, edge = 22 mc >= 22 mc.
        // At 720 mc: fee = 14 mc, edge = 12 mc < 22 mc -> must cap limit price at $0.710, NOT $0.720!
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_004.35, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.69, 0.67, 0.33, 0.31, 500.0, 500.0, 0.69, 0.33, tau55Ms
        ));

        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().price()).isEqualByComparingTo("0.710");
    }

    @Test
    @DisplayName("Should pace early share exposure and scale high-edge dislocations well past 600 shares as TWAP locks in")
    void shouldBoundHighPriceExposureAndScaleHighEdgeDislocations() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. High-price favorite ($0.92 ask) in early window (tau = 58s -> 48s): paced risk keeps exposure small
        long t0 = CANDLE_END_MS - 58_000L;
        for (int i = 0; i < 40; i++) {
            long ts = t0 + i * 250L;
            int before = submittedOrders.size();
            if (i % 4 == 0) {
                strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_035.00, ts);
            }
            if (submittedOrders.size() > before) {
                OrderCommand cmd = submittedOrders.getLast();
                strategy.onExecutionReport(new ExecutionReport(
                        cmd.clientOrderId(), "fill-p-" + i, "FILLED", cmd.price(), cmd.size(), ts + 5L, "Filled"
                ));
                before = submittedOrders.size();
            }
            strategy.onOrderBookQuote(new OrderBookQuote(
                    0.92, 0.90, 0.10, 0.08, 300.0, 300.0, 0.92, 0.10, ts + 10L
            ));
            if (submittedOrders.size() > before) {
                OrderCommand cmd = submittedOrders.getLast();
                strategy.onExecutionReport(new ExecutionReport(
                        cmd.clientOrderId(), "fill-" + i, "FILLED", cmd.price(), cmd.size(), ts + 15L, "Filled"
                ));
            }
        }
        assertThat(strategy.getNetSharesUp()).isBetween(30, 160);

        // 2. Deep dislocation ($0.28 ask with strong edge): paced early, then scales well past 900 shares by tau = 18s!
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        int earlySharesAtTau50 = 0;
        for (int i = 0; i <= 80; i++) {
            long ts = t0 + i * 500L;
            int before = submittedOrders.size();
            if (i % 2 == 0) {
                strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_025.00, ts);
            }
            if (submittedOrders.size() > before) {
                OrderCommand cmd = submittedOrders.getLast();
                strategy.onExecutionReport(new ExecutionReport(
                        cmd.clientOrderId(), "fill-deep-p-" + i, "FILLED", cmd.price(), cmd.size(), ts + 5L, "Filled"
                ));
                before = submittedOrders.size();
            }
            strategy.onOrderBookQuote(new OrderBookQuote(
                    0.28, 0.25, 0.75, 0.72, 500.0, 500.0, 0.28, 0.75, ts + 10L
            ));
            if (submittedOrders.size() > before) {
                OrderCommand cmd = submittedOrders.getLast();
                strategy.onExecutionReport(new ExecutionReport(
                        cmd.clientOrderId(), "fill-deep-" + i, "FILLED", cmd.price(), cmd.size(), ts + 15L, "Filled"
                ));
            }
            if (i == 16) {
                earlySharesAtTau50 = strategy.getNetSharesUp();
            }
        }
        assertThat(earlySharesAtTau50).isLessThan(900);
        assertThat(strategy.getNetSharesUp()).isGreaterThan(950);
    }

    @Test
    @DisplayName("Should refuse negative-EV cross-side noise hedge while flipping decisively on genuine reversal")
    void shouldRefuseNegativeEvCrossSideHedgeAndFlipOnGenuineReversal() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Enter UP @ $0.54 at tau = 56s
        long t1 = CANDLE_END_MS - 56_000L;
        strategy.onPolymarketCryptoPrice(t1, "btcusd", "chainlink", 90_020.00, t1);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.54, 0.52, 0.48, 0.46, 100.0, 100.0, 0.54, 0.48, t1
        ));
        assertThat(submittedOrders).hasSize(1);
        OrderCommand upCmd = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                upCmd.clientOrderId(), "f1", "FILLED", BigDecimal.valueOf(0.54), BigDecimal.valueOf(40), t1 + 10L, "Filled"
        ));
        assertThat(strategy.getNetSharesUp()).isEqualTo(40);

        // 2. Transient dip where BTC is still above target (+$1.00): must NOT hedge against winning UP leg at $0.40 DOWN!
        long t2 = CANDLE_END_MS - 50_000L;
        strategy.onPolymarketCryptoPrice(t2, "btcusd", "chainlink", 90_001.00, t2);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.62, 0.60, 0.40, 0.38, 100.0, 100.0, 0.62, 0.40, t2
        ));
        assertThat(submittedOrders).hasSize(1);

        // 3. Genuine reversal: BTC drops decisively below strike (-$35.00) at tau = 30s -> flips directionally to DOWN!
        long t3 = CANDLE_END_MS - 30_000L;
        strategy.onPolymarketCryptoPrice(t3, "btcusd", "chainlink", 89_965.00, t3);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.35, 0.25, 0.72, 0.62, 100.0, 100.0, 0.35, 0.72, t3
        ));
        assertThat(submittedOrders).hasSize(2);
        assertThat(submittedOrders.get(1).token()).isEqualTo("BTC-DOWN");
    }

    @Test
    @DisplayName("Should anchor settlement projection on live 60s TWAP rolling-shift and cap deficit-chasing inventory")
    void shouldAnchorSettlementProjectionOnLiveTwapAndCapDeficitChasingShares() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // Pre-window and first 25s of settlement window (tau = 90s -> 35s): BTC holds +$50 above target ($90,050)
        long startPreMs = CANDLE_END_MS - 90_000L;
        for (int s = 0; s <= 55; s++) {
            long ts = startPreMs + s * 1_000L;
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_050.00, ts);
            strategy.onPolymarketCryptoTwap60s(ts + 50L, "btcusd", "chainlink", 90_050.00, 60, ts);
        }

        // From tau = 34s down to tau = 20s, BTC drops below strike ($89,955 = -$45) while banked TWAP remains positive for UP.
        // Because banked TWAP still opposes DOWN (lockedDistDown < 0), DOWN must be strictly capped <= 320 shares!
        for (int tauSec = 34; tauSec >= 20; tauSec--) {
            long ts = CANDLE_END_MS - tauSec * 1_000L;
            int before = submittedOrders.size();
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_955.00, ts);
            if (submittedOrders.size() > before) {
                OrderCommand cmd = submittedOrders.getLast();
                strategy.onExecutionReport(new ExecutionReport(
                        cmd.clientOrderId(), "dc-p-" + tauSec, "FILLED", cmd.price(), cmd.size(), ts + 5L, "Filled"
                ));
                before = submittedOrders.size();
            }
            for (int q = 0; q < 3; q++) {
                long qTs = ts + 200L + q * 250L;
                strategy.onOrderBookQuote(new OrderBookQuote(
                        0.38, 0.36, 0.64, 0.62, 500.0, 500.0, 0.38, 0.64, qTs
                ));
                if (submittedOrders.size() > before) {
                    OrderCommand cmd = submittedOrders.getLast();
                    strategy.onExecutionReport(new ExecutionReport(
                            cmd.clientOrderId(), "dc-q-" + tauSec + "-" + q, "FILLED", cmd.price(), cmd.size(), qTs + 5L, "Filled"
                    ));
                    before = submittedOrders.size();
                }
            }
        }
        assertThat(strategy.getNetSharesDown()).isBetween(5, 320);

        // At tau = 6s, live PolyBolt 60s TWAP reports $90,018.00 (+$18 above target) while spot rebounds to $89,985:
        // Rolling-shift projection knows UP will still win settlement, so zero additional DOWN orders may fire!
        int ordersBeforeLate = submittedOrders.size();
        long tau6Ms = CANDLE_END_MS - 6_000L;
        strategy.onPolymarketCryptoPrice(tau6Ms, "btcusd", "chainlink", 89_985.00, tau6Ms);
        strategy.onPolymarketCryptoTwap60s(tau6Ms + 40L, "btcusd", "chainlink", 90_018.00, 60, tau6Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.35, 0.33, 0.67, 0.65, 500.0, 500.0, 0.35, 0.67, tau6Ms + 100L
        ));
        assertThat(submittedOrders.stream().skip(ordersBeforeLate).filter(o -> "BTC-DOWN".equals(o.token())).count())
                .isZero();
    }

    @Test
    @DisplayName("Should allow cheap high-conviction directional flip (< $0.48) when banked TWAP already supports new side")
    void shouldAllowCheapHighConvictionFlipWhenBankedTwapSupportsNewSide() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Small initial UP fill at tau = 58s (+$18 above target)
        long t1 = CANDLE_END_MS - 58_000L;
        strategy.onPolymarketCryptoPrice(t1, "btcusd", "chainlink", 90_018.00, t1);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.62, 0.60, 0.40, 0.38, 100.0, 100.0, 0.62, 0.40, t1
        ));
        assertThat(submittedOrders).hasSize(1);
        OrderCommand firstCmd = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                firstCmd.clientOrderId(), "f-init-up", "FILLED", BigDecimal.valueOf(0.62), BigDecimal.valueOf(25), t1 + 10L, "Filled"
        ));

        // 2. Sustained BTC crash (-$35 below target) from tau = 57s to tau = 30s builds positive banked TWAP for DOWN
        for (int tauSec = 57; tauSec >= 30; tauSec--) {
            long ts = CANDLE_END_MS - tauSec * 1_000L;
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_965.00, ts);
        }

        // Stale CLOB offers cheap DOWN ask at $0.34 (< $0.48): high-conviction flip with banked TWAP must execute!
        long tau30Ms = CANDLE_END_MS - 30_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 200.0, 200.0, 0.68, 0.34, tau30Ms + 100L
        ));
        assertThat(submittedOrders).hasSize(2);
        assertThat(submittedOrders.get(1).token()).isEqualTo("BTC-DOWN");
    }

    @Test
    @DisplayName("Should trade down to tau = 1s while skipping decided markets and sub-second expiry")
    void shouldTradeDownToFinalSecondAndSkipDecidedOrExpiredMarkets() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // Decided market (bestAsk = $0.990 > maxEntryPriceMc) must be skipped
        long tau30Ms = CANDLE_END_MS - 30_000L;
        strategy.onPolymarketCryptoPrice(tau30Ms, "btcusd", "chainlink", 90_120.00, tau30Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.990, 0.980, 0.020, 0.010, 1000.0, 1000.0, 0.990, 0.020, tau30Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // Outside maxTauMs (tau = 65s) must be skipped
        long tau65Ms = CANDLE_END_MS - 65_000L;
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        strategy.onPolymarketCryptoPrice(tau65Ms, "btcusd", "chainlink", 90_050.00, tau65Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau65Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // Late terminal lock-in at tau = 2.5s (>= minTauMs = 1000ms) MUST be allowed!
        long tau2500Ms = CANDLE_END_MS - 2_500L;
        strategy.onPolymarketCryptoPrice(tau2500Ms, "btcusd", "chainlink", 90_020.00, tau2500Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.85, 0.83, 0.17, 0.15, 500.0, 500.0, 0.85, 0.17, tau2500Ms
        ));
        assertThat(submittedOrders).hasSize(1);

        // Below minTauMs (tau = 500ms < 1000ms) must be skipped
        OrderCommand cmd = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                cmd.clientOrderId(), "f-late", "FILLED", cmd.price(), cmd.size(), tau2500Ms + 20L, "Filled"
        ));
        long tau500Ms = CANDLE_END_MS - 500L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.85, 0.83, 0.17, 0.15, 500.0, 500.0, 0.85, 0.17, tau500Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should block entry during early-window spot reversal when adverse spot exceeds thin TWAP cushion at normal prices")
    void shouldBlockEntryDuringEarlyWindowSpotReversalWhenSpotDisagreesWithProjectedTwap() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int s = 0; s < 12; s++) {
            long ts = windowStartMs + (s * 1000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_970.00, ts);
        }

        // At tau = 48s, spot jumps above strike (+$4.00) while projected TWAP is still -$2.80:
        // UP has negative projected TWAP, and DOWN has adverse spot > projected cushion at normal price ($0.47).
        long tau48Ms = CANDLE_END_MS - 48_000L;
        strategy.onPolymarketCryptoPrice(tau48Ms, "btcusd", "chainlink", 90_004.00, tau48Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.53, 0.47, 0.45, 500.0, 500.0, 0.55, 0.47, tau48Ms
        ));

        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should trigger refreshAccountBalance once per market at tau <= 71000 ms and declare zero external streams")
    void shouldTriggerRefreshAccountBalanceOncePerMarketAndDeclareZeroExternalStreams() {
        assertThat(strategy.getRequiredExternalStreams()).isEmpty();
        assertThat(strategy.getRequiredIndicators()).isEmpty();
        assertThat(strategy.getRequiredClobLookbackMs()).isEqualTo(76_000L);

        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        strategy.onPolymarketCryptoPrice(CANDLE_END_MS - 72_000L, "btcusd", "chainlink", 90_010.00, CANDLE_END_MS - 72_000L);
        assertThat(balanceRefreshCount).isZero();

        strategy.onPolymarketCryptoPrice(CANDLE_END_MS - 71_000L, "btcusd", "chainlink", 90_012.00, CANDLE_END_MS - 71_000L);
        assertThat(balanceRefreshCount).isEqualTo(1);

        strategy.onPolymarketCryptoPrice(CANDLE_END_MS - 60_000L, "btcusd", "chainlink", 90_015.00, CANDLE_END_MS - 60_000L);
        assertThat(balanceRefreshCount).isEqualTo(1);
    }

    @Test
    @DisplayName("Should ignore duplicate or older crypto price ticks and block historical snapshot evaluation against newer live quote")
    void shouldIgnoreDuplicateOrOlderCryptoPriceTicksAndBlockHistoricalSnapshotEvaluation() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long tau20Ms = CANDLE_END_MS - 20_000L;
        long tau50Ms = CANDLE_END_MS - 50_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.52, 0.50, 0.52, 0.50, 500.0, 500.0, 0.51, 0.51, tau20Ms
        ));
        strategy.onPolymarketCryptoPrice(tau50Ms, "btcusd", "chainlink", 90_080.00, tau50Ms);
        assertThat(strategy.getLastChainlinkPriceCents()).isEqualTo(9_008_000L);
        assertThat(submittedOrders).isEmpty();

        strategy.onPolymarketCryptoPrice(tau20Ms, "btcusd", "chainlink", 90_000.00, tau20Ms);
        strategy.onPolymarketCryptoPrice(CANDLE_END_MS - 35_000L, "btcusd", "chainlink", 90_095.00, CANDLE_END_MS - 35_000L);
        assertThat(strategy.getLastChainlinkPriceCents()).isEqualTo(9_000_000L);
        assertThat(submittedOrders).isEmpty();

        long tau19Ms = CANDLE_END_MS - 19_000L;
        strategy.onPolymarketCryptoPrice(tau19Ms, "btcusd", "chainlink", 90_080.00, tau19Ms);
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should reject weak mid-price directional whipsaw flip while allowing decisive high-conviction reversal")
    void shouldRejectWeakMidPriceDirectionalFlipWhileAllowingDecisiveHighConvictionReversal() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long t1 = CANDLE_END_MS - 50_000L;
        strategy.onPolymarketCryptoPrice(t1, "btcusd", "chainlink", 90_018.00, t1);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.62, 0.60, 0.40, 0.38, 200.0, 200.0, 0.62, 0.40, t1
        ));
        assertThat(submittedOrders).hasSize(1);
        OrderCommand firstCmd = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                firstCmd.clientOrderId(), "fill-leg1", "FILLED", BigDecimal.valueOf(0.62), BigDecimal.valueOf(40), t1 + 10L, "Filled"
        ));

        // Weak noise wiggle across target (-$1.50 -> z < 1.35 sigma) at DOWN ask = $0.58: must NOT whipsaw flip!
        long t2 = CANDLE_END_MS - 40_000L;
        strategy.onPolymarketCryptoPrice(t2, "btcusd", "chainlink", 89_998.50, t2);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.44, 0.42, 0.58, 0.56, 200.0, 200.0, 0.44, 0.58, t2
        ));
        assertThat(submittedOrders).hasSize(1);

        // Decisive reversal (DOWN ask = $0.68 >= $0.44, BTC -$35.00 -> > 2.0 sigma with favorable momentum): MUST flip!
        long t3 = CANDLE_END_MS - 28_000L;
        strategy.onPolymarketCryptoPrice(t3, "btcusd", "chainlink", 89_965.00, t3);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.34, 0.32, 0.68, 0.66, 200.0, 200.0, 0.34, 0.68, t3
        ));
        assertThat(submittedOrders).hasSize(2);
        assertThat(submittedOrders.get(1).token()).isEqualTo("BTC-DOWN");
    }

    @Test
    @DisplayName("Should block toxic averaging-down when ask drops sharply from side peak during adverse BTC move")
    void shouldBlockToxicAveragingDownWhenAskDropsDuringAdverseSpotMove() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long t1 = CANDLE_END_MS - 52_000L;
        strategy.onPolymarketCryptoPrice(t1, "btcusd", "chainlink", 90_030.00, t1);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.88, 0.86, 0.14, 0.12, 200.0, 200.0, 0.88, 0.14, t1
        ));
        assertThat(submittedOrders).hasSize(1);
        OrderCommand firstCmd = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                firstCmd.clientOrderId(), "fill-peak", "FILLED", BigDecimal.valueOf(0.88), BigDecimal.valueOf(25), t1 + 10L, "Filled"
        ));

        // 4 seconds later, BTC plunges -$16 toward target ($90,030 -> $90,014) and CLOB slashes UP ask from $0.88 to $0.58:
        // Despite naive model probability still > $0.58, toxic averaging-down guard must block buying the falling knife!
        long t2 = t1 + 4_000L;
        strategy.onPolymarketCryptoPrice(t2, "btcusd", "chainlink", 90_014.00, t2);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.58, 0.55, 0.45, 0.42, 200.0, 200.0, 0.58, 0.45, t2
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should allow flip hedge up to opposite leg shares while capping unhedged whipsaw reversal")
    void shouldAllowFlipHedgeUpToOppositeSharesAndCapUnhedgedWhipsawReversal() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Quiet volatility warmup (~$0.90/s) followed by an initial 200-share UP fill at tau = 58s (+$6.00)
        long preWindowStartMs = CANDLE_END_MS - 75_000L;
        for (int s = 0; s <= 15; s++) {
            long ts = preWindowStartMs + s * 1_000L;
            double osc = (s % 2 == 0) ? 0.40 : -0.40;
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_002.00 + osc, ts);
        }
        long tUp = CANDLE_END_MS - 58_000L;
        strategy.onPolymarketCryptoPrice(tUp, "btcusd", "chainlink", 90_008.00, tUp);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.56, 0.54, 0.46, 0.44, 300.0, 300.0, 0.56, 0.46, tUp + 10L
        ));
        assertThat(submittedOrders).hasSize(1);
        OrderCommand firstUp = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                firstUp.clientOrderId(), "fill-up-200", "FILLED", BigDecimal.valueOf(0.56), BigDecimal.valueOf(200), tUp + 20L, "Filled"
        ));
        assertThat(strategy.getNetSharesUp()).isEqualTo(200);

        // 2. Moderate reversal to -$6.20 ($89,993.80): projected TWAP distance stays < $7.50 (< decisiveReversalSurplus),
        //    so Leg 2 DOWN can hedge the 200 UP shares and add at most +140 net shares (<= 340 total DOWN shares).
        for (int tauSec = 57; tauSec >= 22; tauSec--) {
            long ts = CANDLE_END_MS - tauSec * 1_000L;
            int before = submittedOrders.size();
            double osc = (tauSec % 2 == 0) ? 0.20 : -0.20;
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_993.80 + osc, ts);
            if (submittedOrders.size() > before) {
                OrderCommand cmd = submittedOrders.getLast();
                strategy.onExecutionReport(new ExecutionReport(
                        cmd.clientOrderId(), "f-rev-p-" + tauSec, "FILLED", cmd.price(), cmd.size(), ts + 5L, "Filled"
                ));
                before = submittedOrders.size();
            }
            if (tauSec <= 40) {
                for (int q = 0; q < 2; q++) {
                    long qTs = ts + 200L + q * 300L;
                    strategy.onOrderBookQuote(new OrderBookQuote(
                            0.36, 0.34, 0.66, 0.64, 400.0, 400.0, 0.36, 0.66, qTs
                    ));
                    if (submittedOrders.size() > before) {
                        OrderCommand cmd = submittedOrders.getLast();
                        strategy.onExecutionReport(new ExecutionReport(
                                cmd.clientOrderId(), "f-rev-q-" + tauSec + "-" + q, "FILLED", cmd.price(), cmd.size(), qTs + 5L, "Filled"
                        ));
                        before = submittedOrders.size();
                    }
                }
            }
        }

        assertThat(strategy.getNetSharesDown()).isBetween(200, 340);
        assertThat(strategy.getTradesInMarket()).isGreaterThan(2);
    }

    @Test
    @DisplayName("Should enforce fail-fast configuration invariants and expose immutable calibrated defaults")
    void shouldValidateConfigInvariantsAndDefaults() {
        assertThat(strategy.getConfig()).isSameAs(config);
        assertThat(config.minTauMs()).isEqualTo(1_000L);
        assertThat(config.maxTauMs()).isEqualTo(61_000L);
        assertThat(config.cooldownMs()).isEqualTo(110L);
        assertThat(config.maxSlippageMc()).isEqualTo(30);
        assertThat(config.warmupSecsBeforeExpiry()).isEqualTo(120L);

        assertThatThrownBy(() -> new UtterParameciumStrategy(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new UtterParameciumStrategyConfig(
                61_000L, 1_000L, 110L, 250, 970, 200, 590, 22, 30,
                5, 80, 1_250, 18, 4_500, 760, 2_400, 1_700L, 7_500, 150L, 85L, 3_500L, 120L, 700,
                "pm-btc-15m", "BTC-UP", "BTC-DOWN"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new UtterParameciumStrategyConfig(
                1_000L, 61_000L, 110L, 250, 970, 200, 590, 22, 1_000,
                5, 80, 1_250, 18, 4_500, 760, 2_400, 1_700L, 7_500, 150L, 85L, 3_500L, 120L, 700,
                "pm-btc-15m", "BTC-UP", "BTC-DOWN"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
