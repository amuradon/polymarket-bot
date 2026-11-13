package cz.polymarket.bot.strategy;

import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link UtterParameciumStrategy} and {@link UtterParameciumStrategyConfig}.
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

        HourlyPriceCache cache = new HourlyPriceCache(3600L);
        testContext = new StrategyContext() {
            @Override
            public ExecutionRouter getExecutionRouter() {
                return router;
            }

            @Override
            public HourlyPriceCache getPriceCache() {
                return cache;
            }

            @Override
            public void refreshAccountBalance() {
                balanceRefreshCount++;
            }
        };

        strategy.init(testContext);
    }

    @Test
    @DisplayName("Should validate math lookup tables, continuous z-curve, min cushion ramp, and tick rounding helpers")
    void shouldValidateMathTablesAndTickRounding() {
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS).hasSize(67);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[0]).isZero();
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[5]).isEqualTo(1236);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[29]).isEqualTo(15896);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[30]).isEqualTo(16730);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[60]).isEqualTo(47730);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[61]).isEqualTo(48937);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[64]).isEqualTo(52618);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[66]).isEqualTo(55122);

        assertThat(UtterParameciumStrategy.normalCdfMc(0L)).isEqualTo(500);
        assertThat(UtterParameciumStrategy.normalCdfMc(100L)).isEqualTo(841);
        assertThat(UtterParameciumStrategy.normalCdfMc(-100L)).isEqualTo(159);
        assertThat(UtterParameciumStrategy.normalCdfMc(150L)).isEqualTo(933);
        assertThat(UtterParameciumStrategy.normalCdfMc(-150L)).isEqualTo(67);
        assertThat(UtterParameciumStrategy.normalCdfMc(400L)).isEqualTo(999);
        assertThat(UtterParameciumStrategy.normalCdfMc(650L)).isEqualTo(999);
        assertThat(UtterParameciumStrategy.normalCdfMc(-400L)).isEqualTo(1);
        assertThat(UtterParameciumStrategy.normalCdfMc(55L) + UtterParameciumStrategy.normalCdfMc(-55L)).isEqualTo(1000);

        assertThat(UtterParameciumStrategy.requiredMinZCenti(660)).isZero();
        assertThat(UtterParameciumStrategy.requiredMinZCenti(670)).isEqualTo(68L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(700)).isEqualTo(87L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(720)).isEqualTo(100L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(740)).isEqualTo(118L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(760)).isEqualTo(136L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(780)).isEqualTo(160L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(800)).isEqualTo(184L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(820)).isEqualTo(204L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(840)).isEqualTo(225L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(880)).isEqualTo(245L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(920)).isEqualTo(255L);

        assertThat(UtterParameciumStrategy.minEffectiveDistCents(55_000L)).isEqualTo(200L);
        assertThat(UtterParameciumStrategy.minEffectiveDistCents(45_000L)).isEqualTo(200L);
        assertThat(UtterParameciumStrategy.minEffectiveDistCents(30_000L)).isEqualTo(175L);
        assertThat(UtterParameciumStrategy.minEffectiveDistCents(15_000L)).isEqualTo(150L);
        assertThat(UtterParameciumStrategy.minEffectiveDistCents(5_000L)).isEqualTo(150L);

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
    @DisplayName("Should ignore TwapUpdate and skip market with WARN when explicit 60s TWAP target price is missing")
    void shouldIgnoreTwapUpdateAndSkipMarketWhenTargetPriceIsMissing() {
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                CANDLE_START_SEC,
                CANDLE_END_SEC,
                BigDecimal.valueOf(90_000.00),
                new TwapPoint(CANDLE_START_SEC, BigDecimal.valueOf(90_000.00), BigDecimal.valueOf(90_000.00)),
                true
        ));
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
    @DisplayName("Should emit UP signal in divergent Spot vs TWAP scenario when locked TWAP stays above explicit target")
    void shouldEmitUpSignalWhenSpotDropsBelowStrikeButProjectedTwapRemainsAboveStrike() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        assertThat(strategy.getTargetPriceCents()).isEqualTo(9_000_000L);

        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int s = 0; s < 40; s++) {
            long ts = windowStartMs + (s * 1000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_025.00, ts);
        }

        // At tau = 20s, spot suddenly drops -$5 below strike to $89,995.00 while projected TWAP remains +$15.00 above strike
        long tau20Ms = CANDLE_END_MS - 20_000L;
        strategy.onPolymarketCryptoPrice(tau20Ms, "btcusd", "chainlink", 89_995.00, tau20Ms);

        OrderBookQuote quote = new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tau20Ms
        );
        strategy.onOrderBookQuote(quote);

        assertThat(submittedOrders).hasSize(1);
        OrderCommand order = submittedOrders.getFirst();
        assertThat(order.token()).isEqualTo("BTC-UP");
        assertThat(order.side()).isEqualTo("BUY");
        assertThat(order.price()).isEqualByComparingTo("0.700");
        assertThat(order.size().intValue()).isBetween(5, 60);
    }

    @Test
    @DisplayName("Should enforce [380, 840] mc unlocked price bounds, tau <= 38s gate for >= 800 mc, and [5s, 64s] tau bounds")
    void shouldSkipOutOfBoundsEntryPricesAndOutsideTauWindow() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long tau35Ms = CANDLE_END_MS - 35_000L;
        strategy.onPolymarketCryptoPrice(tau35Ms, "btcusd", "chainlink", 90_120.00, tau35Ms);

        // Best ask $0.850 (850 mc > 840 mc unlocked ceiling) must be rejected when TWAP is not truly locked in
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.850, 0.830, 0.170, 0.150, 1000.0, 1000.0, 0.850, 0.170, tau35Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // Underdog best ask $0.370 (370 mc < 380 mc) on UP while DOWN is $0.650 (edge negative for DOWN) must be rejected
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.370, 0.350, 0.650, 0.630, 1000.0, 1000.0, 0.370, 0.650, tau35Ms + 100L
        ));
        assertThat(submittedOrders).isEmpty();

        // Outside tau window: tau = 64.5s > maxTauMs (64s) and tau = 4s < minTauMs (5s)
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau64p5Ms = CANDLE_END_MS - 64_500L;
        strategy.onPolymarketCryptoPrice(tau64p5Ms, "btcusd", "chainlink", 90_050.00, tau64p5Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau64p5Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // High-price pre-lock-in gate: ask = $0.820 (820 mc >= 800 mc) blocked at tau = 42s (> 38s), allowed at tau = 36s (<= 38s)
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau42Ms = CANDLE_END_MS - 42_000L;
        strategy.onPolymarketCryptoPrice(tau42Ms, "btcusd", "chainlink", 90_050.00, tau42Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau42Ms
        ));
        assertThat(submittedOrders).isEmpty();

        long tau36Ms = CANDLE_END_MS - 36_000L;
        strategy.onPolymarketCryptoPrice(tau36Ms, "btcusd", "chainlink", 90_050.00, tau36Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau36Ms
        ));
        assertThat(submittedOrders).hasSize(1);

        OrderCommand allowedOrder = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                allowedOrder.clientOrderId(), "ex-36", "FILLED", allowedOrder.price(), allowedOrder.size(), tau36Ms + 10L, "Filled"
        ));

        long tau4Ms = CANDLE_END_MS - 4_000L;
        strategy.onPolymarketCryptoPrice(tau4Ms, "btcusd", "chainlink", 90_050.00, tau4Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau4Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should scale batch order size progressively with TWAP lock-in and enforce 350ms cooldown")
    void shouldScaleOrderSizeWithTwapLockInAndEnforceCooldown() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. At tau = 55s (early window, 0.70x clamped time factor), moderate edge at ask = 0.65 (< 670 mc, fee = 16 mc):
        // sigmaRem = 669 cents; +$3.85 (+385 cents) gives z = 57 -> pModel = 716 mc -> edge = 50 mc
        long tEarly = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tEarly, "btcusd", "chainlink", 90_003.85, tEarly);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tEarly
        ));

        assertThat(submittedOrders).hasSize(1);
        OrderCommand earlyOrder = submittedOrders.getFirst();
        int earlyShares = earlyOrder.size().intValue();
        assertThat(earlyShares).isBetween(15, 25);

        strategy.onExecutionReport(new ExecutionReport(
                earlyOrder.clientOrderId(), "ex-1", "FILLED", earlyOrder.price(), earlyOrder.size(), tEarly + 20L, "Filled"
        ));

        // 2. Quote 200ms later (< 300ms cooldown) must be blocked
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tEarly + 200L
        ));
        assertThat(submittedOrders).hasSize(1);

        // 3. Compare identical edge = 50 mc at tau = 55s (0.70x time factor) vs tau = 25s (1.083x time factor)
        long tLate = CANDLE_END_MS - 25_000L;
        UtterParameciumStrategy equalEdgeStrategy = new UtterParameciumStrategy(new UtterParameciumStrategyConfig(
                5_000L, 56_000L, 500L, 380, 840, 80, 650, 975, 25, 50,
                5, 30, 65, 600, 2_800, 26_000L, false, 950L, 100,
                7_000, 650L, 950L,
                400L, 105L, 3_500L, 120L, 700, "pm-btc-15m", "BTC-UP", "BTC-DOWN"
        ));
        equalEdgeStrategy.init(testContext);
        submittedOrders.clear();

        // At tau = 55s: sigmaRem = 400 * 4.1847 = 1673 cents; +$9.60 -> z = 57 -> pModel = 716 mc -> edge at 0.65 = 50 mc
        equalEdgeStrategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        equalEdgeStrategy.onPolymarketCryptoPrice(tEarly, "btcusd", "chainlink", 90_009.60, tEarly);
        equalEdgeStrategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tEarly
        ));
        int qAt55s = submittedOrders.getFirst().size().intValue();

        // At tau = 25s: sigmaRem = 400 * 1.2713 = 508 cents; +$2.92 -> z = 57 -> pModel = 716 mc -> identical edge = 50 mc!
        equalEdgeStrategy.reset();
        submittedOrders.clear();
        equalEdgeStrategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        equalEdgeStrategy.onPolymarketCryptoPrice(tLate, "btcusd", "chainlink", 90_002.92, tLate);
        equalEdgeStrategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tLate
        ));
        int qAt25s = submittedOrders.getFirst().size().intValue();

        assertThat(qAt25s).isGreaterThan(qAt55s);
    }

    @Test
    @DisplayName("Should handle sub-cent BBO quotes (bestBid=0.815, bestAsk=0.825) via conservative 10-mc rounding capped at maxEntryPriceMc (0.840)")
    void shouldHandleSubCentBboQuotes() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // At tau = 25s, BTC is +$80 above strike -> Z >= 4.0 -> pModelMc = maxModelProbMc (975 mc)
        long ts = CANDLE_END_MS - 25_000L;
        strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_080.00, ts);

        // Sub-cent BBO: bestBid = 0.815 (floor 810 mc), bestAsk = 0.825 (ceil 830 mc), limit expands to maxEntryPriceMc = 840 mc
        OrderBookQuote subCentQuote = new OrderBookQuote(
                0.825, 0.815, 0.185, 0.175, 500.0, 500.0, 0.825, 0.185, ts
        );
        strategy.onOrderBookQuote(subCentQuote);

        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().price()).isEqualByComparingTo("0.840");
    }

    @Test
    @DisplayName("Should require zero external streams and ignore Binance trade callbacks")
    void shouldRequireZeroExternalStreamsAndIgnoreBinanceCallbacks() {
        assertThat(strategy.getRequiredExternalStreams()).isEmpty();
        assertThat(strategy.getRequiredIndicators()).isEmpty();

        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long ts = CANDLE_END_MS - 30_000L;
        strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_000.00, ts);
        strategy.onBinanceSpotTrade(ts + 10L, 95_000.00, 10.0, false);
        strategy.onBinanceFuturesTrade(ts + 20L, 95_000.00, 10.0, false);

        assertThat(strategy.getLastChainlinkPriceCents()).isEqualTo(9_000_000L);
    }

    @Test
    @DisplayName("Should enforce single-direction commitment per market by default and block whipsaw flip to opposite side")
    void shouldBlockOppositeSideFlipWhenSingleDirectionCommitmentIsActive() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. At tau = 55s, BTC is +$40 above strike -> buys UP
        long tUp = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tUp, "btcusd", "chainlink", 90_040.00, tUp);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tUp
        ));

        assertThat(submittedOrders).hasSize(1);
        OrderCommand upOrder = submittedOrders.getFirst();
        assertThat(upOrder.token()).isEqualTo("BTC-UP");

        strategy.onExecutionReport(new ExecutionReport(
                upOrder.clientOrderId(), "ex-up", "FILLED", upOrder.price(), upOrder.size(), tUp + 10L, "Filled"
        ));
        assertThat(strategy.getNetSharesUp()).isGreaterThan(0);

        // 2. At tau = 40s, BTC drops sharply -$120 below strike ($89,880) so projected TWAP flips strongly DOWN;
        // because allowDirectionFlip is false by default and netSharesUp > 0, DOWN entry MUST be blocked
        long tDown = CANDLE_END_MS - 40_000L;
        strategy.onPolymarketCryptoPrice(tDown, "btcusd", "chainlink", 89_880.00, tDown);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.28, 0.26, 0.74, 0.72, 500.0, 500.0, 0.28, 0.74, tDown
        ));

        assertThat(submittedOrders).hasSize(1);
        assertThat(strategy.getNetSharesDown()).isZero();

        // 3. Resolution holds UP to expiration without emitting any SELL order
        strategy.onCandleResolution(TradeDirection.DOWN);
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should block entry during early-window spot reversal when spot and projected TWAP disagree")
    void shouldBlockEntryDuringEarlyWindowSpotReversalWhenSpotDisagreesWithProjectedTwap() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int s = 0; s < 12; s++) {
            long ts = windowStartMs + (s * 1000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_970.00, ts);
        }

        long tau48Ms = CANDLE_END_MS - 48_000L;
        strategy.onPolymarketCryptoPrice(tau48Ms, "btcusd", "chainlink", 90_004.00, tau48Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.53, 0.47, 0.45, 500.0, 500.0, 0.55, 0.47, tau48Ms
        ));

        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should ignore prior-second TWAP packet and lock exact candle-start targetPriceCents")
    void shouldIgnorePriorSecondTwapPacketAndLockExactCandleStartTargetPrice() {
        strategy.onPolymarketCryptoTwap60s(
                CANDLE_START_MS + 70L,
                "btcusd",
                "chainlink",
                89_998.00,
                60,
                CANDLE_START_MS - 1_000L
        );
        assertThat(strategy.getTargetPriceCents()).isZero();

        strategy.onPolymarketCryptoTwap60s(
                CANDLE_START_MS + 1_100L,
                "btcusd",
                "chainlink",
                90_000.00,
                60,
                CANDLE_START_MS
        );
        assertThat(strategy.getTargetPriceCents()).isEqualTo(9_000_000L);

        strategy.onPolymarketCryptoTwap60s(
                CANDLE_START_MS + 2_100L,
                "btcusd",
                "chainlink",
                90_005.00,
                60,
                CANDLE_START_MS + 1_000L
        );
        assertThat(strategy.getTargetPriceCents()).isEqualTo(9_000_000L);
    }

    @Test
    @DisplayName("Should enforce continuous fat-tail z-score curve from 670 mc ($0.67) through 840 mc ($0.84)")
    void shouldEnforceContinuousZScoreCurveAcross670To840Mc() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Mid-high price $0.76 (760 mc) requires z >= 136 centi-sigma (1.36 sigma):
        // At tau = 55s (sigmaRem = 669 cents), +$8.50 (+850 cents) gives z = 127 (< 136) -> MUST reject $0.76 even though Phi(1.27)=898 > 760+13+25!
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_008.50, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.76, 0.74, 0.26, 0.24, 500.0, 500.0, 0.76, 0.26, tau55Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 2. High price $0.82 (820 mc) at tau = 36s (<= 38s, sigmaRem = 160 * 2.2035 = 352 cents) requires z >= 202 centi-sigma:
        // At +$6.50 (+650 cents), z = 184 (< 202) -> rejected; at +$8.00 (+800 cents), z = 237 (>= 202) -> allowed!
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau36Ms = CANDLE_END_MS - 36_000L;
        strategy.onPolymarketCryptoPrice(tau36Ms, "btcusd", "chainlink", 90_006.50, tau36Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau36Ms
        ));
        assertThat(submittedOrders).isEmpty();

        long tau35Ms = CANDLE_END_MS - 35_000L;
        strategy.onPolymarketCryptoPrice(tau35Ms, "btcusd", "chainlink", 90_008.00, tau35Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau35Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should trigger refreshAccountBalance once per market at tau = maxTauMs + 12000 (76000 ms)")
    void shouldTriggerRefreshAccountBalanceOncePerMarketBeforeActivation() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        assertThat(balanceRefreshCount).isZero();

        long tau77Ms = CANDLE_END_MS - 77_000L;
        strategy.onPolymarketCryptoPrice(tau77Ms, "btcusd", "chainlink", 90_010.00, tau77Ms);
        assertThat(balanceRefreshCount).isZero();

        long tau75Ms = CANDLE_END_MS - 75_000L;
        strategy.onPolymarketCryptoPrice(tau75Ms, "btcusd", "chainlink", 90_012.00, tau75Ms);
        assertThat(balanceRefreshCount).isEqualTo(1);

        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_015.00, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tau55Ms
        ));
        assertThat(balanceRefreshCount).isEqualTo(1);

        long nextCandleStartMs = CANDLE_END_MS;
        long nextCandleEndMs = nextCandleStartMs + 900_000L;
        strategy.onPolymarketCryptoTwap60s(nextCandleStartMs, "btcusd", "chainlink", 90_100.00, 60, nextCandleStartMs);
        strategy.onPolymarketCryptoPrice(nextCandleEndMs - 80_000L, "btcusd", "chainlink", 90_110.00, nextCandleEndMs - 80_000L);
        assertThat(balanceRefreshCount).isEqualTo(1);

        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, nextCandleEndMs - 65_500L
        ));
        assertThat(balanceRefreshCount).isEqualTo(2);
    }

    @Test
    @DisplayName("Should expand limit price only while minEdgeMc, continuous z-score gates, high-price tau gate, and maxSlippageMc remain satisfied")
    void shouldComputeEdgePreservingLimitPriceCappedByMinEdgeZScoreAndMaxSlippage() {
        assertThat(config.maxSlippageMc()).isEqualTo(50);

        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Tight-edge & continuous Z-curve cap at tau = 55s (sigmaRem = 669 cents):
        // Spot = $90,008.20 (+820 cents) -> z = 122 centi-sigma -> pModelMc = 889 mc.
        // requiredMinZCenti: 730->109 (ok), 740->118 (ok), 750->127 (> 122 -> blocked!).
        // So at askUp = $0.73 (730 mc), stepping up reaches 740 mc and stops before 750 mc!
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_008.20, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.73, 0.71, 0.29, 0.27, 500.0, 500.0, 0.73, 0.29, tau55Ms
        ));

        assertThat(submittedOrders).hasSize(1);
        OrderCommand tightZOrder = submittedOrders.getFirst();
        assertThat(tightZOrder.price()).isEqualByComparingTo("0.740");

        // 2. High-price tau gate cap at tau = 42s (> 38s, sigmaRem = 445 cents):
        // Spot = $90,012.00 (+1200 cents) -> z = 269 centi-sigma, pModelMc = 985 mc.
        // At askUp = $0.78 (780 mc): allowed (< 800 mc). Stepping up reaches 790 mc and MUST NOT cross into 800 mc while tau > 38s!
        strategy.reset();
        long tau42Ms = CANDLE_END_MS - 42_000L;
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        strategy.onPolymarketCryptoPrice(tau42Ms, "btcusd", "chainlink", 90_012.00, tau42Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.78, 0.76, 0.24, 0.22, 500.0, 500.0, 0.78, 0.24, tau42Ms
        ));

        assertThat(submittedOrders).hasSize(2);
        OrderCommand tauCappedOrder = submittedOrders.get(1);
        assertThat(tauCappedOrder.price()).isEqualByComparingTo("0.790");
    }

    @Test
    @DisplayName("Should ignore duplicate or older crypto price ticks and block historical snapshot evaluation against newer live quote")
    void shouldIgnoreDuplicateOrOlderCryptoPriceTicksAndBlockHistoricalSnapshotEvaluation() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long tau20Ms = CANDLE_END_MS - 20_000L;
        long tau50Ms = CANDLE_END_MS - 50_000L;
        long tau49Ms = CANDLE_END_MS - 49_000L;
        long tau48Ms = CANDLE_END_MS - 48_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.52, 0.50, 0.52, 0.50, 500.0, 500.0, 0.51, 0.51, tau20Ms
        ));
        strategy.onPolymarketCryptoPrice(tau50Ms, "btcusd", "chainlink", 90_080.00, tau50Ms);
        assertThat(strategy.getLastChainlinkPriceCents()).isEqualTo(9_008_000L);
        assertThat(submittedOrders).isEmpty();

        strategy.onPolymarketCryptoPrice(tau49Ms, "btcusd", "chainlink", 89_920.00, tau49Ms);
        strategy.onPolymarketCryptoPrice(tau48Ms, "btcusd", "chainlink", 90_000.00, tau48Ms);
        strategy.onPolymarketCryptoPrice(tau20Ms, "btcusd", "chainlink", 90_000.00, tau20Ms);
        assertThat(strategy.getLastChainlinkPriceCents()).isEqualTo(9_000_000L);
        assertThat(submittedOrders).isEmpty();

        long tau35Ms = CANDLE_END_MS - 35_000L;
        strategy.onPolymarketCryptoPrice(tau35Ms, "btcusd", "chainlink", 90_095.00, tau35Ms);
        strategy.onPolymarketCryptoPrice(tau20Ms, "btcusd", "chainlink", 90_095.00, tau20Ms);
        assertThat(strategy.getLastChainlinkPriceCents()).isEqualTo(9_000_000L);
        assertThat(submittedOrders).isEmpty();

        long tau19Ms = CANDLE_END_MS - 19_000L;
        strategy.onPolymarketCryptoPrice(tau19Ms, "btcusd", "chainlink", 90_080.00, tau19Ms);
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should retain intra-candle 5s MAD volatility regime across a quiet 60s pre-window lull")
    void shouldRetainCandleVolatilityRegimeAcrossQuietLull() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long regimeStartMs = CANDLE_END_MS - 360_000L;
        for (int i = 0; i < 40; i++) {
            long ts = regimeStartMs + (i * 5_000L);
            double price = (i % 2 == 0) ? 90_025.00 : 90_000.00;
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", price, ts);
        }

        long lullStartMs = CANDLE_END_MS - 115_000L;
        for (int s = 0; s <= 60; s++) {
            long ts = lullStartMs + (s * 1_000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_012.00, ts);
        }

        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.75, 0.73, 0.27, 0.25, 500.0, 500.0, 0.75, 0.27, tau55Ms
        ));
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should stage inventory from 50% at tau>=60s to 100% at tau<=36s and scale dynamically by option price risk, conviction, and TWAP lock-in")
    void shouldStageInventoryByTimeAndScaleDynamicallyByConviction() {
        assertThat(strategy.computeTimeStagedMaxInventory(64_000L)).isEqualTo(300);
        assertThat(strategy.computeTimeStagedMaxInventory(60_000L)).isEqualTo(300);
        assertThat(strategy.computeTimeStagedMaxInventory(48_000L)).isEqualTo(450);
        assertThat(strategy.computeTimeStagedMaxInventory(36_000L)).isEqualTo(600);
        assertThat(strategy.computeTimeStagedMaxInventory(15_000L)).isEqualTo(600);

        // Pre-window (tau > 60s) caps total cumulative inventory at 60 shares:
        assertThat(strategy.computeDynamicMaxInventory(62_000L, 620, 140L, 90, false)).isEqualTo(60);

        // Price-risk & conviction scaling checks:
        // Core cheap contract (ask = 580 <= 670) at tau = 42s with z >= 85 unlocks 100% of timeStagedCap(42s = 525 shares)
        assertThat(strategy.computeDynamicMaxInventory(42_000L, 580, 105L, 90, false)).isEqualTo(525);

        // Pre-lock-in anti-stagnation cap (tau = 42s > 38s): z <= 55 caps at 40% (210 sh); z = 70 ramps linearly to 70% (367 sh)
        assertThat(strategy.computeDynamicMaxInventory(42_000L, 580, 50L, 90, false)).isEqualTo(210);
        assertThat(strategy.computeDynamicMaxInventory(42_000L, 580, 70L, 90, false)).isEqualTo(367);

        // Transition contract (ask = 720 in 671..720): 80% (480 sh) at z = 115, unlocking 110% (660 sh) when tau <= 34s, z >= 135 & edge >= 50
        assertThat(strategy.computeDynamicMaxInventory(32_000L, 720, 115L, 45, false)).isEqualTo(480);
        assertThat(strategy.computeDynamicMaxInventory(32_000L, 720, 140L, 55, false)).isEqualTo(660);

        // Core cheap contract late high-conviction bonus (tau <= 36s, z >= 145, edge >= 55) -> 130% = 780 shares
        assertThat(strategy.computeDynamicMaxInventory(24_000L, 580, 180L, 80, false)).isEqualTo(780);

        // Mid-high contract (ask = 750 in 721..760): 65% = 390 shares unlocked, 95% = 570 shares when tau <= 30s & z >= 160
        assertThat(strategy.computeDynamicMaxInventory(32_000L, 750, 150L, 45, false)).isEqualTo(390);
        assertThat(strategy.computeDynamicMaxInventory(28_000L, 750, 170L, 55, false)).isEqualTo(570);

        // Expensive contract (ask = 780 in 761..799): 42% = 252 shares unlocked, 90% = 540 shares when trulyLockedIn
        assertThat(strategy.computeDynamicMaxInventory(32_000L, 780, 215L, 40, false)).isEqualTo(252);
        assertThat(strategy.computeDynamicMaxInventory(24_000L, 780, 280L, 70, true)).isEqualTo(540);

        // Ultra-expensive contract (ask = 820 in 800..840): 32% = 192 shares unlocked, 90% = 540 shares when trulyLockedIn
        assertThat(strategy.computeDynamicMaxInventory(32_000L, 820, 230L, 40, false)).isEqualTo(192);
        assertThat(strategy.computeDynamicMaxInventory(22_000L, 820, 290L, 65, true)).isEqualTo(540);

        // Locked-in high-price tiers: 841..880 -> 80% (480 sh), 881..920 -> 60% (360 sh)
        assertThat(strategy.computeDynamicMaxInventory(22_000L, 860, 310L, 60, true)).isEqualTo(480);
        assertThat(strategy.computeDynamicMaxInventory(22_000L, 900, 320L, 55, true)).isEqualTo(360);

        // Simulation: unlocked expensive $0.78 contract between tau = 40s and tau = 32s MUST cap at 42% of timeStagedCap(32s = 252 shares)
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        for (long ts = CANDLE_END_MS - 40_000L; ts <= CANDLE_END_MS - 32_000L; ts += 500L) {
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_012.00, ts);
            strategy.onOrderBookQuote(new OrderBookQuote(
                    0.78, 0.76, 0.24, 0.22, 500.0, 500.0, 0.78, 0.24, ts
            ));
            if (!submittedOrders.isEmpty()) {
                OrderCommand last = submittedOrders.getLast();
                strategy.onExecutionReport(new ExecutionReport(
                        last.clientOrderId(), "ex-" + ts, "FILLED", last.price(), last.size(), ts + 10L, "Filled"
                ));
            }
        }
        assertThat(strategy.getNetSharesUp())
                .isGreaterThan(120)
                .isLessThanOrEqualTo(strategy.computeDynamicMaxInventory(32_000L, 780, 230L, 40, false));
    }

    @Test
    @DisplayName("Should block falling-knife entries on adverse 5s/10s spot momentum or sustained CLOB collapse, while ignoring transient ask-sweep spikes")
    void shouldBlockFallingKnifeOnAdverseSpotMomentumOrRapidClobAskDrop() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Adverse Chainlink spot momentum in pre-lock-in window (tau = 55s -> 50s): BTC drops from +$28 to +$21 (-$7 in 5s >= $5.93 threshold)
        long tau55Ms = CANDLE_END_MS - 55_000L;
        long tau50Ms = CANDLE_END_MS - 50_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_028.00, tau55Ms);
        strategy.onPolymarketCryptoPrice(tau50Ms, "btcusd", "chainlink", 90_021.00, tau50Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau50Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 2. Sustained CLOB BBO collapse (toxic flow ahead of Chainlink) on a steady oracle price in pre-lock-in window (tau = 48s -> 45s):
        // CLOB UP BBO was $0.90/$0.92 at tau = 48s and crashes by -120 mc (>= 100 mc) to $0.78/$0.80 at tau = 45s
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau48Ms = CANDLE_END_MS - 48_000L;
        long tau45Ms = CANDLE_END_MS - 45_000L;
        strategy.onPolymarketCryptoPrice(tau48Ms, "btcusd", "chainlink", 90_035.00, tau48Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.92, 0.90, 0.10, 0.08, 500.0, 500.0, 0.92, 0.10, tau48Ms
        ));
        assertThat(submittedOrders).isEmpty();

        strategy.onPolymarketCryptoPrice(tau45Ms, "btcusd", "chainlink", 90_035.00, tau45Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.80, 0.78, 0.22, 0.20, 500.0, 500.0, 0.80, 0.22, tau45Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // Once 8+ seconds pass and CLOB stabilizes at $0.78/$0.80 (tau = 36s > 25s), entry is allowed cleanly
        long tau36Ms = CANDLE_END_MS - 36_000L;
        strategy.onPolymarketCryptoPrice(tau36Ms, "btcusd", "chainlink", 90_035.00, tau36Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.80, 0.78, 0.22, 0.20, 500.0, 500.0, 0.80, 0.22, tau36Ms
        ));
        assertThat(submittedOrders).hasSize(1);

        // 3. Transient ask-sweep spike ($0.53/$0.55 -> $0.53/$0.68 -> $0.54/$0.56 where bid never dropped) MUST NOT block!
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tSweep0 = CANDLE_END_MS - 50_000L;
        strategy.onPolymarketCryptoPrice(tSweep0, "btcusd", "chainlink", 90_020.00, tSweep0);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.53, 0.47, 0.45, 500.0, 500.0, 0.55, 0.47, tSweep0
        ));
        assertThat(submittedOrders).hasSize(2);
        OrderCommand sweepOrder = submittedOrders.getLast();
        strategy.onExecutionReport(new ExecutionReport(
                sweepOrder.clientOrderId(), "ex-sw", "FILLED", sweepOrder.price(), sweepOrder.size(), tSweep0 + 20L, "Filled"
        ));
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.53, 0.47, 0.32, 500.0, 500.0, 0.68, 0.47, tSweep0 + 1_020L
        ));
        long tRefill = tSweep0 + 2_100L;
        strategy.onPolymarketCryptoPrice(tRefill, "btcusd", "chainlink", 90_020.00, tRefill);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.56, 0.54, 0.46, 0.44, 500.0, 500.0, 0.56, 0.46, tRefill
        ));
        assertThat(submittedOrders).hasSize(3);

        // 4. Late falling-knife inside tau <= 25s when TWAP is NOT locked in (lockedContribution < $12.00) MUST still be blocked!
        strategy.reset();
        submittedOrders.clear();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau60Ms = CANDLE_END_MS - 60_000L;
        long tau23Ms = CANDLE_END_MS - 23_000L;
        long tau18Ms = CANDLE_END_MS - 18_000L;
        strategy.onPolymarketCryptoPrice(tau60Ms, "btcusd", "chainlink", 90_002.00, tau60Ms);
        strategy.onPolymarketCryptoPrice(tau23Ms, "btcusd", "chainlink", 90_018.00, tau23Ms);
        strategy.onPolymarketCryptoPrice(tau18Ms, "btcusd", "chainlink", 90_010.00, tau18Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tau18Ms
        ));
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should widen sigmaRemCents when PolyBolt priceTimestampMs lags behind event timestampMs")
    void shouldAccountForOracleLagInRemainingUncertainty() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // At tau = 30s with defaultSigma1s = 160 cents, requiredMinZCenti(820) = 202:
        // Without lag: weightBps[30] = 16730 -> sigmaRem = 267 cents. With +$5.60 (+560 cents), z = 560 / 267 = 2.09 (>= 2.02) -> allowed.
        // With 3,000ms oracle lag: effectiveTauSec = 33 -> weightBps[33] = 19319 -> sigmaRem = 309 cents -> z = 560 / 309 = 1.81 (< 2.02) -> rejected!
        long tau30Ms = CANDLE_END_MS - 30_000L;
        strategy.onPolymarketCryptoPrice(tau30Ms, "btcusd", "chainlink", 90_005.60, tau30Ms - 3_000L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau30Ms
        ));
        assertThat(submittedOrders).isEmpty();

        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        strategy.onPolymarketCryptoPrice(tau30Ms, "btcusd", "chainlink", 90_005.60, tau30Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau30Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should absorb constant venue basis in onCalculatedSpotTwap, veto adverse multi-exchange reversals, and never inflate edge")
    void shouldAbsorbConstantVenueBasisAndProtectAgainstEarlyCalculatedSpotReversal() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Warm up slow venue basis over 5 seconds where multi-exchange spot sits -$4.00 below Chainlink ($89,996 vs $90,000)
        long warmStartMs = CANDLE_END_MS - 70_000L;
        for (int s = 0; s < 5; s++) {
            long ts = warmStartMs + (s * 1_000L);
            strategy.onCalculatedSpotTwap(ts, "btcusd", 89_996.00, 89_996.00);
            strategy.onPolymarketCryptoPrice(ts + 100L, "btcusd", "chainlink", 90_000.00, ts);
        }

        // 2. Asymmetry check: at tau = 52s, Chainlink is flat at +$1.00 ($90,001), while calcSpot jumps +$12.00 above aligned level ($90,009):
        // favorable calcSpot MUST NEVER fabricate a signal when Chainlink itself lacks cushion!
        long tau52Ms = CANDLE_END_MS - 52_000L;
        strategy.onCalculatedSpotTwap(tau52Ms - 100L, "btcusd", 90_009.00, 90_000.00);
        strategy.onPolymarketCryptoPrice(tau52Ms, "btcusd", "chainlink", 90_001.00, tau52Ms - 1_000L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.53, 0.47, 0.45, 500.0, 500.0, 0.55, 0.47, tau52Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 3. Basis alignment check: at tau = 45s, Chainlink is +$18.00 ($90,018) and calcSpot has the normal -$4.00 venue offset ($90,014):
        // alignedCalcSpot = $90,018 (zero adverse divergence) -> UP order MUST be allowed cleanly!
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        for (int s = 0; s < 5; s++) {
            long ts = warmStartMs + (s * 1_000L);
            strategy.onCalculatedSpotTwap(ts, "btcusd", 89_996.00, 89_996.00);
            strategy.onPolymarketCryptoPrice(ts + 100L, "btcusd", "chainlink", 90_000.00, ts);
        }
        long tau45Ms = CANDLE_END_MS - 45_000L;
        strategy.onCalculatedSpotTwap(tau45Ms - 100L, "btcusd", 90_014.00, 90_005.00);
        strategy.onPolymarketCryptoPrice(tau45Ms, "btcusd", "chainlink", 90_018.00, tau45Ms - 100L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 500.0, 500.0, 0.68, 0.34, tau45Ms
        ));
        assertThat(submittedOrders).hasSize(1);

        // 4. Early reversal veto check: 1.5s later (tau = 43.5s), Chainlink still prints $90,018.00 (lagging),
        // but multi-exchange calcSpot has already crashed -$10.00 to $90,004.00 (aligned $90,008 -> -$10.00 adverse lead >= $8.50 block)!
        OrderCommand filledFirst = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                filledFirst.clientOrderId(), "ex-c1", "FILLED", filledFirst.price(), filledFirst.size(), tau45Ms + 10L, "Filled"
        ));
        long tau43p5Ms = CANDLE_END_MS - 43_500L;
        strategy.onCalculatedSpotTwap(tau43p5Ms, "btcusd", 90_004.00, 90_005.00);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.64, 0.62, 0.38, 0.36, 500.0, 500.0, 0.64, 0.38, tau43p5Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should pause adding to an existing >= 150 share position when effective cushion decays below 50% of peak (< $12)")
    void shouldPauseAddingInventoryWhenCushionDecaysFromPeak() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // Build >= 150 UP shares at +$18.00 cushion between tau = 44s and tau = 41.5s
        for (long ts = CANDLE_END_MS - 44_000L; ts <= CANDLE_END_MS - 41_500L; ts += 500L) {
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_018.00, ts);
            strategy.onOrderBookQuote(new OrderBookQuote(
                    0.62, 0.60, 0.40, 0.38, 500.0, 500.0, 0.62, 0.40, ts
            ));
            OrderCommand last = submittedOrders.getLast();
            strategy.onExecutionReport(new ExecutionReport(
                    last.clientOrderId(), "ex-p-" + ts, "FILLED", last.price(), last.size(), ts + 10L, "Filled"
            ));
        }
        int sharesBeforeDecay = strategy.getNetSharesUp();
        assertThat(sharesBeforeDecay).isGreaterThanOrEqualTo(150);
        int ordersBeforeDecay = submittedOrders.size();

        // At tau = 27s (14.5s after last fill so 10s drift lookup is outside 4s staleness), spot softens to +$8.50
        // (47.2% < 50% of $18.00 peak and < $12.00): cushion-decay stop-add MUST pause piling in!
        long tau27Ms = CANDLE_END_MS - 27_000L;
        strategy.onPolymarketCryptoPrice(tau27Ms, "btcusd", "chainlink", 90_008.50, tau27Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.58, 0.56, 0.44, 0.42, 500.0, 500.0, 0.58, 0.44, tau27Ms
        ));
        assertThat(submittedOrders).hasSize(ordersBeforeDecay);

        // Once cushion recovers to +$12.50 (>= $12.00 safe threshold) at tau = 25s, adding resumes
        long tau25Ms = CANDLE_END_MS - 25_000L;
        strategy.onPolymarketCryptoPrice(tau25Ms, "btcusd", "chainlink", 90_012.50, tau25Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.62, 0.60, 0.40, 0.38, 500.0, 500.0, 0.62, 0.40, tau25Ms
        ));
        assertThat(submittedOrders).hasSize(ordersBeforeDecay + 1);
        OrderCommand resumed = submittedOrders.getLast();
        strategy.onExecutionReport(new ExecutionReport(
                resumed.clientOrderId(), "ex-resumed", "FILLED", resumed.price(), resumed.size(), tau25Ms + 10L, "Filled"
        ));

        // Even inside tau <= 20s (when not truly locked in), bid drawdown >= 100 mc ($0.66 -> $0.56) after >= 90 shares MUST block averaging down!
        strategy.reset();
        submittedOrders.clear();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau22Ms = CANDLE_END_MS - 22_000L;
        strategy.onPolymarketCryptoPrice(tau22Ms, "btcusd", "chainlink", 90_010.00, tau22Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 500.0, 500.0, 0.68, 0.34, tau22Ms
        ));
        assertThat(submittedOrders).hasSize(1);
        OrderCommand lateFirst = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                lateFirst.clientOrderId(), "ex-late1", "FILLED", lateFirst.price(), BigDecimal.valueOf(90), tau22Ms + 10L, "Filled"
        ));

        // At tau = 16s, CLOB bid drops by 100 mc ($0.66 -> $0.56) while spot stays +$9.00: anti-martingale stop-add blocks!
        long tau16Ms = CANDLE_END_MS - 16_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.58, 0.56, 0.44, 0.42, 500.0, 500.0, 0.58, 0.44, tau16Ms - 50L
        ));
        strategy.onPolymarketCryptoPrice(tau16Ms, "btcusd", "chainlink", 90_009.00, tau16Ms);
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should compute exact millisecond step-weighted TWAP integral and enforce $2.00 -> $1.50 minimum cushion ramp")
    void shouldComputeTimeWeightedStepIntegralTwapAcrossIrregularTickGaps() {
        UtterParameciumStrategy.ChainlinkTwapBuffer buffer = new UtterParameciumStrategy.ChainlinkTwapBuffer();
        long wStart = 1_000_000L;
        // Price holds at $90,000.00 (9,000,000 cents) from wStart to wStart + 5,000ms (5s), then jumps to $90,060.00 for 1s
        buffer.pushAndEvict(wStart, 9_000_000L, wStart - 65_000L);
        buffer.pushAndEvict(wStart + 5_000L, 9_006_000L, wStart - 60_000L);

        // Exact step-weighted average over 6,000ms = (5000 * 9,000,000 + 1000 * 9,006,000) / 6000 = 9,001,000 cents ($90,010.00)
        assertThat(buffer.meanCentsInWindow(wStart, wStart + 6_000L)).isEqualTo(9_001_000L);

        // Minimum cushion ramp: +$1.70 (+170 cents) must be rejected at tau = 50s (< 200 cents) and accepted at tau = 15s (>= 150 cents)
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau50Ms = CANDLE_END_MS - 50_000L;
        strategy.onPolymarketCryptoPrice(tau50Ms, "btcusd", "chainlink", 90_001.70, tau50Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.52, 0.50, 0.50, 0.48, 500.0, 500.0, 0.52, 0.50, tau50Ms
        ));
        assertThat(submittedOrders).isEmpty();

        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau15Ms = CANDLE_END_MS - 15_000L;
        strategy.onPolymarketCryptoPrice(tau15Ms, "btcusd", "chainlink", 90_001.70, tau15Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.53, 0.47, 0.45, 500.0, 500.0, 0.55, 0.47, tau15Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should persistently block mid-window whipsaws against opposite locked-TWAP debt and allow genuine late breakouts")
    void shouldBlockOpeningNewPositionIntoDeepOppositeLockedTwapDebt() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // Accumulate -$8.00 locked DOWN TWAP debt over the first 25 seconds (tau = 60s -> 35s)
        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int s = 0; s <= 25; s++) {
            long ts = windowStartMs + (s * 1_000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_992.00, ts);
        }

        // 1. At tau = 34s (> 30s), spot whipsaws up to +$25.00: lockedDistUp = -$8.00 <= -$3.50 -> blocked unconditionally while tau > 30s!
        long tau34Ms = CANDLE_END_MS - 34_000L;
        strategy.onPolymarketCryptoPrice(tau34Ms, "btcusd", "chainlink", 90_025.00, tau34Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.53, 0.47, 0.45, 500.0, 500.0, 0.55, 0.47, tau34Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 2. At tau = 28s (<= 30s), even when lockedDistUp is only slightly negative (-$1.00 < 0), modest projTwap (< $10.00) MUST still be blocked!
        long tau28Ms = CANDLE_END_MS - 28_000L;
        strategy.onPolymarketCryptoPrice(tau28Ms, "btcusd", "chainlink", 90_012.00, tau28Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.55, 0.53, 0.47, 0.45, 500.0, 500.0, 0.55, 0.47, tau28Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 3. At tau = 27s (<= 30s), if BTC genuinely surges to +$45.00 so projectedTwap clears max($10, 2 * |lockedDistUp|), UP is allowed!
        long tau27Ms = CANDLE_END_MS - 27_000L;
        strategy.onPolymarketCryptoPrice(tau27Ms, "btcusd", "chainlink", 90_045.00, tau27Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.58, 0.56, 0.44, 0.42, 500.0, 500.0, 0.58, 0.44, tau27Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should declare bounded CLOB lookback window while default TradingStrategy requests full candle")
    void shouldDeclareBoundedClobLookbackWindowWhileDefaultStrategyRequestsFullCandle() {
        TradingStrategy defaultStrategy = new TradingStrategy() {
            @Override
            public void init(StrategyContext context) {
            }

            @Override
            public void onExecutionReport(ExecutionReport report) {
            }
        };

        assertThat(defaultStrategy.getRequiredClobLookbackMs()).isEqualTo(Long.MAX_VALUE);
        assertThat(strategy.getRequiredClobLookbackMs()).isEqualTo(81_000L);
    }

    @Test
    @DisplayName("Should clock volatility on oracle priceTimestampMs despite PolyBolt jitter and enforce 14m Parkinson wave-range floor")
    void shouldClockVolatilityOnOracleTimestampAndEnforceIntraCandleParkinsonRangeFloor() {
        // 1. Arrival jitter test: oracle ticks advance by exact 1,000 ms steps while PolyBolt receipt timestamps
        // alternate +1,350 ms and +650 ms (< 800 ms); $10.00/s jumps occur on the +650 ms receipts and MUST NOT be dropped!
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long jitterBaseObsMs = CANDLE_END_MS - 85_000L;
        for (int s = 0; s <= 20; s++) {
            long obsMs = jitterBaseObsMs + (s * 1_000L);
            long receiptMs = (s % 2 == 0) ? (obsMs + 100L) : (obsMs + 450L);
            double price = (s % 2 == 0) ? 90_010.00 : 90_000.00;
            strategy.onPolymarketCryptoPrice(receiptMs, "btcusd", "chainlink", price, obsMs);
        }

        // At tau = 55s, +$4.50 (+450 cents) in a $10.00/s oscillating regime has z < 0.15 -> MUST be blocked!
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_004.50, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.56, 0.54, 0.46, 0.44, 500.0, 500.0, 0.56, 0.46, tau55Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 2. Intra-candle 14m Parkinson wave-range floor test:
        // Smooth $200.00 wave in the first 3 minutes (s = 0..195s), followed by >10.5 minutes of micro-steps ($0.60/s)
        // so both 1s MAD (64s) and 5s MAD (640s) drop below $1.00/s while 14m candle range remains $200.00!
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        for (int s = 0; s <= 760; s += 5) {
            long ts = CANDLE_START_MS + (s * 1_000L);
            double wavePrice;
            if (s <= 100) {
                wavePrice = 90_000.00 + (s * 2.00);
            } else if (s <= 200) {
                wavePrice = 90_200.00 - ((s - 100) * 2.00);
            } else {
                wavePrice = 90_000.00;
            }
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", wavePrice, ts);
        }
        for (int s = 761; s <= 840; s++) {
            long ts = CANDLE_START_MS + (s * 1_000L);
            double microPrice = (s % 2 == 0) ? 90_000.60 : 90_000.00;
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", microPrice, ts);
        }

        // At tau = 55s, a tiny +$3.50 micro-cushion inside a $200-range wave candle MUST be rejected (pModel < 650 mc),
        // whereas a decisive +$10.00 cushion at tau = 54s is accepted!
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_003.50, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.56, 0.54, 0.46, 0.44, 500.0, 500.0, 0.56, 0.46, tau55Ms
        ));
        assertThat(submittedOrders).isEmpty();

        long tau54Ms = CANDLE_END_MS - 54_000L;
        strategy.onPolymarketCryptoPrice(tau54Ms, "btcusd", "chainlink", 90_010.00, tau54Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.56, 0.54, 0.46, 0.44, 500.0, 500.0, 0.56, 0.46, tau54Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should enforce smooth z-curve across $0.67-$0.72 cliff and cap stagnant low-z inventory pre-lock-in")
    void shouldEnforceSmoothZCurveAcross670To720CliffAndCapStagnantLowZInventoryPreLockIn() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. $0.70 (700 mc) requires z >= 87 centi-sigma:
        // At tau = 55s (sigmaRem = 669 cents), +$5.40 (+540 cents) gives z = 80 (< 87) -> rejected!
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_005.40, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau55Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // At tau = 54s (sigmaRem = 651 cents), +$6.20 (+620 cents) gives z = 95 (>= 87) -> accepted!
        long tau54Ms = CANDLE_END_MS - 54_000L;
        strategy.onPolymarketCryptoPrice(tau54Ms, "btcusd", "chainlink", 90_006.20, tau54Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau54Ms
        ));
        assertThat(submittedOrders).hasSize(1);

        // 2. Anti-stagnation pre-lock-in cap: over 13 quote batches (tau = 58s -> 52s, timeStagedCap = 400 sh)
        // with stagnant ask = $0.56 and weak z <= 55 (+$3.35 cushion), total inventory MUST cap at 40% (160 shares)!
        strategy.reset();
        submittedOrders.clear();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tStartStag = CANDLE_END_MS - 58_000L;
        strategy.onPolymarketCryptoPrice(tStartStag, "btcusd", "chainlink", 90_003.35, tStartStag);
        for (int i = 0; i <= 12; i++) {
            long ts = tStartStag + (i * 500L);
            if (i == 8) {
                strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_003.35, ts);
            }
            int prevCount = submittedOrders.size();
            strategy.onOrderBookQuote(new OrderBookQuote(
                    0.56, 0.54, 0.46, 0.44, 500.0, 500.0, 0.56, 0.46, ts
            ));
            if (submittedOrders.size() > prevCount) {
                OrderCommand last = submittedOrders.getLast();
                strategy.onExecutionReport(new ExecutionReport(
                        last.clientOrderId(), "ex-stag-" + i, "FILLED", last.price(), last.size(), ts + 10L, "Filled"
                ));
            }
        }
        assertThat(strategy.getNetSharesUp()).isBetween(130, 160);
    }

    @Test
    @DisplayName("Should block intra-settlement-window cross-strike whipsaw and apply tightened $2.00/$6.50 calcSpot reversal guard")
    void shouldBlockIntraSettlementWindowOppositeExcursionWhipsawAndApplyTightenedCalcSpotGuard() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Intra-window whipsaw memory: BTC starts +$12.00 UP at tau = 59s..57s, then flips to -$7.00 DOWN for 10s
        // so lockedMean passes below target (zero lockedDebt for DOWN), yet oppositeExcursion = $12.00 (>= $4.50)
        // requires downDistProj >= max($9.00, 1.5 * $12.00) = $18.00 -> modest -$7.00 DOWN MUST be blocked!
        long wStart = CANDLE_END_MS - 60_000L;
        strategy.onPolymarketCryptoPrice(wStart + 1_000L, "btcusd", "chainlink", 90_012.00, wStart + 1_000L);
        strategy.onPolymarketCryptoPrice(wStart + 3_000L, "btcusd", "chainlink", 90_012.00, wStart + 3_000L);
        for (int s = 5; s <= 15; s++) {
            long ts = wStart + (s * 1_000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_993.00, ts);
        }
        long tau45Ms = CANDLE_END_MS - 45_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.44, 0.42, 0.58, 0.56, 500.0, 500.0, 0.44, 0.58, tau45Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // Decisive breakdown to -$28.00 at tau = 40s pushes downDistProj > $18.00 with z >= 1.10 -> allowed!
        for (int s = 16; s <= 19; s++) {
            long ts = wStart + (s * 1_000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_993.00, ts);
        }
        long tau40Ms = CANDLE_END_MS - 40_000L;
        strategy.onPolymarketCryptoPrice(tau40Ms, "btcusd", "chainlink", 89_972.00, tau40Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.42, 0.40, 0.60, 0.58, 500.0, 500.0, 0.42, 0.60, tau40Ms
        ));
        assertThat(submittedOrders).hasSize(1);

        // 2. Tightened onCalculatedSpotTwap guard: -$7.00 adverse multi-exchange lead (>= $6.50 block) vetoes lagging Chainlink!
        strategy.reset();
        submittedOrders.clear();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long warmStartMs = CANDLE_END_MS - 70_000L;
        for (int s = 0; s < 5; s++) {
            long ts = warmStartMs + (s * 1_000L);
            strategy.onCalculatedSpotTwap(ts, "btcusd", 90_000.00, 90_000.00);
            strategy.onPolymarketCryptoPrice(ts + 100L, "btcusd", "chainlink", 90_000.00, ts);
        }
        long tau48Ms = CANDLE_END_MS - 48_000L;
        strategy.onCalculatedSpotTwap(tau48Ms - 100L, "btcusd", 90_003.00, 90_000.00);
        strategy.onPolymarketCryptoPrice(tau48Ms, "btcusd", "chainlink", 90_010.00, tau48Ms - 200L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.58, 0.56, 0.44, 0.42, 500.0, 500.0, 0.58, 0.44, tau48Ms
        ));
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should allow pre-window entry (60s < tau <= 64s) only for ask <= $0.75, z >= 0.95, and zero adverse calcSpot lead")
    void shouldAllowPreWindowEntryUnder65sOnlyForCheapHighConvictionQuotesWithoutAdverseCalcLead() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long warmStartMs = CANDLE_END_MS - 72_000L;
        for (int s = 0; s < 5; s++) {
            long ts = warmStartMs + (s * 1_000L);
            strategy.onCalculatedSpotTwap(ts, "btcusd", 90_012.00, 90_012.00);
            strategy.onPolymarketCryptoPrice(ts + 50L, "btcusd", "chainlink", 90_012.00, ts);
        }

        // 1. At tau = 62s (sigmaRem = 160 * 5.0154 = 802 cents):
        // Weak cushion +$6.00 (z = 74 < 95) MUST be rejected before 60s window starts!
        long tau62Ms = CANDLE_END_MS - 62_000L;
        strategy.onCalculatedSpotTwap(tau62Ms - 50L, "btcusd", 90_006.00, 90_006.00);
        strategy.onPolymarketCryptoPrice(tau62Ms, "btcusd", "chainlink", 90_006.00, tau62Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.60, 0.58, 0.42, 0.40, 500.0, 500.0, 0.60, 0.42, tau62Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 2. Strong cushion +$12.00 (z = 149 >= 95), but expensive ask = $0.78 (> $0.75 pre-window ceiling) MUST be rejected!
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau63Ms = CANDLE_END_MS - 63_000L;
        strategy.onPolymarketCryptoPrice(tau63Ms, "btcusd", "chainlink", 90_012.00, tau63Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.78, 0.76, 0.24, 0.22, 500.0, 500.0, 0.78, 0.24, tau63Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 3. Strong cushion +$12.00 at cheap ask = $0.68, but adverse calcSpot lead (-$4.00 > $3.20 deadband) MUST be rejected!
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        for (int s = 0; s < 5; s++) {
            long ts = warmStartMs + (s * 1_000L);
            strategy.onCalculatedSpotTwap(ts, "btcusd", 90_012.00, 90_012.00);
            strategy.onPolymarketCryptoPrice(ts + 50L, "btcusd", "chainlink", 90_012.00, ts);
        }
        strategy.onCalculatedSpotTwap(tau62Ms - 50L, "btcusd", 90_008.00, 90_010.00);
        strategy.onPolymarketCryptoPrice(tau62Ms, "btcusd", "chainlink", 90_012.00, tau62Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 500.0, 500.0, 0.68, 0.34, tau62Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 4. Contrarian ask < $0.56 (e.g. $0.50) before 60s window starts MUST be rejected even with +$12.00 cushion!
        long tau61p8Ms = CANDLE_END_MS - 61_800L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.50, 0.48, 0.52, 0.50, 500.0, 500.0, 0.50, 0.52, tau61p8Ms - 20L
        ));
        strategy.onCalculatedSpotTwap(tau61p8Ms - 50L, "btcusd", 90_012.00, 90_012.00);
        strategy.onPolymarketCryptoPrice(tau61p8Ms, "btcusd", "chainlink", 90_012.00, tau61p8Ms);
        assertThat(submittedOrders).isEmpty();

        // 5. Clean pre-window setup at tau = 61.5s (+$12.00, z >= 1.10, ask = $0.72 in [$0.56..$0.74], zero adverse lead) succeeds and caps limit <= $0.740!
        long tau61p5Ms = CANDLE_END_MS - 61_500L;
        strategy.onCalculatedSpotTwap(tau61p5Ms - 50L, "btcusd", 90_012.00, 90_012.00);
        strategy.onPolymarketCryptoPrice(tau61p5Ms, "btcusd", "chainlink", 90_012.00, tau61p5Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.72, 0.70, 0.30, 0.28, 500.0, 500.0, 0.72, 0.30, tau61p5Ms
        ));
        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().price()).isLessThanOrEqualTo(new BigDecimal("0.740"));
    }

    @Test
    @DisplayName("Should unlock $0.85-$0.92 entries strictly when TWAP is locked in (tau <= 30s, contrib >= $6.00, spot >= $4.00)")
    void shouldUnlockHighPrice850To920McOnlyWhenTwapIsTrulyLockedInAndSpotAgrees() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int s = 0; s <= 33; s++) {
            long ts = windowStartMs + (s * 1_000L);
            strategy.onCalculatedSpotTwap(ts, "btcusd", 90_014.00, 90_014.00);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_014.00, ts);
        }

        // 1. At tau = 27s, lockedContrib = 33/60 * $14.00 = $7.70 >= $6.00:
        // If spot drops to +$2.50 (< $4.00 required for > 840 mc), ask = $0.89 MUST be blocked!
        long tau27Ms = CANDLE_END_MS - 27_000L;
        strategy.onCalculatedSpotTwap(tau27Ms, "btcusd", 90_002.50, 90_012.00);
        strategy.onPolymarketCryptoPrice(tau27Ms, "btcusd", "chainlink", 90_002.50, tau27Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.89, 0.87, 0.13, 0.11, 500.0, 500.0, 0.89, 0.13, tau27Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 2. At tau = 26s, spot holds +$10.00 (>= $4.00, non-crashing), z > 3.0 in TWAP units, zero adverse calcSpot lead:
        // $0.93 MUST remain blocked (> maxEntryPriceMc = 920), while $0.89 is unlocked and limit caps at <= $0.920!
        long tau26Ms = CANDLE_END_MS - 26_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.93, 0.91, 0.09, 0.07, 500.0, 500.0, 0.93, 0.09, tau26Ms - 20L
        ));
        strategy.onCalculatedSpotTwap(tau26Ms, "btcusd", 90_010.00, 90_013.00);
        strategy.onPolymarketCryptoPrice(tau26Ms, "btcusd", "chainlink", 90_010.00, tau26Ms);
        assertThat(submittedOrders).isEmpty();

        strategy.onOrderBookQuote(new OrderBookQuote(
                0.89, 0.87, 0.13, 0.11, 500.0, 500.0, 0.89, 0.13, tau26Ms + 50L
        ));
        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().price()).isBetween(new BigDecimal("0.890"), new BigDecimal("0.920"));
    }

    @Test
    @DisplayName("Should conservatively boost existing Chainlink cushion and trigger immediate breakout evaluation on onCalculatedSpotTwap")
    void shouldAmplifyExistingChainlinkCushionOnFavorableCalculatedSpotBreakoutWithoutFabricatingDirection() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long warmStartMs = CANDLE_END_MS - 54_000L;
        for (int s = 0; s < 8; s++) {
            long ts = warmStartMs + (s * 1_000L);
            double p = (s % 2 == 0) ? 90_005.00 : 90_006.30;
            strategy.onCalculatedSpotTwap(ts, "btcusd", p, p);
            strategy.onPolymarketCryptoPrice(ts + 50L, "btcusd", "chainlink", p, ts);
        }

        // 1. Without calcSpot breakout at tau = 44s (sigma1s ~ 163 -> sigmaRem ~ 486 cents),
        // raw Chainlink +$5.00 has z = 102 < requiredMinZCenti(750) = 133 -> blocked!
        long tau44Ms = CANDLE_END_MS - 44_000L;
        strategy.onCalculatedSpotTwap(tau44Ms - 50L, "btcusd", 90_005.00, 90_005.00);
        strategy.onPolymarketCryptoPrice(tau44Ms, "btcusd", "chainlink", 90_005.00, tau44Ms - 50L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.75, 0.73, 0.27, 0.25, 500.0, 500.0, 0.75, 0.27, tau44Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 2. 1s later (tau = 43s), multi-exchange spot surges +$16.00 to $90,021.00 (4s velocity >= $4.50)
        // while Chainlink is still at +$5.00: onCalculatedSpotTwap immediately evaluates against cached quote before CLOB reprices!
        long tau43Ms = CANDLE_END_MS - 43_000L;
        strategy.onCalculatedSpotTwap(tau43Ms, "btcusd", 90_021.00, 90_008.00);
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should exempt deep-cushion pullbacks from adverseDrift only for discounted asks (<= $0.76) when multi-exchange spot confirms stabilization")
    void shouldAllowDeepCushionPullbackOnlyWhenMultiExchangeSpotConfirmsStabilization() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long warmStartMs = CANDLE_END_MS - 68_000L;
        for (int s = 0; s <= 15; s++) {
            long ts = warmStartMs + (s * 1_000L);
            strategy.onCalculatedSpotTwap(ts, "btcusd", 90_026.00, 90_026.00);
            strategy.onPolymarketCryptoPrice(ts + 50L, "btcusd", "chainlink", 90_026.00, ts);
        }

        // Pullback over 5s (tau = 52s -> 47s) from +$26.00 to +$17.00 (-$9.00 drift >= $5.93 block),
        // with remaining cushion $17.00 >= $14.00 and 17.00 >= 1.5 * 9.00 = $13.50:
        // 1. If multi-exchange spot leads further down ($90,012 -> -$5.00 adverse lead > $3.20 deadband), MUST remain blocked!
        long tau47Ms = CANDLE_END_MS - 47_000L;
        strategy.onCalculatedSpotTwap(tau47Ms - 1_000L, "btcusd", 90_017.00, 90_022.00);
        strategy.onCalculatedSpotTwap(tau47Ms - 50L, "btcusd", 90_012.00, 90_021.00);
        strategy.onPolymarketCryptoPrice(tau47Ms, "btcusd", "chainlink", 90_017.00, tau47Ms - 50L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 500.0, 500.0, 0.68, 0.34, tau47Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 2. When multi-exchange spot has stabilized at $90,017.00 over the last 4s:
        // expensive ask $0.77 (> $0.76) MUST remain blocked on pullback, while discounted ask $0.68 (<= $0.76) is allowed!
        long tau41Ms = CANDLE_END_MS - 41_000L;
        strategy.onOrderBookQuote(null);
        for (long ts = tau47Ms + 500L; ts <= tau41Ms - 4_500L; ts += 500L) {
            strategy.onCalculatedSpotTwap(ts, "btcusd", 90_026.00, 90_024.00);
        }
        strategy.onPolymarketCryptoPrice(tau41Ms - 5_000L, "btcusd", "chainlink", 90_026.00, tau41Ms - 5_000L);
        for (long ts = tau41Ms - 4_000L; ts <= tau41Ms; ts += 1_000L) {
            strategy.onCalculatedSpotTwap(ts, "btcusd", 90_017.00, 90_020.00);
        }
        strategy.onPolymarketCryptoPrice(tau41Ms, "btcusd", "chainlink", 90_017.00, tau41Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.77, 0.75, 0.25, 0.23, 500.0, 500.0, 0.77, 0.25, tau41Ms
        ));
        assertThat(submittedOrders).isEmpty();

        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 500.0, 500.0, 0.68, 0.34, tau41Ms + 50L
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should cap early low-z batch size at 25 shares and boost high-conviction batch size up to 60 shares")
    void shouldCapEarlyLowZBatchAt25SharesAndBoostHighConvictionBatchUpTo60Shares() {
        // 1. Early uncertain entry (tau = 50s > 38s, cushion = +$4.50 -> z = 77 < 95) at cheap ask = $0.50 (high raw edge):
        // batch size MUST be capped at <= 25 shares!
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau50Ms = CANDLE_END_MS - 50_000L;
        strategy.onPolymarketCryptoPrice(tau50Ms, "btcusd", "chainlink", 90_004.50, tau50Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.50, 0.48, 0.52, 0.50, 500.0, 500.0, 0.50, 0.52, tau50Ms
        ));
        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().size().intValue()).isEqualTo(25);

        // 2. High-conviction deep-edge entry (tau = 30s <= 36s, cushion = +$12.00 -> z > 4.0, ask = $0.60 <= $0.72 -> edge > 350 mc):
        // batch size unlocks 133% boost -> 60 shares!
        strategy.reset();
        submittedOrders.clear();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau30Ms = CANDLE_END_MS - 30_000L;
        strategy.onPolymarketCryptoPrice(tau30Ms, "btcusd", "chainlink", 90_012.00, tau30Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.60, 0.58, 0.42, 0.40, 500.0, 500.0, 0.60, 0.42, tau30Ms
        ));
        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().size().intValue()).isEqualTo(60);

        // 3. Dollar-risk normalization: unlocked expensive contract (ask = $0.78 > $0.76, !trulyLockedIn) caps single batch at 25 shares!
        strategy.reset();
        submittedOrders.clear();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau34Ms = CANDLE_END_MS - 34_000L;
        strategy.onPolymarketCryptoPrice(tau34Ms, "btcusd", "chainlink", 90_012.00, tau34Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.78, 0.76, 0.24, 0.22, 500.0, 500.0, 0.78, 0.24, tau34Ms
        ));
        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().size().intValue()).isEqualTo(25);
    }
}
