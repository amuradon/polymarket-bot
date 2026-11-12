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
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS).hasSize(62);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[0]).isZero();
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[5]).isEqualTo(1236);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[29]).isEqualTo(15896);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[30]).isEqualTo(16730);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[60]).isEqualTo(47730);
        assertThat(UtterParameciumStrategy.TWAP_REM_WEIGHT_BPS[61]).isEqualTo(48937);

        assertThat(UtterParameciumStrategy.normalCdfMc(0L)).isEqualTo(500);
        assertThat(UtterParameciumStrategy.normalCdfMc(100L)).isEqualTo(841);
        assertThat(UtterParameciumStrategy.normalCdfMc(-100L)).isEqualTo(159);
        assertThat(UtterParameciumStrategy.normalCdfMc(150L)).isEqualTo(933);
        assertThat(UtterParameciumStrategy.normalCdfMc(-150L)).isEqualTo(67);
        assertThat(UtterParameciumStrategy.normalCdfMc(400L)).isEqualTo(999);
        assertThat(UtterParameciumStrategy.normalCdfMc(650L)).isEqualTo(999);
        assertThat(UtterParameciumStrategy.normalCdfMc(-400L)).isEqualTo(1);
        assertThat(UtterParameciumStrategy.normalCdfMc(55L) + UtterParameciumStrategy.normalCdfMc(-55L)).isEqualTo(1000);

        assertThat(UtterParameciumStrategy.requiredMinZCenti(710)).isZero();
        assertThat(UtterParameciumStrategy.requiredMinZCenti(720)).isEqualTo(105L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(740)).isEqualTo(132L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(760)).isEqualTo(160L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(780)).isEqualTo(205L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(800)).isEqualTo(250L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(820)).isEqualTo(260L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(840)).isEqualTo(270L);
        assertThat(UtterParameciumStrategy.requiredMinZCenti(900)).isEqualTo(280L);

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
        assertThat(order.size().intValue()).isBetween(5, 30);
    }

    @Test
    @DisplayName("Should enforce [380, 840] mc price bounds, tau <= 38s gate for >= 800 mc, and [5s, 60s] tau bounds")
    void shouldSkipOutOfBoundsEntryPricesAndOutsideTauWindow() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long tau35Ms = CANDLE_END_MS - 35_000L;
        strategy.onPolymarketCryptoPrice(tau35Ms, "btcusd", "chainlink", 90_120.00, tau35Ms);

        // Best ask $0.850 (850 mc > 840 mc) must be rejected
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.850, 0.830, 0.170, 0.150, 1000.0, 1000.0, 0.850, 0.170, tau35Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // Underdog best ask $0.370 (370 mc < 380 mc) on UP while DOWN is $0.650 (edge negative for DOWN) must be rejected
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.370, 0.350, 0.650, 0.630, 1000.0, 1000.0, 0.370, 0.650, tau35Ms + 100L
        ));
        assertThat(submittedOrders).isEmpty();

        // Outside tau window: tau = 61s > maxTauMs (60s) and tau = 4s < minTauMs (5s)
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau61Ms = CANDLE_END_MS - 61_000L;
        strategy.onPolymarketCryptoPrice(tau61Ms, "btcusd", "chainlink", 90_050.00, tau61Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau61Ms
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

        // 1. At tau = 55s (early window, 0.583x time factor), moderate edge (ask = 0.70, pModel = 975 -> edge = 260 mc? Wait:
        // let's use ask = 0.72 with z = 1.60 -> pModel = 945, fee = 14, edge = 211? No, use z = 1.30 -> pModel = 903, ask = 0.70, fee = 15, edge = 188 -> wait:
        // for edge = 45 mc: ask = 0.70 (fee 15), pModel = 760 (z = 0.71, disallowed for >=700). At ask = 0.68 (fee 15), z = 0.66 -> pModel = 745 -> edge = 50 mc!)
        // At tau = 55s (sigmaRem = 669 cents), +$4.42 (+442 cents) gives z = 66 -> pModel = 745 mc -> at ask = 0.68 (fee 15): edge = 52 mc!
        long tEarly = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tEarly, "btcusd", "chainlink", 90_004.42, tEarly);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 500.0, 500.0, 0.68, 0.34, tEarly
        ));

        assertThat(submittedOrders).hasSize(1);
        OrderCommand earlyOrder = submittedOrders.getFirst();
        int earlyShares = earlyOrder.size().intValue();
        assertThat(earlyShares).isBetween(12, 18);

        strategy.onExecutionReport(new ExecutionReport(
                earlyOrder.clientOrderId(), "ex-1", "FILLED", earlyOrder.price(), earlyOrder.size(), tEarly + 20L, "Filled"
        ));

        // 2. Quote 200ms later (< 350ms cooldown) must be blocked
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 500.0, 500.0, 0.68, 0.34, tEarly + 200L
        ));
        assertThat(submittedOrders).hasSize(1);

        // 3. Compare identical edge = 50 mc at tau = 55s (0.70x time factor) vs tau = 25s (1.083x time factor)
        long tLate = CANDLE_END_MS - 25_000L;
        UtterParameciumStrategy equalEdgeStrategy = new UtterParameciumStrategy(new UtterParameciumStrategyConfig(
                5_000L, 56_000L, 500L, 380, 840, 80, 650, 975, 25, 50,
                5, 30, 65, 600, 2_800, 26_000L, false, 950L, 100,
                6_000, 850L, 1_250L,
                400L, 105L, 3_500L, 120L, 700, "pm-btc-15m", "BTC-UP", "BTC-DOWN"
        ));
        equalEdgeStrategy.init(testContext);
        submittedOrders.clear();

        // At tau = 55s: sigmaRem = 400 * 4.1847 = 1673 cents; +$11.10 -> z = 66 -> pModel = 745 mc -> edge at 0.68 = 50 mc
        equalEdgeStrategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        equalEdgeStrategy.onPolymarketCryptoPrice(tEarly, "btcusd", "chainlink", 90_011.10, tEarly);
        equalEdgeStrategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 500.0, 500.0, 0.68, 0.34, tEarly
        ));
        int qAt55s = submittedOrders.getFirst().size().intValue();

        // At tau = 25s: sigmaRem = 400 * 1.2713 = 508 cents; +$3.36 -> z = 66 -> pModel = 745 mc -> identical edge = 50 mc!
        equalEdgeStrategy.reset();
        submittedOrders.clear();
        equalEdgeStrategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        equalEdgeStrategy.onPolymarketCryptoPrice(tLate, "btcusd", "chainlink", 90_003.36, tLate);
        equalEdgeStrategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.66, 0.34, 0.32, 500.0, 500.0, 0.68, 0.34, tLate
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
    @DisplayName("Should enforce continuous fat-tail z-score curve from 720 mc ($0.72) through 840 mc ($0.84)")
    void shouldEnforceContinuousZScoreCurveAcross720To840Mc() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Mid-high price $0.76 (760 mc) requires z >= 160 centi-sigma (1.60 sigma):
        // At tau = 55s (sigmaRem = 669 cents), +$9.50 (+950 cents) gives z = 142 (< 160) -> MUST reject $0.76 even though Phi(1.42)=922 > 760+13+25!
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_009.50, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.76, 0.74, 0.26, 0.24, 500.0, 500.0, 0.76, 0.26, tau55Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // 2. High price $0.82 (820 mc) at tau = 36s (<= 38s, sigmaRem = 160 * 2.2035 = 352 cents) requires z >= 260 centi-sigma:
        // At +$8.00 (+800 cents), z = 227 (< 260) -> rejected; at +$10.00 (+1000 cents), z = 284 (>= 260) -> allowed!
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau36Ms = CANDLE_END_MS - 36_000L;
        strategy.onPolymarketCryptoPrice(tau36Ms, "btcusd", "chainlink", 90_008.00, tau36Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau36Ms
        ));
        assertThat(submittedOrders).isEmpty();

        long tau35Ms = CANDLE_END_MS - 35_000L;
        strategy.onPolymarketCryptoPrice(tau35Ms, "btcusd", "chainlink", 90_010.00, tau35Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau35Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should trigger refreshAccountBalance once per market at tau = maxTauMs + 12000 (72000 ms)")
    void shouldTriggerRefreshAccountBalanceOncePerMarketBeforeActivation() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        assertThat(balanceRefreshCount).isZero();

        long tau73Ms = CANDLE_END_MS - 73_000L;
        strategy.onPolymarketCryptoPrice(tau73Ms, "btcusd", "chainlink", 90_010.00, tau73Ms);
        assertThat(balanceRefreshCount).isZero();

        long tau71Ms = CANDLE_END_MS - 71_000L;
        strategy.onPolymarketCryptoPrice(tau71Ms, "btcusd", "chainlink", 90_012.00, tau71Ms);
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
        // Spot = $90,009.50 (+950 cents) -> z = 142 centi-sigma -> pModelMc = 922 mc.
        // requiredMinZCenti: 730->118 (ok), 740->132 (ok), 750->146 (> 142 -> blocked!).
        // So at askUp = $0.73 (730 mc), stepping up reaches 740 mc and stops before 750 mc!
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_009.50, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.73, 0.71, 0.29, 0.27, 500.0, 500.0, 0.73, 0.29, tau55Ms
        ));

        assertThat(submittedOrders).hasSize(1);
        OrderCommand tightZOrder = submittedOrders.getFirst();
        assertThat(tightZOrder.price()).isEqualByComparingTo("0.740");

        // 2. High-price tau gate cap at tau = 42s (> 38s, sigmaRem = 445 cents):
        // Spot = $90,012.00 (+1200 cents) -> z = 269 centi-sigma, pModelMc = 975 mc.
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
    @DisplayName("Should stage inventory from 50% at tau=60s to 100% at tau<=36s and scale dynamically by option price risk and conviction")
    void shouldStageInventoryByTimeAndScaleDynamicallyByConviction() {
        assertThat(strategy.computeTimeStagedMaxInventory(60_000L)).isEqualTo(300);
        assertThat(strategy.computeTimeStagedMaxInventory(48_000L)).isEqualTo(450);
        assertThat(strategy.computeTimeStagedMaxInventory(36_000L)).isEqualTo(600);
        assertThat(strategy.computeTimeStagedMaxInventory(15_000L)).isEqualTo(600);

        // Price-risk & conviction scaling checks:
        // Core contract (ask = 580 <= 720) at tau = 42s unlocks 100% of timeStagedCap(42s = 525 shares)
        assertThat(strategy.computeDynamicMaxInventory(42_000L, 580, 105L, 90)).isEqualTo(525);

        // Core contract (ask = 720 <= 720) at tau <= 36s unlocks 100% of timeStagedCap (600 shares)
        assertThat(strategy.computeDynamicMaxInventory(32_000L, 720, 115L, 45)).isEqualTo(600);

        // Core contract late high-conviction bonus (tau <= 35s, z >= 140, edge >= 55) -> 115% = 690 shares
        assertThat(strategy.computeDynamicMaxInventory(24_000L, 580, 180L, 80)).isEqualTo(690);

        // Mid-high contract (ask = 750 in 721..760) -> 65% = 390 shares
        assertThat(strategy.computeDynamicMaxInventory(32_000L, 750, 165L, 45)).isEqualTo(390);

        // Expensive contract (ask = 780 in 761..799) -> 40% = 240 shares
        assertThat(strategy.computeDynamicMaxInventory(32_000L, 780, 215L, 40)).isEqualTo(240);

        // Ultra-expensive contract (ask = 820 >= 800) -> 30% = 180 shares
        assertThat(strategy.computeDynamicMaxInventory(32_000L, 820, 265L, 40)).isEqualTo(180);

        // Simulation: expensive $0.78 contract between tau = 40s and tau = 32s MUST cap at 40% of timeStagedCap(32s = 240 shares)
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
                .isGreaterThan(90)
                .isLessThanOrEqualTo(strategy.computeDynamicMaxInventory(32_000L, 780, 230L, 40));
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

        // At tau = 30s with defaultSigma1s = 160 cents, requiredMinZCenti(820) = 260:
        // Without lag: weightBps[30] = 16730 -> sigmaRem = 267 cents. With +$7.20 (+720 cents), z = 720 / 267 = 2.69 (>= 2.60) -> allowed.
        // With 3,000ms oracle lag: effectiveTauSec = 33 -> weightBps[33] = 19319 -> sigmaRem = 309 cents -> z = 720 / 309 = 2.33 (< 2.60) -> rejected!
        long tau30Ms = CANDLE_END_MS - 30_000L;
        strategy.onPolymarketCryptoPrice(tau30Ms, "btcusd", "chainlink", 90_007.20, tau30Ms - 3_000L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau30Ms
        ));
        assertThat(submittedOrders).isEmpty();

        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        strategy.onPolymarketCryptoPrice(tau30Ms, "btcusd", "chainlink", 90_007.20, tau30Ms);
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
        assertThat(strategy.getRequiredClobLookbackMs()).isEqualTo(77_000L);
    }
}
