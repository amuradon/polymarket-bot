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
    @DisplayName("Should validate normalCdfMc, TWAP_REM_WEIGHT_BPS, and sub-cent tick rounding helpers")
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
        assertThat(order.size().intValue()).isBetween(5, 35);
    }

    @Test
    @DisplayName("Should skip expensive or decided markets when bestAsk exceeds maxEntryPriceMc (880 mc = $0.88)")
    void shouldSkipDecidedMarketAboveMaxEntryPrice() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long tau60Ms = CANDLE_END_MS - 60_000L;
        strategy.onPolymarketCryptoPrice(tau60Ms, "btcusd", "chainlink", 90_120.00, tau60Ms);

        // Best ask $0.890 (890 mc > 880 mc) must be rejected
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.890, 0.870, 0.130, 0.110, 1000.0, 1000.0, 0.890, 0.130, tau60Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // Best ask $0.990 (990 mc > 880 mc) must also be rejected
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.990, 0.980, 0.020, 0.010, 1000.0, 1000.0, 0.990, 0.020, tau60Ms + 500L
        ));
        assertThat(submittedOrders).isEmpty();

        // Outside tau window (tau = 65s > 61s and tau = 4s < 5s)
        long tau65Ms = CANDLE_END_MS - 65_000L;
        strategy.onPolymarketCryptoPrice(tau65Ms, "btcusd", "chainlink", 90_050.00, tau65Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau65Ms
        ));
        assertThat(submittedOrders).isEmpty();

        long tau4Ms = CANDLE_END_MS - 4_000L;
        strategy.onPolymarketCryptoPrice(tau4Ms, "btcusd", "chainlink", 90_050.00, tau4Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau4Ms
        ));
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should emit initial burst of 15 shares at tau >= 55s, enforce 400ms cooldown, and scale subsequent orders")
    void shouldEmitInitialBurstEnforceCooldownAndScaleSubsequentOrders() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        long t1 = CANDLE_END_MS - 59_500L;
        strategy.onPolymarketCryptoPrice(t1, "btcusd", "chainlink", 90_045.00, t1);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, t1
        ));

        // 1. First trade at tau >= 55s must request initialBurstShares = 15
        assertThat(submittedOrders).hasSize(1);
        OrderCommand firstOrder = submittedOrders.getFirst();
        assertThat(firstOrder.size()).isEqualByComparingTo("15");

        strategy.onExecutionReport(new ExecutionReport(
                firstOrder.clientOrderId(),
                "ex-1",
                "FILLED",
                firstOrder.price(),
                firstOrder.size(),
                t1 + 20L,
                "Filled"
        ));

        // 2. Quote 200ms later (< 400ms cooldown) must be blocked
        long tTooSoon = t1 + 200L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tTooSoon
        ));
        assertThat(submittedOrders).hasSize(1);

        // 3. Quote 450ms later (>= 400ms cooldown) must emit a scaled second order
        long t2 = t1 + 450L;
        strategy.onPolymarketCryptoPrice(t2, "btcusd", "chainlink", 90_045.00, t2);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, t2
        ));
        assertThat(submittedOrders).hasSize(2);
        OrderCommand secondOrder = submittedOrders.get(1);
        assertThat(secondOrder.size().intValue()).isGreaterThan(5).isLessThanOrEqualTo(30);
    }

    @Test
    @DisplayName("Should handle sub-cent BBO quotes (bestBid=0.855, bestAsk=0.865) via conservative 10-mc rounding capped at maxEntryPriceMc")
    void shouldHandleSubCentBboQuotes() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // At tau = 25s, BTC is +$80 above strike -> Z >= 4.0 -> pModelMc = maxModelProbMc (975 mc)
        long ts = CANDLE_END_MS - 25_000L;
        strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_080.00, ts);

        // Sub-cent BBO: bestBid = 0.855 (floor 850 mc), bestAsk = 0.865 (ceil 870 mc), limit expands to maxEntryPriceMc = 880 mc
        OrderBookQuote subCentQuote = new OrderBookQuote(
                0.865, 0.855, 0.145, 0.135, 500.0, 500.0, 0.865, 0.145, ts
        );
        strategy.onOrderBookQuote(subCentQuote);

        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().price()).isEqualByComparingTo("0.880");
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

        // 1. At tau = 58s, BTC is +$40 above strike -> buys UP
        long tUp = CANDLE_END_MS - 58_000L;
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
        assertThat(strategy.getNetSharesUp()).isEqualTo(15);

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
    @DisplayName("Should require z >= 2.50 sigma before buying high-priced contracts (askMc >= 800)")
    void shouldRequireHighZScoreForHighPricedContracts() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // At tau = 55s with defaultSigma1s = 160 cents ($1.60/s), sigmaRem = 160 * 4.1847 = 669 cents ($6.69).
        // If BTC is +$13.00 (+1300 cents) above strike, z = 1300 / 669 = 1.94 sigma (< 2.50 sigma):
        // must reject askUp = $0.82 (820 mc >= 800 mc) even though pModelMc = 974 > 820 + fee + 25.
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_013.00, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau55Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // When BTC moves to +$25.00 (+2500 cents) above strike, z = 2500 / 651 = 3.84 sigma (> 2.50 sigma) -> order allowed
        long tau54Ms = CANDLE_END_MS - 54_000L;
        strategy.onPolymarketCryptoPrice(tau54Ms, "btcusd", "chainlink", 90_025.00, tau54Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau54Ms
        ));
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should trigger refreshAccountBalance once per market at tau = maxTauMs + 10000 (71000 ms)")
    void shouldTriggerRefreshAccountBalanceOncePerMarketBeforeActivation() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        assertThat(balanceRefreshCount).isZero();

        long tau72Ms = CANDLE_END_MS - 72_000L;
        strategy.onPolymarketCryptoPrice(tau72Ms, "btcusd", "chainlink", 90_010.00, tau72Ms);
        assertThat(balanceRefreshCount).isZero();

        long tau71Ms = CANDLE_END_MS - 71_000L;
        strategy.onPolymarketCryptoPrice(tau71Ms, "btcusd", "chainlink", 90_012.00, tau71Ms);
        assertThat(balanceRefreshCount).isEqualTo(1);

        long tau60Ms = CANDLE_END_MS - 60_000L;
        strategy.onPolymarketCryptoPrice(tau60Ms, "btcusd", "chainlink", 90_015.00, tau60Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tau60Ms
        ));
        assertThat(balanceRefreshCount).isEqualTo(1);

        long nextCandleStartMs = CANDLE_END_MS;
        long nextCandleEndMs = nextCandleStartMs + 900_000L;
        strategy.onPolymarketCryptoTwap60s(nextCandleStartMs, "btcusd", "chainlink", 90_100.00, 60, nextCandleStartMs);
        strategy.onPolymarketCryptoPrice(nextCandleEndMs - 80_000L, "btcusd", "chainlink", 90_110.00, nextCandleEndMs - 80_000L);
        assertThat(balanceRefreshCount).isEqualTo(1);

        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, nextCandleEndMs - 70_500L
        ));
        assertThat(balanceRefreshCount).isEqualTo(2);
    }

    @Test
    @DisplayName("Should expand limit price only while minEdgeMc, z-score gates, maxEntryPriceMc, and maxSlippageMc remain satisfied")
    void shouldComputeEdgePreservingLimitPriceCappedByMinEdgeZScoreAndMaxSlippage() {
        assertThat(config.maxSlippageMc()).isEqualTo(50);

        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. Tight-edge scenario at tau = 55s (defaultSigma1s = 160 -> sigmaRem = 669 cents):
        // Spot = $90,004.96 (+496 cents) -> z = 74 centi-sigma -> pModelMc = 770 mc.
        // At askUp = $0.71 (710 mc): fee = 14 mc, edge = 46 mc >= 25 mc; steps up to 730 mc (edge = 26 mc), stops before 740 mc.
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_004.96, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.71, 0.69, 0.31, 0.29, 500.0, 500.0, 0.71, 0.31, tau55Ms
        ));

        assertThat(submittedOrders).hasSize(1);
        OrderCommand tightEdgeOrder = submittedOrders.getFirst();
        assertThat(tightEdgeOrder.price()).isEqualByComparingTo("0.730");

        // 2. Z-score gate cap scenario on a clean market at tau = 55s (sigmaRem = 669 cents):
        // Spot = $90,013.00 (+1300 cents) -> z = 194 centi-sigma (< 250 required for >= 800 mc), pModelMc = 974 mc.
        // At askUp = $0.78 (780 mc): allowed (< 800 mc). Stepping up reaches 790 mc, MUST NOT cross into 800 mc!
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_013.00, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.78, 0.76, 0.24, 0.22, 500.0, 500.0, 0.78, 0.24, tau55Ms
        ));

        assertThat(submittedOrders).hasSize(2);
        OrderCommand zCappedOrder = submittedOrders.get(1);
        assertThat(zCappedOrder.price()).isEqualByComparingTo("0.790");
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
    @DisplayName("Should retain intra-candle 5s RMS volatility regime across a quiet 60s pre-window lull")
    void shouldRetainCandleVolatilityRegimeAcrossQuietLull() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // Simulate active 5-minute intra-candle volatility: alternating +/- $25 every 5 seconds (sigma_1s = 25 / sqrt(5) = $11.18/s)
        long regimeStartMs = CANDLE_END_MS - 360_000L;
        for (int i = 0; i < 40; i++) {
            long ts = regimeStartMs + (i * 5_000L);
            double price = (i % 2 == 0) ? 90_025.00 : 90_000.00;
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", price, ts);
        }

        // Simulate a deceptive 60-second quiet pause right before tau = 55s at $90,012.00 (+$12 above strike)
        long lullStartMs = CANDLE_END_MS - 115_000L;
        for (int s = 0; s <= 60; s++) {
            long ts = lullStartMs + (s * 1_000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_012.00, ts);
        }

        // At tau = 55s, old 64-tick 1s-MAD would collapse to floor and falsely buy askUp = $0.75 on a tiny +$12 lead;
        // regime 5s-RMS remembers ~$11/s -> sigmaRem(55s) ~ $46 -> z ~ 0.26 -> pModelMc ~ 602 < 650 -> MUST reject!
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.75, 0.73, 0.27, 0.25, 500.0, 500.0, 0.75, 0.27, tau55Ms
        ));
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should stage maximum cumulative inventory with TWAP lock-in progression from 55% at tau=60s to 100% at tau<=35s")
    void shouldCapEarlyInventoryAndUnlockProgressivelyAsTwapLocksIn() {
        assertThat(strategy.computeTimeStagedMaxInventory(60_000L)).isEqualTo(330);
        assertThat(strategy.computeTimeStagedMaxInventory(50_000L)).isEqualTo(438);
        assertThat(strategy.computeTimeStagedMaxInventory(40_000L)).isEqualTo(546);
        assertThat(strategy.computeTimeStagedMaxInventory(35_000L)).isEqualTo(600);
        assertThat(strategy.computeTimeStagedMaxInventory(20_000L)).isEqualTo(600);

        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // Feed strong +$60 lead from tau = 60s to tau = 54s at 400ms intervals -> capped at stagedCap(54s) = 394 shares (< 600!)
        for (long ts = CANDLE_END_MS - 60_000L; ts <= CANDLE_END_MS - 54_000L; ts += 450L) {
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_060.00, ts);
            strategy.onOrderBookQuote(new OrderBookQuote(
                    0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, ts
            ));
            if (!submittedOrders.isEmpty()) {
                OrderCommand last = submittedOrders.getLast();
                strategy.onExecutionReport(new ExecutionReport(
                        last.clientOrderId(), "ex-" + ts, "FILLED", last.price(), last.size(), ts + 10L, "Filled"
                ));
            }
        }

        assertThat(strategy.getNetSharesUp())
                .isGreaterThan(300)
                .isLessThanOrEqualTo(strategy.computeTimeStagedMaxInventory(54_000L));
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
        // CLOB UP BBO was $0.94/$0.96 at tau = 48s and crashes by -120 mc (>= 100 mc) to $0.82/$0.84 at tau = 45s
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tau48Ms = CANDLE_END_MS - 48_000L;
        long tau45Ms = CANDLE_END_MS - 45_000L;
        strategy.onPolymarketCryptoPrice(tau48Ms, "btcusd", "chainlink", 90_035.00, tau48Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.96, 0.94, 0.06, 0.04, 500.0, 500.0, 0.96, 0.06, tau48Ms
        ));
        assertThat(submittedOrders).isEmpty();

        strategy.onPolymarketCryptoPrice(tau45Ms, "btcusd", "chainlink", 90_035.00, tau45Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.84, 0.82, 0.18, 0.16, 500.0, 500.0, 0.84, 0.18, tau45Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // Once 8+ seconds pass and CLOB stabilizes at $0.82/$0.84 (tau = 36s > 25s), entry is allowed cleanly
        long tau36Ms = CANDLE_END_MS - 36_000L;
        strategy.onPolymarketCryptoPrice(tau36Ms, "btcusd", "chainlink", 90_035.00, tau36Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.84, 0.82, 0.18, 0.16, 500.0, 500.0, 0.84, 0.18, tau36Ms
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
        // Thin ask temporarily jumps to 0.68 in a new 1s bucket while bid stays 0.53
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.68, 0.53, 0.47, 0.32, 500.0, 500.0, 0.68, 0.47, tSweep0 + 1_020L
        ));
        // 1.1s later market maker replenishes ask at 0.56 while bid is 0.54 -> must NOT be falsely blocked!
        long tRefill = tSweep0 + 2_100L;
        strategy.onPolymarketCryptoPrice(tRefill, "btcusd", "chainlink", 90_020.00, tRefill);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.56, 0.54, 0.46, 0.44, 500.0, 500.0, 0.56, 0.46, tRefill
        ));
        assertThat(submittedOrders).hasSize(3);
    }

    @Test
    @DisplayName("Should widen sigmaRemCents when PolyBolt priceTimestampMs lags behind event timestampMs")
    void shouldAccountForOracleLagInRemainingUncertainty() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // At tau = 30s with defaultSigma1s = 160 cents:
        // Without lag: weightBps[30] = 16730 -> sigmaRem = 160 * 1.6730 = 267 cents. With +$6.80 (+680 cents), z = 680 / 267 = 2.54 (>= 2.50 for ask 0.82).
        // With 3,000ms oracle lag: effectiveTauSec = 33 -> weightBps[33] = 19319 -> sigmaRem = 160 * 1.9319 = 309 cents -> z = 680 / 309 = 2.20 (< 2.50) -> rejected!
        long tau30Ms = CANDLE_END_MS - 30_000L;
        strategy.onPolymarketCryptoPrice(tau30Ms, "btcusd", "chainlink", 90_006.80, tau30Ms - 3_000L);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau30Ms
        ));
        assertThat(submittedOrders).isEmpty();
    }
}
