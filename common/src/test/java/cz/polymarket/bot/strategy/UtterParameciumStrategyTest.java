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
 * Unit tests for {@link UtterParameciumStrategy} (Model V1) and {@link UtterParameciumStrategyConfig}.
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
        // TwapUpdate from TwapEngine must be completely ignored
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                CANDLE_START_SEC,
                CANDLE_END_SEC,
                BigDecimal.valueOf(90_000.00),
                new TwapPoint(CANDLE_START_SEC, BigDecimal.valueOf(90_000.00), BigDecimal.valueOf(90_000.00)),
                true
        ));
        assertThat(strategy.getTargetPriceCents()).isZero();

        // Feed PolyBolt price.crypto and CLOB quote in the final minute WITHOUT any 60s TWAP target price at candle start
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
        // Explicit target price from PolyBolt 60s TWAP stream = $90,000.00
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        assertThat(strategy.getTargetPriceCents()).isEqualTo(9_000_000L);

        // First 40 seconds of the final 60s window (tau = 60s down to tau = 21s): BTC holds +$25 above strike ($90,025.00)
        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int s = 0; s < 40; s++) {
            long ts = windowStartMs + (s * 1000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_025.00, ts);
        }

        // At tau = 20s (ts = CANDLE_END_MS - 20_000L), spot suddenly drops -$5 below strike to $89,995.00!
        // Locked 40s average is $90,025.00; remaining 20s projected at $89,995.00 ->
        // Projected final TWAP = (40 * 90025 + 20 * 89995) / 60 = $90,015.00 (+$15.00 above strike!)
        long tau20Ms = CANDLE_END_MS - 20_000L;
        strategy.onPolymarketCryptoPrice(tau20Ms, "btcusd", "chainlink", 89_995.00, tau20Ms);

        // CLOB order book panicked on spot drop and offers bestAskUp = $0.65 (650 mc), bestBidUp = $0.63
        OrderBookQuote quote = new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tau20Ms
        );
        strategy.onOrderBookQuote(quote);

        assertThat(submittedOrders).hasSize(1);
        OrderCommand order = submittedOrders.getFirst();
        assertThat(order.token()).isEqualTo("BTC-UP");
        assertThat(order.side()).isEqualTo("BUY");
        assertThat(order.price()).isEqualByComparingTo("0.650");
        assertThat(order.size().intValue()).isBetween(5, 35);
    }

    @Test
    @DisplayName("Should skip decided markets when bestAsk exceeds maxEntryPriceMc (970 mc = $0.97)")
    void shouldSkipDecidedMarketAboveMaxEntryPrice() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // BTC is +$120 above strike at tau = 60s
        long tau60Ms = CANDLE_END_MS - 60_000L;
        strategy.onPolymarketCryptoPrice(tau60Ms, "btcusd", "chainlink", 90_120.00, tau60Ms);

        // Best ask is already $0.990 (990 mc > 970 mc)
        OrderBookQuote decidedQuote = new OrderBookQuote(
                0.990, 0.980, 0.020, 0.010, 1000.0, 1000.0, 0.990, 0.020, tau60Ms
        );
        strategy.onOrderBookQuote(decidedQuote);

        assertThat(submittedOrders).isEmpty();

        // Also verify outside tau window (tau = 65s > 61s and tau = 4s < 5s)
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
    @DisplayName("Should emit initial burst of 15 shares at tau >= 55s, enforce 250ms cooldown, and scale subsequent orders")
    void shouldEmitInitialBurstEnforceCooldownAndScaleSubsequentOrders() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // At tau = 59.5s (ts = CANDLE_END_MS - 59_500L), BTC is +$45 above strike ($90,045)
        long t1 = CANDLE_END_MS - 59_500L;
        strategy.onPolymarketCryptoPrice(t1, "btcusd", "chainlink", 90_045.00, t1);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, t1
        ));

        // 1. First trade at tau >= 55s must request initialBurstShares = 15
        assertThat(submittedOrders).hasSize(1);
        OrderCommand firstOrder = submittedOrders.getFirst();
        assertThat(firstOrder.size()).isEqualByComparingTo("15");

        // Acknowledge fill of first order
        strategy.onExecutionReport(new ExecutionReport(
                firstOrder.clientOrderId(),
                "ex-1",
                "FILLED",
                firstOrder.price(),
                firstOrder.size(),
                t1 + 20L,
                "Filled"
        ));

        // 2. Quote 100ms later (< 250ms cooldown) must be blocked
        long tTooSoon = t1 + 100L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tTooSoon
        ));
        assertThat(submittedOrders).hasSize(1);

        // 3. Quote 260ms later (>= 250ms cooldown) must emit a scaled second order
        long t2 = t1 + 260L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, t2
        ));
        assertThat(submittedOrders).hasSize(2);
        OrderCommand secondOrder = submittedOrders.get(1);
        assertThat(secondOrder.size().intValue()).isGreaterThan(5).isLessThanOrEqualTo(30);
    }

    @Test
    @DisplayName("Should handle sub-cent BBO quotes (bestBid=0.955, bestAsk=0.965) via conservative 10-mc rounding")
    void shouldHandleSubCentBboQuotes() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // At tau = 25s, BTC is +$80 above strike -> Z >= 4.0 -> P_Up = 999 mc
        long ts = CANDLE_END_MS - 25_000L;
        strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_080.00, ts);

        // Sub-cent BBO: bestBid = 0.955 (955 mc -> floor 950 mc), bestAsk = 0.965 (965 mc -> ceil 970 mc)
        OrderBookQuote subCentQuote = new OrderBookQuote(
                0.965, 0.955, 0.045, 0.035, 500.0, 500.0, 0.965, 0.045, ts
        );
        strategy.onOrderBookQuote(subCentQuote);

        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().price()).isEqualByComparingTo("0.970");
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
    @DisplayName("Should accumulate both UP and DOWN positions on mid-window flip and hold to expiration")
    void shouldAccumulateBidirectionalPositionsOnMidWindowFlipAndHoldToExpiration() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // 1. At tau = 58s, BTC is +$35 above strike -> buys UP
        long tUp = CANDLE_END_MS - 58_000L;
        strategy.onPolymarketCryptoPrice(tUp, "btcusd", "chainlink", 90_035.00, tUp);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tUp
        ));

        assertThat(submittedOrders).hasSize(1);
        OrderCommand upOrder = submittedOrders.getFirst();
        assertThat(upOrder.token()).isEqualTo("BTC-UP");
        assertThat(upOrder.side()).isEqualTo("BUY");

        strategy.onExecutionReport(new ExecutionReport(
                upOrder.clientOrderId(), "ex-up", "FILLED", upOrder.price(), upOrder.size(), tUp + 10L, "Filled"
        ));
        assertThat(strategy.getNetSharesUp()).isEqualTo(15);

        // 2. At tau = 50s, BTC drops sharply -$120 below strike ($89,880) so projected TWAP flips strongly DOWN
        long tDown = CANDLE_END_MS - 50_000L;
        strategy.onPolymarketCryptoPrice(tDown, "btcusd", "chainlink", 89_880.00, tDown);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.30, 0.28, 0.72, 0.70, 500.0, 500.0, 0.30, 0.72, tDown
        ));

        assertThat(submittedOrders).hasSize(2);
        OrderCommand downOrder = submittedOrders.get(1);
        assertThat(downOrder.token()).isEqualTo("BTC-DOWN");
        assertThat(downOrder.side()).isEqualTo("BUY");

        strategy.onExecutionReport(new ExecutionReport(
                downOrder.clientOrderId(), "ex-down", "FILLED", downOrder.price(), downOrder.size(), tDown + 10L, "Filled"
        ));
        assertThat(strategy.getNetSharesDown()).isGreaterThanOrEqualTo(5);

        // 3. Resolution holds to expiration without emitting any SELL order
        strategy.onCandleResolution(TradeDirection.DOWN);
        assertThat(submittedOrders).hasSize(2);
    }

    @Test
    @DisplayName("Should block entry during early-window spot reversal when spot and projected TWAP disagree")
    void shouldBlockEntryDuringEarlyWindowSpotReversalWhenSpotDisagreesWithProjectedTwap() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);

        // First 12 seconds of final minute (tau = 60s..49s): BTC is -$30 below strike ($89,970)
        long windowStartMs = CANDLE_END_MS - 60_000L;
        for (int s = 0; s < 12; s++) {
            long ts = windowStartMs + (s * 1000L);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 89_970.00, ts);
        }

        // At tau = 48s, BTC reverses sharply above strike to $90,004.00 (+$4 above strike),
        // while projected TWAP still lags below strike at (12 * 89970 + 48 * 90004) / 60 = $89,997.20 (-$2.80).
        // Neither UP (projected TWAP < strike) nor DOWN (spot > strike at tau = 48s > 25s) may be bought.
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
        // Packet received 70ms after candle start carrying prior-second twapTimestampMs = CANDLE_START_MS - 1000ms must be ignored
        strategy.onPolymarketCryptoTwap60s(
                CANDLE_START_MS + 70L,
                "btcusd",
                "chainlink",
                89_998.00,
                60,
                CANDLE_START_MS - 1_000L
        );
        assertThat(strategy.getTargetPriceCents()).isZero();

        // Exact candle-start TWAP packet arriving 1.1s after candle start locks targetPriceCents
        strategy.onPolymarketCryptoTwap60s(
                CANDLE_START_MS + 1_100L,
                "btcusd",
                "chainlink",
                90_000.00,
                60,
                CANDLE_START_MS
        );
        assertThat(strategy.getTargetPriceCents()).isEqualTo(9_000_000L);

        // Subsequent TWAP packets must not overwrite targetPriceCents
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

        // At tau = 55s with defaultSigma1s = 150 cents ($1.50/s), sigmaRem = 150 * 4.1847 = 627 cents ($6.27).
        // If BTC is only +$12.00 (+1200 cents) above strike, z = 1200 / 627 = 1.91 sigma (< 2.50 sigma):
        // must reject askUp = $0.82 (820 mc >= 800 mc) even though normalCdfMc(191) = 972 > 820 + fee + 25.
        long tau55Ms = CANDLE_END_MS - 55_000L;
        strategy.onPolymarketCryptoPrice(tau55Ms, "btcusd", "chainlink", 90_012.00, tau55Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.82, 0.80, 0.20, 0.18, 500.0, 500.0, 0.82, 0.20, tau55Ms
        ));
        assertThat(submittedOrders).isEmpty();

        // When BTC moves to +$30.00 (+3000 cents) above strike, z > 2.50 sigma -> order allowed
        long tau54Ms = CANDLE_END_MS - 54_000L;
        strategy.onPolymarketCryptoPrice(tau54Ms, "btcusd", "chainlink", 90_030.00, tau54Ms);
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

        // At tau = 72_000 ms (> 71_000 ms = maxTauMs 61_000 + 10_000), balance refresh must NOT be triggered yet
        long tau72Ms = CANDLE_END_MS - 72_000L;
        strategy.onPolymarketCryptoPrice(tau72Ms, "btcusd", "chainlink", 90_010.00, tau72Ms);
        assertThat(balanceRefreshCount).isZero();

        // At tau = 71_000 ms (== maxTauMs + 10_000), balance refresh MUST be triggered once
        long tau71Ms = CANDLE_END_MS - 71_000L;
        strategy.onPolymarketCryptoPrice(tau71Ms, "btcusd", "chainlink", 90_012.00, tau71Ms);
        assertThat(balanceRefreshCount).isEqualTo(1);

        // Subsequent ticks and quotes in the same market must NOT trigger duplicate refreshes
        long tau60Ms = CANDLE_END_MS - 60_000L;
        strategy.onPolymarketCryptoPrice(tau60Ms, "btcusd", "chainlink", 90_015.00, tau60Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.65, 0.63, 0.37, 0.35, 500.0, 500.0, 0.65, 0.37, tau60Ms
        ));
        assertThat(balanceRefreshCount).isEqualTo(1);

        // Next 15m market: resets state and triggers again when entering tau <= 71_000 ms
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
}
