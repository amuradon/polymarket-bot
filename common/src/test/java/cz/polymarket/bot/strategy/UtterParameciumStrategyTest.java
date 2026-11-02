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
    private StrategyContext testContext;

    @BeforeEach
    void setUp() {
        config = UtterParameciumStrategyConfig.defaultModelV1();
        strategy = new UtterParameciumStrategy(config);
        submittedOrders = new ArrayList<>();

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
    @DisplayName("Should infer and lock strike price from 60s Chainlink buffer at startTimeMs when openPrice is 0")
    void shouldInferStrikePriceFrom60sChainlinkBufferWhenOpenPriceIsZero() {
        // Feed 60 seconds of Chainlink ticks in [CANDLE_START_MS - 60_000, CANDLE_START_MS]
        // First 30 ticks at $90,000.00, next 30 ticks at $90,020.00 -> mean = $90,010.00 (9_001_000 cents)
        for (int i = 60; i >= 1; i--) {
            long ts = CANDLE_START_MS - (i * 1000L);
            double price = (i > 30) ? 90_000.00 : 90_020.00;
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", price, ts);
        }

        // Start candle with openPrice = 0
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                CANDLE_START_SEC,
                CANDLE_END_SEC,
                BigDecimal.ZERO,
                null,
                true
        ));

        assertThat(strategy.getEffectiveStrikeCents()).isEqualTo(9_001_000L);
    }

    @Test
    @DisplayName("Should emit UP signal in divergent Spot vs TWAP scenario when locked TWAP stays above strike")
    void shouldEmitUpSignalWhenSpotDropsBelowStrikeButProjectedTwapRemainsAboveStrike() {
        // Strike = $90,000.00
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                CANDLE_START_SEC,
                CANDLE_END_SEC,
                BigDecimal.valueOf(90_000.00),
                new TwapPoint(CANDLE_START_SEC, BigDecimal.valueOf(90_000.00), BigDecimal.valueOf(90_000.00)),
                true
        ));

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
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                CANDLE_START_SEC,
                CANDLE_END_SEC,
                BigDecimal.valueOf(90_000.00),
                new TwapPoint(CANDLE_START_SEC, BigDecimal.valueOf(90_000.00), BigDecimal.valueOf(90_000.00)),
                true
        ));

        // BTC is +$120 above strike at tau = 60s
        long tau60Ms = CANDLE_END_MS - 60_000L;
        strategy.onPolymarketCryptoPrice(tau60Ms, "btcusd", "chainlink", 90_120.00, tau60Ms);

        // Best ask is already $0.990 (990 mc > 970 mc)
        OrderBookQuote decidedQuote = new OrderBookQuote(
                0.990, 0.980, 0.020, 0.010, 1000.0, 1000.0, 0.990, 0.020, tau60Ms
        );
        strategy.onOrderBookQuote(decidedQuote);

        assertThat(submittedOrders).isEmpty();

        // Also verify outside tau window (tau = 65s > 61s and tau = 8s < 10s)
        long tau65Ms = CANDLE_END_MS - 65_000L;
        strategy.onPolymarketCryptoPrice(tau65Ms, "btcusd", "chainlink", 90_050.00, tau65Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau65Ms
        ));
        assertThat(submittedOrders).isEmpty();

        long tau8Ms = CANDLE_END_MS - 8_000L;
        strategy.onPolymarketCryptoPrice(tau8Ms, "btcusd", "chainlink", 90_050.00, tau8Ms);
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tau8Ms
        ));
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should emit initial burst of 15 shares at tau >= 55s, enforce 200ms cooldown, and scale subsequent orders")
    void shouldEmitInitialBurstEnforceCooldownAndScaleSubsequentOrders() {
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                CANDLE_START_SEC,
                CANDLE_END_SEC,
                BigDecimal.valueOf(90_000.00),
                new TwapPoint(CANDLE_START_SEC, BigDecimal.valueOf(90_000.00), BigDecimal.valueOf(90_000.00)),
                true
        ));

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

        // 2. Quote 100ms later (< 200ms cooldown) must be blocked
        long tTooSoon = t1 + 100L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, tTooSoon
        ));
        assertThat(submittedOrders).hasSize(1);

        // 3. Quote 220ms later (>= 200ms cooldown) must emit a scaled second order
        long t2 = t1 + 220L;
        strategy.onOrderBookQuote(new OrderBookQuote(
                0.70, 0.68, 0.32, 0.30, 500.0, 500.0, 0.70, 0.32, t2
        ));
        assertThat(submittedOrders).hasSize(2);
        OrderCommand secondOrder = submittedOrders.get(1);
        assertThat(secondOrder.size().intValue()).isGreaterThan(5).isLessThanOrEqualTo(35);
    }

    @Test
    @DisplayName("Should handle sub-cent BBO quotes (bestBid=0.955, bestAsk=0.965) via conservative 10-mc rounding")
    void shouldHandleSubCentBboQuotes() {
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                CANDLE_START_SEC,
                CANDLE_END_SEC,
                BigDecimal.valueOf(90_000.00),
                new TwapPoint(CANDLE_START_SEC, BigDecimal.valueOf(90_000.00), BigDecimal.valueOf(90_000.00)),
                true
        ));

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
    @DisplayName("Should support Binance lead-lag hybrid stream declaration when binanceLeadWeightBps > 0")
    void shouldSupportBinanceLeadLagMode() {
        assertThat(strategy.getRequiredExternalStreams()).isEmpty();

        UtterParameciumStrategyConfig hybridConfig = new UtterParameciumStrategyConfig(
                10_000L, 61_000L, 200L, 300, 970, 80, 540, 15,
                5, 35, 15, 70, 600, 1_000L, 500L, 3_500L,
                5_000L, 120L, 700, "pm-btc-15m", "BTC-UP", "BTC-DOWN"
        );
        UtterParameciumStrategy hybridStrategy = new UtterParameciumStrategy(hybridConfig);
        hybridStrategy.init(testContext);

        assertThat(hybridStrategy.getRequiredExternalStreams())
                .containsExactly(ExternalStreamType.BINANCE_FUTURES_AGG_TRADES);

        hybridStrategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                CANDLE_START_SEC,
                CANDLE_END_SEC,
                BigDecimal.valueOf(90_000.00),
                null,
                true
        ));

        long ts = CANDLE_END_MS - 30_000L;
        hybridStrategy.onBinanceFuturesTrade(ts - 100L, 90_000.00, 1.0, false);
        hybridStrategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", 90_000.00, ts);
        // Binance Futures jumps +$20.00 before Chainlink updates -> 50% weight = +$10.00 (+1000 cents) lead
        hybridStrategy.onBinanceFuturesTrade(ts + 50L, 90_020.00, 2.0, false);

        assertThat(hybridStrategy.computeEffectiveSpotCents()).isEqualTo(9_001_000L);
    }

    @Test
    @DisplayName("Should accumulate both UP and DOWN positions on mid-window flip and hold to expiration")
    void shouldAccumulateBidirectionalPositionsOnMidWindowFlipAndHoldToExpiration() {
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                CANDLE_START_SEC,
                CANDLE_END_SEC,
                BigDecimal.valueOf(90_000.00),
                null,
                true
        ));

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
}
