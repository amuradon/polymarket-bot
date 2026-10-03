package cz.polymarket.bot.strategy;

import cz.polymarket.bot.cache.HourlyPriceCache;
import cz.polymarket.bot.calculator.KellyPositionSizer;
import cz.polymarket.bot.calculator.RealizedVolatilityCalculator;
import cz.polymarket.bot.calculator.VwapCalculator;
import cz.polymarket.bot.domain.MarketCandle;
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
import static org.assertj.core.api.Assertions.within;

class TWAPArbitrageStrategyTest {

    private TWAPArbitrageStrategy strategy;
    private MockExecutionRouter router;
    private StrategyContext context;

    private static class MockExecutionRouter implements ExecutionRouter {
        final List<OrderCommand> submittedOrders = new ArrayList<>();
        final List<String> cancelledOrders = new ArrayList<>();

        @Override
        public void submitOrder(OrderCommand command) {
            submittedOrders.add(command);
        }

        @Override
        public void cancelOrder(String clientOrderId) {
            cancelledOrders.add(clientOrderId);
        }
    }

    private MarketCandle createCandle(long start, double open, double close, double deltaBtc, double basisBps) {
        return new MarketCandle(
                start, start + 900,
                open, Math.max(open, close), Math.min(open, close), close,
                10.0, 600000.0, deltaBtc,
                open, Math.max(open, close), Math.min(open, close), close,
                10.0, 600000.0, deltaBtc,
                basisBps
        );
    }

    @BeforeEach
    void setUp() {
        router = new MockExecutionRouter();
        HourlyPriceCache cache = new HourlyPriceCache(3600);
        context = new StrategyContext() {
            @Override
            public ExecutionRouter getExecutionRouter() {
                return router;
            }

            @Override
            public HourlyPriceCache getPriceCache() {
                return cache;
            }
        };

        VwapCalculator vwapCalculator = new VwapCalculator();
        RealizedVolatilityCalculator volCalculator = new RealizedVolatilityCalculator();
        KellyPositionSizer sizer = new KellyPositionSizer(10000.0, 0.25, 50.0, 300.0);
        NextCandleProbabilityModel model = new NextCandleProbabilityModel(sizer);
        TWAPArbitrageStrategyConfig config = TWAPArbitrageStrategyConfig.defaultIteration20();

        strategy = new TWAPArbitrageStrategy(vwapCalculator, volCalculator, model, config);
        strategy.init(context);
    }

    @Test
    @DisplayName("Should submit BUY order at Phase 1 (t=60s) on valid bearish signal")
    void shouldSubmitBuyOrderAtPhase1() {
        for (int i = 0; i < 20; i++) {
            double c = (i % 2 == 0) ? 60100.0 : 59900.0;
            double delta = (i == 19) ? -6.0 : 0.0;
            double basis = (i == 19) ? -2.0 : 0.0;
            strategy.onMarketCandleCompleted(createCandle(i * 900L + 1, 60000.0, c, delta, basis));
        }

        long candleStart = 10000L;
        long t60 = candleStart + 60L;
        TwapUpdate twapUpdate = new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60500.0),
                new TwapPoint(t60, BigDecimal.valueOf(60500.0), BigDecimal.valueOf(60500.0)),
                false
        );
        strategy.onTwapUpdate(twapUpdate);

        OrderBookQuote quote = new OrderBookQuote(0.50, 0.50, 0.50, 0.50, 500.0, 500.0, 0.50, 0.50, t60 * 1000);
        strategy.onOrderBookQuote(quote);

        assertThat(router.submittedOrders).hasSize(1);
        OrderCommand order = router.submittedOrders.get(0);
        assertThat(order.side()).isEqualTo("BUY");
        assertThat(order.token()).isEqualTo("BTC-DOWN");
        assertThat(order.price()).isEqualByComparingTo(BigDecimal.valueOf(0.50));
        assertThat(order.size()).isNotNull();
    }

    @Test
    @DisplayName("Should execute Take Profit exit at 0.70 USD when target price is reached")
    void shouldExecuteTakeProfitExit() {
        for (int i = 0; i < 20; i++) {
            double c = (i % 2 == 0) ? 60100.0 : 59900.0;
            double delta = (i == 19) ? -6.0 : 0.0;
            double basis = (i == 19) ? -2.0 : 0.0;
            strategy.onMarketCandleCompleted(createCandle(i * 900L + 1, 60000.0, c, delta, basis));
        }

        long candleStart = 10000L;
        long t60 = candleStart + 60L;
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60500.0),
                new TwapPoint(t60, BigDecimal.valueOf(60500.0), BigDecimal.valueOf(60500.0)),
                false
        ));

        // Submit buy at 0.50
        strategy.onOrderBookQuote(new OrderBookQuote(0.50, 0.50, 0.50, 0.50, 500.0, 500.0, 0.50, 0.50, t60 * 1000));
        assertThat(router.submittedOrders).hasSize(1);
        OrderCommand buyOrder = router.submittedOrders.get(0);

        // Fill position
        strategy.onExecutionReport(new ExecutionReport(
                buyOrder.clientOrderId(),
                "ex-1",
                "FILLED",
                buyOrder.price(),
                buyOrder.size(),
                t60 * 1000,
                "Filled"
        ));
        assertThat(strategy.hasPosition()).isTrue();

        // Price rises: DOWN token reaches 0.70 at t=300s
        long t300 = candleStart + 300L;
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60500.0),
                new TwapPoint(t300, BigDecimal.valueOf(60200.0), BigDecimal.valueOf(60200.0)),
                false
        ));

        strategy.onOrderBookQuote(new OrderBookQuote(0.30, 0.30, 0.70, 0.70, 500.0, 500.0, 0.30, 0.70, t300 * 1000));

        // Verify SELL order was submitted at 0.70
        assertThat(router.submittedOrders).hasSize(2);
        OrderCommand exitOrder = router.submittedOrders.get(1);
        assertThat(exitOrder.side()).isEqualTo("SELL");
        assertThat(exitOrder.token()).isEqualTo("BTC-DOWN");
        assertThat(exitOrder.price()).isEqualByComparingTo(BigDecimal.valueOf(0.70));
        assertThat(strategy.isPositionClosed()).isTrue();
    }

    @Test
    @DisplayName("Should execute Trailing Stop (+0.05) when price rose by +0.14 and then reverses at resolution")
    void shouldExecuteTrailingStopExit() {
        for (int i = 0; i < 20; i++) {
            double c = (i % 2 == 0) ? 60100.0 : 59900.0;
            double delta = (i == 19) ? -6.0 : 0.0;
            double basis = (i == 19) ? -2.0 : 0.0;
            strategy.onMarketCandleCompleted(createCandle(i * 900L + 1, 60000.0, c, delta, basis));
        }

        long candleStart = 10000L;
        long t60 = candleStart + 60L;
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60500.0),
                new TwapPoint(t60, BigDecimal.valueOf(60500.0), BigDecimal.valueOf(60500.0)),
                false
        ));

        // Buy DOWN at 0.50
        strategy.onOrderBookQuote(new OrderBookQuote(0.50, 0.50, 0.50, 0.50, 500.0, 500.0, 0.50, 0.50, t60 * 1000));
        OrderCommand buyOrder = router.submittedOrders.get(0);
        strategy.onExecutionReport(new ExecutionReport(
                buyOrder.clientOrderId(),
                "ex-1",
                "FILLED",
                buyOrder.price(),
                buyOrder.size(),
                t60 * 1000,
                "Filled"
        ));

        // Price rises by +0.14 to 0.64 (arms trailing stop) at t=300s
        long t300 = candleStart + 300L;
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60500.0),
                new TwapPoint(t300, BigDecimal.valueOf(60300.0), BigDecimal.valueOf(60300.0)),
                false
        ));
        strategy.onOrderBookQuote(new OrderBookQuote(0.36, 0.36, 0.64, 0.64, 500.0, 500.0, 0.36, 0.64, t300 * 1000));

        // Resolution arrives with UP outcome (reversal against position)
        strategy.onCandleResolution(TradeDirection.UP);

        // Trailing stop locks exit price at entry (0.50) + 0.05 = 0.55
        assertThat(router.submittedOrders).hasSize(2);
        OrderCommand exitOrder = router.submittedOrders.get(1);
        assertThat(exitOrder.side()).isEqualTo("SELL");
        assertThat(exitOrder.price()).isEqualByComparingTo(BigDecimal.valueOf(0.55));
    }

    @Test
    @DisplayName("Should trigger Late Oracle Arbitrage at Phase 2 (t=600s) when TWAP distance > 80 USD")
    void shouldTriggerLateOracleArbAtPhase2() {
        long candleStart = 10000L;
        long t600 = candleStart + 600L;

        // Start candle with open = 60000.0
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60000.0),
                new TwapPoint(candleStart, BigDecimal.valueOf(60000.0), BigDecimal.valueOf(60000.0)),
                true
        ));

        // Current spot is 60100 at t=600s => dist = +100 USD (> 80 USD)
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60000.0),
                new TwapPoint(t600, BigDecimal.valueOf(60100.0), BigDecimal.valueOf(60100.0)),
                false
        ));

        OrderBookQuote quote = new OrderBookQuote(0.60, 0.60, 0.40, 0.40, 500.0, 500.0, 0.60, 0.40, t600 * 1000);
        strategy.onOrderBookQuote(quote);

        assertThat(router.submittedOrders).hasSize(1);
        OrderCommand order = router.submittedOrders.get(0);
        assertThat(order.side()).isEqualTo("BUY");
        assertThat(order.token()).isEqualTo("BTC-UP");
        assertThat(order.price()).isEqualByComparingTo(BigDecimal.valueOf(0.60));
    }

    @Test
    @DisplayName("Should not submit order when slippage exceeds cap (0.005 USD)")
    void shouldNotSubmitOrderWhenSlippageExceedsCap() {
        for (int i = 0; i < 20; i++) {
            double c = (i % 2 == 0) ? 60100.0 : 59900.0;
            double delta = (i == 19) ? -6.0 : 0.0;
            double basis = (i == 19) ? -2.0 : 0.0;
            strategy.onMarketCandleCompleted(createCandle(i * 900L + 1, 60000.0, c, delta, basis));
        }

        long candleStart = 10000L;
        long t60 = candleStart + 60L;
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60500.0),
                new TwapPoint(t60, BigDecimal.valueOf(60500.0), BigDecimal.valueOf(60500.0)),
                false
        ));

        // Quote where ask is 0.50, but estimated fill price slips to 0.506 (slippage 0.006 > 0.005 cap)
        OrderBookQuote quote = new OrderBookQuote(0.50, 0.50, 0.50, 0.50, 500.0, 500.0, 0.50, 0.506, t60 * 1000);
        strategy.onOrderBookQuote(quote);

        // No order should be submitted due to slippage cap
        assertThat(router.submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should skip trade when 4h volatility is too low (< 0.0006)")
    void shouldSkipTradeWhenVolatilityIsTooLow() {
        // Flat candles with zero volatility
        for (int i = 0; i < 20; i++) {
            strategy.onMarketCandleCompleted(createCandle(i * 900L + 1, 60000.0, 60000.0, -6.0, -2.0));
        }

        long candleStart = 10000L;
        long t60 = candleStart + 60L;
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60500.0),
                new TwapPoint(t60, BigDecimal.valueOf(60500.0), BigDecimal.valueOf(60500.0)),
                false
        ));

        OrderBookQuote quote = new OrderBookQuote(0.50, 0.50, 0.50, 0.50, 500.0, 500.0, 0.50, 0.50, t60 * 1000);
        strategy.onOrderBookQuote(quote);

        assertThat(router.submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should handle order rejection and reset position state")
    void shouldHandleOrderRejection() {
        for (int i = 0; i < 20; i++) {
            double c = (i % 2 == 0) ? 60100.0 : 59900.0;
            double delta = (i == 19) ? -6.0 : 0.0;
            double basis = (i == 19) ? -2.0 : 0.0;
            strategy.onMarketCandleCompleted(createCandle(i * 900L + 1, 60000.0, c, delta, basis));
        }

        long candleStart = 10000L;
        long t60 = candleStart + 60L;
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleStart + 900,
                BigDecimal.valueOf(60500.0),
                new TwapPoint(t60, BigDecimal.valueOf(60500.0), BigDecimal.valueOf(60500.0)),
                false
        ));

        strategy.onOrderBookQuote(new OrderBookQuote(0.50, 0.50, 0.50, 0.50, 500.0, 500.0, 0.50, 0.50, t60 * 1000));
        OrderCommand buyOrder = router.submittedOrders.get(0);

        // Receive REJECTED execution report
        strategy.onExecutionReport(new ExecutionReport(
                buyOrder.clientOrderId(),
                "ex-1",
                "REJECTED",
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                t60 * 1000,
                "Insufficient balance"
        ));

        assertThat(strategy.hasPosition()).isFalse();
        assertThat(strategy.getPositionSide()).isEqualTo(TradeDirection.NO_TRADE);
    }

    @Test
    @DisplayName("Should ignore execution report for unrelated clientOrderId")
    void shouldIgnoreUnrelatedExecutionReport() {
        strategy.onExecutionReport(new ExecutionReport(
                "unrelated-id",
                "ex-2",
                "FILLED",
                BigDecimal.valueOf(0.50),
                BigDecimal.valueOf(100.0),
                System.currentTimeMillis(),
                "Filled"
        ));

        assertThat(strategy.hasPosition()).isFalse();
    }

    @Test
    @DisplayName("Should expose strategy state via getters")
    void shouldExposeStrategyStateAndGetters() {
        long candleStart = 10000L;
        long candleEnd = 10900L;
        strategy.onTwapUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                candleStart,
                candleEnd,
                BigDecimal.valueOf(60000.0),
                new TwapPoint(10010L, BigDecimal.valueOf(60050.0), BigDecimal.valueOf(60050.0)),
                false
        ));

        assertThat(strategy.getActiveCandleStart()).isEqualTo(candleStart);
        assertThat(strategy.getActiveCandleEnd()).isEqualTo(candleEnd);
        assertThat(strategy.getTwapOpenPrice()).isEqualTo(60000.0);
        assertThat(strategy.getCurrentSpotPrice()).isEqualTo(60050.0);
        assertThat(strategy.isPositionClosed()).isFalse();
        assertThat(strategy.isTrailingStopArmed()).isFalse();
        assertThat(strategy.getConfig()).isNotNull();
    }

    @Test
    @DisplayName("Should accurately calculate Polymarket taker fee")
    void shouldCalculateAccurateTakerFee() {
        double fee = strategy.calculateTakerFee(300.0 / 0.56, 0.56);
        assertThat(fee).isCloseTo(9.24, within(1e-4));
    }
}
