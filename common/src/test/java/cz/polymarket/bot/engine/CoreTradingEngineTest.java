package cz.polymarket.bot.engine;

import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.indicators.FastSpotMicroIndicator;
import cz.polymarket.bot.indicators.FastSpotMicroIndicatorConfig;
import cz.polymarket.bot.indicators.SpotTwapIndicator;
import cz.polymarket.bot.indicators.SpotTwapIndicatorConfig;
import cz.polymarket.bot.orderbook.PolymarketOrderBook;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Deterministic synthetic tests verifying {@link CoreTradingEngine} and {@link SimulatedClobExecutionEngine}.
 */
class CoreTradingEngineTest {

    private record ScheduledTask(long tsMs, long seq, Runnable action) {}

    @Test
    @DisplayName("Should reject FAK order when maker pulls liquidity during 196ms Taker Speed Bump")
    void shouldRejectFakOrderWhenLiquidityDisappearsDuringSpeedBump() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.setMarketTokenIds("UP-1", "DOWN-1");
        book.applyTickDelta(true, true, 4900, 200.0, 1_000L);
        book.applyTickDelta(true, false, 5000, 50.0, 1_000L);
        book.applyTickDelta(false, true, 4900, 200.0, 1_000L);
        book.applyTickDelta(false, false, 5100, 200.0, 1_000L);
        book.setLastObservedOneWayLatencyMs(12L);

        List<ExecutionReport> receivedReports = new ArrayList<>();
        TradingStrategy probeStrategy = new TradingStrategy() {
            private StrategyContext ctx;

            @Override
            public void init(StrategyContext context) {
                this.ctx = context;
            }

            @Override
            public void onExecutionReport(ExecutionReport report) {
                receivedReports.add(report);
            }

            @Override
            public double getLastModelProbabilityUp() {
                return 0.65;
            }
        };

        CoreTradingEngine engine = new CoreTradingEngine(
                book,
                new SpotTwapIndicator(SpotTwapIndicatorConfig.defaults()),
                new FastSpotMicroIndicator(FastSpotMicroIndicatorConfig.defaults()),
                probeStrategy,
                "BTC",
                1_000.0
        );

        SimulatedClobExecutionEngine simClob = new SimulatedClobExecutionEngine(book, 220L);
        assertThat(simClob.computeOutboundMatchDelayMs()).isEqualTo(196L);
        assertThat(simClob.computeReturnReportDelayMs()).isEqualTo(24L);

        PriorityQueue<ScheduledTask> pq = new PriorityQueue<>(
                Comparator.comparingLong(ScheduledTask::tsMs).thenComparingLong(ScheduledTask::seq));
        long[] seqGen = {0L};
        simClob.bind((ts, action) -> pq.add(new ScheduledTask(ts, ++seqGen[0], action)), engine::onExecutionReport);
        engine.setExecutionGateway(simClob);

        engine.setCurrentClockMs(1_000L);
        engine.submitOrder(new OrderCommand(
                "fak-1", "mkt-1", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                "BUY", new BigDecimal("0.510"), new BigDecimal("25")
        ));

        // At t = 1_120ms (before tMatch = 1_196ms), maker cancels 0.50 ask and moves to 0.58!
        book.applyTickDelta(true, false, 5000, 0.0, 1_120L);
        book.applyTickDelta(true, false, 5800, 200.0, 1_120L);

        while (!pq.isEmpty()) {
            ScheduledTask task = pq.poll();
            engine.setCurrentClockMs(task.tsMs());
            task.action().run();
        }

        assertThat(receivedReports).hasSize(1);
        assertThat(receivedReports.getFirst().status()).isEqualTo("REJECTED");
        assertThat(receivedReports.getFirst().message()).contains("no orders found to match with FAK order");
        assertThat(engine.getOrdersSubmitted()).isEqualTo(1L);
        assertThat(engine.getOrdersRejected()).isEqualTo(1L);
        assertThat(engine.getOrdersFilled()).isZero();
        assertThat(engine.getAccountBalanceUsd()).isCloseTo(1_000.0, within(1e-6));
    }

    @Test
    @DisplayName("Should reserve collateral across multi-clip BUY orders, close FIFO on SELL, and preserve strategy state across candles")
    void shouldHandleMultiClipCollateralFifoSellAndContinuousCandleRollover() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.setMarketTokenIds("UP-1", "DOWN-1");
        book.applyTickDelta(true, true, 4900, 200.0, 1_000L);
        book.applyTickDelta(true, false, 5000, 40.0, 1_000L);
        book.applyTickDelta(true, false, 5200, 60.0, 1_000L);
        book.applyTickDelta(false, true, 4800, 100.0, 1_000L);
        book.applyTickDelta(false, false, 5000, 100.0, 1_000L);

        int[] resetCount = {0};
        TradingStrategy spyStrategy = new TradingStrategy() {
            @Override
            public void init(StrategyContext context) {
            }

            @Override
            public void reset() {
                resetCount[0]++;
            }

            @Override
            public void onExecutionReport(ExecutionReport report) {
            }

            @Override
            public double getLastModelProbabilityUp() {
                return 0.68;
            }
        };

        CoreTradingEngine engine = new CoreTradingEngine(
                book,
                new SpotTwapIndicator(SpotTwapIndicatorConfig.defaults()),
                new FastSpotMicroIndicator(FastSpotMicroIndicatorConfig.defaults()),
                spyStrategy,
                "BTC",
                60.0 // Tight $60 initial capital!
        );
        SimulatedClobExecutionEngine simClob = new SimulatedClobExecutionEngine(book, 0L);
        simClob.bind(null, engine::onExecutionReport);
        engine.setExecutionGateway(simClob);
        engine.beginCandleCycle(1_800_000L, 1_800_900L, 70_000.0, "UP-1", "DOWN-1", false);

        // Clip 1: BUY 40 shares @ 0.50 ($20.00 + $0.70 fee = $20.70) -> succeeds
        engine.setCurrentClockMs(1_800_100_000L);
        engine.submitOrder(new OrderCommand(
                "buy-1", "mkt-1", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                "BUY", new BigDecimal("0.550"), new BigDecimal("40")
        ));
        // Clip 2: BUY 40 shares @ 0.52 ($20.80 + $0.6989 fee = $21.50) -> succeeds (cash remaining ~$17.80)
        engine.setCurrentClockMs(1_800_200_000L);
        engine.submitOrder(new OrderCommand(
                "buy-2", "mkt-1", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                "BUY", new BigDecimal("0.550"), new BigDecimal("40")
        ));
        // Clip 3: BUY 40 shares ($22+ required > ~$17.80 free cash) -> MUST reject for insufficient balance!
        engine.setCurrentClockMs(1_800_300_000L);
        engine.submitOrder(new OrderCommand(
                "buy-3", "mkt-1", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                "BUY", new BigDecimal("0.550"), new BigDecimal("40")
        ));

        assertThat(engine.getOrdersFilled()).isEqualTo(2L);
        assertThat(engine.getOrdersRejected()).isEqualTo(1L);
        assertThat(engine.getOpenPositions()).hasSize(2);
        assertThat(engine.getOpenPositions().getFirst().modelProbability()).isCloseTo(0.68, within(1e-6));

        // Market rallies to 0.75/0.77; partial FIFO SELL of 50 shares @ 0.75 closes all 40 of buy-1 + 10 of buy-2
        book.applyTickDelta(true, false, 5200, 0.0, 1_800_350_000L);
        book.applyTickDelta(true, false, 7700, 200.0, 1_800_350_000L);
        book.applyTickDelta(true, true, 7500, 200.0, 1_800_350_000L);
        engine.setCurrentClockMs(1_800_400_000L);
        engine.submitOrder(new OrderCommand(
                "sell-1", "mkt-1", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                "SELL", new BigDecimal("0.700"), new BigDecimal("50")
        ));

        assertThat(engine.getOpenPositions()).hasSize(1);
        assertThat(engine.getOpenPositions().getFirst().shares()).isCloseTo(30.0, within(1e-6));
        assertThat(engine.getCompletedTradesHistory()).hasSize(2);

        // Rollover via PolyBolt 60s TWAP at t = 1_800_900s (twapClose = 70_050 >= 70_000 -> UP wins!)
        engine.onPolyBoltCryptoTwap60s(1_800_900_100L, "btcusd", "chainlink", 70_050.0, 60, 1_800_900_000L);

        assertThat(engine.getOpenPositions()).isEmpty();
        assertThat(engine.getCompletedTradesHistory()).hasSize(3);
        assertThat(engine.getCashBalanceUsd()).isCloseTo(engine.getRealizedEquityUsd(), within(1e-6));
        assertThat(engine.getRealizedEquityUsd()).isGreaterThan(80.0);
        // Strategy reset() must NEVER be called on candle rollover!
        assertThat(resetCount[0]).isZero();
    }

    @Test
    @DisplayName("Should deduplicate unchanged executable quotes and skip stale quotes exceeding maxQuoteAgeMs")
    void shouldDeduplicateQuotesAndSkipStaleQuotes() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        List<OrderBookQuote> delivered = new ArrayList<>();
        TradingStrategy collectorStrategy = new TradingStrategy() {
            @Override
            public void init(StrategyContext context) {
            }

            @Override
            public void onExecutionReport(ExecutionReport report) {
            }

            @Override
            public void onOrderBookQuote(OrderBookQuote quote) {
                delivered.add(quote);
            }
        };

        CoreTradingEngine engine = new CoreTradingEngine(
                book,
                new SpotTwapIndicator(SpotTwapIndicatorConfig.defaults()),
                new FastSpotMicroIndicator(FastSpotMicroIndicatorConfig.defaults()),
                collectorStrategy,
                "BTC",
                1_000.0
        );
        engine.setMaxQuoteAgeMs(250L);

        book.applyTickDelta(true, true, 4900, 100.0, 1_000L);
        book.applyTickDelta(true, false, 5100, 100.0, 1_000L);
        book.applyTickDelta(false, true, 4900, 100.0, 1_000L);
        book.applyTickDelta(false, false, 5100, 100.0, 1_000L);
        engine.onOrderBookUpdated(1_010L);
        assertThat(delivered).hasSize(1);

        // Deep level mutation at 0.80 does not change BBO or 1c depth -> must be deduplicated
        book.applyTickDelta(true, false, 8000, 500.0, 1_020L);
        engine.onOrderBookUpdated(1_020L);
        assertThat(delivered).hasSize(1);

        // 1c depth mutation at 0.515 changes depth1cUp -> must dispatch!
        book.applyTickDelta(true, false, 5150, 75.0, 1_030L);
        engine.onOrderBookUpdated(1_030L);
        assertThat(delivered).hasSize(2);
        assertThat(delivered.getLast().depth1cUp()).isCloseTo(175.0, within(1e-6));

        // Advance clock 400ms ahead of book timestamp (age 400ms > 250ms) -> must skip stale quote!
        book.applyTickDelta(true, false, 5150, 90.0, 1_100L);
        engine.setCurrentClockMs(1_500L);
        engine.onOrderBookUpdated(1_500L);
        assertThat(delivered).hasSize(2);
        assertThat(engine.getStaleQuotesSkipped()).isEqualTo(1L);
    }
}
