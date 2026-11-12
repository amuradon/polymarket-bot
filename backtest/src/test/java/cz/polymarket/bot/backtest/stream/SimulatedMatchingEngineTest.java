package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.TradeDirection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class SimulatedMatchingEngineTest {

    @Test
    @DisplayName("Should schedule pending order with 2x round-trip latency scaled by latencyFactor and match against delayed book state")
    void shouldSimulateOrderWithTimeVaryingLatencyAndLatencyFactor() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.applyTickDelta(true, false, 5000, 1000.0, 1000L);
        book.setLastObservedOneWayLatencyMs(15L); // 15ms one-way latency -> 30ms round-trip at factor 1.0

        // Configure latencyFactor = 2.0 -> expected execution delay = 2 * 15ms * 2.0 = 60ms
        SimulatedMatchingEngine engine = new SimulatedMatchingEngine(book, 2.0);
        List<BacktestEvent> scheduled = new ArrayList<>();
        engine.setEventScheduler(scheduled::add);

        OrderCommand buyCmd = new OrderCommand(
                "order-1",
                "mkt-1",
                "BTC-UP",
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                BigDecimal.valueOf(0.55),
                BigDecimal.valueOf(100.0)
        );
        engine.submitOrder(buyCmd, 1000L);

        assertThat(scheduled).hasSize(1);
        BacktestEvent event = scheduled.get(0);
        assertThat(event.timestampMs()).isEqualTo(1060L);
        assertThat(event).isInstanceOf(BacktestEvent.PendingOrderExecutionEvent.class);

        // Simulate the book moving at t = 1030ms (before order reaches exchange at 1060ms):
        // 0.50 ask disappears, new best ask is 0.52
        book.applyTickDelta(true, false, 5000, 0.0, 1030L);
        book.applyTickDelta(true, false, 5200, 1000.0, 1030L);

        // Now execute pending order at t = 1060ms
        ExecutionReport report = engine.executePendingOrder((BacktestEvent.PendingOrderExecutionEvent) event);
        assertThat(report).isNotNull();
        assertThat(report.status()).isEqualTo("FILLED");
        // Must fill at 0.52 (the book price at 1060ms), NOT the stale 0.50 from 1000ms!
        assertThat(report.executedPrice().doubleValue()).isCloseTo(0.52, within(0.0001));
        assertThat(report.executedSize().doubleValue()).isEqualTo(100.0);
        assertThat(engine.hasActiveTrade()).isTrue();

        List<TradeRecord> trades = engine.finalizeTrades(1000L, "2026-08-12T00:00:00Z", TradeDirection.UP, 10000.0);
        assertThat(trades).hasSize(1);
        TradeRecord trade = trades.getFirst();
        assertThat(trade.isWin()).isTrue();
        assertThat(trade.entryPrice()).isCloseTo(0.52, within(0.0001));
        assertThat(trade.exitPrice()).isEqualTo(1.0);
        assertThat(trade.netPnl()).isCloseTo(46.25, within(0.1));
        assertThat(trade.balanceAfterTrade()).isCloseTo(10046.25, within(0.1));
    }

    @Test
    @DisplayName("Should support multiple simultaneous positions in a single market and deplete book liquidity across orders")
    void shouldSupportMultipleSimultaneousPositionsAndDepleteLiquidity() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.applyTickDelta(true, false, 5000, 50.0, 1000L);
        book.applyTickDelta(true, false, 5200, 100.0, 1000L);
        book.applyTickDelta(true, true, 7500, 100.0, 1000L);
        book.setLastObservedOneWayLatencyMs(15L);

        SimulatedMatchingEngine engine = new SimulatedMatchingEngine(book, 1.0);

        // Order 1 at t=1000ms: BUY 50 shares -> consumes all 50 shares at 0.50
        engine.submitOrder(new OrderCommand(
                "buy-1", "mkt-1", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                "BUY", BigDecimal.valueOf(0.55), BigDecimal.valueOf(50.0)
        ), 1000L);

        // Order 2 at t=1200ms: BUY 50 shares -> 0.50 is depleted, fills at 0.52!
        engine.submitOrder(new OrderCommand(
                "buy-2", "mkt-1", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                "BUY", BigDecimal.valueOf(0.55), BigDecimal.valueOf(50.0)
        ), 1200L);

        // Order 3 at t=1400ms: Partial SELL exit of 25 shares @ 0.75 (FIFO closes half of buy-1)
        engine.submitOrder(new OrderCommand(
                "sell-1", "mkt-1", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                "SELL", BigDecimal.valueOf(0.70), BigDecimal.valueOf(25.0)
        ), 1400L);

        List<TradeRecord> trades = engine.finalizeTrades(1000L, "2026-08-12T00:00:00Z", TradeDirection.UP, 10000.0);
        // 1 early-exited trade (25 shares) + 2 remaining open positions settled at resolution (25 shares @ 0.50, 50 shares @ 0.52)
        assertThat(trades).hasSize(3);
        assertThat(trades.get(0).entryPrice()).isCloseTo(0.50, within(0.0001));
        assertThat(trades.get(0).exitPrice()).isCloseTo(0.75, within(0.0001));
        assertThat(trades.get(0).shares()).isEqualTo(25.0);

        assertThat(trades.get(1).entryPrice()).isCloseTo(0.50, within(0.0001));
        assertThat(trades.get(1).exitPrice()).isEqualTo(1.0);
        assertThat(trades.get(1).shares()).isEqualTo(25.0);

        assertThat(trades.get(2).entryPrice()).isCloseTo(0.52, within(0.0001));
        assertThat(trades.get(2).exitPrice()).isEqualTo(1.0);
        assertThat(trades.get(2).shares()).isEqualTo(50.0);
    }

    @Test
    @DisplayName("Should reject order when order book has no liquidity at execution timestamp")
    void shouldRejectOrderWhenOrderBookEmpty() {
        PolymarketOrderBook emptyBook = new PolymarketOrderBook();
        emptyBook.setLastObservedOneWayLatencyMs(10L);
        SimulatedMatchingEngine engine = new SimulatedMatchingEngine(emptyBook, 1.0);
        List<BacktestEvent> scheduled = new ArrayList<>();
        engine.setEventScheduler(scheduled::add);

        OrderCommand buyCmd = new OrderCommand(
                "order-empty",
                "mkt-1",
                "BTC-UP",
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                BigDecimal.valueOf(0.52),
                BigDecimal.valueOf(100.0)
        );
        engine.submitOrder(buyCmd, 1000L);

        assertThat(scheduled).hasSize(1);
        BacktestEvent.PendingOrderExecutionEvent pending = (BacktestEvent.PendingOrderExecutionEvent) scheduled.get(0);
        ExecutionReport report = engine.executePendingOrder(pending);

        assertThat(engine.hasActiveTrade()).isFalse();
        assertThat(report.status()).isEqualTo("REJECTED");
    }
}
