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
    @DisplayName("Should schedule pending order with time-varying latency scaled by latencyFactor and match against delayed book state")
    void shouldSimulateOrderWithTimeVaryingLatencyAndLatencyFactor() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.applyDelta("SELL", 0.50, 1000.0, 1000L);
        book.setLastObservedOneWayLatencyMs(12L); // 12ms observed latency

        // Configure latencyFactor = 2.0 -> expected execution delay = 24ms
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
        assertThat(event.timestampMs()).isEqualTo(1024L);
        assertThat(event).isInstanceOf(BacktestEvent.PendingOrderExecutionEvent.class);

        // Simulate the book moving at t = 1010ms (before order reaches exchange at 1024ms):
        // 0.50 ask disappears, new best ask is 0.52
        book.applyDelta("SELL", 0.50, 0.0, 1010L);
        book.applyDelta("SELL", 0.52, 1000.0, 1010L);

        // Now execute pending order at t = 1024ms
        ExecutionReport report = engine.executePendingOrder((BacktestEvent.PendingOrderExecutionEvent) event);
        assertThat(report).isNotNull();
        assertThat(report.status()).isEqualTo("FILLED");
        // Must fill at 0.52 (the book price at 1024ms), NOT the stale 0.50 from 1000ms!
        assertThat(report.executedPrice().doubleValue()).isCloseTo(0.52, within(0.0001));
        assertThat(report.executedSize().doubleValue()).isEqualTo(100.0);
        assertThat(engine.hasActiveTrade()).isTrue();

        TradeRecord trade = engine.finalizeTrade(1000L, "2026-08-12T00:00:00Z", TradeDirection.UP, 10000.0);
        assertThat(trade).isNotNull();
        assertThat(trade.isWin()).isTrue();
        assertThat(trade.entryPrice()).isCloseTo(0.52, within(0.0001));
        assertThat(trade.exitPrice()).isEqualTo(1.0);
        assertThat(trade.netPnl()).isCloseTo(46.25, within(0.1));
        assertThat(trade.balanceAfterTrade()).isCloseTo(10046.25, within(0.1));
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
