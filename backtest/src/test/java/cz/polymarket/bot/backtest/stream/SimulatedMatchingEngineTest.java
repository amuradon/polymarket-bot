package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.TradeRecord;
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
    @DisplayName("Should simulate order submission with 50ms latency delay and execution report")
    void shouldSimulateOrderWithLatency() {
        PolymarketOrderBook book = new PolymarketOrderBook();
        book.applyDelta("SELL", 0.52, 1000.0, 1000L); // ask at 0.52

        SimulatedMatchingEngine engine = new SimulatedMatchingEngine(book, 50L);
        List<BacktestEvent> scheduled = new ArrayList<>();
        engine.setEventScheduler(scheduled::add);

        // Submit buy order at t = 1000ms
        OrderCommand buyCmd = new OrderCommand(
                "order-1",
                "mkt-1",
                "BTC-UP",
                cz.polymarket.bot.domain.Timeframe.FIFTEEN_MINUTES,
                "BUY",
                BigDecimal.valueOf(0.52),
                BigDecimal.valueOf(100.0)
        );
        engine.submitOrder(buyCmd, 1000L);

        // Verify ExecutionReportEvent scheduled at t = 1050ms (1000 + 50)
        assertThat(scheduled).hasSize(1);
        BacktestEvent event = scheduled.get(0);
        assertThat(event.timestampMs()).isEqualTo(1050L);
        assertThat(event).isInstanceOf(BacktestEvent.ExecutionReportEvent.class);

        BacktestEvent.ExecutionReportEvent execEvent = (BacktestEvent.ExecutionReportEvent) event;
        assertThat(execEvent.report().status()).isEqualTo("FILLED");
        assertThat(execEvent.report().executedPrice().doubleValue()).isEqualTo(0.52);
        assertThat(execEvent.report().executedSize().doubleValue()).isEqualTo(100.0);
        assertThat(engine.hasActiveTrade()).isTrue();

        // Finalize trade at candle close with winning outcome
        TradeRecord trade = engine.finalizeTrade(1000L, "2026-08-12T00:00:00Z", TradeDirection.UP, 10000.0);
        assertThat(trade).isNotNull();
        assertThat(trade.isWin()).isTrue();
        assertThat(trade.exitPrice()).isEqualTo(1.0);
        // Payout = 100 * 1.0 = 100; cost = 100 * 0.52 = 52; fee = 100 * 0.07 * 0.52 * 0.48 ~= 1.7472
        // Net pnl = 100 - 52 - 1.7472 = 46.2528
        assertThat(trade.netPnl()).isCloseTo(46.25, within(0.1));
        assertThat(trade.balanceAfterTrade()).isCloseTo(10046.25, within(0.1));
    }
}
