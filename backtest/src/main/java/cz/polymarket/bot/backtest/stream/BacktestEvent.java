package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.TradeDirection;

/**
 * High-performance chronological event interface for backtest simulation.
 * All events are strictly ordered by timestampMs ascending.
 */
public interface BacktestEvent extends Comparable<BacktestEvent> {

    long timestampMs();

    int priority();

    @Override
    default int compareTo(BacktestEvent o) {
        int cmp = Long.compare(this.timestampMs(), o.timestampMs());
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.priority(), o.priority());
    }

    record BinanceSpotTradeEvent(
            long timestampMs,
            double price,
            double quantity,
            boolean isBuyerMaker
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 10;
        }
    }

    record BinanceFuturesTradeEvent(
            long timestampMs,
            double price,
            double quantity,
            boolean isBuyerMaker
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 10;
        }
    }

    record BinanceFuturesOrderBookEvent(
            long timestampMs,
            double bestBid,
            double bestAsk,
            double depthBids,
            double depthAsks,
            double obi
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 15;
        }
    }

    record PolymarketL2UpdateEvent(
            long timestampMs,
            String eventType,
            double price,
            double size,
            String side,
            String bidsJson,
            String asksJson
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 5;
        }
    }

    record PolymarketQuoteEvent(
            long timestampMs,
            OrderBookQuote quote
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 20;
        }
    }

    record TwapUpdateEvent(
            long timestampMs,
            TwapUpdate update
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 25;
        }
    }

    record ExecutionReportEvent(
            long timestampMs,
            ExecutionReport report
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 1; // High priority: simulated fill arrives at this exact ms
        }
    }

    record CandleLifecycleEvent(
            long timestampMs,
            CandleLifecycleType type,
            long tStart,
            long tEnd,
            TradeDirection actualOutcome,
            MarketCandle completedCandle
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return type == CandleLifecycleType.CANDLE_CLOSE ? 99 : 0;
        }
    }

    enum CandleLifecycleType {
        CANDLE_START,
        TWAP_60S_SAMPLE,
        CANDLE_CLOSE
    }
}
