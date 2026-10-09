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

    /**
     * Timestamp of the event in milliseconds UTC.
     */
    long timestampMs();

    /**
     * Intra-millisecond priority tiebreaker (lower value executes first when timestamps are equal).
     */
    int priority();

    @Override
    default int compareTo(BacktestEvent o) {
        int cmp = Long.compare(this.timestampMs(), o.timestampMs());
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.priority(), o.priority());
    }

    /**
     * Aggressive spot trade execution event on Binance Spot.
     *
     * @param timestampMs transaction timestamp in milliseconds UTC
     * @param price trade execution price
     * @param quantity trade volume in base asset
     * @param isBuyerMaker true if maker was buyer (aggressive sell), false if aggressive buy
     */
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

    /**
     * Aggressive futures trade execution event on Binance Futures.
     *
     * @param timestampMs transaction timestamp in milliseconds UTC
     * @param price futures execution price
     * @param quantity trade volume in contracts/base asset
     * @param isBuyerMaker true if maker was buyer, false if aggressive buy
     */
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

    /**
     * Order book depth and imbalance update from Binance Futures.
     *
     * @param timestampMs event timestamp in milliseconds UTC
     * @param bestBid best bid price
     * @param bestAsk best ask price
     * @param depthBids total top-level bid liquidity
     * @param depthAsks total top-level ask liquidity
     * @param obi order book imbalance ratio
     */
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

    /**
     * Raw Level 2 book update (snapshot or delta) or trade event on Polymarket.
     *
     * @param timestampMs event timestamp in milliseconds UTC
     * @param eventType type of event ("snapshot", "delta", "trade")
     * @param price price level
     * @param size size at price level
     * @param side order side ("BUY" or "SELL")
     * @param bidsJson raw JSON bids array for snapshots
     * @param asksJson raw JSON asks array for snapshots
     */
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

    /**
     * Synthesized or discrete order book quote state event for Polymarket.
     *
     * @param timestampMs event timestamp in milliseconds UTC
     * @param quote order book quote snapshot
     */
    record PolymarketQuoteEvent(
            long timestampMs,
            OrderBookQuote quote
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 20;
        }
    }

    /**
     * Aggregate TWAP reference price update event.
     *
     * @param timestampMs event timestamp in milliseconds UTC
     * @param update latest aggregate TWAP sample update
     */
    record TwapUpdateEvent(
            long timestampMs,
            TwapUpdate update
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 25;
        }
    }

    /**
     * Order execution report arriving from simulated matching engine after simulated latency.
     *
     * @param timestampMs fill delivery timestamp in milliseconds UTC
     * @param report execution report containing fill price and size
     */
    record ExecutionReportEvent(
            long timestampMs,
            ExecutionReport report
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 1; // High priority: simulated fill arrives at this exact ms
        }
    }

    /**
     * Lifecycle milestones marking candle open, 60s sampling, and settlement close.
     *
     * @param timestampMs milestone timestamp in milliseconds UTC
     * @param type lifecycle event stage
     * @param tStart candle start timestamp in epoch seconds
     * @param tEnd candle end timestamp in epoch seconds
     * @param actualOutcome settled binary outcome direction
     * @param completedCandle domain market candle available upon close
     */
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

    /**
     * Types of candle lifecycle milestone events.
     */
    enum CandleLifecycleType {
        /** Candle interval opening milestone */
        CANDLE_START,
        /** 60-second TWAP reference sampling milestone */
        TWAP_60S_SAMPLE,
        /** Candle settlement and resolution closing milestone */
        CANDLE_CLOSE
    }
}
