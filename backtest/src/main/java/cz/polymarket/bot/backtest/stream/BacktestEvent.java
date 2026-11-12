package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.TradeDirection;

/**
 * High-performance chronological event hierarchy for backtest simulation.
 * <p>
 * All events are ordered primarily by {@link #timestampMs()} (the local EC2 capture timestamp
 * {@code ts} in milliseconds UTC), secondarily by {@link #priority()}, and finally by
 * {@link #sequenceNumber()} to preserve exact capture arrival order within the same millisecond.
 */
public sealed interface BacktestEvent extends Comparable<BacktestEvent> {

    /**
     * Local EC2 capture or scheduled simulation timestamp in milliseconds UTC.
     *
     * @return timestamp in milliseconds UTC
     */
    long timestampMs();

    /**
     * Intra-millisecond priority tiebreaker (lower value executes first when timestamps are equal).
     *
     * @return priority rank
     */
    int priority();

    /**
     * Monotonically increasing sequence tiebreaker preserving capture order within the same millisecond.
     *
     * @return sequence number
     */
    default long sequenceNumber() {
        return 0L;
    }

    @Override
    default int compareTo(BacktestEvent o) {
        int cmp = Long.compare(this.timestampMs(), o.timestampMs());
        if (cmp != 0) {
            return cmp;
        }
        int pCmp = Integer.compare(this.priority(), o.priority());
        if (pCmp != 0) {
            return pCmp;
        }
        return Long.compare(this.sequenceNumber(), o.sequenceNumber());
    }

    /**
     * Scheduled order execution event arriving at the exchange matching engine after simulated
     * time-varying network latency ({@code submitTs + round(observedOneWayLatencyMs * latencyFactor)}).
     *
     * @param timestampMs timestamp when the order reaches the matching engine in milliseconds UTC
     * @param command order command submitted by the strategy
     * @param delayMs simulated execution latency in milliseconds
     * @param sequenceNumber tiebreaker sequence number
     */
    record PendingOrderExecutionEvent(
            long timestampMs,
            OrderCommand command,
            long delayMs,
            long sequenceNumber
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 1;
        }
    }

    /**
     * Typed Polymarket CLOB WebSocket frame ({@code book}, {@code price_change}, {@code last_trade_price})
     * grouped by monotonic {@code frame_id} from the compact 10-column CLOB Parquet archive.
     *
     * @param timestampMs local EC2 receipt timestamp ({@code ts}) in milliseconds UTC
     * @param serverTimestampMs exchange server timestamp ({@code server_ts}) in milliseconds UTC
     * @param sequenceNumber capture frame sequence tiebreaker
     * @param eventCode {@link #EVENT_BOOK} ({@code 1}), {@link #EVENT_PRICE_CHANGE} ({@code 2}), or {@link #EVENT_LAST_TRADE} ({@code 3})
     * @param bookIsUp for {@link #EVENT_BOOK}, whether this snapshot targets the UP ({@code true}) or DOWN ({@code false}) book
     * @param levelCount number of valid level entries in {@code isUpFlags}, {@code isBidFlags}, {@code ticks}, and {@code sizes}
     * @param isUpFlags token side flags per level ({@code true} = UP, {@code false} = DOWN)
     * @param isBidFlags order side flags per level ({@code true} = bid/BUY, {@code false} = ask/SELL)
     * @param ticks integer price ticks ({@code 0..10000}) per level
     * @param sizes order book share depths per level ({@code 0.0} removes the level)
     */
    record PolymarketClobFrameEvent(
            long timestampMs,
            long serverTimestampMs,
            long sequenceNumber,
            byte eventCode,
            boolean bookIsUp,
            int levelCount,
            boolean[] isUpFlags,
            boolean[] isBidFlags,
            int[] ticks,
            double[] sizes
    ) implements BacktestEvent {
        /** Full L2 order book snapshot frame ({@code event_code = 1}). */
        public static final byte EVENT_BOOK = 1;
        /** Incremental L2 price level delta batch frame ({@code event_code = 2}). */
        public static final byte EVENT_PRICE_CHANGE = 2;
        /** Trade print timestamp frame ({@code event_code = 3}). */
        public static final byte EVENT_LAST_TRADE = 3;

        @Override
        public int priority() {
            return 5;
        }
    }

    /**
     * Real-time crypto spot/oracle price event from {@code polymarket/price_crypto}.
     *
     * @param timestampMs local EC2 receipt timestamp ({@code ts}) in milliseconds UTC
     * @param sequenceNumber capture sequence number ({@code seq})
     * @param symbol crypto symbol (e.g. {@code "btcusd"})
     * @param source price feed source (e.g. {@code "chainlink"})
     * @param price spot/oracle price in USD
     * @param priceTimestampMs source oracle timestamp in milliseconds UTC
     */
    record PolymarketCryptoPriceEvent(
            long timestampMs,
            long sequenceNumber,
            String symbol,
            String source,
            double price,
            long priceTimestampMs
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 8;
        }
    }

    /**
     * Real-time 60-second TWAP update event from {@code polymarket/price_crypto_twap_60s}.
     *
     * @param timestampMs local EC2 receipt timestamp ({@code ts}) in milliseconds UTC
     * @param sequenceNumber capture sequence number ({@code seq})
     * @param symbol crypto symbol (e.g. {@code "btcusd"})
     * @param source TWAP feed source (e.g. {@code "chainlink"})
     * @param twapPrice 60-second time-weighted average price in USD
     * @param windowSeconds TWAP rolling window length in seconds (typically 60)
     * @param twapTimestampMs source oracle TWAP timestamp in milliseconds UTC
     */
    record PolymarketCryptoTwap60sEvent(
            long timestampMs,
            long sequenceNumber,
            String symbol,
            String source,
            double twapPrice,
            int windowSeconds,
            long twapTimestampMs
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 9;
        }
    }

    /**
     * Aggressive spot trade execution event on Binance Spot.
     *
     * @param timestampMs local EC2 receipt timestamp ({@code ts}) in milliseconds UTC
     * @param sequenceNumber capture sequence / aggregate trade ID tiebreaker
     * @param price trade execution price
     * @param quantity trade volume in base asset
     * @param isBuyerMaker true if maker was buyer (aggressive sell), false if aggressive buy
     */
    record BinanceSpotTradeEvent(
            long timestampMs,
            long sequenceNumber,
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
     * @param timestampMs local EC2 receipt timestamp ({@code ts}) in milliseconds UTC
     * @param sequenceNumber capture sequence / aggregate trade ID tiebreaker
     * @param price futures execution price
     * @param quantity trade volume in contracts/base asset
     * @param isBuyerMaker true if maker was buyer, false if aggressive buy
     */
    record BinanceFuturesTradeEvent(
            long timestampMs,
            long sequenceNumber,
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
     * Order book depth and imbalance update from Binance Futures ({@code depthPartial} or {@code depthDiff}).
     *
     * @param timestampMs local EC2 receipt timestamp ({@code ts}) in milliseconds UTC
     * @param sequenceNumber capture sequence number
     * @param bestBid best bid price
     * @param bestAsk best ask price
     * @param depthBids total top-level bid liquidity
     * @param depthAsks total top-level ask liquidity
     * @param obi order book imbalance ratio
     */
    record BinanceFuturesOrderBookEvent(
            long timestampMs,
            long sequenceNumber,
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
     * Liquidation force-order event from Binance Futures ({@code forceOrder}).
     *
     * @param timestampMs local EC2 receipt timestamp ({@code ts}) in milliseconds UTC
     * @param sequenceNumber capture sequence number
     * @param side liquidation side ({@code "BUY"} or {@code "SELL"})
     * @param price order limit price
     * @param averagePrice average fill price
     * @param quantity liquidated quantity
     */
    record BinanceFuturesForceOrderEvent(
            long timestampMs,
            long sequenceNumber,
            String side,
            double price,
            double averagePrice,
            double quantity
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 16;
        }
    }

    /**
     * Perpetual futures trade execution event from Deribit ({@code deribit/futures/<INSTRUMENT>/trades}).
     *
     * @param timestampMs local EC2 receipt timestamp ({@code ts}) in milliseconds UTC
     * @param sequenceNumber trade sequence number ({@code trade_seq})
     * @param instrumentName Deribit instrument name (e.g. {@code "BTC-PERPETUAL"})
     * @param direction trade direction ({@code "buy"} or {@code "sell"})
     * @param price trade execution price
     * @param amount trade amount in USD/contracts
     * @param markPrice Deribit mark price
     * @param indexPrice Deribit index price
     */
    record DeribitFuturesTradeEvent(
            long timestampMs,
            long sequenceNumber,
            String instrumentName,
            String direction,
            double price,
            double amount,
            double markPrice,
            double indexPrice
    ) implements BacktestEvent {
        @Override
        public int priority() {
            return 18;
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
