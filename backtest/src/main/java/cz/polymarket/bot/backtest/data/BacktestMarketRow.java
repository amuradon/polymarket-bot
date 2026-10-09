package cz.polymarket.bot.backtest.data;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;

/**
 * Immutable row representation of a 15-minute market interval loaded from parquet dataset or binary cache.
 * Combines Binance Spot OHLCV, Binance Futures OHLCV, Basis, Polymarket quotes at various candle second offsets,
 * simulated fill estimates, and resolution outcomes.
 *
 * @param tStart start timestamp in epoch seconds UTC
 * @param tEnd end timestamp in epoch seconds UTC
 * @param datetimeUtc ISO-8601 formatted datetime UTC
 * @param dateStr date string (yyyy-MM-dd)
 * @param twapOpen 60-second opening TWAP reference price
 * @param twapClose 60-second closing TWAP reference price
 * @param actualOutcome settled binary outcome ("UP" or "DOWN")
 * @param sOpen Binance Spot open price
 * @param sHigh Binance Spot high price
 * @param sLow Binance Spot low price
 * @param sClose Binance Spot close price
 * @param sVolBtc Binance Spot volume in BTC
 * @param sVolUsd Binance Spot volume in USD
 * @param sDeltaBtc Binance Spot cumulative volume delta in BTC
 * @param fOpen Binance Futures open price
 * @param fClose Binance Futures close price
 * @param fVolBtc Binance Futures volume in BTC
 * @param fVolUsd Binance Futures volume in USD
 * @param fDeltaBtc Binance Futures cumulative volume delta in BTC
 * @param basisOpenBps basis spread at candle open in basis points
 * @param basisCloseBps basis spread at candle close in basis points
 * @param pmAsk0 Polymarket best ask for UP at t = 0s
 * @param pmBid0 Polymarket best bid for UP at t = 0s
 * @param pmAsk60 Polymarket best ask for UP at t = 60s
 * @param pmBid60 Polymarket best bid for UP at t = 60s
 * @param pmAsk180 Polymarket best ask for UP at t = 180s
 * @param pmBid180 Polymarket best bid for UP at t = 180s
 * @param pmAsk300 Polymarket best ask for UP at t = 300s
 * @param pmBid300 Polymarket best bid for UP at t = 300s
 * @param pmFill100Up simulated fill price for $100 order on UP contract
 * @param pmFee100Up simulated fee for $100 order on UP contract
 * @param pmFill100Down simulated fill price for $100 order on DOWN contract
 * @param pmFee100Down simulated fee for $100 order on DOWN contract
 * @param pmMaxPrice highest observed contract price during candle
 * @param pmMinPrice lowest observed contract price during candle
 * @param pmDepth1cUp contract depth within 1 cent of ask for UP
 * @param pmDepth1cDown contract depth within 1 cent of bid for DOWN
 */
public record BacktestMarketRow(
        long tStart,
        long tEnd,
        String datetimeUtc,
        String dateStr,
        double twapOpen,
        double twapClose,
        String actualOutcome,
        double sOpen,
        double sHigh,
        double sLow,
        double sClose,
        double sVolBtc,
        double sVolUsd,
        double sDeltaBtc,
        double fOpen,
        double fClose,
        double fVolBtc,
        double fVolUsd,
        double fDeltaBtc,
        double basisOpenBps,
        double basisCloseBps,
        double pmAsk0,
        double pmBid0,
        double pmAsk60,
        double pmBid60,
        double pmAsk180,
        double pmBid180,
        double pmAsk300,
        double pmBid300,
        double pmFill100Up,
        double pmFee100Up,
        double pmFill100Down,
        double pmFee100Down,
        double pmMaxPrice,
        double pmMinPrice,
        double pmDepth1cUp,
        double pmDepth1cDown
) {

    /**
     * Converts this backtest market row to a domain {@link MarketCandle}.
     *
     * @return constructed MarketCandle instance
     */
    public MarketCandle toMarketCandle() {
        return new MarketCandle(
                tStart,
                tEnd,
                sOpen,
                sHigh,
                sLow,
                sClose,
                sVolBtc,
                sVolUsd,
                sDeltaBtc,
                fOpen,
                Math.max(fOpen, fClose),
                Math.min(fOpen, fClose),
                fClose,
                fVolBtc,
                fVolUsd,
                fDeltaBtc,
                basisOpenBps
        );
    }

    /**
     * Extracts an {@link OrderBookQuote} snapshot representing book state at a given timing second offset.
     *
     * @param timingSec second offset into the candle (e.g. 0, 60, 180, 300)
     * @return OrderBookQuote corresponding to the specified candle second
     */
    public OrderBookQuote toOrderBookQuote(int timingSec) {
        double askUp;
        double bidUp;
        double execUp;
        double execDown;

        if (timingSec == 60) {
            askUp = pmAsk60 > 0 ? pmAsk60 : (pmAsk0 > 0 ? pmAsk0 : 0.50);
            bidUp = pmBid60 > 0 ? pmBid60 : (pmBid0 > 0 ? pmBid0 : 0.49);
            execUp = askUp;
            execDown = Math.round((1.0 - bidUp) * 10000.0) / 10000.0;
        } else if (timingSec == 180) {
            askUp = pmAsk180 > 0 ? pmAsk180 : (pmAsk60 > 0 ? pmAsk60 : 0.50);
            bidUp = pmBid180 > 0 ? pmBid180 : (pmBid60 > 0 ? pmBid60 : 0.49);
            execUp = askUp;
            execDown = Math.round((1.0 - bidUp) * 10000.0) / 10000.0;
        } else {
            askUp = pmAsk0 > 0 ? pmAsk0 : 0.50;
            bidUp = pmBid0 > 0 ? pmBid0 : 0.49;
            execUp = pmFill100Up > 0 ? pmFill100Up : askUp;
            double rawExecDown = 1.0 - bidUp;
            execDown = pmFill100Down > 0 ? pmFill100Down : (Math.round(rawExecDown * 10000.0) / 10000.0);
        }

        double askDown = Math.max(0.01, Math.round((1.0 - bidUp) * 10000.0) / 10000.0);
        double bidDown = Math.max(0.01, Math.round((1.0 - askUp) * 10000.0) / 10000.0);
        double depthUp = pmDepth1cUp > 0 ? pmDepth1cUp : 300.0;
        double depthDown = pmDepth1cDown > 0 ? pmDepth1cDown : 300.0;

        return new OrderBookQuote(
                askUp,
                bidUp,
                askDown,
                bidDown,
                depthUp,
                depthDown,
                Math.max(0.01, execUp),
                Math.max(0.01, execDown),
                (tStart + timingSec) * 1000L
        );
    }
}
