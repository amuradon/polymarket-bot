package cz.polymarket.bot.backtest.data;

import cz.polymarket.bot.domain.MarketCandle;
import cz.polymarket.bot.domain.OrderBookQuote;

/**
 * Immutable row representation of a 15-minute market interval loaded from parquet dataset.
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
