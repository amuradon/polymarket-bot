package cz.polymarket.bot.backtest.cache;

/**
 * Compact immutable record representing a raw market interval and Polymarket quote state,
 * stored efficiently in memory-mapped binary cache (264 bytes per record).
 *
 * @param tStart candle start timestamp in epoch seconds UTC
 * @param tEnd candle end timestamp in epoch seconds UTC
 * @param sOpen Binance spot opening price
 * @param sHigh Binance spot highest price
 * @param sLow Binance spot lowest price
 * @param sClose Binance spot closing price
 * @param sVolBtc Binance spot total trading volume in BTC
 * @param sVolUsd Binance spot total trading volume in USD
 * @param sDeltaBtc Binance spot cumulative volume delta in BTC
 * @param fOpen Binance futures opening price
 * @param fHigh Binance futures highest price
 * @param fLow Binance futures lowest price
 * @param fClose Binance futures closing price
 * @param fVolBtc Binance futures total trading volume in BTC
 * @param fVolUsd Binance futures total trading volume in USD
 * @param fDeltaBtc Binance futures cumulative volume delta in BTC
 * @param actualOutcomeUp true if TWAP settled UP, false if DOWN
 * @param pmAsk0 Polymarket UP contract best ask at t = 0s
 * @param pmBid0 Polymarket UP contract best bid at t = 0s
 * @param pmAsk60 Polymarket UP contract best ask at t = 60s
 * @param pmBid60 Polymarket UP contract best bid at t = 60s
 * @param pmAsk180 Polymarket UP contract best ask at t = 180s
 * @param pmBid180 Polymarket UP contract best bid at t = 180s
 * @param pmAsk300 Polymarket UP contract best ask at t = 300s
 * @param pmBid300 Polymarket UP contract best bid at t = 300s
 * @param pmFill100Up simulated fill price for $100 market order on UP contract
 * @param pmFee100Up simulated taker fee for $100 market order on UP contract
 * @param pmFill100Down simulated fill price for $100 market order on DOWN contract
 * @param pmFee100Down simulated taker fee for $100 market order on DOWN contract
 * @param pmMaxPrice highest traded Polymarket contract price during candle
 * @param pmMinPrice lowest traded Polymarket contract price during candle
 * @param pmDepth1cUp book depth within 1 cent of best ask for UP contract
 * @param pmDepth1cDown book depth within 1 cent of best bid for DOWN contract
 */
public record CachedMarketRow(
        long tStart,
        long tEnd,
        double sOpen,
        double sHigh,
        double sLow,
        double sClose,
        double sVolBtc,
        double sVolUsd,
        double sDeltaBtc,
        double fOpen,
        double fHigh,
        double fLow,
        double fClose,
        double fVolBtc,
        double fVolUsd,
        double fDeltaBtc,
        boolean actualOutcomeUp,
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
}
