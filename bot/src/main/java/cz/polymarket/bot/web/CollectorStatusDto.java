package cz.polymarket.bot.web;

import java.util.List;
import java.util.Map;

/**
 * Data transfer object representing the live operational status of the Data Collector subsystem.
 *
 * @param symbol                             configured trading symbol (e.g. BTCUSDT)
 * @param timeframes                         configured Polymarket market timeframes (e.g. ["15m", "5m"])
 * @param lookaheadCount                     configured number of future Polymarket markets recorded ahead per timeframe (default 1)
 * @param healthy                            whether all enabled collector streams are connected and persisting data
 * @param binanceSpotBookTickerEnabled       whether the Binance Spot {@code bookTicker} stream is enabled
 * @param binanceFuturesBookTickerEnabled    whether the Binance USD-M Futures {@code bookTicker} stream is enabled
 * @param binanceStreams                     connection state of enabled Binance {@code bookTicker} WebSocket streams
 * @param polymarketPolyBoltTwapState        connection state of the Polymarket PolyBolt {@code price.crypto.twap} WebSocket
 * @param polymarketPolyBoltCryptoPriceState connection state of the Polymarket PolyBolt {@code price.crypto} WebSocket
 * @param polymarketClobStreams              connection state of each active per-market collector CLOB WebSocket stream
 * @param trackedPolymarketSlugs             list of currently tracked Polymarket market slugs across all timeframes
 * @param binanceRecordsWritten              total Binance {@code bookTicker} events written to daily files since startup
 * @param polyBoltRecordsWritten             total PolyBolt events written to daily files since startup
 * @param polymarketRecordsWritten           total Polymarket market events written to per-market files since startup
 */
public record CollectorStatusDto(
        String symbol,
        List<String> timeframes,
        int lookaheadCount,
        boolean healthy,
        boolean binanceSpotBookTickerEnabled,
        boolean binanceFuturesBookTickerEnabled,
        Map<String, String> binanceStreams,
        String polymarketPolyBoltTwapState,
        String polymarketPolyBoltCryptoPriceState,
        Map<String, String> polymarketClobStreams,
        List<String> trackedPolymarketSlugs,
        long binanceRecordsWritten,
        long polyBoltRecordsWritten,
        long polymarketRecordsWritten
) {
}
