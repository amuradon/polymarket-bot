package cz.polymarket.bot.data.web;

import java.util.List;
import java.util.Map;

/**
 * Data transfer object representing the live operational status of the Data Collector application.
 *
 * @param symbol                             configured trading symbol (e.g. BTCUSDT)
 * @param timeframe                          configured Polymarket market timeframe (e.g. 15m)
 * @param writeMode                          current write mode ({@code "ACTIVE"}, {@code "STANDBY"}, or {@code "FROZEN"})
 * @param readyForCutover                    whether all enabled WebSocket streams are connected and receiving data
 * @param standbyBufferedRecords             number of records currently held in the standby memory buffer
 * @param binanceSpotBookTickerEnabled       whether the Binance Spot {@code bookTicker} stream is enabled
 * @param binanceFuturesBookTickerEnabled    whether the Binance USD-M Futures {@code bookTicker} stream is enabled
 * @param binanceStreams                     connection state of enabled Binance {@code bookTicker} WebSocket streams
 * @param polymarketPolyBoltTwapState        connection state of the Polymarket PolyBolt {@code price.crypto.twap} WebSocket
 * @param polymarketPolyBoltCryptoPriceState connection state of the Polymarket PolyBolt {@code price.crypto} WebSocket
 * @param polymarketClobStreams              connection state of each active per-market Polymarket CLOB WebSocket stream
 * @param trackedPolymarketSlugs             list of currently tracked Polymarket market slugs (current + lookahead-count)
 * @param binanceRecordsWritten              total Binance {@code bookTicker} events written to daily files since startup
 * @param polyBoltRecordsWritten             total PolyBolt ({@code price.crypto} and {@code price.crypto.twap}) events written to daily files since startup
 * @param polymarketRecordsWritten           total Polymarket market events written to per-market files since startup
 */
public record CollectorStatusDto(
        String symbol,
        String timeframe,
        String writeMode,
        boolean readyForCutover,
        long standbyBufferedRecords,
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
