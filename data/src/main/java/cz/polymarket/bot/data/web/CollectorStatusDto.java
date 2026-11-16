package cz.polymarket.bot.data.web;

import java.util.List;
import java.util.Map;

/**
 * Data transfer object representing the live operational status of the Data Collector application.
 *
 * @param symbol                             configured trading symbol (e.g. BTCUSDT)
 * @param timeframe                          configured Polymarket market timeframe (e.g. 15m)
 * @param writeMode                          current write mode ({@code "ACTIVE"}, {@code "STANDBY"}, or {@code "FROZEN"})
 * @param readyForCutover                    whether all WebSocket streams are connected and receiving data
 * @param standbyBufferedRecords             number of records currently held in the standby memory buffer
 * @param binanceStreams                     connection state of each of the 4 Binance ({@code aggTrades} and {@code bookTicker}) WebSocket streams
 * @param krakenUsdtUsdEnabled               whether listening and collecting of the Kraken {@code USDTUSD} stream is enabled
 * @param krakenStreams                      connection state of each active Kraken Spot trade WebSocket stream (e.g. {@code BTCUSD}, {@code USDTUSD})
 * @param coinbaseStreamState                connection state of the Coinbase Spot trade WebSocket stream
 * @param polymarketPolyBoltTwapState        connection state of the Polymarket PolyBolt {@code price.crypto.twap} WebSocket
 * @param polymarketPolyBoltCryptoPriceState connection state of the Polymarket PolyBolt {@code price.crypto} WebSocket
 * @param polymarketClobStreams              connection state of each active per-market Polymarket CLOB WebSocket stream
 * @param trackedPolymarketSlugs             list of currently tracked Polymarket market slugs (current + lookahead-count)
 * @param binanceRecordsWritten              total Binance events written to daily files since startup
 * @param krakenRecordsWritten               total Kraken Spot trade events written to daily files since startup
 * @param coinbaseRecordsWritten             total Coinbase Spot trade events written to daily files since startup
 * @param polyBoltRecordsWritten             total PolyBolt ({@code price.crypto} and {@code price.crypto.twap}) events written to daily files since startup
 * @param polymarketRecordsWritten           total Polymarket market events written to per-market files since startup
 */
public record CollectorStatusDto(
        String symbol,
        String timeframe,
        String writeMode,
        boolean readyForCutover,
        long standbyBufferedRecords,
        Map<String, String> binanceStreams,
        boolean krakenUsdtUsdEnabled,
        Map<String, String> krakenStreams,
        String coinbaseStreamState,
        String polymarketPolyBoltTwapState,
        String polymarketPolyBoltCryptoPriceState,
        Map<String, String> polymarketClobStreams,
        List<String> trackedPolymarketSlugs,
        long binanceRecordsWritten,
        long krakenRecordsWritten,
        long coinbaseRecordsWritten,
        long polyBoltRecordsWritten,
        long polymarketRecordsWritten
) {
}
