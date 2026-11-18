package cz.polymarket.bot.state;

import cz.polymarket.bot.engine.Position;
import cz.polymarket.bot.engine.StrategyMode;

import java.util.List;

/**
 * Serializable runtime state snapshot persisted before instance shutdown (and periodically off the hot path)
 * so a newly deployed or rolled-back instance seamlessly resumes the active market's target price
 * ({@code twapOpenPrice}), {@link StrategyMode}, and any open {@link Position}s.
 *
 * @param savedAtTimestampMs               epoch milliseconds when the snapshot was persisted
 * @param symbol                           configured trading symbol (e.g. {@code "BTCUSDT"})
 * @param timeframe                        configured Polymarket market timeframe (e.g. {@code "15m"})
 * @param strategyMode                     active {@link StrategyMode} ({@code LIVE}, {@code PAPER}, or {@code OFF})
 * @param activeCandleStartSec             start timestamp of the active market interval in epoch seconds
 * @param activeCandleEndSec               end timestamp of the active market interval in epoch seconds
 * @param twapOpenPrice                    captured 60-second Chainlink TWAP target price for the active market
 * @param activeMarketSlug                 slug of the active Polymarket market (or {@code ""})
 * @param conditionId                      Polymarket condition ID of the active market (or {@code ""})
 * @param yesTokenId                       Up (YES) conditional token ID (or {@code ""})
 * @param noTokenId                        Down (NO) conditional token ID (or {@code ""})
 * @param lastTwapOracleTimestampMs        high-water mark oracle timestamp for PolyBolt TWAP
 * @param lastCryptoPriceOracleTimestampMs high-water mark oracle timestamp for PolyBolt spot price
 * @param currentSpotPrice                 latest observed Chainlink spot price
 * @param candleHigh                       highest observed spot price in the active candle
 * @param candleLow                        lowest observed spot price in the active candle
 * @param initialCapital                   initial bankroll in USD
 * @param currentBalance                   current balance in USD
 * @param openPositions                    list of open positions belonging to the active candle
 */
public record RuntimeStateSnapshot(
        long savedAtTimestampMs,
        String symbol,
        String timeframe,
        StrategyMode strategyMode,
        long activeCandleStartSec,
        long activeCandleEndSec,
        double twapOpenPrice,
        String activeMarketSlug,
        String conditionId,
        String yesTokenId,
        String noTokenId,
        long lastTwapOracleTimestampMs,
        long lastCryptoPriceOracleTimestampMs,
        double currentSpotPrice,
        double candleHigh,
        double candleLow,
        double initialCapital,
        double currentBalance,
        List<Position> openPositions
) {

    /**
     * Normalizes nullable string and collection fields on deserialization.
     */
    public RuntimeStateSnapshot {
        symbol = symbol != null ? symbol : "";
        timeframe = timeframe != null ? timeframe : "";
        activeMarketSlug = activeMarketSlug != null ? activeMarketSlug : "";
        conditionId = conditionId != null ? conditionId : "";
        yesTokenId = yesTokenId != null ? yesTokenId : "";
        noTokenId = noTokenId != null ? noTokenId : "";
        openPositions = openPositions != null ? List.copyOf(openPositions) : List.of();
    }
}
