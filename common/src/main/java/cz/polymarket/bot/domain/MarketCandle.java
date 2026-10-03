package cz.polymarket.bot.domain;

/**
 * Immutable domain representation of a 15-minute market interval (candle)
 * containing both spot and futures metrics, delta volumes, and basis spread.
 */
public record MarketCandle(
        long intervalStartSec,
        long intervalEndSec,
        double spotOpen,
        double spotHigh,
        double spotLow,
        double spotClose,
        double spotVolumeBtc,
        double spotVolumeUsd,
        double spotDeltaBtc,
        double futuresOpen,
        double futuresHigh,
        double futuresLow,
        double futuresClose,
        double futuresVolumeBtc,
        double futuresVolumeUsd,
        double futuresDeltaBtc,
        double basisOpenBps
) {
    public MarketCandle {
        if (intervalStartSec <= 0 || intervalEndSec <= intervalStartSec) {
            throw new IllegalArgumentException("Invalid interval range: start=" + intervalStartSec + ", end=" + intervalEndSec);
        }
        if (spotOpen <= 0 || spotClose <= 0) {
            throw new IllegalArgumentException("Spot prices must be positive");
        }
    }
}
