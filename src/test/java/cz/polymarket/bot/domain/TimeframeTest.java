package cz.polymarket.bot.domain;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimeframeTest {

    @Test
    void shouldParseValidTimeframeCodes() {
        assertThat(Timeframe.fromCode("5m")).isEqualTo(Timeframe.FIVE_MINUTES);
        assertThat(Timeframe.fromCode("15m")).isEqualTo(Timeframe.FIFTEEN_MINUTES);
    }

    @Test
    void shouldThrowOnInvalidCode() {
        assertThatThrownBy(() -> Timeframe.fromCode("1m"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported timeframe");
    }

    @Test
    void shouldCalculateCorrectCandleStartForFiveMinutes() {
        // 14:03:25 UTC -> candle start should be 14:00:00 UTC
        Instant instant1 = Instant.parse("2026-09-03T14:03:25Z");
        Instant candleStart1 = Timeframe.FIVE_MINUTES.getCandleStart(instant1);
        assertThat(candleStart1).isEqualTo(Instant.parse("2026-09-03T14:00:00Z"));

        // 14:07:49 UTC -> candle start should be 14:05:00 UTC
        Instant instant2 = Instant.parse("2026-09-03T14:07:49Z");
        Instant candleStart2 = Timeframe.FIVE_MINUTES.getCandleStart(instant2);
        assertThat(candleStart2).isEqualTo(Instant.parse("2026-09-03T14:05:00Z"));

        // 14:05:00 UTC -> candle start is exactly 14:05:00 UTC
        Instant instant3 = Instant.parse("2026-09-03T14:05:00Z");
        Instant candleStart3 = Timeframe.FIVE_MINUTES.getCandleStart(instant3);
        assertThat(candleStart3).isEqualTo(Instant.parse("2026-09-03T14:05:00Z"));
    }

    @Test
    void shouldCalculateCorrectCandleStartForFifteenMinutes() {
        // 14:07:49 UTC -> candle start should be 14:00:00 UTC
        Instant instant1 = Instant.parse("2026-09-03T14:07:49Z");
        Instant candleStart1 = Timeframe.FIFTEEN_MINUTES.getCandleStart(instant1);
        assertThat(candleStart1).isEqualTo(Instant.parse("2026-09-03T14:00:00Z"));

        // 14:17:12 UTC -> candle start should be 14:15:00 UTC
        Instant instant2 = Instant.parse("2026-09-03T14:17:12Z");
        Instant candleStart2 = Timeframe.FIFTEEN_MINUTES.getCandleStart(instant2);
        assertThat(candleStart2).isEqualTo(Instant.parse("2026-09-03T14:15:00Z"));
    }

    @Test
    void shouldCalculateCandleEnd() {
        Instant candleStart = Instant.parse("2026-09-03T14:00:00Z");

        Instant end5m = Timeframe.FIVE_MINUTES.getCandleEnd(candleStart);
        assertThat(end5m).isEqualTo(Instant.parse("2026-09-03T14:05:00Z"));

        Instant end15m = Timeframe.FIFTEEN_MINUTES.getCandleEnd(candleStart);
        assertThat(end15m).isEqualTo(Instant.parse("2026-09-03T14:15:00Z"));
    }
}
