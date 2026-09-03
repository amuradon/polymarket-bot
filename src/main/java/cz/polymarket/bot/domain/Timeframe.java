package cz.polymarket.bot.domain;

import java.time.Instant;

public enum Timeframe {
    FIVE_MINUTES("5m", 300),
    FIFTEEN_MINUTES("15m", 900);

    private final String code;
    private final long durationSeconds;

    Timeframe(String code, long durationSeconds) {
        this.code = code;
        this.durationSeconds = durationSeconds;
    }

    @com.fasterxml.jackson.annotation.JsonValue
    public String getCode() {
        return code;
    }

    public long getDurationSeconds() {
        return durationSeconds;
    }

    public Instant getCandleStart(Instant timestamp) {
        long epochSecond = timestamp.getEpochSecond();
        long startEpochSecond = (epochSecond / durationSeconds) * durationSeconds;
        return Instant.ofEpochSecond(startEpochSecond);
    }

    public Instant getCandleEnd(Instant candleStart) {
        return candleStart.plusSeconds(durationSeconds);
    }

    @com.fasterxml.jackson.annotation.JsonCreator
    public static Timeframe fromCode(String code) {
        for (Timeframe tf : values()) {
            if (tf.code.equalsIgnoreCase(code)) {
                return tf;
            }
        }
        throw new IllegalArgumentException("Unsupported timeframe: " + code);
    }
}
