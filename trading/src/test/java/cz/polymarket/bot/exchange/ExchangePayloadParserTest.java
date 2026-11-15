package cz.polymarket.bot.exchange;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExchangePayloadParserTest {

    private final ExchangePayloadParser parser = new ExchangePayloadParser(new com.fasterxml.jackson.databind.ObjectMapper());

    @Test
    void shouldParseBinanceMarketCandlesWithCvdDelta() {
        String json = """
                [
                  [
                    1700000000000,
                    "90000.00",
                    "90500.00",
                    "89900.00",
                    "90200.00",
                    "10.00000000",
                    1700000899999,
                    "902000.00000000",
                    150,
                    "7.00000000",
                    "631400.00000000",
                    "0"
                  ]
                ]
                """;
        var candles = parser.parseBinanceMarketCandles(json);
        assertThat(candles).hasSize(1);
        var c = candles.get(0);
        assertThat(c.intervalStartSec()).isEqualTo(1700000000L);
        assertThat(c.intervalEndSec()).isEqualTo(1700000899L);
        assertThat(c.spotOpen()).isEqualTo(90000.0);
        assertThat(c.spotHigh()).isEqualTo(90500.0);
        assertThat(c.spotLow()).isEqualTo(89900.0);
        assertThat(c.spotClose()).isEqualTo(90200.0);
        assertThat(c.spotVolumeBtc()).isEqualTo(10.0);
        assertThat(c.spotVolumeUsd()).isEqualTo(902000.0);
        // delta = 2 * 7.0 - 10.0 = 4.0
        assertThat(c.spotDeltaBtc()).isEqualTo(4.0);
    }
}
