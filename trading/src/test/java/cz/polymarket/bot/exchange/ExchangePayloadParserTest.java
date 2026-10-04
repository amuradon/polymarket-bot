package cz.polymarket.bot.exchange;

import cz.polymarket.bot.domain.BinanceTicker;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ExchangePayloadParserTest {

    private final ExchangePayloadParser parser = new ExchangePayloadParser(new com.fasterxml.jackson.databind.ObjectMapper());

    @Test
    void shouldParseBinanceTickerWithEventTimeAndPrice() {
        String json = "{\"e\":\"24hrTicker\",\"E\":1788437674016,\"s\":\"BTCUSDT\",\"c\":\"78058.10000000\"}";
        Optional<BinanceTicker> ticker = parser.parseBinanceTicker(json);
        assertThat(ticker).isPresent();
        assertThat(ticker.get().eventTimeMs()).isEqualTo(1788437674016L);
        assertThat(ticker.get().price()).isEqualByComparingTo("78058.10000000");
    }

    @Test
    void shouldReturnEmptyWhenBinanceMessageHasNoPrice() {
        String json = "{\"result\":null,\"id\":1}";
        Optional<BinanceTicker> ticker = parser.parseBinanceTicker(json);
        assertThat(ticker).isEmpty();
    }

    @Test
    void shouldParseCoinbaseTickerPrice() {
        String json = "{\"type\":\"ticker\",\"sequence\":135510524695,\"product_id\":\"BTC-USD\",\"price\":\"78017.50\"}";
        Optional<BigDecimal> price = parser.parseCoinbaseTicker(json);
        assertThat(price).isPresent().contains(new BigDecimal("78017.50"));
    }

    @Test
    void shouldReturnEmptyForNonTickerCoinbaseMessage() {
        String json = "{\"type\":\"subscriptions\",\"channels\":[{\"name\":\"ticker\"}]}";
        Optional<BigDecimal> price = parser.parseCoinbaseTicker(json);
        assertThat(price).isEmpty();
    }

    @Test
    void shouldParseKrakenV2TickerPrice() {
        String json = "{\"channel\":\"ticker\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"last\":78016.5}]}";
        Optional<BigDecimal> price = parser.parseKrakenTicker(json);
        assertThat(price).isPresent().contains(new BigDecimal("78016.5"));
    }

    @Test
    void shouldReturnEmptyForKrakenHeartbeatOrStatusMessage() {
        String json = "{\"channel\":\"status\",\"data\":[{\"system\":\"online\"}]}";
        Optional<BigDecimal> price = parser.parseKrakenTicker(json);
        assertThat(price).isEmpty();
    }

    @Test
    void shouldParseBinance1sKlines() {
        String json = """
                [
                  [1788437619000,"78032.17","78032.17","78032.16","78032.17","0.01155000",1788437619999,"901.27156280",4,"0.01148000","895.80931160","0"],
                  [1788437620000,"78032.17","78032.17","78032.16","78032.16","0.02092000",1788437620999,"1632.43285380",5,"0.00666000","519.69425220","0"]
                ]
                """;
        Map<Long, BigDecimal> klines = parser.parseBinanceKlines(json);
        assertThat(klines).hasSize(2);
        assertThat(klines.get(1788437619L)).isEqualByComparingTo("78032.17");
        assertThat(klines.get(1788437620L)).isEqualByComparingTo("78032.16");
    }

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
