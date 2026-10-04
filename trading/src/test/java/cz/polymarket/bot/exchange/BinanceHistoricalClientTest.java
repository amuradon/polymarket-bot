package cz.polymarket.bot.exchange;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BinanceHistoricalClientTest {

    @Test
    @SuppressWarnings("unchecked")
    void shouldFetchAndParseKlines() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse<String> httpResponse = mock(HttpResponse.class);

        String sampleKlines = """
                [
                  [1788437619000,"78032.17","78032.17","78032.16","78032.17","0.01155000",1788437619999,"901.27156280",4,"0.01148000","895.80931160","0"],
                  [1788437620000,"78032.17","78032.17","78032.16","78032.16","0.02092000",1788437620999,"1632.43285380",5,"0.00666000","519.69425220","0"]
                ]
                """;

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(sampleKlines);
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(httpResponse);

        ExchangePayloadParser parser = new ExchangePayloadParser(new ObjectMapper());
        BinanceHistoricalClient client = new BinanceHistoricalClient("https://api.binance.com/api/v3", parser, httpClient);

        Map<Long, BigDecimal> klines = client.fetch1sKlines(1788437619L, 1788437620L);

        assertThat(klines).hasSize(2);
        assertThat(klines.get(1788437619L)).isEqualByComparingTo("78032.17");
        assertThat(klines.get(1788437620L)).isEqualByComparingTo("78032.16");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldFetchAndParse15mKlines() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse<String> httpResponse = mock(HttpResponse.class);

        String sampleKlines = """
                [
                  [1700000000000,"90000.00","90500.00","89900.00","90200.00","10.00000000",1700000899999,"902000.00000000",150,"7.00000000","631400.00000000","0"]
                ]
                """;

        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn(sampleKlines);
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(httpResponse);

        ExchangePayloadParser parser = new ExchangePayloadParser(new ObjectMapper());
        BinanceHistoricalClient client = new BinanceHistoricalClient("https://api.binance.com/api/v3", parser, httpClient);

        var candles = client.fetch15mKlines(24);
        assertThat(candles).hasSize(1);
        assertThat(candles.get(0).spotClose()).isEqualTo(90200.0);
        assertThat(candles.get(0).spotDeltaBtc()).isEqualTo(4.0);
    }
}
