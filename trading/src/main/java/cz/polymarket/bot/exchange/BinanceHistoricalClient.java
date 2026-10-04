package cz.polymarket.bot.exchange;

import cz.polymarket.bot.domain.MarketCandle;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class BinanceHistoricalClient {

    private final String baseUrl;
    private final ExchangePayloadParser parser;
    private final HttpClient httpClient;

    @Inject
    public BinanceHistoricalClient(
            @ConfigProperty(name = "polymarket.exchange.binance.rest-url", defaultValue = "https://api.binance.com/api/v3") String baseUrl,
            ExchangePayloadParser parser,
            HttpClient httpClient) {
        this.baseUrl = baseUrl;
        this.parser = parser;
        this.httpClient = httpClient;
    }

    public Map<Long, BigDecimal> fetch1sKlines(long startSec, long endSec) {
        try {
            long startMs = startSec * 1000L;
            long endMs = endSec * 1000L;
            String url = String.format("%s/klines?symbol=BTCUSDT&interval=1s&startTime=%d&endTime=%d&limit=1000",
                    baseUrl, startMs, endMs);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("User-Agent", "polymarket-bot")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Failed to fetch klines from Binance: HTTP " + response.statusCode() + " - " + response.body());
            }

            return parser.parseBinanceKlines(response.body());
        } catch (Exception e) {
            throw new RuntimeException("Error fetching Binance 1s klines: " + e.getMessage(), e);
        }
    }

    public List<MarketCandle> fetch15mKlines(int limit) {
        try {
            int safeLimit = Math.max(1, Math.min(100, limit));
            String url = String.format("%s/klines?symbol=BTCUSDT&interval=15m&limit=%d", baseUrl, safeLimit);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("User-Agent", "polymarket-bot")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Failed to fetch 15m klines from Binance: HTTP " + response.statusCode() + " - " + response.body());
            }

            return parser.parseBinanceMarketCandles(response.body());
        } catch (Exception e) {
            throw new RuntimeException("Error fetching Binance 15m klines: " + e.getMessage(), e);
        }
    }
}
