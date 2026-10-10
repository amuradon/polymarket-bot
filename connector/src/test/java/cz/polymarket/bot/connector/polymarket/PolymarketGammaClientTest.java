package cz.polymarket.bot.connector.polymarket;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PolymarketMarketInfo} and {@link PolymarketGammaClient} using synthetic HTTP fixtures.
 */
class PolymarketGammaClientTest {

    private Vertx vertx;
    private HttpServer server;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldNormalizeSymbolAndBuildSlug() {
        assertThat(PolymarketMarketInfo.normalizeAssetSlugPrefix("BTCUSDT")).isEqualTo("btc");
        assertThat(PolymarketMarketInfo.normalizeAssetSlugPrefix("ETH")).isEqualTo("eth");
        assertThat(PolymarketMarketInfo.normalizePolyBoltSymbol("BTCUSDT")).isEqualTo("btcusd");
        assertThat(PolymarketMarketInfo.normalizePolyBoltSymbol("SOL")).isEqualTo("solusd");
        assertThat(PolymarketMarketInfo.parseTimeframeSeconds("15m")).isEqualTo(900L);
        assertThat(PolymarketMarketInfo.parseTimeframeSeconds("5m")).isEqualTo(300L);
        assertThat(PolymarketMarketInfo.parseTimeframeSeconds("1h")).isEqualTo(3600L);
        assertThat(PolymarketMarketInfo.buildMarketSlug("BTCUSDT", "15m", 1789041600L))
                .isEqualTo("btc-updown-15m-1789041600");
    }

    @Test
    void shouldFetchMarketBySlugWithMinimalParsing() throws Exception {
        server = vertx.createHttpServer()
                .requestHandler(req -> {
                    String query = req.query();
                    if (query != null && query.contains("slug=btc-updown-15m-1789041600")) {
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("""
                                        [
                                          {
                                            "id": "m-100",
                                            "slug": "btc-updown-15m-1789041600",
                                            "conditionId": "0xabc123",
                                            "question": "Bitcoin Up or Down - 15m",
                                            "outcomes": "[\\"Up\\", \\"Down\\"]",
                                            "clobTokenIds": "[\\"token_up_100\\", \\"token_down_100\\"]",
                                            "outcomePrices": "[\\"1\\", \\"0\\"]",
                                            "closed": true
                                          }
                                        ]
                                        """);
                    } else if (query != null && query.contains("slug=btc-updown-15m-1789042500")) {
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("""
                                        [
                                          {
                                            "id": "m-200",
                                            "slug": "btc-updown-15m-1789042500",
                                            "clobTokenIds": ["token_up_200", "token_down_200"],
                                            "closed": false
                                          }
                                        ]
                                        """);
                    } else {
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("[]");
                    }
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketGammaClient client = new PolymarketGammaClient(
                "http://localhost:" + port,
                HttpClient.newHttpClient(),
                new ObjectMapper()
        );

        Optional<PolymarketMarketInfo> resolved = client.fetchMarketBySlug("btc-updown-15m-1789041600", 900L);
        assertThat(resolved).isPresent();
        assertThat(resolved.get().slug()).isEqualTo("btc-updown-15m-1789041600");
        assertThat(resolved.get().startSec()).isEqualTo(1789041600L);
        assertThat(resolved.get().endSec()).isEqualTo(1789042500L);
        assertThat(resolved.get().clobTokenIds()).containsExactly("token_up_100", "token_down_100");
        assertThat(resolved.get().closed()).isTrue();
        assertThat(resolved.get().rawJson()).contains("\"outcomePrices\"");

        Optional<PolymarketMarketInfo> openMarket = client.fetchMarketBySlug("btc-updown-15m-1789042500", 900L);
        assertThat(openMarket).isPresent();
        assertThat(openMarket.get().clobTokenIds()).containsExactly("token_up_200", "token_down_200");
        assertThat(openMarket.get().closed()).isFalse();
    }
}
