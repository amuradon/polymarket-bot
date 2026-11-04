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
    void shouldFetchMarketBySlugFromMarketsSlugPath() throws Exception {
        server = vertx.createHttpServer()
                .requestHandler(req -> {
                    String path = req.path();
                    if ("/markets/slug/btc-updown-15m-1792741500".equals(path)) {
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("""
                                        {
                                          "$schema": "https://gamma-api.polymarket.com/schemas/Market.json",
                                          "id": "6037795",
                                          "question": "Bitcoin Up or Down - October 23, 3:45AM-4:00AM ET",
                                          "conditionId": "0xf9ceae0be10beb7ee63716f66a3a501519dc156fa5a5c120c8478bded2541107",
                                          "slug": "btc-updown-15m-1792741500",
                                          "outcomes": "[\\"Up\\", \\"Down\\"]",
                                          "outcomePrices": "[\\"0.475\\", \\"0.525\\"]",
                                          "active": true,
                                          "closed": false,
                                          "clobTokenIds": "[\\"66369013841841957711596258160966320387479333440501225054235536600123613731292\\", \\"100885549506856203411133717430816201540241829528439305131467520579115934905068\\"]"
                                        }
                                        """);
                    } else if ("/markets/slug/btc-updown-15m-1789041600".equals(path)) {
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("""
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
                                        """);
                    } else {
                        req.response()
                                .setStatusCode(404)
                                .putHeader("Content-Type", "application/json")
                                .end("{}");
                    }
                });

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketGammaClient client = new PolymarketGammaClient(
                "http://localhost:" + port,
                HttpClient.newHttpClient(),
                new ObjectMapper()
        );

        Optional<PolymarketMarketInfo> contractMarket = client.fetchMarketBySlug("btc-updown-15m-1792741500", 900L);
        assertThat(contractMarket).isPresent();
        assertThat(contractMarket.get().slug()).isEqualTo("btc-updown-15m-1792741500");
        assertThat(contractMarket.get().conditionId())
                .isEqualTo("0xf9ceae0be10beb7ee63716f66a3a501519dc156fa5a5c120c8478bded2541107");
        assertThat(contractMarket.get().startSec()).isEqualTo(1792741500L);
        assertThat(contractMarket.get().endSec()).isEqualTo(1792742400L);
        assertThat(contractMarket.get().yesTokenId())
                .isEqualTo("66369013841841957711596258160966320387479333440501225054235536600123613731292");
        assertThat(contractMarket.get().noTokenId())
                .isEqualTo("100885549506856203411133717430816201540241829528439305131467520579115934905068");
        assertThat(contractMarket.get().closed()).isFalse();

        Optional<PolymarketMarketInfo> resolved = client.fetchMarketBySlug("btc-updown-15m-1789041600", 900L);
        assertThat(resolved).isPresent();
        assertThat(resolved.get().slug()).isEqualTo("btc-updown-15m-1789041600");
        assertThat(resolved.get().conditionId()).isEqualTo("0xabc123");
        assertThat(resolved.get().startSec()).isEqualTo(1789041600L);
        assertThat(resolved.get().endSec()).isEqualTo(1789042500L);
        assertThat(resolved.get().clobTokenIds()).containsExactly("token_up_100", "token_down_100");
        assertThat(resolved.get().yesTokenId()).isEqualTo("token_up_100");
        assertThat(resolved.get().noTokenId()).isEqualTo("token_down_100");
        assertThat(resolved.get().closed()).isTrue();
        assertThat(resolved.get().rawJson()).contains("\"outcomePrices\"");

        Optional<PolymarketMarketInfo> missing = client.fetchMarketBySlug("btc-updown-15m-9999999999", 900L);
        assertThat(missing).isEmpty();
    }
}
