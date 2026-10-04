package cz.polymarket.bot.paper.web;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class PaperDashboardResourceTest {

    @jakarta.inject.Inject
    cz.polymarket.bot.paper.storage.PaperTradeRepository repository;

    @Test
    void shouldServeDashboardOnRootPath() {
        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("Polymarket Bot :: Paper Trading"))
                .body(containsString("TWAPArbitrageStrategy"))
                .body(containsString("Initial Capital"))
                .body(containsString("Current Balance"))
                .body(containsString("Execution History"));
    }

    @Test
    void shouldServeDashboardWithRecordedTradesOnRootPath() {
        repository.recordTrade(new cz.polymarket.bot.domain.TradeRecord(
                "2026-10-04T12:00:00Z",
                1728043200L,
                cz.polymarket.bot.strategy.TradeDirection.UP,
                0.60,
                0.70,
                300.0,
                500.0,
                4.5,
                45.5,
                "Take Profit (0.70)",
                0.75,
                0.60,
                0.15,
                true,
                10045.5
        ));

        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("Polymarket Bot :: Paper Trading"))
                .body(containsString("WIN"))
                .body(containsString("Take Profit (0.70)"))
                .body(containsString("$10045.5"));
    }

    @Test
    void shouldReturnPaperStatusApi() {
        given()
                .when().get("/api/paper/status")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("strategyName", equalTo("TWAPArbitrageStrategy"))
                .body("status", equalTo("RUNNING"))
                .body("orderLatencyMs", equalTo(50))
                .body("initialCapital", notNullValue())
                .body("currentBalance", notNullValue());
    }

    @Test
    void shouldReturnMetricsApi() {
        given()
                .when().get("/api/paper/metrics")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("totalTrades", notNullValue())
                .body("winRatePct", notNullValue())
                .body("initialCapital", notNullValue());
    }

    @Test
    void shouldReturnTradesApi() {
        given()
                .when().get("/api/paper/trades")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON);
    }

    @Test
    void shouldHandleResetApi() {
        given()
                .when().post("/api/paper/reset")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("status", equalTo("SUCCESS"))
                .body("currentBalance", notNullValue());
    }
}
