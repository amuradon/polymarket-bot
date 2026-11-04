package cz.polymarket.bot.live.web;

import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.live.storage.LiveTradeRepository;
import cz.polymarket.bot.strategy.TradeDirection;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class LiveDashboardResourceTest {

    @Inject
    LiveTradeRepository repository;

    @Test
    void shouldServeDashboardOnRootPath() {
        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("Polymarket Bot :: Live Trading"))
                .body(containsString("UtterParameciumStrategy"))
                .body(containsString("Initial Capital"))
                .body(containsString("Current Balance"))
                .body(containsString("Execution History"));
    }

    @Test
    void shouldServeDashboardWithRecordedTradesOnRootPath() {
        repository.recordTrade(new TradeRecord(
                "2026-11-04T12:00:00Z",
                1793793600L,
                TradeDirection.UP,
                0.55,
                1.00,
                55.0,
                100.0,
                0.0,
                45.0,
                "Resolution (TWAP 60s)",
                0.70,
                0.55,
                0.15,
                true,
                1045.0
        ));

        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("Polymarket Bot :: Live Trading"))
                .body(containsString("WIN"))
                .body(containsString("Resolution (TWAP 60s)"))
                .body(containsString("$1045.0"));
    }

    @Test
    void shouldReturnLiveStatusApi() {
        given()
                .when().get("/api/live/status")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("strategyName", equalTo("UtterParameciumStrategy"))
                .body("status", equalTo("DRY_RUN"))
                .body("dryRun", equalTo(true))
                .body("orderType", equalTo("FAK"))
                .body("initialCapital", notNullValue())
                .body("currentBalance", notNullValue());
    }

    @Test
    void shouldReturnMetricsApi() {
        given()
                .when().get("/api/live/metrics")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("totalTrades", notNullValue())
                .body("winRatePct", notNullValue())
                .body("initialCapital", notNullValue());
    }

    @Test
    void shouldReturnTradesAndPositionsApi() {
        given()
                .when().get("/api/live/trades")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON);

        given()
                .when().get("/api/live/positions")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON);
    }

    @Test
    void shouldHandleResetApi() {
        given()
                .when().post("/api/live/reset")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("status", equalTo("SUCCESS"))
                .body("currentBalance", notNullValue());
    }
}
