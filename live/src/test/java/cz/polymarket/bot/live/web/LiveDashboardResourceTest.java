package cz.polymarket.bot.live.web;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class LiveDashboardResourceTest {

    @Test
    void shouldServeDashboardWith4CoreTelemetryOnRootPath() {
        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("Polymarket Bot :: Live Trading"))
                .body(containsString("UtterParameciumStrategy"))
                .body(containsString("Initial Capital"))
                .body(containsString("Current Balance"))
                .body(containsString("4-Core Dedicated Pipeline Utilization"))
                .body(containsString("OS TCP Rx Buffer"))
                .body(containsString("vCPU #1: I/O Reader"))
                .body(containsString("vCPU #2: CLOB Book Worker"))
                .body(containsString("vCPU #3: Strategy Calculator"))
                .body(containsString("vCPU #4: Order &amp; I/O Worker"));
    }

    @Test
    void shouldReturnLiveStatusApiWithTelemetry() {
        given()
                .when().get("/api/live/status")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("strategyName", equalTo("UtterParameciumStrategy"))
                .body("status", equalTo("LIVE"))
                .body("orderType", equalTo("FAK"))
                .body("initialCapital", notNullValue())
                .body("currentBalance", notNullValue())
                .body("telemetry", notNullValue())
                .body("telemetry.threads", hasSize(4));
    }

    @Test
    void shouldReturnLiveTelemetryApi() {
        given()
                .when().get("/api/live/telemetry")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("gcName", notNullValue())
                .body("threads", hasSize(4))
                .body("threads[0].role", equalTo("vCPU #1: I/O Reader"))
                .body("threads[1].role", equalTo("vCPU #2: CLOB Book Worker"))
                .body("threads[2].role", equalTo("vCPU #3: Strategy Calculator"))
                .body("threads[3].role", equalTo("vCPU #4: Order & I/O Worker"));
    }

    @Test
    void shouldReturnPositionsApi() {
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
