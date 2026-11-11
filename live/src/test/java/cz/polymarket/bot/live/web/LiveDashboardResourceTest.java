package cz.polymarket.bot.live.web;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import cz.polymarket.bot.live.engine.LiveTradingEngine;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class LiveDashboardResourceTest {

    @Inject
    LiveTradingEngine engine;

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
                .body(containsString("Disconnect &amp; Slow-Consumer Forensics"))
                .body(containsString("Recent Telemetry History"))
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
                .body("telemetry.threads", hasSize(4))
                .body("telemetry.history", hasSize(greaterThanOrEqualTo(1)));
    }

    @Test
    void shouldReturnLiveTelemetryApiWithHistoryAndRecordedDisconnectForensics() {
        engine.onStreamUnexpectedDisconnect(new WebSocketStreamTelemetry(
                "POLYMARKET-CLOB-btc-updown-15m-1794207600",
                ConnectionState.RECONNECTING,
                "198.51.100.47:443",
                4,
                150,
                1L,
                2L,
                110L,
                864L,
                319_621L,
                1_931L,
                1L,
                1_794_207_823_000L,
                1013,
                "slow consumer: send buffer full"
        ));

        given()
                .when().get("/api/live/telemetry")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("gcName", notNullValue())
                .body("nativeTransportEnabled", notNullValue())
                .body("openSslAvailable", notNullValue())
                .body("threads", hasSize(4))
                .body("threads[0].role", equalTo("vCPU #1: I/O Reader"))
                .body("threads[1].role", equalTo("vCPU #2: CLOB Book Worker"))
                .body("threads[2].role", equalTo("vCPU #3: Strategy Calculator"))
                .body("threads[3].role", equalTo("vCPU #4: Order & I/O Worker"))
                .body("history", hasSize(greaterThanOrEqualTo(1)))
                .body("disconnectEvents", hasSize(greaterThanOrEqualTo(1)))
                .body("disconnectEvents[0].streamName", equalTo("POLYMARKET-CLOB-btc-updown-15m-1794207600"))
                .body("disconnectEvents[0].remoteAddress", equalTo("198.51.100.47:443"))
                .body("disconnectEvents[0].closeCode", equalTo(1013))
                .body("disconnectEvents[0].closeReason", equalTo("slow consumer: send buffer full"))
                .body("disconnectEvents[0].peerState", notNullValue());

        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("Failover Status"))
                .body(containsString("198.51.100.47:443"));
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
