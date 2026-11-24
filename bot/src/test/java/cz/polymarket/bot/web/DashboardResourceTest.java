package cz.polymarket.bot.web;

import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.engine.StrategyTradingEngine;
import cz.polymarket.bot.strategy.OrderCommand;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * {@code @QuarkusTest} component tests verifying all unified REST and Qute HTML dashboard endpoints
 * ({@code GET /}, {@code GET /api/status}, {@code GET /api/health}, {@code POST /api/mode},
 * {@code GET /api/telemetry}, {@code GET /api/positions}, {@code GET /api/paper/metrics},
 * {@code GET /api/paper/trades}, {@code POST /api/reset}, {@code GET /api/collector/status}).
 */
@QuarkusTest
class DashboardResourceTest {

    @Inject
    StrategyTradingEngine engine;

    @BeforeEach
    void resetState() {
        given()
                .contentType(ContentType.JSON)
                .body("{\"mode\":\"LIVE\"}")
                .when().post("/api/mode")
                .then().statusCode(200);
        given()
                .queryParam("paper", true)
                .when().post("/api/reset")
                .then().statusCode(200);
    }

    @Test
    void shouldRenderUnifiedDashboardSwitchModesExecutePaperTradeAndExposeAllApiEndpoints() {
        // 1. Default LIVE mode status & health (streams-enabled=false in %test -> healthy=true)
        given()
                .when().get("/api/health")
                .then()
                .statusCode(200)
                .body("healthy", equalTo(true))
                .body("strategyMode", equalTo("LIVE"));

        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .contentType(containsString("text/html"))
                .body(containsString("Polymarket Bot"))
                .body(containsString("OraclePulseLockStrategy"))
                .body(containsString("INACTIVE (PRESERVED)"));

        // 2. Invalid mode returns 400 Bad Request
        given()
                .contentType(ContentType.JSON)
                .body("{\"mode\":\"UNKNOWN_MODE\"}")
                .when().post("/api/mode")
                .then()
                .statusCode(400);

        // 3. Switch to PAPER mode via POST /api/mode, execute a synthetic paper trade, and verify /api/positions & /api/paper/*
        given()
                .contentType(ContentType.JSON)
                .body("{\"mode\":\"PAPER\"}")
                .when().post("/api/mode")
                .then()
                .statusCode(200)
                .body("strategyMode", equalTo("PAPER"));

        long tStartMs = 1_785_542_400_000L;
        engine.onPolymarketCryptoTwap60s(tStartMs, "btcusd", "chainlink", 88_000.0, 60, tStartMs);
        engine.submitOrder(new OrderCommand(
                "dash-paper-1", "0xMarket15m", "BTC-UP",
                Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.50), BigDecimal.valueOf(100.0)
        ));

        // Wait briefly for configured paper latency if > 0
        org.awaitility.Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(2))
                .untilAsserted(() -> given()
                        .when().get("/api/positions")
                        .then()
                        .statusCode(200)
                        .body("$", hasSize(1))
                        .body("[0].clientOrderId", equalTo("dash-paper-1")));

        // Close paper trade at 0.70 take profit
        engine.submitOrder(new OrderCommand(
                "dash-paper-exit-1", "0xMarket15m", "BTC-UP",
                Timeframe.FIFTEEN_MINUTES, "SELL", BigDecimal.valueOf(0.70), BigDecimal.valueOf(100.0)
        ));

        org.awaitility.Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(2))
                .untilAsserted(() -> given()
                        .when().get("/api/paper/trades")
                        .then()
                        .statusCode(200)
                        .body("$", hasSize(1)));

        given()
                .when().get("/api/paper/metrics")
                .then()
                .statusCode(200)
                .body("totalTrades", equalTo(1));

        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .body(containsString("Paper Trade Execution History"));

        // 4. Switch to OFF mode and verify HTML renders Collector-Only notice & /api/telemetry + /api/collector/status respond
        given()
                .contentType(ContentType.JSON)
                .body("{\"mode\":\"OFF\"}")
                .when().post("/api/mode")
                .then()
                .statusCode(200)
                .body("strategyMode", equalTo("OFF"));

        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .body(containsString("COLLECTOR-ONLY MODE"));

        given()
                .when().get("/api/telemetry")
                .then()
                .statusCode(200)
                .body("threads", hasSize(4));

        given()
                .when().get("/api/collector/status")
                .then()
                .statusCode(200)
                .body("lookaheadCount", equalTo(1))
                .body("binanceSpotBookTickerEnabled", equalTo(false))
                .body("binanceFuturesBookTickerEnabled", equalTo(false));
    }
}
