package cz.polymarket.bot.backtest;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class BacktestResourceTest {

    @Test
    @DisplayName("Should return backtest engine readiness status")
    void shouldReturnBacktestEngineStatus() {
        given()
                .when().get("/backtest")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("status", equalTo("READY"))
                .body("module", equalTo("backtest"))
                .body("engine", equalTo("In-Memory Simulation Engine"));
    }

    @Test
    @DisplayName("Should return 400 Bad Request when strategyName is missing in run request")
    void shouldReturn400WhenStrategyNameIsMissing() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of("initialCapital", 10000.0))
                .when().post("/backtest/run")
                .then()
                .statusCode(400)
                .contentType(ContentType.JSON)
                .body("error", containsString("strategyName is mandatory"));
    }

    @Test
    @DisplayName("Should return 400 Bad Request when strategyName is unknown")
    void shouldReturn400WhenStrategyNameIsUnknown() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of("strategyName", "NonExistentStrategy"))
                .when().post("/backtest/run")
                .then()
                .statusCode(400)
                .contentType(ContentType.JSON)
                .body("error", containsString("Unknown strategy"));
    }

    @jakarta.inject.Inject
    cz.polymarket.bot.backtest.cache.BinaryMarketCacheService cacheService;

    @Test
    @DisplayName("Should execute backtest for TWAPArbitrageStrategy and return 200 with metrics")
    void shouldExecuteBacktestSuccessfully() {
        long tStart = 1785542400L;
        cacheService.writeMarketRowsCache("BTCUSDT", "2026-08", java.util.List.of(
                new cz.polymarket.bot.backtest.cache.CachedMarketRow(
                        tStart, tStart + 900L,
                        60000.0, 60050.0, 59950.0, 60000.0,
                        10.0, 600000.0, 0.0,
                        60000.0, 60050.0, 59950.0, 60000.0,
                        10.0, 600000.0, 0.0,
                        true,
                        0.50, 0.48, 0.50, 0.48, 0.50, 0.48, 0.50, 0.48,
                        0.50, 0.015, 0.50, 0.015,
                        0.55, 0.45, 300.0, 300.0
                )
        ));

        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "TWAPArbitrageStrategy",
                        "symbol", "BTCUSDT",
                        "startDate", "2026-08-01",
                        "endDate", "2026-08-01"
                ))
                .when().post("/backtest/run")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("strategyName", equalTo("TWAPArbitrageStrategy"))
                .body("symbol", equalTo("BTCUSDT"))
                .body("totalMarkets", equalTo(1))
                .body("jsonFilePath", notNullValue());
    }
}
