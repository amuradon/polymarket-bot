package cz.polymarket.bot.backtest;

import cz.polymarket.bot.backtest.cache.BinaryMarketCacheService;
import cz.polymarket.bot.backtest.cache.CachedMarketRow;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class BacktestResourceTest {

    @Inject
    BinaryMarketCacheService cacheService;

    @Test
    @DisplayName("GET /api/status should return backtest engine readiness status")
    void shouldReturnBacktestEngineStatus() {
        given()
                .when().get("/api/status")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("status", equalTo("READY"))
                .body("module", equalTo("backtest"))
                .body("engine", equalTo("In-Memory Simulation Engine"));
    }

    @Test
    @DisplayName("POST /api/backtest/run should return 400 Bad Request when strategyName is missing")
    void shouldReturn400WhenStrategyNameIsMissing() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of("initialCapital", 10000.0))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(400)
                .contentType(ContentType.JSON)
                .body("error", containsString("strategyName is mandatory"));
    }

    @Test
    @DisplayName("POST /api/backtest/run should return 400 Bad Request when strategyName is unknown")
    void shouldReturn400WhenStrategyNameIsUnknown() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of("strategyName", "NonExistentStrategy"))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(400)
                .contentType(ContentType.JSON)
                .body("error", containsString("Unknown strategy"));
    }

    @Test
    @DisplayName("POST /api/backtest/run should return 400 Bad Request when startDate is after endDate")
    void shouldReturn400WhenStartDateAfterEndDate() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "TWAPArbitrageStrategy",
                        "startDate", "2026-08-10",
                        "endDate", "2026-08-01"
                ))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(400)
                .contentType(ContentType.JSON)
                .body("error", containsString("startDate must not be after endDate"));
    }

    @Test
    @DisplayName("GET /api/backtest/run/{jobId} should return 404 for unknown job ID")
    void shouldReturn404ForUnknownJob() {
        given()
                .when().get("/api/backtest/run/unknown-job-id")
                .then()
                .statusCode(404)
                .contentType(ContentType.JSON)
                .body("error", containsString("Job not found"));
    }

    @Test
    @DisplayName("POST /api/backtest/run should queue async job (202) and GET /api/backtest/run/{jobId} should return completed results with file path")
    void shouldExecuteBacktestAsynchronously() {
        long tStart = 1785542400L;
        cacheService.writeMarketRowsCache("BTCUSDT", "2026-08", List.of(
                new CachedMarketRow(
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

        String jobId = given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "TWAPArbitrageStrategy",
                        "symbol", "BTCUSDT",
                        "startDate", "2026-08-01",
                        "endDate", "2026-08-01"
                ))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(202)
                .contentType(ContentType.JSON)
                .body("jobId", notNullValue())
                .body("status", org.hamcrest.Matchers.anyOf(equalTo("QUEUED"), equalTo("IN_PROGRESS"), equalTo("COMPLETED")))
                .body("strategyName", equalTo("TWAPArbitrageStrategy"))
                .body("symbol", equalTo("BTCUSDT"))
                .extract().path("jobId");

        assertThat(jobId).isNotBlank();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            String status = given()
                    .when().get("/api/backtest/run/" + jobId)
                    .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .extract().path("status");

            assertThat(status).isEqualTo("COMPLETED");
        });

        String resultFilePath = given()
                .when().get("/api/backtest/run/" + jobId)
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("jobId", equalTo(jobId))
                .body("status", equalTo("COMPLETED"))
                .body("strategyName", equalTo("TWAPArbitrageStrategy"))
                .body("symbol", equalTo("BTCUSDT"))
                .body("totalMarkets", equalTo(1))
                .body("resultFilePath", notNullValue())
                .body("jsonFilePath", notNullValue())
                .extract().path("resultFilePath");

        assertThat(resultFilePath).isNotBlank();
        File exportedFile = new File(resultFilePath);
        assertThat(exportedFile).exists().isNotEmpty();
    }
}
