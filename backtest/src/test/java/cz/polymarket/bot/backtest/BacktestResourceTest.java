package cz.polymarket.bot.backtest;

import cz.polymarket.bot.backtest.TypedClobParquetTestHelper.ClobRow;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class BacktestResourceTest {

    @Inject
    CollectorParquetDatasetService datasetService;

    @Test
    @DisplayName("GET / and /vendor/echarts.min.js should serve light-theme Web UI and local ECharts")
    void shouldServeLightThemeWebUiAndEcharts() {
        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("Polymarket Backtest Studio"))
                .body(containsString("<meta name=\"color-scheme\" content=\"light\">"));

        given()
                .when().get("/vendor/echarts.min.js")
                .then()
                .statusCode(200)
                .body(containsString("echarts"));
    }

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
    @DisplayName("GET /api/backtest/metadata should return all registered strategies and engine defaults")
    void shouldReturnMetadata() {
        given()
                .when().get("/api/backtest/metadata")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("defaultStrategy", equalTo("OraclePulseLockStrategy"))
                .body("defaultInitialCapital", equalTo(10000.0f))
                .body("strategies.size()", greaterThanOrEqualTo(9));
    }

    @Test
    @DisplayName("POST /api/backtest/run should validate strategy, strategyParams, dates, latency, and exchange delays fail-fast")
    void shouldValidateRunRequestsFailFast() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of("startDate", "2026-08-10", "endDate", "2026-08-01"))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(400)
                .body("error", containsString("startDate must not be after endDate"));

        given()
                .contentType(ContentType.JSON)
                .body(Map.of("strategyName", "NonExistentStrategy"))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(400)
                .body("error", containsString("Unknown strategy"));

        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "OraclePulseLockStrategy",
                        "strategyParams", Map.of("unknownParam", 123)))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(400)
                .body("error", containsString("Unknown strategy parameter"));

        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "UtterParameciumStrategy",
                        "latencyFactor", -1.0))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(400)
                .body("error", containsString("latencyFactor must be non-negative"));

        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "UtterParameciumStrategy",
                        "binanceDelayMs", -5))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(400)
                .body("error", containsString("binanceDelayMs must be non-negative"));
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
    @DisplayName("Full lifecycle: run backtest asynchronously, persist to DuckDB, compare DAY & MARKET curves, rename, query trades, and delete")
    void shouldExecutePersistCompareRenameAndDeleteBacktest() throws Exception {
        long tStart = 1785542400L; // 2026-08-01 00:00:00 UTC
        Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
        Path twapFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");
        Files.createDirectories(pmDir);
        Files.createDirectories(twapFile.getParent());

        Path pmFile = pmDir.resolve("btc-updown-15m-" + tStart + ".parquet");
        if (!Files.exists(pmFile) || !Files.exists(twapFile)) {
            try (Connection conn = datasetService.getDuckDbSessionManager().openSession();
                 Statement stmt = conn.createStatement()) {
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile, List.of(
                        ClobRow.discovered(tStart * 1000L - 2000L, "101", "102"),
                        ClobRow.bookLevel(tStart * 1000L + 1000L, tStart * 1000L + 990L, 8, true, true, 4900, 300.0),
                        ClobRow.bookLevel(tStart * 1000L + 1000L, tStart * 1000L + 990L, 8, true, false, 5100, 300.0)
                ));

                String normTwap = twapFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (tStart * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (tStart * 1000L) + "::UBIGINT), " +
                        "(" + ((tStart + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60050.0::DOUBLE, '60050.0'::VARCHAR, 60::INT, " + ((tStart + 900L) * 1000L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");
            }
        }

        var submitResp = given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "label", "Integration Test Run",
                        "strategyName", "UtterParameciumStrategy",
                        "symbol", "BTCUSDT",
                        "startDate", "2026-08-01",
                        "endDate", "2026-08-01",
                        "initialCapital", 5000.0,
                        "latencyFactor", 1.0,
                        "strategyParams", Map.of("minEdgeMc", 25)
                ))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(202)
                .contentType(ContentType.JSON)
                .body("jobId", notNullValue())
                .body("runId", notNullValue())
                .body("label", equalTo("Integration Test Run"))
                .body("strategyName", equalTo("UtterParameciumStrategy"))
                .body("symbol", equalTo("BTCUSDT"))
                .extract();

        String jobId = submitResp.path("jobId");
        String runId = submitResp.path("runId");
        assertThat(jobId).isNotBlank();
        assertThat(runId).isNotBlank();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            String status = given()
                    .when().get("/api/backtest/run/" + jobId)
                    .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .extract().path("status");
            assertThat(status).isEqualTo("COMPLETED");
        });

        // Re-running identical config without forceNewRun merges into existing runId & skips stored days
        var mergedSubmit = given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "UtterParameciumStrategy",
                        "symbol", "BTCUSDT",
                        "startDate", "2026-08-01",
                        "endDate", "2026-08-01",
                        "initialCapital", 5000.0,
                        "latencyFactor", 1.0,
                        "strategyParams", Map.of("minEdgeMc", 25)
                ))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(202)
                .extract();
        String mergedJobId = mergedSubmit.path("jobId");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var poll = given()
                    .when().get("/api/backtest/run/" + mergedJobId)
                    .then()
                    .statusCode(200)
                    .extract();
            assertThat(poll.<String>path("status")).isEqualTo("COMPLETED");
            assertThat(poll.<String>path("runId")).isEqualTo(runId);
            assertThat(poll.<Boolean>path("mergedIntoExisting")).isTrue();
            assertThat(poll.<Integer>path("skippedDays")).isGreaterThanOrEqualTo(1);
        });

        // Explicit forceNewRun=true creates a distinct runId
        var forcedSubmit = given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "UtterParameciumStrategy",
                        "symbol", "BTCUSDT",
                        "startDate", "2026-08-01",
                        "endDate", "2026-08-01",
                        "initialCapital", 5000.0,
                        "latencyFactor", 1.0,
                        "strategyParams", Map.of("minEdgeMc", 25),
                        "forceNewRun", true
                ))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(202)
                .extract();
        String forcedJobId = forcedSubmit.path("jobId");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var poll = given()
                    .when().get("/api/backtest/run/" + forcedJobId)
                    .then()
                    .statusCode(200)
                    .extract();
            assertThat(poll.<String>path("status")).isEqualTo("COMPLETED");
            assertThat(poll.<Boolean>path("mergedIntoExisting")).isFalse();
            assertThat(poll.<String>path("runId")).isNotEqualTo(runId);
        });
        String forcedRunId = given().when().get("/api/backtest/run/" + forcedJobId).then().extract().path("runId");

        given()
                .when().get("/api/backtest/jobs")
                .then()
                .statusCode(200)
                .body("size()", greaterThanOrEqualTo(1));

        given()
                .when().get("/api/backtest/runs/" + runId)
                .then()
                .statusCode(200)
                .body("summary.runId", equalTo(runId))
                .body("summary.label", equalTo("Integration Test Run"))
                .body("config.initialCapital", equalTo(5000.0f))
                .body("config.strategyOverrides.minEdgeMc", equalTo(25));

        given()
                .contentType(ContentType.JSON)
                .body(Map.of("runIds", List.of(runId), "granularity", "MARKET"))
                .when().post("/api/backtest/compare")
                .then()
                .statusCode(200)
                .body("granularity", equalTo("MARKET"))
                .body("series.size()", equalTo(1))
                .body("series[0].points.size()", equalTo(1));

        given()
                .contentType(ContentType.JSON)
                .body(Map.of("label", "Renamed Integration Run"))
                .when().patch("/api/backtest/runs/" + runId)
                .then()
                .statusCode(200)
                .body("label", equalTo("Renamed Integration Run"));

        given()
                .when().get("/api/backtest/runs/" + runId + "/trades")
                .then()
                .statusCode(200)
                .body("runId", equalTo(runId));

        given()
                .when().delete("/api/backtest/runs/" + forcedRunId)
                .then()
                .statusCode(204);

        given()
                .when().delete("/api/backtest/runs/" + runId)
                .then()
                .statusCode(204);

        given()
                .when().get("/api/backtest/runs/" + runId)
                .then()
                .statusCode(404);
    }
}
