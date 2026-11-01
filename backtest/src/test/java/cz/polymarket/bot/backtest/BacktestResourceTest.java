package cz.polymarket.bot.backtest;

import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
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
    CollectorParquetDatasetService datasetService;

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
    @DisplayName("POST /api/backtest/run should return 400 Bad Request when latencyFactor is negative")
    void shouldReturn400WhenLatencyFactorNegative() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "TWAPArbitrageStrategy",
                        "latencyFactor", -1.0
                ))
                .when().post("/api/backtest/run")
                .then()
                .statusCode(400)
                .contentType(ContentType.JSON)
                .body("error", containsString("latencyFactor must be non-negative"));
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
    void shouldExecuteBacktestAsynchronously() throws Exception {
        long tStart = 1785542400L; // 2026-08-01 00:00:00 UTC
        Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
        Path twapFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");
        Files.createDirectories(pmDir);
        Files.createDirectories(twapFile.getParent());

        Path pmFile = pmDir.resolve("btc-updown-15m-" + tStart + ".parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            String normPm = pmFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L - 2000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, '0x1'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"clobTokenIds\":\"[\\\"101\\\", \\\"102\\\"]\"}'::JSON), " +
                    "(" + (tStart * 1000L + 1000L) + "::UBIGINT, 'clob_ws'::VARCHAR, 'book'::VARCHAR, '0x1'::VARCHAR, '101'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"asset_id\":\"101\",\"timestamp\":\"" + (tStart * 1000L + 990L) + "\",\"bids\":[{\"price\":\"0.49\",\"size\":\"300.0\"}],\"asks\":[{\"price\":\"0.51\",\"size\":\"300.0\"}]}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) TO '" + normPm + "' (FORMAT PARQUET)");

            String normTwap = twapFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (tStart * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (tStart * 1000L) + "::UBIGINT), " +
                    "(" + ((tStart + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60050.0::DOUBLE, '60050.0'::VARCHAR, 60::INT, " + ((tStart + 900L) * 1000L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");
        }

        String jobId = given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "TWAPArbitrageStrategy",
                        "symbol", "BTCUSDT",
                        "startDate", "2026-08-01",
                        "endDate", "2026-08-01",
                        "latencyFactor", 1.0
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
