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

    @Test
    @DisplayName("Should execute backtest for TWAPArbitrageStrategy and return 200 with metrics")
    void shouldExecuteBacktestSuccessfully() {
        File datasetFile = new File("D:/Polymarket/btc_nextCandle/unified_market_data.parquet");
        if (!datasetFile.exists()) {
            return;
        }

        File sampleFile = new File("target/test_sample.parquet");
        if (!sampleFile.exists()) {
            sampleFile.getParentFile().mkdirs();
            try (var conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                 var stmt = conn.createStatement()) {
                stmt.execute("COPY (SELECT * FROM read_parquet('" + datasetFile.getAbsolutePath().replace('\\', '/') + "') LIMIT 5) TO '" + sampleFile.getAbsolutePath().replace('\\', '/') + "' (FORMAT PARQUET)");
            } catch (Exception ignored) {
            }
        }

        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "strategyName", "TWAPArbitrageStrategy",
                        "datasetPath", sampleFile.getAbsolutePath()
                ))
                .when().post("/backtest/run")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("strategyName", equalTo("TWAPArbitrageStrategy"))
                .body("totalMarkets", equalTo(5))
                .body("jsonFilePath", notNullValue());
    }
}
