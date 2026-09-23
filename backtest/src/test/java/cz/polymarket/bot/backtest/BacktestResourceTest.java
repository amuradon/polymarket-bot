package cz.polymarket.bot.backtest;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

@QuarkusTest
class BacktestResourceTest {

    @Test
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
}
