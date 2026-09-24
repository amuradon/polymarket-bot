package cz.polymarket.bot.backtest.web;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@QuarkusTest
class DataDownloadResourceTest {

    @Test
    void shouldAcceptValidDownloadRequest() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "symbol", "BTCUSDT",
                        "start", "2026-08-01",
                        "end", "2026-08-02"
                ))
                .when()
                .post("/api/1/data/download")
                .then()
                .statusCode(202)
                .body("jobId", not(emptyOrNullString()))
                .body("symbol", equalTo("BTCUSDT"))
                .body("status", not(emptyOrNullString()));
    }

    @Test
    void shouldRejectBlankSymbol() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "symbol", "",
                        "start", "2026-08-01",
                        "end", "2026-08-02"
                ))
                .when()
                .post("/api/1/data/download")
                .then()
                .statusCode(400)
                .body("error", equalTo("symbol must not be blank"));
    }

    @Test
    void shouldRejectStartAfterEnd() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "symbol", "BTCUSDT",
                        "start", "2026-08-05",
                        "end", "2026-08-01"
                ))
                .when()
                .post("/api/1/data/download")
                .then()
                .statusCode(400);
    }

    @Test
    void shouldReturn404ForUnknownJob() {
        given()
                .when()
                .get("/api/1/data/download/unknown-job-id")
                .then()
                .statusCode(404);
    }

    @Test
    void shouldGetJobStatus() {
        String jobId = given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "symbol", "ETHUSDT",
                        "start", "2026-08-01",
                        "end", "2026-08-01"
                ))
                .when()
                .post("/api/1/data/download")
                .then()
                .statusCode(202)
                .extract()
                .path("jobId");

        given()
                .when()
                .get("/api/1/data/download/" + jobId)
                .then()
                .statusCode(200)
                .body("jobId", equalTo(jobId))
                .body("symbol", equalTo("ETHUSDT"));
    }
}
