package cz.polymarket.bot.web;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
class BtcUsdResourceTest {

    @Test
    void shouldRenderBtcUsdWebConsole() {
        given()
                .when().get("/btc-usd")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("BTC-USD TWAP Console"))
                .body(containsString("id=\"timeframe-select\""))
                .body(containsString("data-y-range=\"30\""))
                .body(containsString("data-y-range=\"50\""))
                .body(containsString("data-x-range-minutes=\"5\""))
                .body(containsString("id=\"chart-container\""))
                .body(containsString("id=\"chart-legend\""))
                .body(containsString("id=\"target-badge\""))
                .body(containsString("lightweight-charts"))
                .body(containsString("/css/btc-usd.css"))
                .body(containsString("/js/btc-usd.js"));
    }

    @Test
    void shouldServeStaticCssAndJs() {
        given()
                .when().get("/css/btc-usd.css")
                .then()
                .statusCode(200);

        given()
                .when().get("/js/btc-usd.js")
                .then()
                .statusCode(200);
    }

    @Test
    void shouldReturnCurrentCandleDataJsonForFiveMinutes() {
        given()
                .queryParam("timeframe", "5m")
                .when().get("/api/twap")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("timeframe", equalTo("5m"))
                .body("candleStart", notNullValue())
                .body("candleEnd", notNullValue())
                .body("openPrice", notNullValue())
                .body("points", notNullValue());
    }

    @Test
    void shouldReturnCurrentCandleDataJsonForFifteenMinutes() {
        given()
                .queryParam("timeframe", "15m")
                .when().get("/api/twap")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("timeframe", equalTo("15m"))
                .body("candleStart", notNullValue())
                .body("candleEnd", notNullValue())
                .body("openPrice", notNullValue());
    }

    @Test
    void shouldReturn400ForInvalidTimeframe() {
        given()
                .queryParam("timeframe", "1h")
                .when().get("/api/twap")
                .then()
                .statusCode(400)
                .body("error", containsString("Unsupported timeframe"));
    }
}
