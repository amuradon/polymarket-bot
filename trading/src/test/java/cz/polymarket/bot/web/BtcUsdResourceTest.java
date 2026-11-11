package cz.polymarket.bot.web;

import cz.polymarket.bot.domain.CandleTwapState;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.exchange.HistoricalDataReconstructor;
import cz.polymarket.bot.service.TwapEngine;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
class BtcUsdResourceTest {

    @Inject
    TwapEngine twapEngine;

    @BeforeEach
    void setUpSyntheticHistory() {
        twapEngine.initialize(Instant.parse("2026-09-03T14:02:00Z"));
    }

    @Alternative
    @Priority(1)
    @ApplicationScoped
    public static class SyntheticHistoricalDataReconstructor extends HistoricalDataReconstructor {

        public SyntheticHistoricalDataReconstructor() {
            super(null, null, null);
        }

        @Override
        public CandleTwapState reconstructCandle(Timeframe timeframe, Instant now) {
            return new CandleTwapState(
                    timeframe,
                    1788444000L,
                    timeframe == Timeframe.FIFTEEN_MINUTES ? 1788444900L : 1788444300L,
                    new BigDecimal("78823.52"),
                    List.of(new TwapPoint(1788444000L, new BigDecimal("78823.52"), new BigDecimal("78825.00"))));
        }
    }

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
