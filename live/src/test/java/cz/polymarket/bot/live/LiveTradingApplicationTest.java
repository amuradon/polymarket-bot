package cz.polymarket.bot.live;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@QuarkusTest
class LiveTradingApplicationTest {

    @Test
    void shouldServeBtcUsdConsoleInLiveMode() {
        given()
                .when().get("/btc-usd")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("BTC-USD TWAP Console"));
    }

    @Test
    void shouldExposeLiveTradingEngineDefaultStatus() {
        given()
                .when().get("/api/live/status")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("strategyName", equalTo("UtterParameciumStrategy"))
                .body("status", equalTo("DRY_RUN"))
                .body("dryRun", equalTo(true))
                .body("orderType", equalTo("FAK"));
    }
}
