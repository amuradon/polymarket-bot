package cz.polymarket.bot.live;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

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
}
