package cz.polymarket.bot.paper;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

@QuarkusTest
class PaperTradingApplicationTest {

    @Test
    void shouldServePaperDashboardInPaperMode() {
        given()
                .when().get("/")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("Polymarket Bot :: Paper Trading"));
    }
}
