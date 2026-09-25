package cz.polymarket.bot.backtest.openapi;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
class SwaggerUiOpenApiTest {

    @Test
    @DisplayName("GET /q/openapi should return OpenAPI 3.x schema containing backtest and data ingestion endpoints")
    void shouldServeOpenApiDocument() {
        given()
                .queryParam("format", "json")
                .when().get("/q/openapi")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("openapi", startsWith("3."))
                .body("info.title", equalTo("Polymarket Bot - Backtest & Data Ingestion API"))
                .body("info.version", equalTo("1.0.0"))
                .body("paths", hasKey("/api/1/data/download"))
                .body("paths", hasKey("/api/1/data/download/{jobId}"))
                .body("paths", hasKey("/backtest"));
    }

    @Test
    @DisplayName("GET /q/swagger-ui should serve Swagger UI HTML page")
    void shouldServeSwaggerUi() {
        given()
                .when().get("/q/swagger-ui/")
                .then()
                .statusCode(200)
                .contentType(ContentType.HTML)
                .body(containsString("swagger-ui"));
    }

    @Test
    @DisplayName("GET /swagger-ui should redirect to /q/swagger-ui")
    void shouldRedirectRootSwaggerUi() {
        given()
                .redirects().follow(false)
                .when().get("/swagger-ui")
                .then()
                .statusCode(anyOf(equalTo(303), equalTo(307), equalTo(302), equalTo(301)))
                .header("Location", containsString("/q/swagger-ui"));
    }
}
