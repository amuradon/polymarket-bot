package cz.polymarket.bot.paper;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
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

    @Test
    void shouldConfigureNativeAccessManifestAttributeForPaperJar() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("src/main/resources/application.properties"), StandardCharsets.UTF_8);

        assertThat(lines)
                .contains("quarkus.package.jar.manifest.attributes.\"Enable-Native-Access\"=ALL-UNNAMED");
    }
}
