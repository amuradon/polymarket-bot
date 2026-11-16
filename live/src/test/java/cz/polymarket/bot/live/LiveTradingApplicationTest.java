package cz.polymarket.bot.live;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

@QuarkusTest
class LiveTradingApplicationTest {

    @Test
    void shouldExposeLiveTradingEngineDefaultStatus() {
        given()
                .when().get("/api/live/status")
                .then()
                .statusCode(200)
                .contentType(ContentType.JSON)
                .body("strategyName", equalTo("UtterParameciumStrategy"))
                .body("status", equalTo("LIVE"))
                .body("orderType", equalTo("FAK"));
    }

    @Test
    void shouldDefaultProductionLogLevelToInfoWithTraceMinLevel() {
        String rootLevel = ConfigProvider.getConfig().getValue("quarkus.log.level", String.class);
        String minLevel = ConfigProvider.getConfig().getValue("quarkus.log.min-level", String.class);
        String botCategoryLevel = ConfigProvider.getConfig()
                .getValue("quarkus.log.category.\"cz.polymarket.bot\".level", String.class);

        assertThat(rootLevel).isEqualTo("INFO");
        assertThat(minLevel).isEqualTo("TRACE");
        assertThat(botCategoryLevel).isEqualTo("INFO");
    }

    @Test
    void shouldConfigureRecognizedSecretsManagerAwsRegionAndNativeAccessManifestAttribute() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("src/main/resources/application.properties"), StandardCharsets.UTF_8);

        assertThat(lines)
                .contains("%prod.quarkus.secretsmanager.aws.region=eu-west-1")
                .contains("quarkus.package.jar.manifest.attributes.\"Enable-Native-Access\"=ALL-UNNAMED")
                .noneMatch(line -> line.startsWith("%prod.quarkus.secretsmanager.region="));
    }
}
