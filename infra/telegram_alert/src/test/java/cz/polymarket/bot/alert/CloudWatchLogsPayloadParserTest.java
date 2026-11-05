package cz.polymarket.bot.alert;

import cz.polymarket.bot.alert.model.CloudWatchLogsPayload;
import cz.polymarket.bot.alert.model.LogEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Synthetic unit tests for {@link CloudWatchLogsPayloadParser} decoding, decompressing,
 * and deserializing CloudWatch Logs subscription filter payloads.
 */
class CloudWatchLogsPayloadParserTest {

    private final CloudWatchLogsPayloadParser parser = new CloudWatchLogsPayloadParser();

    private static String gzipAndBase64(String json) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(json.getBytes(StandardCharsets.UTF_8));
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    @Test
    @DisplayName("Should successfully parse synthetic CloudWatch DATA_MESSAGE payload")
    void shouldParseValidDataMessage() throws Exception {
        String json = """
                {
                  "messageType": "DATA_MESSAGE",
                  "owner": "123456789012",
                  "logGroup": "/polymarket-bot/live-trading",
                  "logStream": "i-0abcdef123456/polymarket-live",
                  "subscriptionFilters": ["TelegramAlertSubscriptionFilter"],
                  "logEvents": [
                    {
                      "id": "event-1",
                      "timestamp": 1762336800000,
                      "message": "2026-11-05 10:00:00 ERROR [cz.polymarket.bot] Connection dropped"
                    },
                    {
                      "id": "event-2",
                      "timestamp": 1762336801000,
                      "message": "2026-11-05 10:00:01 WARN [cz.polymarket.bot] Reconnecting in 5s"
                    }
                  ]
                }
                """;
        String encoded = gzipAndBase64(json);

        CloudWatchLogsPayload payload = parser.parse(encoded);

        assertThat(payload.messageType()).isEqualTo("DATA_MESSAGE");
        assertThat(payload.owner()).isEqualTo("123456789012");
        assertThat(payload.logGroup()).isEqualTo("/polymarket-bot/live-trading");
        assertThat(payload.logStream()).isEqualTo("i-0abcdef123456/polymarket-live");
        assertThat(payload.subscriptionFilters()).containsExactly("TelegramAlertSubscriptionFilter");
        assertThat(payload.logEvents()).hasSize(2);

        LogEvent event1 = payload.logEvents().get(0);
        assertThat(event1.id()).isEqualTo("event-1");
        assertThat(event1.timestamp()).isEqualTo(1762336800000L);
        assertThat(event1.message()).contains("Connection dropped");

        LogEvent event2 = payload.logEvents().get(1);
        assertThat(event2.id()).isEqualTo("event-2");
        assertThat(event2.timestamp()).isEqualTo(1762336801000L);
        assertThat(event2.message()).contains("Reconnecting in 5s");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException on null or blank base64 data")
    void shouldThrowOnNullOrBlankData() {
        assertThatThrownBy(() -> parser.parse(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parser.parse("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException on corrupt or non-gzip payload")
    void shouldThrowOnCorruptPayload() {
        String badBase64 = Base64.getEncoder().encodeToString("not-gzipped".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> parser.parse(badBase64))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to decompress or parse");
    }
}
