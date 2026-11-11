package cz.polymarket.bot.alert;

import com.amazonaws.services.lambda.runtime.Context;
import cz.polymarket.bot.alert.model.TelegramCredentials;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Synthetic unit and integration tests for {@link TelegramAlertLambdaHandler}.
 */
class TelegramAlertLambdaHandlerTest {

    private static String gzipAndBase64(String json) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(json.getBytes(StandardCharsets.UTF_8));
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    @Test
    @DisplayName("Should return 200 with notice when event is missing awslogs payload")
    void shouldHandleMissingAwsLogs() {
        TelegramAlertLambdaHandler handler = new TelegramAlertLambdaHandler(
                new CloudWatchLogsPayloadParser(),
                new LogEventMatcher(),
                new TelegramAlertFormatter(),
                new TelegramSecretsResolver(id -> "{}", "sec"),
                new TelegramNotifier((url, body) -> new TelegramNotifier.TelegramHttpResponse(200, "{}"))
        );

        Map<String, Object> result = handler.handleRequest(Map.of(), null);

        assertThat(result.get("statusCode")).isEqualTo(200);
        assertThat(result.get("body").toString()).contains("No awslogs payload");
    }

    @Test
    @DisplayName("Should skip non DATA_MESSAGE message types")
    void shouldSkipNonDataMessages() throws Exception {
        String json = "{\"messageType\": \"CONTROL_MESSAGE\"}";
        String encoded = gzipAndBase64(json);

        TelegramAlertLambdaHandler handler = new TelegramAlertLambdaHandler(
                new CloudWatchLogsPayloadParser(),
                new LogEventMatcher(),
                new TelegramAlertFormatter(),
                new TelegramSecretsResolver(id -> "{}", "sec"),
                new TelegramNotifier((url, body) -> new TelegramNotifier.TelegramHttpResponse(200, "{}"))
        );

        Map<String, Object> event = Map.of("awslogs", Map.of("data", encoded));
        Map<String, Object> result = handler.handleRequest(event, null);

        assertThat(result.get("statusCode")).isEqualTo(200);
        assertThat(result.get("body").toString()).contains("CONTROL_MESSAGE");
    }

    @Test
    @DisplayName("Should process log events, filter errors/warnings, and dispatch alerts to Telegram")
    void shouldProcessLogEventsAndDispatchAlerts() throws Exception {
        String json = """
                {
                  "messageType": "DATA_MESSAGE",
                  "owner": "123456789012",
                  "logGroup": "/polymarket-bot/live-trading",
                  "logStream": "i-0abcdef123456/polymarket-live",
                  "subscriptionFilters": ["TelegramFilter"],
                  "logEvents": [
                    {
                      "id": "e1",
                      "timestamp": 1762336800000,
                      "message": "2026-11-05 10:00:00 INFO [cz.polymarket.bot] Normal tick processed"
                    },
                    {
                      "id": "e2",
                      "timestamp": 1762336801000,
                      "message": "2026-11-05 10:00:01 WARN [cz.polymarket.bot] High memory usage"
                    },
                    {
                      "id": "e3",
                      "timestamp": 1762336802000,
                      "message": "Uncaught java.lang.NullPointerException: order cannot be null"
                    }
                  ]
                }
                """;
        String encoded = gzipAndBase64(json);

        List<String> sentMessages = new ArrayList<>();
        TelegramNotifier.TelegramHttpTransport transport = (url, body) -> {
            sentMessages.add(body);
            return new TelegramNotifier.TelegramHttpResponse(200, "{\"ok\":true}");
        };

        String secretJson = "{\"bot_token\":\"test-bot-token\",\"chat_id\":\"-100123\"}";
        TelegramSecretsResolver secretsResolver = new TelegramSecretsResolver(id -> secretJson, "sec-id");
        TelegramNotifier notifier = new TelegramNotifier(transport);

        TelegramAlertLambdaHandler handler = new TelegramAlertLambdaHandler(
                new CloudWatchLogsPayloadParser(),
                new LogEventMatcher(),
                new TelegramAlertFormatter(),
                secretsResolver,
                notifier
        );

        Map<String, Object> event = Map.of("awslogs", Map.of("data", encoded));
        Map<String, Object> result = handler.handleRequest(event, null);

        assertThat(result.get("statusCode")).isEqualTo(200);
        assertThat(result.get("body").toString()).contains("\"events_processed\":3");
        assertThat(result.get("body").toString()).contains("\"alerts_sent\":2");

        assertThat(sentMessages).hasSize(2);
        assertThat(sentMessages.get(0)).contains("[WARNING: APPLICATION WARNING]");
        assertThat(sentMessages.get(1)).contains("[FATAL: UNCAUGHT EXCEPTION]");
    }
}
