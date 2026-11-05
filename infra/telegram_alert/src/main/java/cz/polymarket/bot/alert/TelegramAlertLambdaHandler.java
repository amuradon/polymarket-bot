package cz.polymarket.bot.alert;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.alert.model.CloudWatchLogsPayload;
import cz.polymarket.bot.alert.model.LogEvent;
import cz.polymarket.bot.alert.model.TelegramCredentials;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * AWS Lambda handler triggered by CloudWatch Logs subscription filter to dispatch
 * warning, error, fatal, and Java exception/error notifications to Telegram.
 */
public class TelegramAlertLambdaHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final CloudWatchLogsPayloadParser payloadParser;
    private final LogEventMatcher logEventMatcher;
    private final TelegramAlertFormatter alertFormatter;
    private final TelegramSecretsResolver secretsResolver;
    private final TelegramNotifier telegramNotifier;

    /**
     * Default constructor invoked by the AWS Lambda runtime.
     */
    public TelegramAlertLambdaHandler() {
        this(
                new CloudWatchLogsPayloadParser(),
                new LogEventMatcher(),
                new TelegramAlertFormatter(),
                new TelegramSecretsResolver(),
                new TelegramNotifier()
        );
    }

    /**
     * Parameterized constructor for dependency injection and synthetic testing.
     *
     * @param payloadParser   parser for CloudWatch Base64/GZIP payloads
     * @param logEventMatcher filter identifying warning/error/exception lines
     * @param alertFormatter  formatter producing Telegram HTML markup
     * @param secretsResolver resolver providing Telegram bot credentials
     * @param telegramNotifier client dispatching alerts to Telegram Bot API
     */
    public TelegramAlertLambdaHandler(
            CloudWatchLogsPayloadParser payloadParser,
            LogEventMatcher logEventMatcher,
            TelegramAlertFormatter alertFormatter,
            TelegramSecretsResolver secretsResolver,
            TelegramNotifier telegramNotifier) {
        this.payloadParser = Objects.requireNonNull(payloadParser, "payloadParser must not be null");
        this.logEventMatcher = Objects.requireNonNull(logEventMatcher, "logEventMatcher must not be null");
        this.alertFormatter = Objects.requireNonNull(alertFormatter, "alertFormatter must not be null");
        this.secretsResolver = Objects.requireNonNull(secretsResolver, "secretsResolver must not be null");
        this.telegramNotifier = Objects.requireNonNull(telegramNotifier, "telegramNotifier must not be null");
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        if (event == null || !event.containsKey("awslogs")) {
            System.err.println("[WARN] Received event without 'awslogs.data'; ignoring");
            return createResponse(200, "{\"status\":\"ignored\",\"reason\":\"No awslogs payload\"}");
        }

        Object awslogsObj = event.get("awslogs");
        if (!(awslogsObj instanceof Map<?, ?> awslogsMap)) {
            System.err.println("[WARN] 'awslogs' property is not a map; ignoring");
            return createResponse(200, "{\"status\":\"ignored\",\"reason\":\"Invalid awslogs structure\"}");
        }

        Object dataObj = awslogsMap.get("data");
        if (!(dataObj instanceof String base64Data) || base64Data.isBlank()) {
            System.err.println("[WARN] 'awslogs.data' is missing or empty; ignoring");
            return createResponse(200, "{\"status\":\"ignored\",\"reason\":\"Empty awslogs data\"}");
        }

        CloudWatchLogsPayload payload;
        try {
            payload = payloadParser.parse(base64Data);
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to decompress/parse CloudWatch Logs payload: " + e.getMessage());
            return createResponse(400, "{\"status\":\"error\",\"reason\":\"Invalid payload: " + e.getMessage() + "\"}");
        }

        if (!"DATA_MESSAGE".equals(payload.messageType())) {
            System.out.println("[INFO] Skipping non-data CloudWatch message type: " + payload.messageType());
            return createResponse(200, "{\"status\":\"skipped\",\"messageType\":\"" + payload.messageType() + "\"}");
        }

        TelegramCredentials credentials;
        try {
            credentials = secretsResolver.resolveCredentials();
        } catch (Exception e) {
            System.err.println("[ERROR] Cannot resolve Telegram credentials: " + e.getMessage());
            return createResponse(500, "{\"status\":\"error\",\"reason\":\"Failed to resolve Telegram credentials\"}");
        }

        int eventsProcessed = 0;
        int alertsSent = 0;

        for (LogEvent logEvent : payload.logEvents()) {
            eventsProcessed++;
            String message = logEvent.message();

            if (logEventMatcher.shouldAlert(message)) {
                String formattedAlert = alertFormatter.format(
                        payload.logGroup(),
                        payload.logStream(),
                        logEvent.timestamp(),
                        message
                );

                boolean delivered = telegramNotifier.sendMessage(credentials, formattedAlert);
                if (delivered) {
                    alertsSent++;
                }
            }
        }

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("events_processed", eventsProcessed);
        summary.put("alerts_sent", alertsSent);

        String jsonBody;
        try {
            jsonBody = OBJECT_MAPPER.writeValueAsString(summary);
        } catch (Exception e) {
            jsonBody = "{\"events_processed\":" + eventsProcessed + ",\"alerts_sent\":" + alertsSent + "}";
        }

        return createResponse(200, jsonBody);
    }

    private static Map<String, Object> createResponse(int statusCode, String body) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("statusCode", statusCode);
        response.put("body", body);
        return response;
    }
}
