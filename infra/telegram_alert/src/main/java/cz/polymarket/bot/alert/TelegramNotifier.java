package cz.polymarket.bot.alert;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.alert.model.TelegramCredentials;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Dispatches formatted alert messages to the Telegram Bot API.
 */
public class TelegramNotifier {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Response representation for Telegram Bot API HTTP invocations.
     *
     * @param statusCode HTTP response status code
     * @param body       response body string
     */
    public record TelegramHttpResponse(int statusCode, String body) {}

    /**
     * Strategy interface for executing HTTP POST requests against the Telegram API.
     */
    @FunctionalInterface
    public interface TelegramHttpTransport {
        /**
         * Dispatches an HTTP POST request.
         *
         * @param apiUrl      full Telegram Bot API URL
         * @param jsonPayload serialized JSON payload
         * @return response containing HTTP status code and body
         * @throws Exception on network or serialization failure
         */
        TelegramHttpResponse post(String apiUrl, String jsonPayload) throws Exception;
    }

    private final TelegramHttpTransport transport;

    /**
     * Constructs a notifier using standard {@link HttpClient}.
     */
    public TelegramNotifier() {
        this(createDefaultTransport());
    }

    /**
     * Constructs a notifier with a custom {@link TelegramHttpTransport}.
     *
     * @param transport HTTP transport implementation
     */
    public TelegramNotifier(TelegramHttpTransport transport) {
        this.transport = Objects.requireNonNull(transport, "transport must not be null");
    }

    /**
     * Sends an HTML message to the specified Telegram chat.
     *
     * @param credentials validated Telegram bot credentials
     * @param messageHtml HTML-formatted message text
     * @return {@code true} if message was accepted by Telegram (HTTP 2xx), {@code false} otherwise
     */
    public boolean sendMessage(TelegramCredentials credentials, String messageHtml) {
        if (credentials == null || messageHtml == null || messageHtml.isBlank()) {
            System.err.println("[WARN] Missing credentials or message body; skipping notification");
            return false;
        }

        try {
            String apiUrl = "https://api.telegram.org/bot" + credentials.botToken() + "/sendMessage";

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("chat_id", credentials.chatId());
            payload.put("text", messageHtml);
            payload.put("parse_mode", "HTML");
            payload.put("disable_web_page_preview", true);

            String requestBody = OBJECT_MAPPER.writeValueAsString(payload);

            TelegramHttpResponse response = transport.post(apiUrl, requestBody);
            int statusCode = response.statusCode();
            if (statusCode >= 200 && statusCode < 300) {
                System.out.println("[INFO] Successfully delivered Telegram alert to chat_id '" + credentials.chatId() + "'");
                return true;
            }

            System.err.println("[ERROR] Telegram API returned non-200 HTTP status (" + statusCode + "): " + response.body());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[ERROR] Thread interrupted while sending Telegram alert: " + e.getMessage());
            return false;
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to send Telegram alert: " + e.getMessage());
            return false;
        }
    }

    private static TelegramHttpTransport createDefaultTransport() {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(DEFAULT_TIMEOUT)
                .build();

        return (url, json) -> {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(DEFAULT_TIMEOUT)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new TelegramHttpResponse(resp.statusCode(), resp.body());
        };
    }
}
