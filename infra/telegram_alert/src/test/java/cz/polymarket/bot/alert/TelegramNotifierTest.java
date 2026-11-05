package cz.polymarket.bot.alert;

import cz.polymarket.bot.alert.model.TelegramCredentials;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Synthetic unit tests for {@link TelegramNotifier} verifying HTTP POST generation and status handling.
 */
class TelegramNotifierTest {

    @Test
    @DisplayName("Should successfully send message when Telegram API returns 200 OK")
    void shouldSendMessageSuccessfully() {
        TelegramNotifier.TelegramHttpTransport transport = (url, json) -> {
            assertThat(url).contains("fake-bot-token");
            assertThat(json).contains("\"chat_id\":\"12345678\"");
            assertThat(json).contains("\"text\":\"<b>Alert</b>\"");
            return new TelegramNotifier.TelegramHttpResponse(200, "{\"ok\":true}");
        };

        TelegramNotifier notifier = new TelegramNotifier(transport);
        TelegramCredentials creds = new TelegramCredentials("fake-bot-token", "12345678");

        boolean result = notifier.sendMessage(creds, "<b>Alert</b>");

        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Should return false when Telegram API returns non-200 HTTP status")
    void shouldReturnFalseOnNon200Status() {
        TelegramNotifier.TelegramHttpTransport transport = (url, json) ->
                new TelegramNotifier.TelegramHttpResponse(400, "{\"ok\":false,\"description\":\"Bad Request: chat not found\"}");

        TelegramNotifier notifier = new TelegramNotifier(transport);
        TelegramCredentials creds = new TelegramCredentials("fake-bot-token", "12345678");

        boolean result = notifier.sendMessage(creds, "<b>Alert</b>");

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("Should return false and handle network exception gracefully")
    void shouldReturnFalseOnNetworkException() {
        TelegramNotifier.TelegramHttpTransport transport = (url, json) -> {
            throw new IOException("Connection reset by peer");
        };

        TelegramNotifier notifier = new TelegramNotifier(transport);
        TelegramCredentials creds = new TelegramCredentials("fake-bot-token", "12345678");

        boolean result = notifier.sendMessage(creds, "<b>Alert</b>");

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("Should return false without network request if credentials or message are empty")
    void shouldReturnFalseIfInputInvalid() {
        AtomicBoolean called = new AtomicBoolean(false);
        TelegramNotifier.TelegramHttpTransport transport = (url, json) -> {
            called.set(true);
            return new TelegramNotifier.TelegramHttpResponse(200, "{}");
        };
        TelegramNotifier notifier = new TelegramNotifier(transport);

        assertThat(notifier.sendMessage(null, "text")).isFalse();
        assertThat(notifier.sendMessage(new TelegramCredentials("token", "chat"), null)).isFalse();
        assertThat(notifier.sendMessage(new TelegramCredentials("token", "chat"), "   ")).isFalse();
        assertThat(called.get()).isFalse();
    }
}
