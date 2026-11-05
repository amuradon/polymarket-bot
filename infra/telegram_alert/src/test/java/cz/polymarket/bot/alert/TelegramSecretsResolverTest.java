package cz.polymarket.bot.alert;

import cz.polymarket.bot.alert.model.TelegramCredentials;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Synthetic unit tests for {@link TelegramSecretsResolver} resolving and caching
 * credentials from AWS Secrets Manager JSON payloads.
 */
class TelegramSecretsResolverTest {

    @Test
    @DisplayName("Should successfully resolve credentials with standard keys (bot_token, chat_id)")
    void shouldResolveStandardKeys() {
        String secretJson = """
                {
                  "bot_token": "123456789:ABCdefGHIjklMNOpqrsTUVwxyz",
                  "chat_id": "-1001234567890"
                }
                """;

        TelegramSecretsResolver resolver = new TelegramSecretsResolver(id -> secretJson, "test-secret-id");
        TelegramCredentials creds = resolver.resolveCredentials();

        assertThat(creds.botToken()).isEqualTo("123456789:ABCdefGHIjklMNOpqrsTUVwxyz");
        assertThat(creds.chatId()).isEqualTo("-1001234567890");
    }

    @Test
    @DisplayName("Should resolve credentials with alternative keys (telegram_bot_token, telegram_chat_id)")
    void shouldResolveAlternativeKeys() {
        String secretJson = """
                {
                  "telegram_bot_token": "token-xyz",
                  "telegram_chat_id": "chat-999"
                }
                """;

        TelegramSecretsResolver resolver = new TelegramSecretsResolver(id -> secretJson, "test-secret-id");
        TelegramCredentials creds = resolver.resolveCredentials();

        assertThat(creds.botToken()).isEqualTo("token-xyz");
        assertThat(creds.chatId()).isEqualTo("chat-999");
    }

    @Test
    @DisplayName("Should cache resolved credentials and avoid repeated Secrets Manager API calls")
    void shouldCacheCredentialsAcrossCalls() {
        String secretJson = """
                {
                  "token": "cached-token",
                  "chatId": "cached-chat"
                }
                """;

        AtomicInteger callCount = new AtomicInteger(0);
        TelegramSecretsResolver.RawSecretFetcher fetcher = id -> {
            callCount.incrementAndGet();
            return secretJson;
        };

        TelegramSecretsResolver resolver = new TelegramSecretsResolver(fetcher, "test-secret-id");

        TelegramCredentials first = resolver.resolveCredentials();
        TelegramCredentials second = resolver.resolveCredentials();

        assertThat(first).isSameAs(second);
        assertThat(callCount.get()).isEqualTo(1);

        resolver.forceRefresh();
        TelegramCredentials third = resolver.resolveCredentials();
        assertThat(callCount.get()).isEqualTo(2);
        assertThat(third.botToken()).isEqualTo("cached-token");
    }

    @Test
    @DisplayName("Should throw IllegalStateException if mandatory keys are missing in secret payload")
    void shouldThrowIfKeysMissing() {
        String badJson = "{\"unknown_key\": \"value\"}";
        TelegramSecretsResolver resolver = new TelegramSecretsResolver(id -> badJson, "test-secret-id");

        assertThatThrownBy(resolver::resolveCredentials)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing mandatory Telegram bot_token key");
    }

    @Test
    @DisplayName("Should throw IllegalStateException when secret retrieval throws an exception")
    void shouldThrowWhenSecretNotFound() {
        TelegramSecretsResolver.RawSecretFetcher fetcher = id -> {
            throw new RuntimeException("Secret missing-secret not found");
        };

        TelegramSecretsResolver resolver = new TelegramSecretsResolver(fetcher, "missing-secret");
        assertThatThrownBy(resolver::resolveCredentials)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to retrieve secret 'missing-secret'");
    }
}
