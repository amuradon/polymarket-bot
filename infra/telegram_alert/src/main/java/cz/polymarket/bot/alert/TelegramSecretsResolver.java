package cz.polymarket.bot.alert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.alert.model.TelegramCredentials;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

import java.util.Objects;

/**
 * Resolves and caches Telegram bot authentication credentials from AWS Secrets Manager.
 */
public class TelegramSecretsResolver {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_SECRET_ID = "polymarket-sniper/telegram-credentials";
    private static final String DEFAULT_REGION = "eu-west-1";

    /**
     * Strategy interface for retrieving raw secret strings from Secrets Manager or synthetic test fixtures.
     */
    @FunctionalInterface
    public interface RawSecretFetcher {
        /**
         * Fetches the raw secret string payload.
         *
         * @param secretId AWS Secrets Manager secret identifier
         * @return raw secret string
         */
        String fetchSecretString(String secretId);
    }

    private final RawSecretFetcher secretFetcher;
    private final String secretId;

    private volatile TelegramCredentials cachedCredentials;

    /**
     * Constructs a resolver using default AWS Secrets Manager client and environment variables:
     * <ul>
     *   <li>{@code TELEGRAM_SECRET_ID}: secret identifier (default {@code polymarket-sniper/telegram-credentials})</li>
     *   <li>{@code AWS_REGION}: AWS region (default {@code eu-west-1})</li>
     * </ul>
     */
    public TelegramSecretsResolver() {
        this(createDefaultFetcher(), resolveSecretId());
    }

    /**
     * Constructs a resolver with a custom {@link RawSecretFetcher} and secret ID.
     *
     * @param secretFetcher secret string provider
     * @param secretId      secret identifier
     */
    public TelegramSecretsResolver(RawSecretFetcher secretFetcher, String secretId) {
        this.secretFetcher = Objects.requireNonNull(secretFetcher, "secretFetcher must not be null");
        this.secretId = (secretId != null && !secretId.isBlank()) ? secretId.trim() : DEFAULT_SECRET_ID;
    }

    /**
     * Resolves the Telegram credentials, caching them across warm invocations.
     *
     * @return validated {@link TelegramCredentials}
     * @throws IllegalStateException if the secret cannot be retrieved or contains invalid JSON/keys
     */
    public TelegramCredentials resolveCredentials() {
        TelegramCredentials creds = this.cachedCredentials;
        if (creds != null) {
            return creds;
        }

        synchronized (this) {
            if (this.cachedCredentials != null) {
                return this.cachedCredentials;
            }

            TelegramCredentials resolved = fetchFromSecretsManager();
            this.cachedCredentials = resolved;
            return resolved;
        }
    }

    /**
     * Clears cached credentials to force resolution on the next call.
     */
    public synchronized void forceRefresh() {
        this.cachedCredentials = null;
    }

    private TelegramCredentials fetchFromSecretsManager() {
        String secretString;
        try {
            secretString = secretFetcher.fetchSecretString(this.secretId);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to retrieve secret '" + this.secretId + "' from AWS Secrets Manager: " + e.getMessage(), e);
        }

        if (secretString == null || secretString.isBlank()) {
            throw new IllegalStateException("Secret '" + this.secretId + "' does not contain a valid SecretString payload");
        }

        try {
            JsonNode json = OBJECT_MAPPER.readTree(secretString);

            String botToken = extractKey(json, "bot_token", "telegram_bot_token", "token");
            String chatId = extractKey(json, "chat_id", "telegram_chat_id", "chatId");

            if (botToken.isBlank()) {
                throw new IllegalStateException("Secret '" + this.secretId + "' is missing mandatory Telegram bot_token key");
            }
            if (chatId.isBlank()) {
                throw new IllegalStateException("Secret '" + this.secretId + "' is missing mandatory Telegram chat_id key");
            }

            return new TelegramCredentials(botToken, chatId);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse JSON secret payload for '" + this.secretId + "': " + e.getMessage(), e);
        }
    }

    private static String extractKey(JsonNode json, String... keys) {
        for (String key : keys) {
            JsonNode node = json.path(key);
            if (!node.isMissingNode() && !node.isNull()) {
                String val = node.asText("").trim();
                if (!val.isBlank()) {
                    return val;
                }
            }
        }
        return "";
    }

    private static String resolveSecretId() {
        String envId = System.getenv("TELEGRAM_SECRET_ID");
        if (envId != null && !envId.isBlank()) {
            return envId.trim();
        }
        return DEFAULT_SECRET_ID;
    }

    private static RawSecretFetcher createDefaultFetcher() {
        String regionStr = System.getenv("AWS_REGION");
        if (regionStr == null || regionStr.isBlank()) {
            regionStr = DEFAULT_REGION;
        }
        SecretsManagerClient client = SecretsManagerClient.builder()
                .region(Region.of(regionStr.trim()))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
        return id -> client.getSecretValue(GetSecretValueRequest.builder().secretId(id).build()).secretString();
    }
}
