package cz.polymarket.bot.connector.polymarket.auth;

/**
 * Abstraction for retrieving raw secret strings from a secret store (such as AWS Secrets Manager).
 * Enables deterministic in-memory unit and integration testing without contacting live AWS endpoints.
 */
@FunctionalInterface
public interface SecretValueFetcher {

    /**
     * Fetches the plaintext {@code SecretString} for the given secret identifier and optional version stage.
     *
     * @param secretId     secret name or ARN (e.g. {@code polymarket-sniper/clob-credentials})
     * @param versionStage optional version stage (e.g. {@code AWSCURRENT}), or {@code null}
     * @return raw secret string (typically a JSON object)
     */
    String fetchSecretString(String secretId, String versionStage);
}
