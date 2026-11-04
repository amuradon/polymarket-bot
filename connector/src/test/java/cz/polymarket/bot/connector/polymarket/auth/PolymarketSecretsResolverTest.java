package cz.polymarket.bot.connector.polymarket.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PolymarketSecretsResolver} verifying plain value resolution,
 * CloudFormation dynamic reference parsing ({@code {{resolve:secretsmanager:...}}}),
 * single-fetch secret caching, and fallback resolution from {@code polymarket-sniper/clob-credentials}.
 */
class PolymarketSecretsResolverTest {

    private static final String TEST_SIGNER_KEY =
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    private static final String EXPECTED_SIGNER_EOA =
            "0x2c7536E3605D9C16a7a3D7b1898e529396a65c23";
    private static final String TEST_DEPOSIT_WALLET =
            "0x1111111111111111111111111111111111111111";

    private static final String SYNTHETIC_SECRET_JSON = """
            {
              "signer_key": "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
              "api_key": "550e8400-e29b-41d4-a716-446655440000",
              "passphrase": "test_passphrase_123",
              "secret": "c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM=",
              "address": "0x1111111111111111111111111111111111111111"
            }
            """;

    @Test
    void shouldResolveCloudFormationDynamicReferencesWithSingleSecretFetch() {
        AtomicInteger fetchCount = new AtomicInteger(0);
        SecretValueFetcher mockFetcher = (secretId, stage) -> {
            assertThat(secretId).isEqualTo("polymarket-sniper/clob-credentials");
            fetchCount.incrementAndGet();
            return SYNTHETIC_SECRET_JSON;
        };

        PolymarketSecretsResolver resolver = new PolymarketSecretsResolver(
                new ObjectMapper(),
                mockFetcher,
                Optional.of("polymarket-sniper/clob-credentials"),
                Optional.of("{{resolve:secretsmanager:polymarket-sniper/clob-credentials:SecretString:signer_key}}"),
                Optional.of("{{resolve:secretsmanager:polymarket-sniper/clob-credentials:SecretString:api_key}}"),
                Optional.of("{{resolve:secretsmanager:polymarket-sniper/clob-credentials:SecretString:passphrase}}"),
                Optional.of("{{resolve:secretsmanager:polymarket-sniper/clob-credentials:SecretString:secret}}"),
                Optional.of("{{resolve:secretsmanager:polymarket-sniper/clob-credentials:SecretString:address}}"),
                Optional.empty(),
                Optional.empty(),
                Map.of()
        );

        PolymarketCredentials creds = resolver.resolveCredentials();
        assertThat(fetchCount.get()).isEqualTo(1);
        assertThat(creds.signerKeyHex()).isEqualTo(TEST_SIGNER_KEY);
        assertThat(creds.apiKey()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(creds.passphrase()).isEqualTo("test_passphrase_123");
        assertThat(creds.secret()).isEqualTo("c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM=");
        assertThat(creds.address()).isEqualTo(TEST_DEPOSIT_WALLET);
        assertThat(creds.makerAddress()).isEqualTo(TEST_DEPOSIT_WALLET);
        assertThat(creds.signerAddress()).isEqualTo(EXPECTED_SIGNER_EOA);
    }

    @Test
    void shouldFallbackToSecretBundleWhenEnvVarsOmitted() {
        AtomicInteger fetchCount = new AtomicInteger(0);
        SecretValueFetcher mockFetcher = (secretId, stage) -> {
            fetchCount.incrementAndGet();
            return SYNTHETIC_SECRET_JSON;
        };

        PolymarketSecretsResolver resolver = new PolymarketSecretsResolver(
                new ObjectMapper(),
                mockFetcher,
                Optional.of("polymarket-sniper/clob-credentials"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Map.of()
        );

        PolymarketCredentials creds = resolver.resolveCredentials();
        assertThat(fetchCount.get()).isEqualTo(1);
        assertThat(creds.apiKey()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(creds.makerAddress()).isEqualTo(TEST_DEPOSIT_WALLET);
        assertThat(creds.signerAddress()).isEqualTo(EXPECTED_SIGNER_EOA);
    }

    @Test
    void shouldResolveDirectValuesWithoutCallingSecretsManager() {
        AtomicInteger fetchCount = new AtomicInteger(0);
        SecretValueFetcher mockFetcher = (secretId, stage) -> {
            fetchCount.incrementAndGet();
            throw new AssertionError("Should not call Secrets Manager when all values are plain");
        };

        PolymarketSecretsResolver resolver = new PolymarketSecretsResolver(
                new ObjectMapper(),
                mockFetcher,
                Optional.empty(),
                Optional.of(TEST_SIGNER_KEY),
                Optional.of("direct-api-key"),
                Optional.of("direct-passphrase"),
                Optional.of("c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM="),
                Optional.of(TEST_DEPOSIT_WALLET),
                Optional.empty(),
                Optional.empty(),
                Map.of()
        );

        PolymarketCredentials creds = resolver.resolveCredentials();
        assertThat(fetchCount.get()).isZero();
        assertThat(creds.apiKey()).isEqualTo("direct-api-key");
        assertThat(creds.signerAddress()).isEqualTo(EXPECTED_SIGNER_EOA);
    }

    @Test
    void shouldRejectInvalidDynamicReferenceSyntax() {
        assertThatThrownBy(() -> PolymarketSecretsResolver.parseDynamicSecretRef("{{resolve:ssm:foo}}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Secrets Manager dynamic reference");
    }
}
