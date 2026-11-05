package cz.polymarket.bot.connector.polymarket.auth;

import io.quarkiverse.amazon.common.runtime.JsonConfigFlattener;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PolymarketSecretsResolver} and {@code quarkus-amazon-secretsmanager} JSON flattening
 * for {@code polymarket-sniper/clob-credentials}.
 */
class PolymarketSecretsResolverTest {

    private static final String TEST_SIGNER_KEY =
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    private static final String EXPECTED_SIGNER_EOA =
            "0x2c7536E3605D9C16a7a3D7b1898e529396a65c23";
    private static final String TEST_DEPOSIT_WALLET =
            "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";

    private static final String SYNTHETIC_SECRET_JSON = """
            {
              "signer_key": "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
              "api_key": "550e8400-e29b-41d4-a716-446655440000",
              "passphrase": "test_passphrase_123",
              "secret": "c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM=",
              "address": "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"
            }
            """;

    @Test
    void shouldFlattenAwsSecretsManagerJsonViaQuarkusAmazonSecretsManagerAndResolveCredentials() {
        Map<String, String> flattened = JsonConfigFlattener.expandValue("polymarket.secrets", SYNTHETIC_SECRET_JSON);

        assertThat(flattened)
                .containsEntry("polymarket.secrets.signer_key", TEST_SIGNER_KEY)
                .containsEntry("polymarket.secrets.api_key", "550e8400-e29b-41d4-a716-446655440000")
                .containsEntry("polymarket.secrets.passphrase", "test_passphrase_123")
                .containsEntry("polymarket.secrets.secret", "c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM=")
                .containsEntry("polymarket.secrets.address", "0x70997970c51812dc3a010c7d01b50e0d17dc79c8");

        PolymarketSecretsResolver resolver = new PolymarketSecretsResolver(
                Optional.ofNullable(flattened.get("polymarket.secrets.signer_key")),
                Optional.ofNullable(flattened.get("polymarket.secrets.api_key")),
                Optional.ofNullable(flattened.get("polymarket.secrets.passphrase")),
                Optional.ofNullable(flattened.get("polymarket.secrets.secret")),
                Optional.ofNullable(flattened.get("polymarket.secrets.address")),
                Optional.empty(),
                Optional.empty()
        );

        PolymarketCredentials creds = resolver.resolveCredentials();
        assertThat(creds.isCompleteForTrading()).isTrue();
        assertThat(creds.signerKeyHex()).isEqualTo(TEST_SIGNER_KEY);
        assertThat(creds.apiKey()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(creds.passphrase()).isEqualTo("test_passphrase_123");
        assertThat(creds.secret()).isEqualTo("c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM=");
        assertThat(creds.address()).isEqualTo(TEST_DEPOSIT_WALLET);
        assertThat(creds.makerAddress()).isEqualTo(TEST_DEPOSIT_WALLET);
        assertThat(creds.signerAddress()).isEqualTo(EXPECTED_SIGNER_EOA);
    }

    @Test
    void shouldResolveDirectValuesFromJvmArgsOrEnvVarsInDevAndTestProfiles() {
        PolymarketSecretsResolver resolver = new PolymarketSecretsResolver(
                Optional.of(TEST_SIGNER_KEY),
                Optional.of("direct-api-key"),
                Optional.of("direct-passphrase"),
                Optional.of("c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM="),
                Optional.of(TEST_DEPOSIT_WALLET),
                Optional.empty(),
                Optional.empty()
        );

        PolymarketCredentials creds = resolver.resolveCredentials();
        assertThat(creds.isCompleteForTrading()).isTrue();
        assertThat(creds.apiKey()).isEqualTo("direct-api-key");
        assertThat(creds.makerAddress()).isEqualTo(TEST_DEPOSIT_WALLET);
        assertThat(creds.signerAddress()).isEqualTo(EXPECTED_SIGNER_EOA);
    }

    @Test
    void shouldHandleEmptyCredentialsWhenNotConfiguredInDryRunMode() {
        PolymarketSecretsResolver resolver = new PolymarketSecretsResolver(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
        );

        PolymarketCredentials creds = resolver.resolveCredentials();
        assertThat(creds.hasL2Credentials()).isFalse();
        assertThat(creds.isCompleteForTrading()).isFalse();
    }
}
