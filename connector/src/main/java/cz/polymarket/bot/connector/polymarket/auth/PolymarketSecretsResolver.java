package cz.polymarket.bot.connector.polymarket.auth;

import cz.polymarket.bot.connector.polymarket.clob.PolymarketEip712Signer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;

/**
 * Resolves Polymarket CLOB L2 and EIP-712 / EIP-1271 signing credentials from Quarkus configuration.
 * <p>
 * Profile behavior:
 * <ul>
 *   <li><b>{@code prod} profile</b>: Credentials are loaded from AWS Secrets Manager
 *       ({@code polymarket-sniper/clob-credentials} in {@code eu-west-1}) via
 *       {@code quarkus-amazon-secretsmanager} ({@code %prod.quarkus.secretsmanager.config.enabled=true}).</li>
 *   <li><b>{@code dev} and {@code test} profiles</b>: AWS Secrets Manager {@code ConfigSource} is disabled
 *       ({@code quarkus.secretsmanager.config.enabled=false}) and credentials are read from JVM arguments
 *       ({@code -Dpolymarket.connector.*=...}) or environment variables ({@code POLYMARKET_CONNECTOR_*=...}).</li>
 * </ul>
 * All 6 credentials ({@code signer-key}, {@code api-key}, {@code passphrase}, {@code secret}, {@code address},
 * {@code signer-address}) are strictly mandatory; if any is missing or invalid, resolution fails immediately
 * with an exception.
 */
@ApplicationScoped
public class PolymarketSecretsResolver {

    private final String signerKey;
    private final String apiKey;
    private final String passphrase;
    private final String secret;
    private final String address;
    private final String signerAddress;

    /**
     * CDI constructor injecting Polymarket credentials from Quarkus configuration.
     *
     * @param signerKey     hexadecimal secp256k1 private key ({@code polymarket.connector.signer-key})
     * @param apiKey        Polymarket CLOB L2 API key ({@code polymarket.connector.api-key})
     * @param passphrase    Polymarket CLOB L2 passphrase ({@code polymarket.connector.passphrase})
     * @param secret        Base64-encoded Polymarket CLOB L2 HMAC secret ({@code polymarket.connector.secret})
     * @param address       Polymarket Deposit / Maker wallet address ({@code polymarket.connector.address})
     * @param signerAddress Polymarket EOA signer address ({@code polymarket.connector.signer-address})
     */
    @Inject
    public PolymarketSecretsResolver(
            @ConfigProperty(name = "polymarket.connector.signer-key")
            Optional<String> signerKey,
            @ConfigProperty(name = "polymarket.connector.api-key")
            Optional<String> apiKey,
            @ConfigProperty(name = "polymarket.connector.passphrase")
            Optional<String> passphrase,
            @ConfigProperty(name = "polymarket.connector.secret")
            Optional<String> secret,
            @ConfigProperty(name = "polymarket.connector.address")
            Optional<String> address,
            @ConfigProperty(name = "polymarket.connector.signer-address")
            Optional<String> signerAddress) {
        this.signerKey = sanitize(signerKey);
        this.apiKey = sanitize(apiKey);
        this.passphrase = sanitize(passphrase);
        this.secret = sanitize(secret);
        this.address = sanitize(address);
        this.signerAddress = sanitize(signerAddress);
    }

    /**
     * Resolves and strictly validates all 6 Polymarket credentials from configuration without deriving
     * {@code signerAddress} from {@code signerKey}. Throws {@link IllegalStateException} or
     * {@link IllegalArgumentException} if any credential is missing or malformed.
     *
     * @return validated {@link PolymarketCredentials}
     */
    public PolymarketCredentials resolveCredentials() {
        requireNonBlank(this.signerKey, "polymarket.connector.signer-key");
        requireNonBlank(this.apiKey, "polymarket.connector.api-key");
        requireNonBlank(this.passphrase, "polymarket.connector.passphrase");
        requireNonBlank(this.secret, "polymarket.connector.secret");
        requireNonBlank(this.address, "polymarket.connector.address");
        requireNonBlank(this.signerAddress, "polymarket.connector.signer-address");

        String normalizedAddress = PolymarketEip712Signer.toEip55ChecksumAddress(this.address);
        String normalizedSignerAddress = PolymarketEip712Signer.toEip55ChecksumAddress(this.signerAddress);

        return new PolymarketCredentials(
                this.signerKey,
                this.apiKey,
                this.passphrase,
                this.secret,
                normalizedAddress,
                normalizedAddress,
                normalizedSignerAddress
        );
    }

    private static void requireNonBlank(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Mandatory configuration property '" + propertyName + "' is not set");
        }
    }

    private static String sanitize(Optional<String> value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        return value.get().trim();
    }
}
