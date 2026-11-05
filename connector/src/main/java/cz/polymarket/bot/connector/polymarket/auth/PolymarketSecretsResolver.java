package cz.polymarket.bot.connector.polymarket.auth;

import cz.polymarket.bot.connector.polymarket.clob.PolymarketEip712Signer;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;

/**
 * Resolves Polymarket CLOB L2 and EIP-712 / EIP-1271 signing credentials from Quarkus configuration.
 * <p>
 * Profile behavior:
 * <ul>
 *   <li><b>{@code prod} profile</b>: Credentials are loaded automatically from AWS Secrets Manager
 *       ({@code polymarket-sniper/clob-credentials} in {@code eu-west-1}) via the
 *       {@code quarkus-amazon-secretsmanager} {@code ConfigSource}
 *       ({@code %prod.quarkus.secretsmanager.config.enabled=true}).</li>
 *   <li><b>{@code dev} and {@code test} profiles</b>: AWS Secrets Manager {@code ConfigSource} is disabled
 *       ({@code quarkus.secretsmanager.config.enabled=false}) and credentials are resolved from JVM arguments
 *       ({@code -Dpolymarket.connector.*=...} or {@code -DPOLYMARKET_*=...}) or environment variables
 *       ({@code POLYMARKET_API_KEY}, {@code POLYMARKET_SECRET}, {@code POLYMARKET_PASSPHRASE},
 *       {@code POLYMARKET_PRIVATE_KEY}, {@code POLYMARKET_MAKER_ADDRESS}).</li>
 * </ul>
 */
@ApplicationScoped
public class PolymarketSecretsResolver {

    private final String signerKey;
    private final String apiKey;
    private final String passphrase;
    private final String secret;
    private final String address;
    private final String makerAddress;
    private final String signerAddress;

    /**
     * CDI constructor injecting Polymarket credentials from SmallRye Config (backed by
     * {@code quarkus-amazon-secretsmanager} in {@code prod}, or JVM arguments / environment variables in {@code dev}/{@code test}).
     *
     * @param signerKey     hexadecimal secp256k1 private key ({@code signer_key} / {@code POLYMARKET_SIGNER_KEY})
     * @param apiKey        Polymarket CLOB L2 API key ({@code api_key} / {@code POLYMARKET_API_KEY})
     * @param passphrase    Polymarket CLOB L2 passphrase ({@code passphrase} / {@code POLYMARKET_PASSPHRASE})
     * @param secret        Base64-encoded Polymarket CLOB L2 HMAC secret ({@code secret} / {@code POLYMARKET_SECRET})
     * @param address       Polymarket Deposit / Proxy wallet address ({@code address} / {@code POLYMARKET_ADDRESS})
     * @param makerAddress  optional maker address override ({@code POLYMARKET_MAKER_ADDRESS}; defaults to {@code address})
     * @param signerAddress optional explicit EOA address (derived automatically from {@code signerKey} if omitted)
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
            @ConfigProperty(name = "polymarket.connector.maker-address")
            Optional<String> makerAddress,
            @ConfigProperty(name = "polymarket.connector.signer-address")
            Optional<String> signerAddress) {
        this.signerKey = sanitize(signerKey);
        this.apiKey = sanitize(apiKey);
        this.passphrase = sanitize(passphrase);
        this.secret = sanitize(secret);
        this.address = sanitize(address);
        this.makerAddress = sanitize(makerAddress);
        this.signerAddress = sanitize(signerAddress);
    }

    /**
     * Resolves and validates the Polymarket credentials, deriving the EIP-55 EOA {@code signerAddress}
     * from {@code signerKey} when not explicitly configured.
     *
     * @return resolved {@link PolymarketCredentials}
     */
    public PolymarketCredentials resolveCredentials() {
        String resolvedSignerAddr = this.signerAddress;
        if (!this.signerKey.isBlank()) {
            try {
                PolymarketEip712Signer signer = new PolymarketEip712Signer(this.signerKey);
                resolvedSignerAddr = signer.getSignerEoaAddress();
            } catch (Exception e) {
                Log.warnf("Unable to derive EOA address from configured signerKey: %s", e.getMessage());
            }
        } else if (!resolvedSignerAddr.isBlank()) {
            resolvedSignerAddr = PolymarketEip712Signer.toEip55ChecksumAddress(resolvedSignerAddr);
        }

        String rawMaker = !this.makerAddress.isBlank() ? this.makerAddress : this.address;
        String rawAddress = !this.address.isBlank() ? this.address : this.makerAddress;
        String normalizedAddress = !rawAddress.isBlank()
                ? PolymarketEip712Signer.toEip55ChecksumAddress(rawAddress)
                : "";
        String normalizedMaker = !rawMaker.isBlank()
                ? PolymarketEip712Signer.toEip55ChecksumAddress(rawMaker)
                : "";

        return new PolymarketCredentials(
                this.signerKey,
                this.apiKey,
                this.passphrase,
                this.secret,
                normalizedAddress,
                normalizedMaker,
                resolvedSignerAddr
        );
    }

    private static String sanitize(Optional<String> value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        String trimmed = value.get().trim();
        return trimmed.isEmpty() ? "" : trimmed;
    }
}
