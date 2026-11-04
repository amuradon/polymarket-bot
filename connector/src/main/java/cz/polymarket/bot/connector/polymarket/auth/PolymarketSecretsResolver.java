package cz.polymarket.bot.connector.polymarket.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.connector.polymarket.clob.PolymarketEip712Signer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves Polymarket CLOB and PolyBolt credentials from environment variables, MicroProfile config,
 * CloudFormation dynamic references ({@code {{resolve:secretsmanager:<secret_id>:SecretString:<json_key>}}}),
 * or AWS Secrets Manager secret bundles (defaulting to {@code polymarket-sniper/clob-credentials}).
 * <p>
 * Caches fetched secret payloads in memory per {@code (secretId, versionStage)} so a single
 * {@code GetSecretValue} request populates all credential fields.
 */
@ApplicationScoped
public class PolymarketSecretsResolver {

    /** Default AWS Secrets Manager secret ID shared with {@code polymarket-stale-order-sniper}. */
    public static final String DEFAULT_CLOB_SECRET_ID = "polymarket-sniper/clob-credentials";

    private static final String DYNAMIC_PREFIX = "{{resolve:secretsmanager:";
    private static final String DYNAMIC_SUFFIX = "}}";

    private final ObjectMapper objectMapper;
    private final SecretValueFetcher secretFetcher;
    private final String credentialsSecretId;
    private final String signerKeyRaw;
    private final String apiKeyRaw;
    private final String passphraseRaw;
    private final String secretRaw;
    private final String addressRaw;
    private final String makerAddressRaw;
    private final String signerAddressRaw;
    private final Map<String, String> envOverrides;

    private final Map<String, String> secretCache = new ConcurrentHashMap<>();

    /**
     * CDI constructor for {@link PolymarketSecretsResolver}.
     *
     * @param objectMapper        Jackson mapper
     * @param secretFetcher       secret value fetcher (AWS Secrets Manager)
     * @param credentialsSecretId optional secret ID bundle (e.g. {@code polymarket-sniper/clob-credentials})
     * @param signerKeyRaw        configured signer private key or dynamic reference
     * @param apiKeyRaw           configured API key or dynamic reference
     * @param passphraseRaw       configured passphrase or dynamic reference
     * @param secretRaw           configured L2 HMAC secret or dynamic reference
     * @param addressRaw          configured Deposit/Proxy wallet address or dynamic reference
     * @param makerAddressRaw     configured maker address or dynamic reference
     * @param signerAddressRaw    configured EOA signer address or dynamic reference
     */
    @Inject
    public PolymarketSecretsResolver(
            ObjectMapper objectMapper,
            SecretValueFetcher secretFetcher,
            @ConfigProperty(name = "polymarket.connector.credentials-secret-id")
            Optional<String> credentialsSecretId,
            @ConfigProperty(name = "polymarket.connector.signer-key")
            Optional<String> signerKeyRaw,
            @ConfigProperty(name = "polymarket.connector.api-key")
            Optional<String> apiKeyRaw,
            @ConfigProperty(name = "polymarket.connector.passphrase")
            Optional<String> passphraseRaw,
            @ConfigProperty(name = "polymarket.connector.secret")
            Optional<String> secretRaw,
            @ConfigProperty(name = "polymarket.connector.address")
            Optional<String> addressRaw,
            @ConfigProperty(name = "polymarket.connector.maker-address")
            Optional<String> makerAddressRaw,
            @ConfigProperty(name = "polymarket.connector.signer-address")
            Optional<String> signerAddressRaw) {
        this(
                objectMapper,
                secretFetcher,
                credentialsSecretId,
                signerKeyRaw,
                apiKeyRaw,
                passphraseRaw,
                secretRaw,
                addressRaw,
                makerAddressRaw,
                signerAddressRaw,
                System.getenv()
        );
    }

    /**
     * Package-private constructor accepting an explicit environment map for deterministic testing.
     */
    public PolymarketSecretsResolver(
            ObjectMapper objectMapper,
            SecretValueFetcher secretFetcher,
            Optional<String> credentialsSecretId,
            Optional<String> signerKeyRaw,
            Optional<String> apiKeyRaw,
            Optional<String> passphraseRaw,
            Optional<String> secretRaw,
            Optional<String> addressRaw,
            Optional<String> makerAddressRaw,
            Optional<String> signerAddressRaw,
            Map<String, String> envOverrides) {
        this.objectMapper = objectMapper;
        this.secretFetcher = secretFetcher;
        this.envOverrides = envOverrides != null ? envOverrides : Map.of();
        this.credentialsSecretId = firstNonBlank(credentialsSecretId, env("POLYMARKET_CREDENTIALS_SECRET_ID"));
        this.signerKeyRaw = firstNonBlank(signerKeyRaw, env("POLYMARKET_SIGNER_KEY"));
        this.apiKeyRaw = firstNonBlank(apiKeyRaw, env("POLYMARKET_API_KEY"));
        this.passphraseRaw = firstNonBlank(passphraseRaw, env("POLYMARKET_PASSPHRASE"));
        this.secretRaw = firstNonBlank(secretRaw, env("POLYMARKET_SECRET"));
        this.addressRaw = firstNonBlank(addressRaw, env("POLYMARKET_ADDRESS"));
        this.makerAddressRaw = firstNonBlank(makerAddressRaw, env("POLYMARKET_MAKER_ADDRESS"));
        this.signerAddressRaw = firstNonBlank(signerAddressRaw, env("POLYMARKET_SIGNER_ADDRESS"));
    }

    /**
     * Resolves a single configuration or environment string that may be either a plain value
     * or a CloudFormation dynamic reference {@code {{resolve:secretsmanager:...}}}.
     *
     * @param rawOrDynamic raw value or dynamic reference
     * @return resolved string (or empty string if blank)
     */
    public String resolveValue(String rawOrDynamic) {
        if (rawOrDynamic == null) {
            return "";
        }
        String trimmed = stripWrappingQuotes(rawOrDynamic.trim());
        if (trimmed.isEmpty()) {
            return "";
        }
        DynamicSecretRef ref = parseDynamicSecretRef(trimmed);
        if (ref == null) {
            return trimmed;
        }
        String secretPayload = fetchCachedSecret(ref.secretId(), ref.versionStage());
        if (ref.jsonKey() == null || ref.jsonKey().isBlank()) {
            return secretPayload.trim();
        }
        return extractJsonField(secretPayload, ref.jsonKey());
    }

    /**
     * Resolves the full set of Polymarket Level 1 and Level 2 credentials.
     *
     * @return resolved {@link PolymarketCredentials}
     */
    public PolymarketCredentials resolveCredentials() {
        String signerKey = resolveFieldWithSecretFallback(signerKeyRaw, "signer_key", "private_key", "pk");
        String apiKey = resolveFieldWithSecretFallback(apiKeyRaw, "api_key");
        String passphrase = resolveFieldWithSecretFallback(passphraseRaw, "passphrase");
        String secret = resolveFieldWithSecretFallback(secretRaw, "secret");
        String address = resolveFieldWithSecretFallback(addressRaw, "address", "funder_address", "maker_address");
        String makerAddress = resolveFieldWithSecretFallback(makerAddressRaw, "maker_address", "address", "funder_address");
        if (makerAddress.isEmpty()) {
            makerAddress = address;
        }
        if (address.isEmpty()) {
            address = makerAddress;
        }

        String signerAddress = resolveFieldWithSecretFallback(signerAddressRaw, "signer_address");
        if (signerAddress.isEmpty() && !signerKey.isEmpty()) {
            signerAddress = PolymarketEip712Signer.deriveEip55AddressFromPrivateKey(signerKey);
        } else if (!signerAddress.isEmpty()) {
            signerAddress = PolymarketEip712Signer.toEip55ChecksumAddress(signerAddress);
        }
        if (!address.isEmpty()) {
            address = PolymarketEip712Signer.toEip55ChecksumAddress(address);
        }
        if (!makerAddress.isEmpty()) {
            makerAddress = PolymarketEip712Signer.toEip55ChecksumAddress(makerAddress);
        }

        return new PolymarketCredentials(
                signerKey,
                apiKey,
                passphrase,
                secret,
                address,
                makerAddress,
                signerAddress
        );
    }

    /**
     * Parses a CloudFormation Secrets Manager dynamic reference of the form
     * {@code {{resolve:secretsmanager:secret-id:SecretString:json-key}}}.
     *
     * @param value string to inspect
     * @return parsed {@link DynamicSecretRef}, or {@code null} if {@code value} is a plain string
     */
    public static DynamicSecretRef parseDynamicSecretRef(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = stripWrappingQuotes(value.trim());
        if (!trimmed.startsWith("{{resolve:")) {
            return null;
        }
        if (!trimmed.startsWith(DYNAMIC_PREFIX) || !trimmed.endsWith(DYNAMIC_SUFFIX)) {
            throw new IllegalArgumentException(
                    "Unsupported dynamic reference syntax (expected Secrets Manager dynamic reference): " + value
            );
        }
        String inner = trimmed.substring(DYNAMIC_PREFIX.length(), trimmed.length() - DYNAMIC_SUFFIX.length());
        int idx = inner.indexOf(":SecretString");
        if (idx >= 0) {
            String secretId = inner.substring(0, idx).trim();
            if (secretId.isEmpty()) {
                throw new IllegalArgumentException("Empty secret_id in dynamic reference: " + value);
            }
            String after = inner.substring(idx + ":SecretString".length());
            String rest = after.startsWith(":") ? after.substring(1) : after;
            String[] parts = rest.split(":", -1);
            String jsonKey = parts.length > 0 && !parts[0].isBlank() ? parts[0].trim() : null;
            String versionStage = parts.length > 1 && !parts[1].isBlank() ? parts[1].trim() : null;
            return new DynamicSecretRef(secretId, jsonKey, versionStage);
        }
        String secretId = inner.trim();
        if (secretId.isEmpty() || secretId.contains(":")) {
            throw new IllegalArgumentException("Malformed Secrets Manager dynamic reference: " + value);
        }
        return new DynamicSecretRef(secretId, null, null);
    }

    private String resolveFieldWithSecretFallback(String configuredValue, String... jsonKeys) {
        String resolved = resolveValue(configuredValue);
        if (!resolved.isEmpty()) {
            return resolved;
        }
        if (credentialsSecretId == null || credentialsSecretId.isBlank()) {
            return "";
        }
        String secretPayload = fetchCachedSecret(credentialsSecretId, null);
        for (String key : jsonKeys) {
            String candidate = extractJsonFieldOptional(secretPayload, key);
            if (!candidate.isEmpty()) {
                return candidate;
            }
        }
        return "";
    }

    private String fetchCachedSecret(String secretId, String versionStage) {
        String cacheKey = secretId + "::" + (versionStage != null ? versionStage : "");
        return secretCache.computeIfAbsent(cacheKey, k -> secretFetcher.fetchSecretString(secretId, versionStage));
    }

    private String extractJsonField(String secretPayload, String jsonKey) {
        String val = extractJsonFieldOptional(secretPayload, jsonKey);
        if (val.isEmpty()) {
            throw new IllegalStateException("Key '" + jsonKey + "' missing or blank in Secrets Manager JSON payload");
        }
        return val;
    }

    private String extractJsonFieldOptional(String secretPayload, String jsonKey) {
        try {
            JsonNode root = objectMapper.readTree(secretPayload);
            JsonNode field = root.get(jsonKey);
            if (field == null || field.isNull()) {
                return "";
            }
            if (field.isTextual()) {
                return field.asText().trim();
            }
            if (field.isNumber() || field.isBoolean()) {
                return field.asText().trim();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse Secrets Manager JSON payload", e);
        }
        return "";
    }

    private String env(String key) {
        String val = envOverrides.get(key);
        return val != null ? val.trim() : "";
    }

    private static String firstNonBlank(Optional<String> primary, String fallback) {
        if (primary != null && primary.isPresent() && !primary.get().isBlank()) {
            return primary.get().trim();
        }
        return fallback != null ? fallback.trim() : "";
    }

    private static String stripWrappingQuotes(String s) {
        if (s.length() >= 2) {
            if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\""))) {
                return s.substring(1, s.length() - 1).trim();
            }
        }
        return s;
    }

    /**
     * Parsed CloudFormation Secrets Manager dynamic reference.
     *
     * @param secretId     Secrets Manager secret identifier
     * @param jsonKey      optional JSON property name inside {@code SecretString}
     * @param versionStage optional version stage (e.g. {@code AWSCURRENT})
     */
    public record DynamicSecretRef(String secretId, String jsonKey, String versionStage) {
    }
}
