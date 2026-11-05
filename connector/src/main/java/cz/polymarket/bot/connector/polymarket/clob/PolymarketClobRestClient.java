package cz.polymarket.bot.connector.polymarket.clob;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.connector.polymarket.auth.PolymarketCredentials;
import cz.polymarket.bot.connector.polymarket.auth.PolymarketSecretsResolver;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Low-latency REST client for Polymarket CLOB ({@code https://clob.polymarket.com}) implementing
 * Level 2 HMAC-SHA256 authentication, EIP-712 / EIP-1271 signed order submission ({@code POST /order}),
 * order cancellation ({@code DELETE /order}), conditional token balance lookup ({@code GET /balance-allowance}),
 * and connection-pool keepalive heartbeats ({@code GET /time}).
 */
@ApplicationScoped
public class PolymarketClobRestClient {

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final PolymarketSecretsResolver secretsResolver;

    /**
     * Constructs the Polymarket CLOB REST client.
     *
     * @param baseUrl         Polymarket CLOB REST base URL (default {@code https://clob.polymarket.com})
     * @param httpClient      shared HTTP client
     * @param objectMapper    Jackson object mapper
     * @param secretsResolver Polymarket credentials resolver
     */
    @Inject
    public PolymarketClobRestClient(
            @ConfigProperty(name = "polymarket.connector.clob.rest-url", defaultValue = "https://clob.polymarket.com")
            String baseUrl,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            PolymarketSecretsResolver secretsResolver) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.secretsResolver = secretsResolver;
    }

    /**
     * Submits a signed {@link ClobOrderEnvelope} to {@code POST /order} with Level 2 HMAC-SHA256 headers.
     *
     * @param envelope signed order envelope
     * @return parsed {@link ClobOrderResponse}
     */
    public ClobOrderResponse submitOrder(ClobOrderEnvelope envelope) {
        try {
            byte[] bodyBytes = objectMapper.writeValueAsBytes(envelope);
            return submitRawOrderJson(bodyBytes);
        } catch (Exception e) {
            Log.errorf("Failed to submit CLOB order: %s", e.getMessage());
            return new ClobOrderResponse(e.getMessage(), "", "0", "0", "error", List.of(), false);
        }
    }

    /**
     * Submits pre-serialized signed order JSON bytes to {@code POST /order} with Level 2 HMAC-SHA256 headers.
     *
     * @param jsonBodyBytes UTF-8 JSON bytes of a signed {@link ClobOrderEnvelope}
     * @return parsed {@link ClobOrderResponse}
     */
    public ClobOrderResponse submitRawOrderJson(byte[] jsonBodyBytes) {
        PolymarketCredentials creds = requireL2Credentials();
        String path = "/order";
        long timestampSec = Instant.now().getEpochSecond();
        String timestampStr = String.valueOf(timestampSec);
        String signature = computeL2HmacSignature(creds.secret(), timestampStr, "POST", path, jsonBodyBytes);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("POLY_ADDRESS", creds.signerAddress())
                    .header("POLY_API_KEY", creds.apiKey())
                    .header("POLY_PASSPHRASE", creds.passphrase())
                    .header("POLY_TIMESTAMP", timestampStr)
                    .header("POLY_SIGNATURE", signature)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(jsonBodyBytes))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                Log.warnf("CLOB POST /order returned HTTP %d: %s", response.statusCode(), response.body());
                return new ClobOrderResponse(
                        "HTTP " + response.statusCode() + ": " + response.body(),
                        "",
                        "0",
                        "0",
                        "rejected",
                        List.of(),
                        false
                );
            }
            return objectMapper.readValue(response.body(), ClobOrderResponse.class);
        } catch (Exception e) {
            throw new IllegalStateException("CLOB POST /order transport error: " + e.getMessage(), e);
        }
    }

    /**
     * Cancels an open order on the Polymarket CLOB via {@code DELETE /order}.
     *
     * @param orderId CLOB order ID ({@code 0x...})
     * @return parsed {@link ClobCancelResponse}
     */
    public ClobCancelResponse cancelOrder(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("orderId must not be blank");
        }
        PolymarketCredentials creds = requireL2Credentials();
        String path = "/order";
        byte[] bodyBytes = ("{\"orderID\":\"" + orderId.trim() + "\"}").getBytes(StandardCharsets.UTF_8);
        String timestampStr = String.valueOf(Instant.now().getEpochSecond());
        String signature = computeL2HmacSignature(creds.secret(), timestampStr, "DELETE", path, bodyBytes);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("POLY_ADDRESS", creds.signerAddress())
                    .header("POLY_API_KEY", creds.apiKey())
                    .header("POLY_PASSPHRASE", creds.passphrase())
                    .header("POLY_TIMESTAMP", timestampStr)
                    .header("POLY_SIGNATURE", signature)
                    .method("DELETE", HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("CLOB DELETE /order HTTP " + response.statusCode() + ": " + response.body());
            }
            return objectMapper.readValue(response.body(), ClobCancelResponse.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to cancel CLOB order " + orderId + ": " + e.getMessage(), e);
        }
    }

    /**
     * Queries {@code GET /balance-allowance?asset_type=CONDITIONAL&signature_type=3&token_id={tokenId}}
     * and returns the conditional token balance in micro-shares ({@code 1 share = 1,000,000}).
     * <p>
     * Note: Polymarket L2 HMAC authentication signs only the base path {@code /balance-allowance}
     * without the query string.
     *
     * @param tokenId 256-bit decimal conditional token ID
     * @return balance in micro-shares
     */
    public long getConditionalTokenBalanceMicroShares(String tokenId) {
        if (tokenId == null || tokenId.isBlank()) {
            throw new IllegalArgumentException("tokenId must not be blank");
        }
        PolymarketCredentials creds = requireL2Credentials();
        String cleanPath = "/balance-allowance";
        String fullPathWithQuery = cleanPath
                + "?asset_type=CONDITIONAL&signature_type=3&token_id="
                + URLEncoder.encode(tokenId.trim(), StandardCharsets.UTF_8);
        String timestampStr = String.valueOf(Instant.now().getEpochSecond());
        String signature = computeL2HmacSignature(creds.secret(), timestampStr, "GET", cleanPath, new byte[0]);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + fullPathWithQuery))
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/json")
                    .header("POLY_ADDRESS", creds.signerAddress())
                    .header("POLY_API_KEY", creds.apiKey())
                    .header("POLY_PASSPHRASE", creds.passphrase())
                    .header("POLY_TIMESTAMP", timestampStr)
                    .header("POLY_SIGNATURE", signature)
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200 || response.body() == null || response.body().isBlank()) {
                throw new IllegalStateException("CLOB GET /balance-allowance HTTP " + response.statusCode());
            }
            JsonNode root = objectMapper.readTree(response.body());
            String balanceStr = root.path("balance").asText("0").trim();
            if (balanceStr.contains(".")) {
                double val = Double.parseDouble(balanceStr);
                return val > 0.0 && Double.isFinite(val) ? Math.round(val * 1_000_000.0) : 0L;
            }
            return Long.parseLong(balanceStr);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to query conditional token balance for " + tokenId, e);
        }
    }

    /**
     * Sends a lightweight unauthenticated {@code GET /time} keepalive heartbeat to keep the HTTP connection warm.
     *
     * @return true if the CLOB server responded with HTTP 200
     */
    public boolean pingKeepalive() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/time"))
                    .timeout(Duration.ofSeconds(5))
                    .header("Accept", "application/json")
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            Log.debugf("CLOB GET /time keepalive failed: %s", e.getMessage());
            return false;
        }
    }

    /**
     * Computes the Polymarket Level 2 HMAC-SHA256 signature encoded as URL-safe Base64.
     *
     * @param base64Secret URL-safe or standard Base64-encoded L2 secret
     * @param timestamp    Unix epoch seconds string
     * @param method       uppercase HTTP method ({@code "GET"}, {@code "POST"}, {@code "DELETE"})
     * @param requestPath  request path (any query string after {@code '?'} is automatically stripped)
     * @param bodyBytes    raw request body bytes (or empty array for GET)
     * @return URL-safe Base64 HMAC-SHA256 signature
     */
    public static String computeL2HmacSignature(
            String base64Secret,
            String timestamp,
            String method,
            String requestPath,
            byte[] bodyBytes) {
        try {
            byte[] keyBytes = decodeBase64Flexible(base64Secret.trim());
            int queryIdx = requestPath.indexOf('?');
            String cleanPath = queryIdx >= 0 ? requestPath.substring(0, queryIdx) : requestPath;

            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(keyBytes, "HmacSHA256"));
            mac.update(timestamp.getBytes(StandardCharsets.UTF_8));
            mac.update(method.toUpperCase().getBytes(StandardCharsets.UTF_8));
            mac.update(cleanPath.getBytes(StandardCharsets.UTF_8));
            if (bodyBytes != null && bodyBytes.length > 0) {
                mac.update(bodyBytes);
            }
            return Base64.getUrlEncoder().encodeToString(mac.doFinal());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to compute Polymarket L2 HMAC-SHA256 signature", e);
        }
    }

    private PolymarketCredentials requireL2Credentials() {
        PolymarketCredentials creds = secretsResolver.resolveCredentials();
        if (!creds.hasL2Credentials() || creds.signerAddress().isBlank()) {
            throw new IllegalStateException(
                    "Missing Polymarket CLOB L2 credentials (apiKey, secret, passphrase, signerAddress)"
            );
        }
        return creds;
    }

    private static byte[] decodeBase64Flexible(String secret) {
        try {
            return Base64.getUrlDecoder().decode(secret);
        } catch (IllegalArgumentException ignored) {
            return Base64.getDecoder().decode(secret);
        }
    }
}
