package cz.polymarket.bot.connector.polymarket.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Default {@link SecretValueFetcher} that retrieves secrets from AWS Secrets Manager in {@code eu-west-1}.
 * <p>
 * Supports both direct HTTPS SigV4 requests to {@code secretsmanager.<region>.amazonaws.com}
 * (resolving credentials from environment variables or EC2 Instance Metadata Service v2 / IMDSv2
 * so it works inside minimal JRE containers without the AWS CLI installed) and a fallback to the
 * {@code aws secretsmanager get-secret-value} CLI command (matching {@code secrets.rs} in the
 * Rust reference implementation).
 */
@ApplicationScoped
public class AwsSecretsManagerFetcher implements SecretValueFetcher {

    private static final Logger LOG = Logger.getLogger(AwsSecretsManagerFetcher.class);
    private static final DateTimeFormatter AMZ_DATE_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DATE_STAMP_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String awsRegion;

    /**
     * Constructs the AWS Secrets Manager fetcher.
     *
     * @param httpClient   shared HTTP client
     * @param objectMapper Jackson mapper
     * @param awsRegion    configured AWS region (defaults to {@code eu-west-1})
     */
    @Inject
    public AwsSecretsManagerFetcher(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            @ConfigProperty(name = "polymarket.aws.region", defaultValue = "eu-west-1")
            String awsRegion) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.awsRegion = resolveRegion(awsRegion);
    }

    @Override
    public String fetchSecretString(String secretId, String versionStage) {
        if (secretId == null || secretId.isBlank()) {
            throw new IllegalArgumentException("secretId must not be blank");
        }
        try {
            AwsCredentials creds = resolveAwsCredentials();
            if (creds != null) {
                return fetchViaSigV4Https(secretId.trim(), versionStage, creds);
            }
        } catch (Exception e) {
            LOG.debugf("HTTPS SigV4 Secrets Manager fetch failed for %s (%s), falling back to AWS CLI",
                    secretId, e.getMessage());
        }
        return fetchViaAwsCli(secretId.trim(), versionStage);
    }

    private String fetchViaSigV4Https(String secretId, String versionStage, AwsCredentials creds) throws Exception {
        String host = "secretsmanager." + awsRegion + ".amazonaws.com";
        String endpoint = "https://" + host + "/";
        String amzTarget = "secretsmanager.GetSecretValue";
        String contentType = "application/x-amz-json-1.1";

        StringBuilder payloadBuilder = new StringBuilder("{\"SecretId\":\"")
                .append(escapeJson(secretId))
                .append("\"");
        if (versionStage != null && !versionStage.isBlank()) {
            payloadBuilder.append(",\"VersionStage\":\"").append(escapeJson(versionStage.trim())).append("\"");
        }
        payloadBuilder.append("}");
        byte[] payloadBytes = payloadBuilder.toString().getBytes(StandardCharsets.UTF_8);
        String payloadHash = sha256Hex(payloadBytes);

        Instant now = Instant.now();
        String amzDate = AMZ_DATE_FMT.format(now);
        String dateStamp = DATE_STAMP_FMT.format(now);

        StringBuilder canonicalHeaders = new StringBuilder()
                .append("content-type:").append(contentType).append("\n")
                .append("host:").append(host).append("\n")
                .append("x-amz-date:").append(amzDate).append("\n");
        String signedHeaders = "content-type;host;x-amz-date";
        if (creds.sessionToken() != null && !creds.sessionToken().isBlank()) {
            canonicalHeaders.append("x-amz-security-token:").append(creds.sessionToken()).append("\n");
            signedHeaders += ";x-amz-security-token";
        }
        canonicalHeaders.append("x-amz-target:").append(amzTarget).append("\n");
        signedHeaders += ";x-amz-target";

        String canonicalRequest = "POST\n/\n\n"
                + canonicalHeaders + "\n"
                + signedHeaders + "\n"
                + payloadHash;

        String credentialScope = dateStamp + "/" + awsRegion + "/secretsmanager/aws4_request";
        String stringToSign = "AWS4-HMAC-SHA256\n"
                + amzDate + "\n"
                + credentialScope + "\n"
                + sha256Hex(canonicalRequest.getBytes(StandardCharsets.UTF_8));

        byte[] signingKey = getSignatureKey(creds.secretAccessKey(), dateStamp, awsRegion, "secretsmanager");
        String signature = HexFormat.of().formatHex(hmacSha256(signingKey, stringToSign));
        String authorizationHeader = "AWS4-HMAC-SHA256 Credential=" + creds.accessKeyId() + "/" + credentialScope
                + ", SignedHeaders=" + signedHeaders
                + ", Signature=" + signature;

        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", contentType)
                .header("X-Amz-Date", amzDate)
                .header("X-Amz-Target", amzTarget)
                .header("Authorization", authorizationHeader);
        if (creds.sessionToken() != null && !creds.sessionToken().isBlank()) {
            reqBuilder.header("X-Amz-Security-Token", creds.sessionToken());
        }
        HttpRequest request = reqBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(payloadBytes)).build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("AWS Secrets Manager returned HTTP " + response.statusCode());
        }
        JsonNode root = objectMapper.readTree(response.body());
        String secretString = root.path("SecretString").asText("");
        if (secretString.isEmpty()) {
            throw new IllegalStateException("SecretString was empty in Secrets Manager response for " + secretId);
        }
        return secretString;
    }

    private String fetchViaAwsCli(String secretId, String versionStage) {
        List<String> cmd = new ArrayList<>();
        cmd.add("aws");
        cmd.add("secretsmanager");
        cmd.add("get-secret-value");
        cmd.add("--region");
        cmd.add(awsRegion);
        cmd.add("--secret-id");
        cmd.add(secretId);
        if (versionStage != null && !versionStage.isBlank()) {
            cmd.add("--version-stage");
            cmd.add(versionStage.trim());
        }
        cmd.add("--query");
        cmd.add("SecretString");
        cmd.add("--output");
        cmd.add("text");

        try {
            Process process = new ProcessBuilder(cmd).start();
            byte[] stdoutBytes;
            try (InputStream is = process.getInputStream();
                 ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                is.transferTo(baos);
                stdoutBytes = baos.toByteArray();
            }
            boolean finished = process.waitFor(15, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                throw new IllegalStateException("Timed out waiting for aws secretsmanager get-secret-value");
            }
            if (process.exitValue() != 0) {
                throw new IllegalStateException("aws secretsmanager get-secret-value exited with code " + process.exitValue());
            }
            String secret = new String(stdoutBytes, StandardCharsets.UTF_8).trim();
            if (secret.isEmpty() || "None".equals(secret)) {
                throw new IllegalStateException("AWS Secrets Manager returned empty SecretString for " + secretId);
            }
            return secret;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to fetch secret '" + secretId + "' from AWS Secrets Manager", e);
        }
    }

    private AwsCredentials resolveAwsCredentials() {
        String envKey = System.getenv("AWS_ACCESS_KEY_ID");
        String envSecret = System.getenv("AWS_SECRET_ACCESS_KEY");
        String envToken = System.getenv("AWS_SESSION_TOKEN");
        if (envKey != null && !envKey.isBlank() && envSecret != null && !envSecret.isBlank()) {
            return new AwsCredentials(envKey.trim(), envSecret.trim(), envToken != null ? envToken.trim() : null);
        }
        // Fallback to EC2 IMDSv2 role credentials
        try {
            HttpRequest tokenReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://169.254.169.254/latest/api/token"))
                    .timeout(Duration.ofSeconds(2))
                    .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
                    .PUT(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> tokenResp = httpClient.send(tokenReq, HttpResponse.BodyHandlers.ofString());
            if (tokenResp.statusCode() != 200 || tokenResp.body() == null || tokenResp.body().isBlank()) {
                return null;
            }
            String imdsToken = tokenResp.body().trim();

            HttpRequest roleReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://169.254.169.254/latest/meta-data/iam/security-credentials/"))
                    .timeout(Duration.ofSeconds(2))
                    .header("X-aws-ec2-metadata-token", imdsToken)
                    .GET()
                    .build();
            HttpResponse<String> roleResp = httpClient.send(roleReq, HttpResponse.BodyHandlers.ofString());
            if (roleResp.statusCode() != 200 || roleResp.body() == null || roleResp.body().isBlank()) {
                return null;
            }
            String roleName = roleResp.body().lines().findFirst().orElse("").trim();
            if (roleName.isEmpty()) {
                return null;
            }

            HttpRequest credsReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://169.254.169.254/latest/meta-data/iam/security-credentials/" + roleName))
                    .timeout(Duration.ofSeconds(2))
                    .header("X-aws-ec2-metadata-token", imdsToken)
                    .GET()
                    .build();
            HttpResponse<String> credsResp = httpClient.send(credsReq, HttpResponse.BodyHandlers.ofString());
            if (credsResp.statusCode() != 200 || credsResp.body() == null || credsResp.body().isBlank()) {
                return null;
            }
            JsonNode node = objectMapper.readTree(credsResp.body());
            String ak = node.path("AccessKeyId").asText("");
            String sk = node.path("SecretAccessKey").asText("");
            String tok = node.path("Token").asText("");
            if (!ak.isEmpty() && !sk.isEmpty()) {
                return new AwsCredentials(ak, sk, tok);
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private static String resolveRegion(String configured) {
        String envRegion = System.getenv("AWS_REGION");
        if (envRegion != null && !envRegion.isBlank()) {
            return envRegion.trim();
        }
        String defaultRegion = System.getenv("AWS_DEFAULT_REGION");
        if (defaultRegion != null && !defaultRegion.isBlank()) {
            return defaultRegion.trim();
        }
        return (configured != null && !configured.isBlank()) ? configured.trim() : "eu-west-1";
    }

    private static String sha256Hex(byte[] data) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(md.digest(data));
    }

    private static byte[] hmacSha256(byte[] key, String data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] getSignatureKey(String key, String dateStamp, String regionName, String serviceName) throws Exception {
        byte[] kSecret = ("AWS4" + key).getBytes(StandardCharsets.UTF_8);
        byte[] kDate = hmacSha256(kSecret, dateStamp);
        byte[] kRegion = hmacSha256(kDate, regionName);
        byte[] kService = hmacSha256(kRegion, serviceName);
        return hmacSha256(kService, "aws4_request");
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private record AwsCredentials(String accessKeyId, String secretAccessKey, String sessionToken) {
    }
}
