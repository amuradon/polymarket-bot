package cz.polymarket.bot.connector.polymarket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * REST client for Polymarket Gamma API ({@code https://gamma-api.polymarket.com}) supporting
 * market discovery by slug and resolution status polling with minimal JSON parsing.
 */
@ApplicationScoped
public class PolymarketGammaClient {

    private static final Logger LOG = Logger.getLogger(PolymarketGammaClient.class);

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    /**
     * Constructs the Polymarket Gamma API client.
     *
     * @param baseUrl      Gamma API base URL
     * @param httpClient   shared HTTP client
     * @param objectMapper Jackson object mapper
     */
    @Inject
    public PolymarketGammaClient(
            @ConfigProperty(name = "polymarket.connector.gamma.base-url", defaultValue = "https://gamma-api.polymarket.com")
            String baseUrl,
            HttpClient httpClient,
            ObjectMapper objectMapper) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    /**
     * Queries the Gamma API for a specific market by its slug, extracting only the token IDs
     * and closed status needed for stream management while preserving the raw JSON response.
     *
     * @param slug            market slug (e.g. btc-updown-15m-1789041600)
     * @param durationSeconds market interval duration in seconds (e.g. 900 for 15m)
     * @return parsed {@link PolymarketMarketInfo} if the market exists on Polymarket
     */
    public Optional<PolymarketMarketInfo> fetchMarketBySlug(String slug, long durationSeconds) {
        if (slug == null || slug.isBlank()) {
            return Optional.empty();
        }
        String encodedSlug = URLEncoder.encode(slug, StandardCharsets.UTF_8);
        String url = baseUrl + "/markets?slug=" + encodedSlug;

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("User-Agent", "polymarket-bot")
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200 || response.body() == null || response.body().isBlank()) {
                return Optional.empty();
            }

            JsonNode root = objectMapper.readTree(response.body());
            if (root.isArray() && !root.isEmpty()) {
                return parseMarketNode(root.get(0), durationSeconds);
            }
        } catch (Exception e) {
            LOG.debugf("Failed to fetch Gamma market for slug %s: %s", slug, e.getMessage());
        }
        return Optional.empty();
    }

    private Optional<PolymarketMarketInfo> parseMarketNode(JsonNode item, long durationSeconds) {
        if (item == null || !item.isObject()) {
            return Optional.empty();
        }
        try {
            String slug = item.path("slug").asText("");
            long startSec = extractStartSecFromSlug(slug);
            long endSec = startSec > 0 ? startSec + durationSeconds : 0L;
            List<String> clobTokens = parseStringListField(item.get("clobTokenIds"));
            if (clobTokens.isEmpty()) {
                return Optional.empty();
            }
            boolean closed = item.path("closed").asBoolean(false);
            return Optional.of(new PolymarketMarketInfo(
                    slug,
                    startSec,
                    endSec,
                    List.copyOf(clobTokens),
                    closed,
                    objectMapper.writeValueAsString(item)
            ));
        } catch (Exception e) {
            LOG.warnf("Failed to parse Gamma market node: %s", e.getMessage());
            return Optional.empty();
        }
    }

    private long extractStartSecFromSlug(String slug) {
        if (slug == null || slug.isBlank()) {
            return 0L;
        }
        int lastDash = slug.lastIndexOf('-');
        if (lastDash >= 0 && lastDash + 1 < slug.length()) {
            try {
                return Long.parseLong(slug.substring(lastDash + 1));
            } catch (NumberFormatException ignored) {
            }
        }
        return 0L;
    }

    private List<String> parseStringListField(JsonNode fieldNode) {
        List<String> values = new ArrayList<>();
        if (fieldNode == null || fieldNode.isNull()) {
            return values;
        }
        try {
            JsonNode arrayNode = fieldNode;
            if (fieldNode.isTextual()) {
                arrayNode = objectMapper.readTree(fieldNode.asText());
            }
            if (arrayNode != null && arrayNode.isArray()) {
                for (JsonNode elem : arrayNode) {
                    String val = elem.asText();
                    if (val != null && !val.isBlank()) {
                        values.add(val);
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return values;
    }
}
