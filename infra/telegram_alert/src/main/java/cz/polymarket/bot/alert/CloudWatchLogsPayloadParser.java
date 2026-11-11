package cz.polymarket.bot.alert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.alert.model.CloudWatchLogsPayload;
import cz.polymarket.bot.alert.model.LogEvent;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Decodes Base64, decompresses GZIP, and parses the raw CloudWatch Logs subscription filter payload.
 */
public class CloudWatchLogsPayloadParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Parses the Base64-encoded, GZIP-compressed string into a structured {@link CloudWatchLogsPayload}.
     *
     * @param base64GzipData raw Base64 string from {@code event.awslogs.data}
     * @return deserialized {@link CloudWatchLogsPayload}
     * @throws IllegalArgumentException if the data is null, blank, corrupt, or invalid JSON
     */
    public CloudWatchLogsPayload parse(String base64GzipData) {
        if (base64GzipData == null || base64GzipData.isBlank()) {
            throw new IllegalArgumentException("CloudWatch Logs payload data cannot be null or blank");
        }

        try {
            byte[] compressed = Base64.getDecoder().decode(base64GzipData.trim());
            String decompressedJson;
            try (InputStream bais = new ByteArrayInputStream(compressed);
                 GZIPInputStream gzis = new GZIPInputStream(bais)) {
                decompressedJson = new String(gzis.readAllBytes(), StandardCharsets.UTF_8);
            }

            JsonNode root = OBJECT_MAPPER.readTree(decompressedJson);
            String messageType = root.path("messageType").asText("");
            String owner = root.path("owner").asText("");
            String logGroup = root.path("logGroup").asText("");
            String logStream = root.path("logStream").asText("");

            List<String> subscriptionFilters = new ArrayList<>();
            JsonNode filtersNode = root.path("subscriptionFilters");
            if (filtersNode.isArray()) {
                for (JsonNode f : filtersNode) {
                    subscriptionFilters.add(f.asText());
                }
            }

            List<LogEvent> logEvents = new ArrayList<>();
            JsonNode eventsNode = root.path("logEvents");
            if (eventsNode.isArray()) {
                for (JsonNode ev : eventsNode) {
                    String id = ev.path("id").asText("");
                    long timestamp = ev.path("timestamp").asLong(0L);
                    String message = ev.path("message").asText("");
                    logEvents.add(new LogEvent(id, timestamp, message));
                }
            }

            return new CloudWatchLogsPayload(
                    messageType,
                    owner,
                    logGroup,
                    logStream,
                    subscriptionFilters,
                    logEvents
            );
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to decompress or parse CloudWatch Logs payload: " + e.getMessage(), e);
        }
    }
}
