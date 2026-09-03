package cz.polymarket.bot.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.service.TwapEngine;
import io.quarkus.websockets.next.OnClose;
import io.quarkus.websockets.next.OnOpen;
import io.quarkus.websockets.next.OnTextMessage;
import io.quarkus.websockets.next.WebSocket;
import io.quarkus.websockets.next.WebSocketConnection;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@WebSocket(path = "/ws/twap")
public class TwapWebSocketEndpoint {

    private static final Logger LOG = Logger.getLogger(TwapWebSocketEndpoint.class);

    private final ObjectMapper objectMapper;
    private final Map<String, WebSocketConnection> activeConnections = new ConcurrentHashMap<>();
    private final Map<String, String> connectionTimeframes = new ConcurrentHashMap<>();

    @Inject
    public TwapWebSocketEndpoint(TwapEngine twapEngine, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        twapEngine.registerListener(this::broadcastUpdate);
    }

    @OnOpen
    public void onOpen(WebSocketConnection connection) {
        LOG.infof("WebSocket client connected: %s", connection.id());
        activeConnections.put(connection.id(), connection);
        connectionTimeframes.put(connection.id(), "5m"); // Default 5m
    }

    @OnTextMessage
    public void onMessage(String message, WebSocketConnection connection) {
        try {
            JsonNode node = objectMapper.readTree(message);
            if ("setTimeframe".equals(node.path("action").asText()) && node.hasNonNull("timeframe")) {
                String tf = node.get("timeframe").asText();
                connectionTimeframes.put(connection.id(), tf);
                LOG.infof("Client %s set timeframe to %s", connection.id(), tf);
            }
        } catch (Exception e) {
            LOG.warnf("Error parsing websocket message from client %s: %s", connection.id(), e.getMessage());
        }
    }

    @OnClose
    public void onClose(WebSocketConnection connection) {
        LOG.infof("WebSocket client disconnected: %s", connection.id());
        activeConnections.remove(connection.id());
        connectionTimeframes.remove(connection.id());
    }

    public void broadcastUpdate(TwapUpdate update) {
        String updateTimeframe = update.timeframe().getCode();
        String json;
        try {
            json = objectMapper.writeValueAsString(update);
        } catch (Exception e) {
            LOG.warnf("Failed to serialize TwapUpdate: %s", e.getMessage());
            return;
        }

        for (Map.Entry<String, WebSocketConnection> entry : activeConnections.entrySet()) {
            String connId = entry.getKey();
            WebSocketConnection conn = entry.getValue();
            String desiredTf = connectionTimeframes.getOrDefault(connId, "5m");

            if (desiredTf.equalsIgnoreCase(updateTimeframe)) {
                conn.sendText(json).subscribe().with(
                        v -> {},
                        err -> LOG.warnf("Failed to send text to client %s: %s", connId, err.getMessage())
                );
            }
        }
    }
}
