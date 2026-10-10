package cz.polymarket.bot.connector.ws;

/**
 * Callback interface for receiving raw WebSocket text payloads paired with the local receipt timestamp.
 */
@FunctionalInterface
public interface RawWebSocketListener {

    /**
     * Invoked on the Vert.x event loop immediately when a text message arrives from the WebSocket.
     *
     * @param rawJson         unmodified JSON payload from the WebSocket frame
     * @param recvTimestampMs local system epoch timestamp in milliseconds at frame arrival
     */
    void onMessage(String rawJson, long recvTimestampMs);
}
