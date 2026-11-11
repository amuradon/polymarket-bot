package cz.polymarket.bot.connector.ws;

/**
 * Listener notified whenever a {@link ReconnectingWebSocketStream} closes unexpectedly or encounters
 * a transport error while running.
 */
@FunctionalInterface
public interface WebSocketDisconnectListener {

    /**
     * Invoked on unexpected WebSocket disconnection with the stream's telemetry snapshot captured
     * at the exact instant of the disconnect.
     *
     * @param telemetry point-in-time stream telemetry snapshot at the moment of disconnection
     */
    void onUnexpectedDisconnect(WebSocketStreamTelemetry telemetry);
}
