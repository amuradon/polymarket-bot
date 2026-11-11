package cz.polymarket.bot.connector.ws;

/**
 * Callback interface for receiving raw WebSocket payloads paired with the local receipt timestamp.
 * <p>
 * Supports both decoded {@link String} delivery via {@link #onMessage(String, long)} and zero-decode
 * {@link TimestampedFrame} ({@code byte[]}) delivery via {@link #onMessageBytes(TimestampedFrame)}.
 */
@FunctionalInterface
public interface RawWebSocketListener {

    /**
     * Invoked when a text message arrives from the WebSocket.
     *
     * @param rawJson         unmodified JSON payload from the WebSocket frame
     * @param recvTimestampMs local system epoch timestamp in milliseconds at frame arrival
     */
    void onMessage(String rawJson, long recvTimestampMs);

    /**
     * Invoked with the raw {@link TimestampedFrame} before UTF-8 {@link String} decoding.
     * Consumers that persist or inspect raw UTF-8 {@code byte[]} directly can override this method
     * to avoid allocating a {@link String} for every WebSocket message.
     *
     * @param frame raw timestamped WebSocket frame
     */
    default void onMessageBytes(TimestampedFrame frame) {
        onMessage(frame.rawText(), frame.recvTs());
    }
}
