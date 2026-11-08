package cz.polymarket.bot.connector.ws;

/**
 * Immutable WebSocket text frame paired with its local epoch millisecond receipt timestamp
 * captured immediately upon socket arrival on the I/O reader thread.
 *
 * @param rawText raw UTF-8 JSON text payload received from the WebSocket
 * @param recvTs  local receipt timestamp in epoch milliseconds
 */
public record TimestampedFrame(String rawText, long recvTs) {
}
