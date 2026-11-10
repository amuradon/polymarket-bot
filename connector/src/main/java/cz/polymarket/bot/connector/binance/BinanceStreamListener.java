package cz.polymarket.bot.connector.binance;

import java.nio.charset.StandardCharsets;

/**
 * Listener contract for receiving raw WebSocket payloads from Binance Spot and Futures streams.
 * Supports both decoded {@link String} delivery and zero-decode UTF-8 {@code byte[]} delivery.
 */
@FunctionalInterface
public interface BinanceStreamListener {

    /**
     * Invoked when a raw JSON payload arrives on one of the Binance WebSocket streams.
     *
     * @param streamType      the Binance stream that produced the message
     * @param rawJson         raw JSON payload received from the WebSocket
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    void onStreamMessage(BinanceStreamType streamType, String rawJson, long recvTimestampMs);

    /**
     * Invoked with the raw UTF-8 {@code byte[]} payload before {@link String} decoding.
     * Consumers such as Zstandard disk writers can override this method to write {@code byte[]} directly
     * with zero {@link String} allocation.
     *
     * @param streamType      the Binance stream that produced the message
     * @param rawUtf8Bytes    unmodified raw UTF-8 bytes from the WebSocket frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    default void onStreamFrame(BinanceStreamType streamType, byte[] rawUtf8Bytes, long recvTimestampMs) {
        if (rawUtf8Bytes != null && rawUtf8Bytes.length > 0) {
            onStreamMessage(streamType, new String(rawUtf8Bytes, StandardCharsets.UTF_8), recvTimestampMs);
        }
    }
}
