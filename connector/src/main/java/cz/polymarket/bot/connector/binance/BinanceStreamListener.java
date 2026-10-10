package cz.polymarket.bot.connector.binance;

/**
 * Listener contract for receiving raw WebSocket payloads from Binance Spot and Futures streams.
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
}
