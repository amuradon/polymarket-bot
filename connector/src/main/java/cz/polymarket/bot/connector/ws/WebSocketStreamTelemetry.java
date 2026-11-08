package cz.polymarket.bot.connector.ws;

/**
 * Point-in-time diagnostic telemetry snapshot for a single {@link ReconnectingWebSocketStream}.
 *
 * @param streamName            human-readable stream identifier
 * @param state                 current connection state
 * @param queueDepth            current number of frames waiting in {@code inboundQueue}
 * @param maxQueueDepth         high-water mark of frames waiting in {@code inboundQueue}
 * @param lastQueueLagMs        wait time in milliseconds of the most recently drained frame ({@code drainStartMs - recvTs})
 * @param maxQueueLagMs         maximum observed queue wait time in milliseconds
 * @param lastProcessTimeUs     processing duration in microseconds of the most recent batch/frame
 * @param maxProcessTimeUs      maximum observed processing duration in microseconds
 * @param totalFramesReceived   total number of non-heartbeat frames enqueued since stream creation
 * @param coalescedFramesCount  total number of intermediate frames coalesced or skipped during batch draining
 * @param reconnectCount        total number of reconnection attempts triggered since startup
 * @param lastMessageTimestampMs epoch millisecond timestamp of the last received frame or pong
 * @param lastCloseCode         WebSocket close status code from the last unexpected disconnect (or {@code 0} if none)
 * @param lastCloseReason       WebSocket close reason string from the last unexpected disconnect (or {@code ""})
 */
public record WebSocketStreamTelemetry(
        String streamName,
        ConnectionState state,
        int queueDepth,
        int maxQueueDepth,
        long lastQueueLagMs,
        long maxQueueLagMs,
        long lastProcessTimeUs,
        long maxProcessTimeUs,
        long totalFramesReceived,
        long coalescedFramesCount,
        long reconnectCount,
        long lastMessageTimestampMs,
        int lastCloseCode,
        String lastCloseReason
) {
}
