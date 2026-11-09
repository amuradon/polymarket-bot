package cz.polymarket.bot.live.monitoring;

/**
 * Diagnostic snapshot captured at the exact moment a WebSocket stream disconnects unexpectedly
 * (e.g. {@code code=1013, reason=slow consumer: send buffer full}), preserving the stream queue state,
 * OS TCP receive buffer state, and 4-core CPU utilization at the time of disconnection.
 *
 * @param timestampMs            epoch timestamp in milliseconds when the disconnect occurred
 * @param streamName             identifier of the disconnected WebSocket stream
 * @param closeCode              WebSocket close status code (e.g. {@code 1013}, or {@code -1} if abnormal)
 * @param closeReason            WebSocket close reason string
 * @param reconnectCount         cumulative reconnect count of the stream at disconnect
 * @param queueDepthAtDisconnect number of frames waiting in the stream's inbound queue at disconnect
 * @param maxQueueDepth          peak inbound queue depth observed on the stream
 * @param lastQueueLagMs         most recent queue wait lag in milliseconds on the stream
 * @param maxQueueLagMs          peak queue wait lag in milliseconds observed on the stream
 * @param lastProcessTimeUs      most recent batch processing duration in microseconds
 * @param maxProcessTimeUs       peak batch processing duration in microseconds
 * @param tcpRxQueueBytes        OS TCP receive queue unread bytes at the moment of disconnect
 * @param tcpRcvCollapsed        Linux kernel {@code TCPRcvCollapsed} counter at disconnect
 * @param processCpuLoadPct      JVM process CPU utilization percentage at disconnect
 * @param ioReaderCpuPct         vCPU #1 ({@code clob-ws-io-reader}) CPU utilization percentage at disconnect
 * @param clobBookWorkerCpuPct   vCPU #2 ({@code clob-book-worker}) CPU utilization percentage at disconnect
 */
public record WebSocketDisconnectEvent(
        long timestampMs,
        String streamName,
        int closeCode,
        String closeReason,
        long reconnectCount,
        int queueDepthAtDisconnect,
        int maxQueueDepth,
        long lastQueueLagMs,
        long maxQueueLagMs,
        long lastProcessTimeUs,
        long maxProcessTimeUs,
        long tcpRxQueueBytes,
        long tcpRcvCollapsed,
        double processCpuLoadPct,
        double ioReaderCpuPct,
        double clobBookWorkerCpuPct
) {
}
