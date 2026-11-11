package cz.polymarket.bot.live.web;

import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import cz.polymarket.bot.live.monitoring.TelemetryHistoryPoint;
import cz.polymarket.bot.live.monitoring.ThreadUtilizationTelemetry;
import cz.polymarket.bot.live.monitoring.WebSocketDisconnectEvent;

import java.util.List;

/**
 * Comprehensive telemetry DTO combining OS kernel TCP buffer metrics, JVM ZGC metrics,
 * native transport/TLS status, 4-core dedicated thread utilization, live engine order execution latency,
 * per-stream WebSocket queue depths, unexpected disconnect diagnostic events, and historical telemetry series.
 *
 * @param kernelRmemMaxBytes     configured Linux {@code net.core.rmem_max} in bytes
 * @param currentTcpRxQueueBytes current unread bytes across active OS TCP receive queues
 * @param maxTcpRxQueueBytes     high-water mark of unread bytes across OS TCP receive queues
 * @param currentTcpTxQueueBytes current unsent bytes across active OS TCP transmit queues
 * @param maxTcpTxQueueBytes     high-water mark of unsent bytes across OS TCP transmit queues
 * @param activeTcpSockets       number of active non-listening TCP connections
 * @param tcpRcvCollapsed        Linux kernel {@code TCPRcvCollapsed} packet counter
 * @param tcpOfoQueue            Linux kernel {@code TCPOFOQueue} packet counter
 * @param tcpBacklogDrop         Linux kernel {@code TCPBacklogDrop} packet counter
 * @param gcName                 active JVM garbage collector name (e.g. {@code "ZGC"})
 * @param nativeTransportEnabled whether native Linux {@code EpollEventLoop} transport is active
 * @param openSslAvailable       whether native BoringSSL ({@code netty-tcnative}) is active
 * @param processCpuLoadPct      current JVM process CPU utilization percentage
 * @param maxProcessCpuLoadPct   peak JVM process CPU utilization percentage
 * @param systemCpuLoadPct       current host system CPU utilization percentage
 * @param gcCount                total GC collection count
 * @param totalGcPauseMs         total GC pause duration in milliseconds
 * @param usedHeapMb             used JVM heap memory in MiB
 * @param maxHeapMb              max JVM heap memory in MiB
 * @param ordersSubmitted        total live orders submitted to Polymarket CLOB REST API
 * @param ordersFilled           total live orders successfully matched/filled
 * @param ordersRejected         total live orders rejected or unfilled (e.g. FAK no match)
 * @param staleQuotesSkipped     total quotes skipped due to exceeding {@code maxQuoteAgeMs}
 * @param conflatedQuotesCount   total intermediate orderbook quotes overwritten in {@code latestQuoteSlot}
 * @param lastQuoteAgeMs         age in milliseconds of the most recently evaluated orderbook quote
 * @param maxQuoteAgeMsObserved  maximum observed age in milliseconds of an evaluated orderbook quote
 * @param lastOrderRttMs         round-trip execution latency in milliseconds of the most recent CLOB order
 * @param maxOrderRttMs          maximum observed round-trip execution latency in milliseconds
 * @param threads                utilization snapshots for the 4 dedicated pipeline threads
 * @param streams                queue depth and latency snapshots for all active WebSocket streams
 * @param disconnectEvents       recent unexpected WebSocket disconnect diagnostic snapshots
 * @param history                periodic historical telemetry time-series samples
 */
public record LiveTelemetryDto(
        long kernelRmemMaxBytes,
        long currentTcpRxQueueBytes,
        long maxTcpRxQueueBytes,
        long currentTcpTxQueueBytes,
        long maxTcpTxQueueBytes,
        int activeTcpSockets,
        long tcpRcvCollapsed,
        long tcpOfoQueue,
        long tcpBacklogDrop,
        String gcName,
        boolean nativeTransportEnabled,
        boolean openSslAvailable,
        double processCpuLoadPct,
        double maxProcessCpuLoadPct,
        double systemCpuLoadPct,
        long gcCount,
        long totalGcPauseMs,
        long usedHeapMb,
        long maxHeapMb,
        long ordersSubmitted,
        long ordersFilled,
        long ordersRejected,
        long staleQuotesSkipped,
        long conflatedQuotesCount,
        long lastQuoteAgeMs,
        long maxQuoteAgeMsObserved,
        long lastOrderRttMs,
        long maxOrderRttMs,
        List<ThreadUtilizationTelemetry> threads,
        List<WebSocketStreamTelemetry> streams,
        List<WebSocketDisconnectEvent> disconnectEvents,
        List<TelemetryHistoryPoint> history
) {
}
