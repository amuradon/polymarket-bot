package cz.polymarket.bot.live.monitoring;

/**
 * Immutable historical point-in-time telemetry sample stored in the bounded ring buffer
 * for time-series analysis of 4-core CPU utilization, OS TCP buffers, and WebSocket queue lag.
 *
 * @param timestampMs                epoch timestamp in milliseconds when the sample was recorded
 * @param processCpuLoadPct          JVM process CPU utilization percentage ({@code 0..100%})
 * @param systemCpuLoadPct           host system CPU utilization percentage ({@code 0..100%})
 * @param userWorkerCpuPct           vCPU #0 ({@code clob-user-worker}) CPU utilization percentage
 * @param ioReaderCpuPct             vCPU #1 ({@code market-ws-io-reader}) CPU utilization percentage
 * @param marketStrategyWorkerCpuPct vCPU #2 ({@code market-strategy-worker}) CPU utilization percentage
 * @param orderWorkerCpuPct          vCPU #3 ({@code order-execution-io-worker}) CPU utilization percentage
 * @param tcpRxQueueBytes            unread bytes queued in OS TCP receive buffers
 * @param clobQueueDepth             current inbound queue depth of the active CLOB market stream
 * @param clobMaxQueueLagMs          peak observed inbound queue lag in milliseconds of the CLOB market stream
 * @param lastQuoteAgeMs             age in milliseconds of the most recently evaluated orderbook quote
 * @param usedHeapMb                 currently used JVM heap memory in MiB
 */
public record TelemetryHistoryPoint(
        long timestampMs,
        double processCpuLoadPct,
        double systemCpuLoadPct,
        double userWorkerCpuPct,
        double ioReaderCpuPct,
        double marketStrategyWorkerCpuPct,
        double orderWorkerCpuPct,
        long tcpRxQueueBytes,
        int clobQueueDepth,
        long clobMaxQueueLagMs,
        long lastQuoteAgeMs,
        long usedHeapMb
) {
}
