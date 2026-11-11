package cz.polymarket.bot.live.monitoring;

/**
 * Immutable historical point-in-time telemetry sample stored in the bounded ring buffer
 * for time-series analysis of CPU utilization, OS TCP buffers, and WebSocket queue lag.
 *
 * @param timestampMs          epoch timestamp in milliseconds when the sample was recorded
 * @param processCpuLoadPct    JVM process CPU utilization percentage ({@code 0..100%})
 * @param systemCpuLoadPct     host system CPU utilization percentage ({@code 0..100%})
 * @param ioReaderCpuPct       vCPU #1 ({@code clob-ws-io-reader}) CPU utilization percentage
 * @param clobBookWorkerCpuPct vCPU #2 ({@code clob-book-worker}) CPU utilization percentage
 * @param strategyWorkerCpuPct vCPU #3 ({@code strategy-calculator-worker}) CPU utilization percentage
 * @param orderWorkerCpuPct    vCPU #4 ({@code order-execution-io-worker}) CPU utilization percentage
 * @param tcpRxQueueBytes      unread bytes queued in OS TCP receive buffers
 * @param clobQueueDepth       current inbound queue depth of the active CLOB market stream
 * @param clobMaxQueueLagMs    peak observed inbound queue lag in milliseconds of the CLOB market stream
 * @param lastQuoteAgeMs       age in milliseconds of the most recently evaluated orderbook quote
 * @param usedHeapMb           currently used JVM heap memory in MiB
 */
public record TelemetryHistoryPoint(
        long timestampMs,
        double processCpuLoadPct,
        double systemCpuLoadPct,
        double ioReaderCpuPct,
        double clobBookWorkerCpuPct,
        double strategyWorkerCpuPct,
        double orderWorkerCpuPct,
        long tcpRxQueueBytes,
        int clobQueueDepth,
        long clobMaxQueueLagMs,
        long lastQuoteAgeMs,
        long usedHeapMb
) {
}
