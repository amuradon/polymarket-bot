package cz.polymarket.bot.live.monitoring;

/**
 * Per-thread CPU and work duty-cycle telemetry snapshot for one of the 4 dedicated threads
 * in the Live Trading Engine architecture ({@code ws-io-reader}, {@code clob-book-worker},
 * {@code strategy-calculator-worker}, {@code order-execution-io-worker}).
 *
 * @param role                 architectural role label (e.g. {@code "vCPU #1: I/O Reader"})
 * @param threadName           JVM thread name
 * @param threadState          current {@link Thread.State} name (e.g. {@code "RUNNABLE"}, {@code "WAITING"})
 * @param cpuUtilizationPct    current single-core OS CPU utilization percentage ({@code 0.0..100.0})
 * @param maxCpuUtilizationPct peak observed single-core OS CPU utilization percentage ({@code 0.0..100.0})
 * @param busyDutyCyclePct     current active work duty cycle percentage vs parked wait time ({@code 0.0..100.0})
 * @param maxBusyDutyCyclePct  peak observed active work duty cycle percentage ({@code 0.0..100.0})
 * @param totalCpuTimeMs       cumulative OS CPU time consumed by this thread in milliseconds
 */
public record ThreadUtilizationTelemetry(
        String role,
        String threadName,
        String threadState,
        double cpuUtilizationPct,
        double maxCpuUtilizationPct,
        double busyDutyCyclePct,
        double maxBusyDutyCyclePct,
        long totalCpuTimeMs
) {
}
