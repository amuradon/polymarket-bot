package cz.polymarket.bot.live.monitoring;

import jakarta.enterprise.context.ApplicationScoped;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Low-overhead OS kernel network buffer, JVM ZGC, per-thread utilization, and historical
 * telemetry ring-buffer monitor.
 * <p>
 * Periodically invoked off the hot path (once per second on {@code order-execution-io-worker}) to read:
 * <ul>
 *   <li>Linux kernel socket queues from {@code /proc/net/tcp} and {@code /proc/net/tcp6} ({@code rx_queue} and {@code tx_queue})</li>
 *   <li>Configured kernel max TCP receive buffer from {@code /proc/sys/net/core/rmem_max}</li>
 *   <li>Kernel TCP packet collapse and out-of-order counters from {@code /proc/net/netstat}</li>
 *   <li>Per-thread OS CPU time via {@link ThreadMXBean#getThreadCpuTime(long)} and nanosecond work duty cycle</li>
 *   <li>JVM CPU load, Garbage Collector pauses, and heap usage</li>
 *   <li>Bounded historical time-series samples and WebSocket disconnect diagnostic events</li>
 * </ul>
 */
@ApplicationScoped
public class SystemNetworkTelemetryMonitor {

    /**
     * Maximum number of periodic historical telemetry points retained in memory (360 points = 60 minutes at 10s interval).
     */
    public static final int MAX_HISTORY_POINTS = 360;

    /**
     * Maximum number of unexpected WebSocket disconnect diagnostic snapshots retained in memory.
     */
    public static final int MAX_DISCONNECT_EVENTS = 50;

    private static final Path PROC_NET_TCP = Path.of("/proc/net/tcp");
    private static final Path PROC_NET_TCP6 = Path.of("/proc/net/tcp6");
    private static final Path PROC_RMEM_MAX = Path.of("/proc/sys/net/core/rmem_max");
    private static final Path PROC_NET_NETSTAT = Path.of("/proc/net/netstat");

    /**
     * Descriptor of a monitored dedicated thread passed during periodic sampling.
     *
     * @param role             human-readable core/thread role
     * @param fallbackName     fallback thread name when the thread is not yet started
     * @param thread           live {@link Thread} reference (may be {@code null} if not started)
     * @param cumulativeBusyNs cumulative nanosecond busy time tracked by the worker loop (or {@code -1} to derive from CPU time)
     */
    public record MonitoredThreadSpec(
            String role,
            String fallbackName,
            Thread thread,
            long cumulativeBusyNs
    ) {
    }

    /**
     * Parsed snapshot of Linux kernel TCP socket queues from {@code /proc/net/tcp} and {@code /proc/net/tcp6}.
     *
     * @param rxQueueBytes  sum of unread bytes across active TCP receive queues
     * @param txQueueBytes  sum of unsent bytes across active TCP transmit queues
     * @param activeSockets number of active non-listening TCP sockets
     */
    public record TcpQueueStats(
            long rxQueueBytes,
            long txQueueBytes,
            int activeSockets
    ) {
    }

    /**
     * Parsed snapshot of Linux kernel TCP extended counters from {@code /proc/net/netstat}.
     *
     * @param tcpRcvCollapsed packets collapsed in receive queue due to buffer pressure ({@code TCPRcvCollapsed})
     * @param tcpOfoQueue     packets queued out-of-order ({@code TCPOFOQueue})
     * @param tcpBacklogDrop  packets dropped because socket backlog was full ({@code TCPBacklogDrop})
     */
    public record TcpNetstatStats(
            long tcpRcvCollapsed,
            long tcpOfoQueue,
            long tcpBacklogDrop
    ) {
    }

    /**
     * Immutable point-in-time snapshot of OS network, JVM, and per-thread utilization metrics.
     *
     * @param kernelRmemMaxBytes     configured Linux {@code net.core.rmem_max} in bytes
     * @param currentTcpRxQueueBytes current bytes queued in OS TCP receive buffers
     * @param maxTcpRxQueueBytes     high-water mark of bytes queued in OS TCP receive buffers
     * @param currentTcpTxQueueBytes current bytes queued in OS TCP transmit buffers
     * @param maxTcpTxQueueBytes     high-water mark of bytes queued in OS TCP transmit buffers
     * @param activeTcpSockets       number of active TCP connections
     * @param tcpRcvCollapsed        kernel {@code TCPRcvCollapsed} counter
     * @param tcpOfoQueue            kernel {@code TCPOFOQueue} counter
     * @param tcpBacklogDrop         kernel {@code TCPBacklogDrop} counter
     * @param gcName                 active JVM Garbage Collector name (e.g. {@code "ZGC"})
     * @param processCpuLoadPct      current JVM process CPU utilization ({@code 0..100%})
     * @param maxProcessCpuLoadPct   peak JVM process CPU utilization ({@code 0..100%})
     * @param systemCpuLoadPct       current host system CPU utilization ({@code 0..100%})
     * @param gcCount                total GC collection count
     * @param totalGcPauseMs         total GC collection time in milliseconds
     * @param usedHeapMb             currently used JVM heap memory in MiB
     * @param maxHeapMb              maximum JVM heap memory in MiB
     * @param threads                utilization snapshots for the 4 dedicated threads
     */
    public record SystemTelemetrySnapshot(
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
            double processCpuLoadPct,
            double maxProcessCpuLoadPct,
            double systemCpuLoadPct,
            long gcCount,
            long totalGcPauseMs,
            long usedHeapMb,
            long maxHeapMb,
            List<ThreadUtilizationTelemetry> threads
    ) {
    }

    private static final class ThreadHistory {
        long lastThreadId = -1L;
        long lastSampleWallNs;
        long lastCpuTimeNs;
        long lastBusyTimeNs;
        double lastCpuPct;
        double maxCpuPct;
        double lastBusyPct;
        double maxBusyPct;
    }

    private final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    private final MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
    private final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    private final Map<String, ThreadHistory> threadHistories = new ConcurrentHashMap<>();

    private final AtomicLong maxTcpRxQueueBytes = new AtomicLong(0L);
    private final AtomicLong maxTcpTxQueueBytes = new AtomicLong(0L);
    private volatile double maxProcessCpuLoadPct = 0.0;
    private final AtomicReference<SystemTelemetrySnapshot> latestSnapshot = new AtomicReference<>(emptySnapshot());

    private final Deque<TelemetryHistoryPoint> historyRing = new ArrayDeque<>(MAX_HISTORY_POINTS);
    private final Deque<WebSocketDisconnectEvent> disconnectEventsRing = new ArrayDeque<>(MAX_DISCONNECT_EVENTS);

    public SystemNetworkTelemetryMonitor() {
        try {
            if (threadMxBean.isThreadCpuTimeSupported() && !threadMxBean.isThreadCpuTimeEnabled()) {
                threadMxBean.setThreadCpuTimeEnabled(true);
            }
        } catch (Exception ignored) {
        }
    }

    /**
     * Samples OS kernel TCP buffers, JVM CPU/GC/memory, and per-thread utilization for the provided thread specs.
     *
     * @param threadSpecs descriptors of the 4 dedicated threads
     * @return updated {@link SystemTelemetrySnapshot}
     */
    public SystemTelemetrySnapshot sample(List<MonitoredThreadSpec> threadSpecs) {
        long nowNs = System.nanoTime();

        long rmemMax = readKernelRmemMaxBytes();
        TcpQueueStats tcpStats = readKernelTcpQueues();
        updateMaxLong(maxTcpRxQueueBytes, tcpStats.rxQueueBytes());
        updateMaxLong(maxTcpTxQueueBytes, tcpStats.txQueueBytes());

        TcpNetstatStats netstat = readKernelTcpNetstat();

        double procCpuPct = 0.0;
        double sysCpuPct = 0.0;
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean osBean) {
            double p = osBean.getProcessCpuLoad();
            double s = osBean.getCpuLoad();
            if (p >= 0.0 && Double.isFinite(p)) {
                procCpuPct = roundOneDecimal(p * 100.0);
                if (procCpuPct > maxProcessCpuLoadPct) {
                    maxProcessCpuLoadPct = procCpuPct;
                }
            }
            if (s >= 0.0 && Double.isFinite(s)) {
                sysCpuPct = roundOneDecimal(s * 100.0);
            }
        }

        long totalGcCount = 0L;
        long totalGcTimeMs = 0L;
        StringBuilder gcNames = new StringBuilder();
        for (GarbageCollectorMXBean gc : gcBeans) {
            long c = gc.getCollectionCount();
            if (c > 0) {
                totalGcCount += c;
            }
            long t = gc.getCollectionTime();
            if (t > 0) {
                totalGcTimeMs += t;
            }
            if (gcNames.isEmpty() && gc.getName() != null) {
                gcNames.append(gc.getName());
            }
        }
        String gcName = gcNames.isEmpty() ? "Standard" : gcNames.toString();

        MemoryUsage heap = memoryMxBean.getHeapMemoryUsage();
        long usedHeapMb = heap.getUsed() / (1024L * 1024L);
        long maxHeapMb = Math.max(usedHeapMb, heap.getMax() / (1024L * 1024L));

        List<ThreadUtilizationTelemetry> threadTelemetries = sampleThreads(threadSpecs, nowNs);

        SystemTelemetrySnapshot snapshot = new SystemTelemetrySnapshot(
                rmemMax,
                tcpStats.rxQueueBytes(),
                maxTcpRxQueueBytes.get(),
                tcpStats.txQueueBytes(),
                maxTcpTxQueueBytes.get(),
                tcpStats.activeSockets(),
                netstat.tcpRcvCollapsed(),
                netstat.tcpOfoQueue(),
                netstat.tcpBacklogDrop(),
                gcName,
                procCpuPct,
                maxProcessCpuLoadPct,
                sysCpuPct,
                totalGcCount,
                totalGcTimeMs,
                usedHeapMb,
                maxHeapMb,
                threadTelemetries
        );
        latestSnapshot.set(snapshot);
        return snapshot;
    }

    /**
     * Returns the most recently sampled {@link SystemTelemetrySnapshot}, or triggers a fresh sample if none exists yet.
     *
     * @param threadSpecs descriptors of the 4 dedicated threads
     * @return current {@link SystemTelemetrySnapshot}
     */
    public SystemTelemetrySnapshot getLatestOrSample(List<MonitoredThreadSpec> threadSpecs) {
        SystemTelemetrySnapshot current = latestSnapshot.get();
        if (current.threads().isEmpty() && threadSpecs != null && !threadSpecs.isEmpty()) {
            return sample(threadSpecs);
        }
        return current;
    }

    /**
     * Appends a periodic telemetry history point to the bounded in-memory ring buffer.
     * When the buffer reaches {@link #MAX_HISTORY_POINTS}, the oldest entry is evicted.
     *
     * @param point point-in-time telemetry sample
     */
    public void recordHistoryPoint(TelemetryHistoryPoint point) {
        if (point == null) {
            return;
        }
        synchronized (historyRing) {
            while (historyRing.size() >= MAX_HISTORY_POINTS) {
                historyRing.pollFirst();
            }
            historyRing.addLast(point);
        }
    }

    /**
     * Returns an immutable chronological copy of all retained historical telemetry points.
     *
     * @return list of {@link TelemetryHistoryPoint}
     */
    public List<TelemetryHistoryPoint> getHistory() {
        synchronized (historyRing) {
            return List.copyOf(historyRing);
        }
    }

    /**
     * Appends an unexpected WebSocket disconnect diagnostic event to the bounded ring buffer.
     * When the buffer reaches {@link #MAX_DISCONNECT_EVENTS}, the oldest event is evicted.
     *
     * @param event disconnect diagnostic snapshot
     */
    public void recordDisconnectEvent(WebSocketDisconnectEvent event) {
        if (event == null) {
            return;
        }
        synchronized (disconnectEventsRing) {
            while (disconnectEventsRing.size() >= MAX_DISCONNECT_EVENTS) {
                disconnectEventsRing.pollFirst();
            }
            disconnectEventsRing.addLast(event);
        }
    }

    /**
     * Returns an immutable copy of all retained unexpected WebSocket disconnect events.
     *
     * @return list of {@link WebSocketDisconnectEvent}
     */
    public List<WebSocketDisconnectEvent> getDisconnectEvents() {
        synchronized (disconnectEventsRing) {
            return List.copyOf(disconnectEventsRing);
        }
    }

    private List<ThreadUtilizationTelemetry> sampleThreads(List<MonitoredThreadSpec> specs, long nowNs) {
        if (specs == null || specs.isEmpty()) {
            return List.of();
        }
        List<ThreadUtilizationTelemetry> result = new ArrayList<>(specs.size());
        for (MonitoredThreadSpec spec : specs) {
            Thread t = spec.thread();
            long currentThreadId = t != null ? t.threadId() : -1L;
            String threadName = t != null ? t.getName() : spec.fallbackName();
            String stateStr = t != null ? t.getState().name() : "INACTIVE";
            long cpuNs = 0L;
            if (t != null && t.isAlive() && threadMxBean.isThreadCpuTimeSupported()) {
                try {
                    long raw = threadMxBean.getThreadCpuTime(currentThreadId);
                    if (raw > 0L) {
                        cpuNs = raw;
                    }
                } catch (Exception ignored) {
                }
            }
            long busyNs = spec.cumulativeBusyNs() >= 0L ? Math.max(cpuNs, spec.cumulativeBusyNs()) : cpuNs;

            ThreadHistory hist = threadHistories.computeIfAbsent(spec.role(), k -> new ThreadHistory());
            synchronized (hist) {
                if (currentThreadId != hist.lastThreadId) {
                    hist.lastThreadId = currentThreadId;
                    hist.lastSampleWallNs = nowNs;
                    hist.lastCpuTimeNs = cpuNs;
                    hist.lastBusyTimeNs = busyNs;
                } else {
                    long elapsedWallNs = nowNs - hist.lastSampleWallNs;
                    if (hist.lastSampleWallNs > 0L && elapsedWallNs >= 50_000_000L) {
                        long deltaCpuNs = Math.max(0L, cpuNs - hist.lastCpuTimeNs);
                        long deltaBusyNs = Math.max(0L, busyNs - hist.lastBusyTimeNs);
                        hist.lastCpuPct = roundOneDecimal(Math.min(100.0, (deltaCpuNs * 100.0) / elapsedWallNs));
                        hist.lastBusyPct = roundOneDecimal(Math.min(100.0, (deltaBusyNs * 100.0) / elapsedWallNs));
                        if (hist.lastCpuPct > hist.maxCpuPct) {
                            hist.maxCpuPct = hist.lastCpuPct;
                        }
                        if (hist.lastBusyPct > hist.maxBusyPct) {
                            hist.maxBusyPct = hist.lastBusyPct;
                        }
                        hist.lastSampleWallNs = nowNs;
                        hist.lastCpuTimeNs = cpuNs;
                        hist.lastBusyTimeNs = busyNs;
                    } else if (hist.lastSampleWallNs == 0L) {
                        hist.lastSampleWallNs = nowNs;
                        hist.lastCpuTimeNs = cpuNs;
                        hist.lastBusyTimeNs = busyNs;
                    }
                }
                result.add(new ThreadUtilizationTelemetry(
                        spec.role(),
                        threadName,
                        stateStr,
                        hist.lastCpuPct,
                        hist.maxCpuPct,
                        hist.lastBusyPct,
                        hist.maxBusyPct,
                        cpuNs / 1_000_000L
                ));
            }
        }
        return result;
    }

    /**
     * Parses lines from Linux {@code /proc/net/tcp} or {@code /proc/net/tcp6} and aggregates
     * {@code rx_queue} and {@code tx_queue} bytes across active non-listening sockets.
     *
     * @param lines lines read from {@code /proc/net/tcp}
     * @return aggregated {@link TcpQueueStats}
     */
    public static TcpQueueStats parseProcNetTcpLines(List<String> lines) {
        if (lines == null || lines.isEmpty()) {
            return new TcpQueueStats(0L, 0L, 0);
        }
        long totalRxBytes = 0L;
        long totalTxBytes = 0L;
        int activeCount = 0;

        for (String rawLine : lines) {
            if (rawLine == null) {
                continue;
            }
            String line = rawLine.trim();
            if (line.isEmpty() || line.startsWith("sl")) {
                continue;
            }
            String[] parts = line.split("\\s+");
            if (parts.length < 5) {
                continue;
            }
            String stateHex = parts[3];
            // Skip LISTEN (0A) and TIME_WAIT (06) sockets
            if ("0A".equalsIgnoreCase(stateHex) || "06".equalsIgnoreCase(stateHex)) {
                continue;
            }
            String txRx = parts[4];
            int colonIdx = txRx.indexOf(':');
            if (colonIdx <= 0 || colonIdx >= txRx.length() - 1) {
                continue;
            }
            try {
                long tx = Long.parseUnsignedLong(txRx.substring(0, colonIdx), 16);
                long rx = Long.parseUnsignedLong(txRx.substring(colonIdx + 1), 16);
                totalTxBytes += tx;
                totalRxBytes += rx;
                activeCount++;
            } catch (NumberFormatException ignored) {
            }
        }
        return new TcpQueueStats(totalRxBytes, totalTxBytes, activeCount);
    }

    /**
     * Parses lines from Linux {@code /proc/net/netstat} to extract {@code TCPRcvCollapsed},
     * {@code TCPOFOQueue}, and {@code TCPBacklogDrop}.
     *
     * @param lines lines read from {@code /proc/net/netstat}
     * @return parsed {@link TcpNetstatStats}
     */
    public static TcpNetstatStats parseProcNetNetstatLines(List<String> lines) {
        if (lines == null || lines.size() < 2) {
            return new TcpNetstatStats(0L, 0L, 0L);
        }
        String[] headers = null;
        for (String line : lines) {
            if (line == null || !line.startsWith("TcpExt:")) {
                continue;
            }
            String[] tokens = line.trim().split("\\s+");
            if (headers == null) {
                headers = tokens;
            } else {
                long collapsed = 0L;
                long ofo = 0L;
                long backlogDrop = 0L;
                int len = Math.min(headers.length, tokens.length);
                for (int i = 1; i < len; i++) {
                    switch (headers[i]) {
                        case "TCPRcvCollapsed" -> collapsed = parseSafeLong(tokens[i]);
                        case "TCPOFOQueue" -> ofo = parseSafeLong(tokens[i]);
                        case "TCPBacklogDrop" -> backlogDrop = parseSafeLong(tokens[i]);
                        default -> {
                        }
                    }
                }
                return new TcpNetstatStats(collapsed, ofo, backlogDrop);
            }
        }
        return new TcpNetstatStats(0L, 0L, 0L);
    }

    private static TcpQueueStats readKernelTcpQueues() {
        long rx = 0L;
        long tx = 0L;
        int sockets = 0;
        try {
            if (Files.isReadable(PROC_NET_TCP)) {
                TcpQueueStats v4 = parseProcNetTcpLines(Files.readAllLines(PROC_NET_TCP));
                rx += v4.rxQueueBytes();
                tx += v4.txQueueBytes();
                sockets += v4.activeSockets();
            }
            if (Files.isReadable(PROC_NET_TCP6)) {
                TcpQueueStats v6 = parseProcNetTcpLines(Files.readAllLines(PROC_NET_TCP6));
                rx += v6.rxQueueBytes();
                tx += v6.txQueueBytes();
                sockets += v6.activeSockets();
            }
        } catch (Exception ignored) {
        }
        return new TcpQueueStats(rx, tx, sockets);
    }

    private static long readKernelRmemMaxBytes() {
        try {
            if (Files.isReadable(PROC_RMEM_MAX)) {
                List<String> lines = Files.readAllLines(PROC_RMEM_MAX);
                if (!lines.isEmpty() && lines.getFirst() != null) {
                    return parseSafeLong(lines.getFirst().trim());
                }
            }
        } catch (Exception ignored) {
        }
        return 0L;
    }

    private static TcpNetstatStats readKernelTcpNetstat() {
        try {
            if (Files.isReadable(PROC_NET_NETSTAT)) {
                return parseProcNetNetstatLines(Files.readAllLines(PROC_NET_NETSTAT));
            }
        } catch (Exception ignored) {
        }
        return new TcpNetstatStats(0L, 0L, 0L);
    }

    private static long parseSafeLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return 0L;
        }
    }

    private static double roundOneDecimal(double val) {
        return Math.round(val * 10.0) / 10.0;
    }

    private static void updateMaxLong(AtomicLong holder, long candidate) {
        long prev = holder.get();
        while (candidate > prev) {
            if (holder.compareAndSet(prev, candidate)) {
                return;
            }
            prev = holder.get();
        }
    }

    private static SystemTelemetrySnapshot emptySnapshot() {
        return new SystemTelemetrySnapshot(
                0L, 0L, 0L, 0L, 0L, 0, 0L, 0L, 0L,
                "ZGC", 0.0, 0.0, 0.0, 0L, 0L, 0L, 0L,
                List.of()
        );
    }
}
