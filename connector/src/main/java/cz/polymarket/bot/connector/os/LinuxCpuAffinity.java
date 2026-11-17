package cz.polymarket.bot.connector.os;

import io.quarkus.logging.Log;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Zero-dependency Linux CPU thread-affinity helper backed by Java 25 Foreign Function &amp; Memory
 * ({@link Linker#nativeLinker()}) invoking glibc {@code sched_setaffinity(2)}.
 * <p>
 * Automatically pins dedicated hot-path threads ({@code CPU 0..3}) on 4-vCPU Linux hosts (such as AWS
 * {@code c8gn.xlarge}) while acting as a safe no-op on Windows/macOS or smaller hosts (&lt; 4 online CPUs,
 * such as {@code t4g.small} Data Collector instances).
 */
public final class LinuxCpuAffinity {

    /** Minimum number of online host CPUs required before 4-core pinning is activated. */
    public static final int MIN_DEDICATED_TOPOLOGY_CPUS = 4;

    private static final int CPU_SET_SIZE_BYTES = 128;
    private static final int MAX_SUPPORTED_CPU_INDEX = 63;
    private static final Path SYS_CPU_ONLINE_PATH = Path.of("/sys/devices/system/cpu/online");

    private static final boolean IS_LINUX = System.getProperty("os.name", "")
            .toLowerCase(Locale.ROOT)
            .startsWith("linux");
    private static final int ONLINE_CPU_COUNT = detectOnlineCpuCount();
    private static final MethodHandle SCHED_SETAFFINITY_HANDLE = resolveSchedSetAffinityHandle();

    private LinuxCpuAffinity() {
    }

    /**
     * Returns the number of online logical CPUs reported by the host OS kernel
     * (independent of any inherited {@code taskset} process mask).
     *
     * @return online host CPU count
     */
    public static int getOnlineCpuCount() {
        return ONLINE_CPU_COUNT;
    }

    /**
     * Returns whether native 4-core Linux thread pinning is supported on the current host.
     *
     * @return {@code true} if running on Linux with {@code sched_setaffinity} available and at least 4 online CPUs
     */
    public static boolean isFourCorePinningActive() {
        return IS_LINUX && SCHED_SETAFFINITY_HANDLE != null && ONLINE_CPU_COUNT >= MIN_DEDICATED_TOPOLOGY_CPUS;
    }

    /**
     * Pins the calling OS thread ({@code pid = 0}) to the specified zero-based physical/logical {@code cpuIndex}
     * when running on a Linux host with at least {@link #MIN_DEDICATED_TOPOLOGY_CPUS} online CPUs.
     *
     * @param cpuIndex zero-based target CPU index ({@code 0..63})
     * @param roleLabel human-readable thread role for startup diagnostics
     * @return {@code true} if {@code sched_setaffinity} succeeded on the current thread, {@code false} if skipped or failed
     */
    public static boolean pinCurrentThreadIfSupported(int cpuIndex, String roleLabel) {
        if (cpuIndex < 0 || cpuIndex > MAX_SUPPORTED_CPU_INDEX) {
            throw new IllegalArgumentException("cpuIndex must be in [0, " + MAX_SUPPORTED_CPU_INDEX + "], got: " + cpuIndex);
        }
        if (!isFourCorePinningActive() || cpuIndex >= ONLINE_CPU_COUNT) {
            return false;
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cpuSet = arena.allocate(CPU_SET_SIZE_BYTES, Long.BYTES);
            cpuSet.set(ValueLayout.JAVA_LONG_UNALIGNED, 0L, 1L << cpuIndex);
            int rc = (int) SCHED_SETAFFINITY_HANDLE.invokeExact(0, (long) CPU_SET_SIZE_BYTES, cpuSet);
            if (rc == 0) {
                Log.infof("Pinned thread '%s' (role=%s) to CPU #%d via sched_setaffinity (onlineCpus=%d)",
                        Thread.currentThread().getName(), roleLabel, cpuIndex, ONLINE_CPU_COUNT);
                return true;
            }
            Log.warnf("sched_setaffinity returned rc=%d for thread '%s' (role=%s, targetCpu=%d)",
                    rc, Thread.currentThread().getName(), roleLabel, cpuIndex);
        } catch (Throwable t) {
            Log.warnf("Failed to pin thread '%s' (role=%s) to CPU #%d: %s",
                    Thread.currentThread().getName(), roleLabel, cpuIndex, t.getMessage());
        }
        return false;
    }

    /**
     * Parses a Linux kernel CPU list specification (such as {@code "0-3"}, {@code "0-1"}, or {@code "0,2-5"})
     * from {@code /sys/devices/system/cpu/online} into the total count of listed CPUs.
     *
     * @param onlineSpec raw kernel CPU range string
     * @return number of online CPUs parsed, or {@code 0} if blank/malformed
     */
    public static int parseOnlineCpuCount(String onlineSpec) {
        if (onlineSpec == null) {
            return 0;
        }
        String trimmed = onlineSpec.trim();
        if (trimmed.isEmpty()) {
            return 0;
        }
        int count = 0;
        String[] segments = trimmed.split(",");
        for (String seg : segments) {
            String part = seg.trim();
            if (part.isEmpty()) {
                continue;
            }
            int dash = part.indexOf('-');
            try {
                if (dash < 0) {
                    int cpu = Integer.parseInt(part);
                    if (cpu >= 0) {
                        count++;
                    }
                } else {
                    int start = Integer.parseInt(part.substring(0, dash).trim());
                    int end = Integer.parseInt(part.substring(dash + 1).trim());
                    if (start >= 0 && end >= start) {
                        count += (end - start + 1);
                    }
                }
            } catch (NumberFormatException ignored) {
                return 0;
            }
        }
        return count;
    }

    private static int detectOnlineCpuCount() {
        if (IS_LINUX) {
            try {
                if (Files.isReadable(SYS_CPU_ONLINE_PATH)) {
                    String spec = Files.readString(SYS_CPU_ONLINE_PATH, StandardCharsets.UTF_8);
                    int parsed = parseOnlineCpuCount(spec);
                    if (parsed > 0) {
                        return parsed;
                    }
                }
            } catch (Exception ignored) {
            }
        }
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }

    private static MethodHandle resolveSchedSetAffinityHandle() {
        if (!IS_LINUX) {
            return null;
        }
        try {
            Linker linker = Linker.nativeLinker();
            SymbolLookup stdlib = linker.defaultLookup();
            return stdlib.find("sched_setaffinity")
                    .map(symbol -> linker.downcallHandle(
                            symbol,
                            FunctionDescriptor.of(
                                    ValueLayout.JAVA_INT,
                                    ValueLayout.JAVA_INT,
                                    ValueLayout.JAVA_LONG,
                                    ValueLayout.ADDRESS
                            )
                    ))
                    .orElse(null);
        } catch (Throwable t) {
            Log.debugf("Native sched_setaffinity lookup unavailable: %s", t.getMessage());
            return null;
        }
    }
}
