package cz.polymarket.bot.live.monitoring;

import cz.polymarket.bot.live.monitoring.SystemNetworkTelemetryMonitor.MonitoredThreadSpec;
import cz.polymarket.bot.live.monitoring.SystemNetworkTelemetryMonitor.SystemTelemetrySnapshot;
import cz.polymarket.bot.live.monitoring.SystemNetworkTelemetryMonitor.TcpNetstatStats;
import cz.polymarket.bot.live.monitoring.SystemNetworkTelemetryMonitor.TcpQueueStats;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SystemNetworkTelemetryMonitorTest {

    @Test
    void shouldParseProcNetTcpHexQueuesAndSkipListeningAndTimeWaitSockets() {
        List<String> procNetTcpLines = List.of(
                "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
                // LISTEN (st=0A) -> should be ignored even if rx_queue > 0
                "   0: 00000000:1F90 00000000:0000 0A 00000000:00000100 00:00000000 00000000   185        0 12345 1 0000000000000000 100 0 0 10 0",
                // ESTABLISHED (st=01): tx_queue=0x00000200 (512), rx_queue=0x00001000 (4096)
                "   1: 0A000115:A3B4 681214E2:01BB 01 00000200:00001000 01:00000014 00000000   185        0 22345 2 0000000000000000 20 4 30 10 -1",
                // ESTABLISHED (st=01): tx_queue=0x00000080 (128), rx_queue=0x00000800 (2048)
                "   2: 0A000115:A3B6 681214E3:01BB 01 00000080:00000800 01:00000014 00000000   185        0 22346 2 0000000000000000 20 4 30 10 -1",
                // TIME_WAIT (st=06) -> should be ignored
                "   3: 0A000115:A3B8 681214E3:01BB 06 00000400:00000400 00:00000000 00000000     0        0 0 3 0000000000000000"
        );

        TcpQueueStats stats = SystemNetworkTelemetryMonitor.parseProcNetTcpLines(procNetTcpLines);

        assertThat(stats.activeSockets()).isEqualTo(2);
        assertThat(stats.rxQueueBytes()).isEqualTo(4096L + 2048L);
        assertThat(stats.txQueueBytes()).isEqualTo(512L + 128L);
    }

    @Test
    void shouldParseProcNetNetstatExtendedTcpCounters() {
        List<String> procNetNetstatLines = List.of(
                "TcpExt: SyncookiesSent SyncookiesRecv TCPRcvCollapsed TCPOFOQueue TCPBacklogDrop",
                "TcpExt: 0 0 14 29 3",
                "IpExt: InNoRoutes InTruncatedPkts",
                "IpExt: 0 0"
        );

        TcpNetstatStats stats = SystemNetworkTelemetryMonitor.parseProcNetNetstatLines(procNetNetstatLines);

        assertThat(stats.tcpRcvCollapsed()).isEqualTo(14L);
        assertThat(stats.tcpOfoQueue()).isEqualTo(29L);
        assertThat(stats.tcpBacklogDrop()).isEqualTo(3L);
    }

    @Test
    void shouldSampleFourDedicatedThreadsAndTrackCpuAndBusyUtilization() throws Exception {
        SystemNetworkTelemetryMonitor monitor = new SystemNetworkTelemetryMonitor();
        Thread current = Thread.currentThread();

        List<MonitoredThreadSpec> initialSpecs = List.of(
                new MonitoredThreadSpec("vCPU #1: I/O Reader", "vert.x-eventloop-thread-0", current, -1L),
                new MonitoredThreadSpec("vCPU #2: CLOB Book Worker", "clob-book-worker", current, 10_000_000L),
                new MonitoredThreadSpec("vCPU #3: Strategy Calculator", "strategy-calculator-worker", null, 0L),
                new MonitoredThreadSpec("vCPU #4: Order & I/O Worker", "order-execution-io-worker", current, 20_000_000L)
        );

        SystemTelemetrySnapshot first = monitor.sample(initialSpecs);
        assertThat(first.threads()).hasSize(4);
        assertThat(first.threads().get(2).threadState()).isEqualTo("INACTIVE");
        assertThat(first.maxHeapMb()).isGreaterThan(0L);

        Thread.sleep(65L);

        List<MonitoredThreadSpec> secondSpecs = List.of(
                new MonitoredThreadSpec("vCPU #1: I/O Reader", "vert.x-eventloop-thread-0", current, -1L),
                new MonitoredThreadSpec("vCPU #2: CLOB Book Worker", "clob-book-worker", current, 40_000_000L),
                new MonitoredThreadSpec("vCPU #3: Strategy Calculator", "strategy-calculator-worker", null, 0L),
                new MonitoredThreadSpec("vCPU #4: Order & I/O Worker", "order-execution-io-worker", current, 50_000_000L)
        );

        SystemTelemetrySnapshot second = monitor.sample(secondSpecs);
        assertThat(second.threads()).hasSize(4);
        assertThat(second.threads().get(1).busyDutyCyclePct()).isGreaterThan(0.0);
        assertThat(second.threads().get(1).maxBusyDutyCyclePct()).isGreaterThanOrEqualTo(second.threads().get(1).busyDutyCyclePct());
    }
}
