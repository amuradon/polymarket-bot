package cz.polymarket.bot.monitoring;

import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor.MonitoredThreadSpec;
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor.SystemTelemetrySnapshot;
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor.TcpNetstatStats;
import cz.polymarket.bot.monitoring.SystemNetworkTelemetryMonitor.TcpQueueStats;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SystemNetworkTelemetryMonitor} verifying `/proc/net/tcp` and `/proc/net/netstat` parsing,
 * 4-core thread CPU/duty-cycle tracking, baseline transitions, and bounded ring buffers.
 */
class SystemNetworkTelemetryMonitorTest {

    @Test
    void shouldParseProcNetTcpHexQueuesAndSkipListeningAndTimeWaitSockets() {
        List<String> procNetTcpLines = List.of(
                "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
                "   0: 00000000:1F90 00000000:0000 0A 00000000:00000100 00:00000000 00000000   185        0 12345 1 0000000000000000 100 0 0 10 0",
                "   1: 0A000115:A3B4 681214E2:01BB 01 00000200:00001000 01:00000014 00000000   185        0 22345 2 0000000000000000 20 4 30 10 -1",
                "   2: 0A000115:A3B6 681214E3:01BB 01 00000080:00000800 01:00000014 00000000   185        0 22346 2 0000000000000000 20 4 30 10 -1",
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
    void shouldSampleFourDedicatedThreadsResetBaselineOnNullToLiveTransitionAndMaintainBoundedBuffers() throws Exception {
        SystemNetworkTelemetryMonitor monitor = new SystemNetworkTelemetryMonitor();
        Thread current = Thread.currentThread();

        List<MonitoredThreadSpec> beforeConnect = List.of(
                new MonitoredThreadSpec("vCPU #0: User WS & Control", "clob-user-worker", null, 0L),
                new MonitoredThreadSpec("vCPU #1: Market WS I/O", "market-ws-io-reader", null, -1L),
                new MonitoredThreadSpec("vCPU #2: Book & Strategy", "market-strategy-worker", null, 0L),
                new MonitoredThreadSpec("vCPU #3: Order Execution", "order-execution-io-worker", null, 0L)
        );
        SystemTelemetrySnapshot first = monitor.sample(beforeConnect);
        assertThat(first.threads()).hasSize(4);
        assertThat(first.threads().getFirst().threadState()).isEqualTo("INACTIVE");

        Thread.sleep(60L);

        List<MonitoredThreadSpec> onConnect = List.of(
                new MonitoredThreadSpec("vCPU #0: User WS & Control", "clob-user-worker", current, 5_000_000_000L),
                new MonitoredThreadSpec("vCPU #1: Market WS I/O", "market-ws-io-reader", current, -1L),
                new MonitoredThreadSpec("vCPU #2: Book & Strategy", "market-strategy-worker", current, 2_000_000_000L),
                new MonitoredThreadSpec("vCPU #3: Order Execution", "order-execution-io-worker", current, 1_000_000_000L)
        );
        SystemTelemetrySnapshot afterTransition = monitor.sample(onConnect);
        assertThat(afterTransition.threads().getFirst().busyDutyCyclePct()).isZero();

        for (int i = 0; i < SystemNetworkTelemetryMonitor.MAX_HISTORY_POINTS + 5; i++) {
            monitor.recordHistoryPoint(new TelemetryHistoryPoint(
                    1_700_000_000_000L + i * 10_000L, 10.0, 12.0, 1.0, 2.0, 3.0, 0.5, 0L, 0, 1L, 1L, 128L
            ));
        }
        assertThat(monitor.getHistory()).hasSize(SystemNetworkTelemetryMonitor.MAX_HISTORY_POINTS);
    }
}
