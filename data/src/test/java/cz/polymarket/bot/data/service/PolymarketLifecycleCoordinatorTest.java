package cz.polymarket.bot.data.service;

import com.github.luben.zstd.ZstdInputStream;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketGammaClient;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import cz.polymarket.bot.data.storage.PolymarketMarketStorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link PolymarketLifecycleCoordinator} verifying:
 * 1. Tracking of current market + {@code lookahead-count} (default 5) future markets (6 total)
 * 2. Unparsed raw recording of per-market CLOB streams without Gamma REST polling for resolution
 * 3. Finalizing the ended market archive upon receiving {@code closeCode=1000} from the CLOB WebSocket server
 *    and opening the next future market slot on the subsequent discovery cycle
 */
class PolymarketLifecycleCoordinatorTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldTrackCurrentPlusFiveFutureMarketsAndFinalizeMarketOnServerCloseCode1000WithoutGammaPolling() throws Exception {
        PolymarketGammaClient gammaClient = mock(PolymarketGammaClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);

        Map<String, RawWebSocketListener> clobListenersBySlug = new HashMap<>();
        Map<String, Consumer<WebSocketStreamTelemetry>> normalCloseListenersBySlug = new HashMap<>();
        Map<String, BooleanSupplier> shouldReconnectSuppliersBySlug = new HashMap<>();
        doAnswer(invocation -> {
            String slug = invocation.getArgument(0);
            RawWebSocketListener listener = invocation.getArgument(2);
            Consumer<WebSocketStreamTelemetry> normalCloseListener = invocation.getArgument(3);
            BooleanSupplier shouldReconnectSupplier = invocation.getArgument(4);
            clobListenersBySlug.put(slug, listener);
            normalCloseListenersBySlug.put(slug, normalCloseListener);
            shouldReconnectSuppliersBySlug.put(slug, shouldReconnectSupplier);
            return null;
        }).when(clobClient).openMarketStream(any(), any(), any(), any(), any());

        long baseStartSec = 1789041600L; // cycle 0
        long durationSec = 900L;

        // Stub Gamma API for cycles 0..7
        for (int i = 0; i <= 7; i++) {
            long startSec = baseStartSec + (i * durationSec);
            String slug = "btc-updown-15m-" + startSec;
            PolymarketMarketInfo info = new PolymarketMarketInfo(
                    slug,
                    startSec,
                    startSec + durationSec,
                    List.of("token_up_" + i, "token_down_" + i),
                    false,
                    "{\"slug\":\"" + slug + "\",\"closed\":false}"
            );
            when(gammaClient.fetchMarketBySlug(eq(slug), eq(durationSec))).thenReturn(Optional.of(info));
        }

        PolymarketMarketStorageManager storageManager = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        PolymarketLifecycleCoordinator coordinator = new PolymarketLifecycleCoordinator(
                gammaClient,
                clobClient,
                storageManager,
                "BTCUSDT",
                "15m",
                5
        );

        // 1. Start at baseStartSec + 10s -> should track current (0) + 5 future (1..5) = 6 markets
        coordinator.start(baseStartSec + 10L);

        String slug0 = "btc-updown-15m-" + baseStartSec;
        String slug5 = "btc-updown-15m-" + (baseStartSec + 5 * durationSec);
        String slug6 = "btc-updown-15m-" + (baseStartSec + 6 * durationSec);

        assertThat(coordinator.getTrackedMarketSlugs()).hasSize(6);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug0, slug5);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug6);
        assertThat(shouldReconnectSuppliersBySlug.get(slug0).getAsBoolean()).isTrue();
        verify(gammaClient, times(1)).fetchMarketBySlug(eq(slug0), eq(durationSec));

        // 2. Emit raw CLOB messages via zero-decode TimestampedFrame -> saved directly without parsing
        clobListenersBySlug.get(slug0).onMessageBytes(
                new TimestampedFrame(
                        "[{\"event_type\":\"book\",\"asset_id\":\"token_up_0\",\"bids\":[{\"price\":\"0.50\",\"size\":\"100\"}]}]".getBytes(StandardCharsets.UTF_8),
                        (baseStartSec + 20L) * 1000L
                )
        );

        // 3. Cycle 0 ends (now = baseStartSec + 905): coordinator MUST NOT poll Gamma REST API for slug0 resolution,
        //    keeps slug0 open while waiting for CLOB server closeCode=1000 before opening slug6,
        //    and disables reconnect for slug0 now that its interval has ended
        coordinator.discoverAndRefreshMarkets(baseStartSec + durationSec + 5L);
        assertThat(shouldReconnectSuppliersBySlug.get(slug0).getAsBoolean()).isFalse();
        verify(gammaClient, times(1)).fetchMarketBySlug(eq(slug0), eq(durationSec));
        verify(clobClient, never()).closeMarketStream(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug6);

        // 4. CLOB WebSocket sends market_resolved frame and closes stream with closeCode=1000
        clobListenersBySlug.get(slug0).onMessageBytes(
                new TimestampedFrame(
                        "{\"event_type\":\"market_resolved\",\"winning_asset_id\":\"token_up_0\",\"winning_outcome\":\"Up\"}".getBytes(StandardCharsets.UTF_8),
                        (baseStartSec + durationSec + 12L) * 1000L
                )
        );
        normalCloseListenersBySlug.get(slug0).accept(new WebSocketStreamTelemetry(
                "POLYMARKET-CLOB-" + slug0,
                ConnectionState.DISCONNECTED,
                "ws-subscriptions-clob.polymarket.com:443",
                0,
                10,
                0L,
                2L,
                5L,
                20L,
                2L,
                0L,
                0L,
                (baseStartSec + durationSec + 12L) * 1000L,
                1000,
                "all subscribed assets resolved"
        ));

        // Market 0 is immediately finalized without any additional Gamma REST call
        verify(gammaClient, times(1)).fetchMarketBySlug(eq(slug0), eq(durationSec));
        verify(clobClient).closeMarketStream(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug0);

        // Next discovery cycle opens slug6 (current 1 + 5 future = 1..6)
        coordinator.discoverAndRefreshMarkets(baseStartSec + durationSec + 15L);
        verify(clobClient).openMarketStream(eq(slug6), eq(List.of("token_up_6", "token_down_6")), any(), any(), any());
        assertThat(coordinator.getTrackedMarketSlugs()).hasSize(6);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug6);

        // 5. Verify the closed Zstd file for slug0 contains all raw events including the CLOB market_resolved frame
        Path market0File = tempDir.resolve("polymarket/btc-updown-15m/" + slug0 + ".jsonl.zst");
        assertThat(Files.exists(market0File)).isTrue();

        List<String> lines = new ArrayList<>();
        try (ZstdInputStream zis = new ZstdInputStream(Files.newInputStream(market0File));
             BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        assertThat(lines).anyMatch(l -> l.contains("\"type\":\"market_discovered\""));
        assertThat(lines).anyMatch(l -> l.contains("\"type\":\"clob\"") && l.contains("\"event_type\":\"book\""));
        assertThat(lines).anyMatch(l -> l.contains("\"type\":\"clob\"") && l.contains("\"event_type\":\"market_resolved\""));

        coordinator.stop();
    }

    @Test
    void shouldForceCloseMarketFiveMinutesAfterEndSecWithoutCloseCode1000AndCloseOnPostEndDisconnect() throws Exception {
        PolymarketGammaClient gammaClient = mock(PolymarketGammaClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);

        Map<String, RawWebSocketListener> clobListenersBySlug = new HashMap<>();
        Map<String, Consumer<WebSocketStreamTelemetry>> closeListenersBySlug = new HashMap<>();
        Map<String, BooleanSupplier> shouldReconnectBySlug = new HashMap<>();
        doAnswer(invocation -> {
            String slug = invocation.getArgument(0);
            RawWebSocketListener listener = invocation.getArgument(2);
            Consumer<WebSocketStreamTelemetry> closeListener = invocation.getArgument(3);
            BooleanSupplier shouldReconnect = invocation.getArgument(4);
            clobListenersBySlug.put(slug, listener);
            closeListenersBySlug.put(slug, closeListener);
            shouldReconnectBySlug.put(slug, shouldReconnect);
            return null;
        }).when(clobClient).openMarketStream(any(), any(), any(), any(), any());

        long baseStartSec = 1789050600L; // cycle 0
        long durationSec = 900L;

        for (int i = 0; i <= 8; i++) {
            long startSec = baseStartSec + (i * durationSec);
            String slug = "btc-updown-15m-" + startSec;
            PolymarketMarketInfo info = new PolymarketMarketInfo(
                    slug,
                    startSec,
                    startSec + durationSec,
                    List.of("token_up_" + i, "token_down_" + i),
                    false,
                    "{\"slug\":\"" + slug + "\",\"closed\":false}"
            );
            when(gammaClient.fetchMarketBySlug(eq(slug), eq(durationSec))).thenReturn(Optional.of(info));
        }

        PolymarketMarketStorageManager storageManager = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        PolymarketLifecycleCoordinator coordinator = new PolymarketLifecycleCoordinator(
                gammaClient,
                clobClient,
                storageManager,
                "BTCUSDT",
                "15m",
                5
        );

        coordinator.start(baseStartSec + 10L);

        String slug0 = "btc-updown-15m-" + baseStartSec;
        String slug1 = "btc-updown-15m-" + (baseStartSec + durationSec);
        String slug6 = "btc-updown-15m-" + (baseStartSec + 6 * durationSec);
        String slug7 = "btc-updown-15m-" + (baseStartSec + 7 * durationSec);

        long market0EndSec = baseStartSec + durationSec;

        // 1. At endSec + 299s (4m 59s after end of cycle 0) without closeCode=1000: slug0 is still open
        coordinator.discoverAndRefreshMarkets(market0EndSec + 299L);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug6);
        verify(clobClient, never()).closeMarketStream(slug0);

        // 2. At endSec + 300s (5 minutes after end of cycle 0): hard safeguard forcefully closes slug0 and opens slug6
        coordinator.discoverAndRefreshMarkets(market0EndSec + 300L);
        verify(clobClient).closeMarketStream(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug6);

        // Verify slug0 .jsonl.zst archive is readable and closed cleanly
        Path market0File = tempDir.resolve("polymarket/btc-updown-15m/" + slug0 + ".jsonl.zst");
        assertThat(Files.exists(market0File)).isTrue();
        try (ZstdInputStream zis = new ZstdInputStream(Files.newInputStream(market0File));
             BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {
            assertThat(reader.readLine()).contains("\"type\":\"market_discovered\"");
        }

        // 3. Now advance past cycle 1 endSec (market1EndSec + 5s) and simulate an abnormal disconnect (closeCode=1006):
        //    shouldReconnect must be false, and the close callback immediately finalizes slug1 without waiting 5 minutes
        long market1EndSec = baseStartSec + 2 * durationSec;
        coordinator.discoverAndRefreshMarkets(market1EndSec + 5L);
        assertThat(shouldReconnectBySlug.get(slug1).getAsBoolean()).isFalse();

        closeListenersBySlug.get(slug1).accept(new WebSocketStreamTelemetry(
                "POLYMARKET-CLOB-" + slug1,
                ConnectionState.DISCONNECTED,
                "ws-subscriptions-clob.polymarket.com:443",
                0,
                5,
                0L,
                1L,
                2L,
                10L,
                1L,
                0L,
                0L,
                (market1EndSec + 5L) * 1000L,
                1006,
                "abnormal closure after market end"
        ));

        verify(clobClient).closeMarketStream(slug1);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug1);

        // Next refresh opens slug7
        coordinator.discoverAndRefreshMarkets(market1EndSec + 10L);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug7);

        coordinator.stop();
    }
}
