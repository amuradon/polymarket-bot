package cz.polymarket.bot.collector;

import cz.polymarket.bot.collector.storage.PolymarketMarketStorageManager;
import cz.polymarket.bot.collector.storage.ZstdJsonlWriter;
import cz.polymarket.bot.collector.storage.ZstdJsonlWriterTest;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketGammaClient;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.TimestampedFrame;
import cz.polymarket.bot.connector.ws.WebSocketStreamTelemetry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * Unit tests for {@link PolymarketLifecycleCoordinator} verifying:
 * <ol>
 *   <li>Tracking of current market + {@code lookahead-count = 1} future market (2 total), immediately opening the next lookahead upon {@code closeCode=1000}</li>
 *   <li>Finalizing the ended market archive upon {@code closeCode=1000}, {@code +300s} safeguard, or post-end disconnect</li>
 *   <li>Appending across instance restarts without emitting duplicate {@code market_discovered} lines</li>
 * </ol>
 */
class PolymarketLifecycleCoordinatorTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldTrackCurrentPlusOneFutureMarketFinalizeOnCloseCode1000AndImmediatelyOpenNextLookahead() throws Exception {
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
        }).when(clobClient).openCollectorMarketStream(any(), any(), any(), any(), any());

        long baseStartSec = 1789041600L; // cycle 0
        long durationSec = 900L;

        for (int i = 0; i <= 5; i++) {
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

        PolymarketMarketStorageManager storage1 = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        PolymarketLifecycleCoordinator coordinator1 = new PolymarketLifecycleCoordinator(
                gammaClient,
                clobClient,
                storage1,
                "BTCUSDT",
                "15m",
                1
        );

        String slug0 = "btc-updown-15m-" + baseStartSec;
        String slug1 = "btc-updown-15m-" + (baseStartSec + durationSec);
        String slug2 = "btc-updown-15m-" + (baseStartSec + 2 * durationSec);

        // 1. Start with lookaheadCount = 1 -> tracks current (0) + 1 future (1) = 2 markets
        coordinator1.start(baseStartSec + 5L);
        assertThat(coordinator1.getTrackedMarketSlugs()).containsExactlyInAnyOrder(slug0, slug1);
        assertThat(coordinator1.getTrackedMarketSlugs()).doesNotContain(slug2);

        clobListenersBySlug.get(slug0).onMessageBytes(new TimestampedFrame(
                "[{\"event_type\":\"book\",\"asset_id\":\"token_up_0\"}]".getBytes(StandardCharsets.UTF_8),
                (baseStartSec + 6L) * 1000L
        ));

        // 2. Simulate 3s upgrade restart mid-candle: close instance 1 and start instance 2 on same data dir
        coordinator1.stop();
        storage1.closeAll();

        PolymarketMarketStorageManager storage2 = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        PolymarketLifecycleCoordinator coordinator2 = new PolymarketLifecycleCoordinator(
                gammaClient,
                clobClient,
                storage2,
                "BTCUSDT",
                "15m",
                1
        );
        coordinator2.start(baseStartSec + 9L);

        // During post-end resolution tail of slug0 (now = endSec + 5s), slug2 is held back so tracked count stays <= 2
        coordinator2.discoverAndRefreshMarkets(baseStartSec + durationSec + 5L);
        assertThat(shouldReconnectBySlug.get(slug0).getAsBoolean()).isFalse();
        assertThat(coordinator2.getTrackedMarketSlugs()).containsExactlyInAnyOrder(slug0, slug1);
        verify(clobClient, never()).closeCollectorMarketStream(slug0);

        clobListenersBySlug.get(slug0).onMessageBytes(new TimestampedFrame(
                "{\"event_type\":\"market_resolved\",\"winning_outcome\":\"Up\"}".getBytes(StandardCharsets.UTF_8),
                (baseStartSec + durationSec + 10L) * 1000L
        ));
        closeListenersBySlug.get(slug0).accept(new WebSocketStreamTelemetry(
                "POLYMARKET-CLOB-" + slug0,
                ConnectionState.DISCONNECTED,
                "ws-subscriptions-clob.polymarket.com:443",
                0, 10, 0L, 2L, 5L, 20L, 2L, 0L, 0L,
                (baseStartSec + durationSec + 10L) * 1000L,
                1000,
                "all subscribed assets resolved"
        ));

        // Immediately upon closeCode=1000, slug0 is finalized AND slug2 (next lookahead) is opened without waiting 30s!
        verify(clobClient).closeCollectorMarketStream(slug0);
        assertThat(coordinator2.getTrackedMarketSlugs()).containsExactlyInAnyOrder(slug1, slug2);

        // Verify slug0 archive has strictly ONE market_discovered record despite restart in daily yyyy-MM-dd directory
        Path market0File = tempDir.resolve("polymarket/btc-updown-15m/2026-09-10/" + slug0 + ".jsonl.zst");
        assertThat(Files.exists(market0File)).isTrue();
        List<String> lines = ZstdJsonlWriterTest.readAllZstdLines(market0File);
        assertThat(lines.stream().filter(l -> l.contains("\"type\":\"market_discovered\"")).count()).isEqualTo(1L);
        assertThat(lines).anyMatch(l -> l.contains("\"event_type\":\"book\""));
        assertThat(lines).anyMatch(l -> l.contains("\"event_type\":\"market_resolved\""));

        coordinator2.stop();
        storage2.closeAll();
    }

    @Test
    void shouldForceCloseFiveMinutesAfterEndSecWithoutCloseCode1000() {
        PolymarketGammaClient gammaClient = mock(PolymarketGammaClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);

        long baseStartSec = 1789050600L;
        long durationSec = 900L;
        for (int i = 0; i <= 5; i++) {
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

        PolymarketMarketStorageManager storage = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        PolymarketLifecycleCoordinator coordinator = new PolymarketLifecycleCoordinator(
                gammaClient,
                clobClient,
                storage,
                "BTCUSDT",
                "15m",
                1
        );
        coordinator.start(baseStartSec + 10L);

        String slug0 = "btc-updown-15m-" + baseStartSec;
        String slug2 = "btc-updown-15m-" + (baseStartSec + 2 * durationSec);
        long market0EndSec = baseStartSec + durationSec;

        coordinator.discoverAndRefreshMarkets(market0EndSec + 299L);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug0);
        verify(clobClient, never()).closeCollectorMarketStream(slug0);

        coordinator.discoverAndRefreshMarkets(market0EndSec + 300L);
        verify(clobClient).closeCollectorMarketStream(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug2);

        coordinator.stop();
        storage.closeAll();
    }

    @Test
    void shouldResumeLegacyFlatMarketArchiveOnDeploymentUpgradeAndCreateSubsequentMarketsInDailySubdirectory() throws Exception {
        PolymarketGammaClient gammaClient = mock(PolymarketGammaClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);

        Map<String, RawWebSocketListener> clobListenersBySlug = new HashMap<>();
        Map<String, Consumer<WebSocketStreamTelemetry>> closeListenersBySlug = new HashMap<>();
        doAnswer(invocation -> {
            String slug = invocation.getArgument(0);
            clobListenersBySlug.put(slug, invocation.getArgument(2));
            closeListenersBySlug.put(slug, invocation.getArgument(3));
            return null;
        }).when(clobClient).openCollectorMarketStream(any(), any(), any(), any(), any());

        long baseStartSec = 1789041600L; // 2026-09-10 12:00:00 UTC
        long durationSec = 900L;
        for (int i = 0; i <= 3; i++) {
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

        String slug0 = "btc-updown-15m-" + baseStartSec;
        String slug1 = "btc-updown-15m-" + (baseStartSec + durationSec);
        String slug2 = "btc-updown-15m-" + (baseStartSec + 2 * durationSec);

        // Simulate pre-deploy version having written active market slug0 in legacy flat structure
        Path legacyFile0 = tempDir.resolve("polymarket/btc-updown-15m/" + slug0 + ".jsonl.zst");
        try (ZstdJsonlWriter legacyWriter = new ZstdJsonlWriter(legacyFile0)) {
            legacyWriter.writePolymarketEvent(
                    baseStartSec * 1000L,
                    "market_discovered",
                    "{\"slug\":\"" + slug0 + "\",\"closed\":false}"
            );
            legacyWriter.writePolymarketEvent(
                    (baseStartSec + 5L) * 1000L,
                    "clob",
                    "[{\"event_type\":\"book\",\"asset_id\":\"token_up_0\"}]"
            );
        }

        // Start newly deployed storage + coordinator mid-candle
        PolymarketMarketStorageManager newStorage = new PolymarketMarketStorageManager(tempDir.toString(), "BTCUSDT", "15m");
        PolymarketLifecycleCoordinator newCoordinator = new PolymarketLifecycleCoordinator(
                gammaClient,
                clobClient,
                newStorage,
                "BTCUSDT",
                "15m",
                1
        );
        newCoordinator.start(baseStartSec + 30L);

        // Active market slug0 receives additional CLOB frames after deployment; lookahead slug1 was NOT in legacy dir
        clobListenersBySlug.get(slug0).onMessageBytes(new TimestampedFrame(
                "{\"event_type\":\"last_trade_price\",\"price\":\"0.55\"}".getBytes(StandardCharsets.UTF_8),
                (baseStartSec + 35L) * 1000L
        ));
        clobListenersBySlug.get(slug1).onMessageBytes(new TimestampedFrame(
                "[{\"event_type\":\"book\",\"asset_id\":\"token_up_1\"}]".getBytes(StandardCharsets.UTF_8),
                (baseStartSec + 36L) * 1000L
        ));

        // Advance into slug1 window, then close slug0 cleanly -> lookahead slug2 is opened in daily directory too
        newCoordinator.discoverAndRefreshMarkets(baseStartSec + durationSec + 5L);
        closeListenersBySlug.get(slug0).accept(new WebSocketStreamTelemetry(
                "POLYMARKET-CLOB-" + slug0,
                ConnectionState.DISCONNECTED,
                "ws-subscriptions-clob.polymarket.com:443",
                0, 10, 0L, 2L, 5L, 20L, 2L, 0L, 0L,
                (baseStartSec + durationSec + 5L) * 1000L,
                1000,
                "all subscribed assets resolved"
        ));

        newCoordinator.stop();
        newStorage.closeAll();

        // slug0 resumed in legacy file with strictly ONE market_discovered and NO daily split file
        Path dailyFile0 = tempDir.resolve("polymarket/btc-updown-15m/2026-09-10/" + slug0 + ".jsonl.zst");
        assertThat(Files.exists(dailyFile0)).isFalse();
        List<String> lines0 = ZstdJsonlWriterTest.readAllZstdLines(legacyFile0);
        assertThat(lines0).hasSize(3);
        assertThat(lines0.stream().filter(l -> l.contains("\"type\":\"market_discovered\"")).count()).isEqualTo(1L);
        assertThat(lines0.get(2)).contains("\"event_type\":\"last_trade_price\"");

        // Brand-new markets slug1 and slug2 were written in daily yyyy-MM-dd directory
        Path dailyFile1 = tempDir.resolve("polymarket/btc-updown-15m/2026-09-10/" + slug1 + ".jsonl.zst");
        Path dailyFile2 = tempDir.resolve("polymarket/btc-updown-15m/2026-09-10/" + slug2 + ".jsonl.zst");
        assertThat(Files.exists(dailyFile1)).isTrue();
        assertThat(Files.exists(dailyFile2)).isTrue();
        assertThat(Files.exists(tempDir.resolve("polymarket/btc-updown-15m/" + slug1 + ".jsonl.zst"))).isFalse();
        assertThat(Files.exists(tempDir.resolve("polymarket/btc-updown-15m/" + slug2 + ".jsonl.zst"))).isFalse();

        List<String> lines1 = ZstdJsonlWriterTest.readAllZstdLines(dailyFile1);
        assertThat(lines1.stream().filter(l -> l.contains("\"type\":\"market_discovered\"")).count()).isEqualTo(1L);
        assertThat(lines1).anyMatch(l -> l.contains("\"event_type\":\"book\""));
    }
}
