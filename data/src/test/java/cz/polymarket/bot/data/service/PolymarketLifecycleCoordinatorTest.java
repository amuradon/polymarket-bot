package cz.polymarket.bot.data.service;

import com.github.luben.zstd.ZstdInputStream;
import cz.polymarket.bot.connector.polymarket.PolymarketClobClient;
import cz.polymarket.bot.connector.polymarket.PolymarketGammaClient;
import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link PolymarketLifecycleCoordinator} verifying:
 * 1. Tracking of current market + {@code lookahead-count} (default 5) future markets (6 total)
 * 2. Unparsed raw recording of per-market CLOB streams
 * 3. Disconnecting the ended market stream and opening the next future market stream upon resolution
 */
class PolymarketLifecycleCoordinatorTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldTrackCurrentPlusFiveFutureMarketsAndRotateOnResolution() throws Exception {
        PolymarketGammaClient gammaClient = mock(PolymarketGammaClient.class);
        PolymarketClobClient clobClient = mock(PolymarketClobClient.class);

        Map<String, RawWebSocketListener> clobListenersBySlug = new HashMap<>();
        doAnswer(invocation -> {
            String slug = invocation.getArgument(0);
            RawWebSocketListener listener = invocation.getArgument(2);
            clobListenersBySlug.put(slug, listener);
            return null;
        }).when(clobClient).openMarketStream(any(), any(), any());

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
                5,
                300L
        );

        // 1. Start at baseStartSec + 10s -> should track current (0) + 5 future (1..5) = 6 markets
        coordinator.start(baseStartSec + 10L);

        String slug0 = "btc-updown-15m-" + baseStartSec;
        String slug5 = "btc-updown-15m-" + (baseStartSec + 5 * durationSec);
        String slug6 = "btc-updown-15m-" + (baseStartSec + 6 * durationSec);

        assertThat(coordinator.getTrackedMarketSlugs()).hasSize(6);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug0, slug5);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug6);

        // 2. Emit raw CLOB messages -> saved directly without parsing
        clobListenersBySlug.get(slug0).onMessage(
                "[{\"event_type\":\"book\",\"asset_id\":\"token_up_0\",\"bids\":[{\"price\":\"0.50\",\"size\":\"100\"}]}]",
                (baseStartSec + 20L) * 1000L
        );

        // 3. Cycle 0 ends (now = baseStartSec + 905), but Gamma still has closed=false -> keep slug0 open, do not open slug6 yet
        coordinator.discoverAndRefreshMarkets(baseStartSec + durationSec + 5L);
        verify(clobClient, never()).closeMarketStream(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug6);

        // 4. Gamma marks slug0 closed=true -> coordinator records resolution, disconnects slug0 stream, and opens slug6 (current 1 + 5 future = 1..6)
        PolymarketMarketInfo resolved0 = new PolymarketMarketInfo(
                slug0,
                baseStartSec,
                baseStartSec + durationSec,
                List.of("token_up_0", "token_down_0"),
                true,
                "{\"slug\":\"" + slug0 + "\",\"closed\":true,\"outcomePrices\":\"[\\\"1\\\",\\\"0\\\"]\"}"
        );
        when(gammaClient.fetchMarketBySlug(eq(slug0), eq(durationSec))).thenReturn(Optional.of(resolved0));

        coordinator.discoverAndRefreshMarkets(baseStartSec + durationSec + 15L);

        verify(clobClient).closeMarketStream(slug0);
        verify(clobClient).openMarketStream(eq(slug6), eq(List.of("token_up_6", "token_down_6")), any());
        assertThat(coordinator.getTrackedMarketSlugs()).hasSize(6);
        assertThat(coordinator.getTrackedMarketSlugs()).doesNotContain(slug0);
        assertThat(coordinator.getTrackedMarketSlugs()).contains(slug6);

        // 5. Verify the closed Zstd file for slug0 contains all raw events
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
        assertThat(lines).anyMatch(l -> l.contains("\"type\":\"market_resolved\"") && l.contains("\"closed\":true"));

        coordinator.stop();
    }
}
