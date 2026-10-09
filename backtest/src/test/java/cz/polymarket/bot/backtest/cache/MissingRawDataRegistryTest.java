package cz.polymarket.bot.backtest.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MissingRawDataRegistryTest {

    @Test
    @DisplayName("Should track missing Polymarket intervals and orderbook hours and persist to JSON")
    void shouldTrackAndPersistMissingData(@TempDir Path tempDir) throws IOException {
        Path jsonPath = tempDir.resolve("missing_raw_data_BTCUSDT.json");

        MissingRawDataRegistry registry = new MissingRawDataRegistry("BTCUSDT");
        assertThat(registry.isPolymarketMissing(1790812800L)).isFalse();
        assertThat(registry.isFuturesOrderBookMissing("2026-10-01-08")).isFalse();

        registry.recordMissingPolymarket(1790812800L);
        registry.recordMissingPolymarket(1790813700L);
        registry.recordMissingPolymarketAll(List.of(1790814600L, 1790815500L));
        registry.recordMissingFuturesOrderBook("2026-10-01-08");

        assertThat(registry.isPolymarketMissing(1790812800L)).isTrue();
        assertThat(registry.isPolymarketMissing(1790813700L)).isTrue();
        assertThat(registry.isPolymarketMissing(1790814600L)).isTrue();
        assertThat(registry.isPolymarketMissing(1790815500L)).isTrue();
        assertThat(registry.isPolymarketMissing(1790899999L)).isFalse();

        assertThat(registry.isFuturesOrderBookMissing("2026-10-01-08")).isTrue();
        assertThat(registry.isFuturesOrderBookMissing("2026-10-01-09")).isFalse();

        // Save and reload
        registry.save(jsonPath);
        assertThat(jsonPath).exists();

        MissingRawDataRegistry reloaded = MissingRawDataRegistry.load(jsonPath, "BTCUSDT");
        assertThat(reloaded.getSymbol()).isEqualTo("BTCUSDT");
        assertThat(reloaded.isPolymarketMissing(1790812800L)).isTrue();
        assertThat(reloaded.isPolymarketMissing(1790815500L)).isTrue();
        assertThat(reloaded.isFuturesOrderBookMissing("2026-10-01-08")).isTrue();
        assertThat(reloaded.isFuturesOrderBookMissing("2026-10-01-09")).isFalse();
    }

    @Test
    @DisplayName("Should return empty registry when loading non-existent file")
    void shouldReturnEmptyWhenFileNotFound(@TempDir Path tempDir) {
        Path nonExistent = tempDir.resolve("missing_raw_data_BTCUSDT.json");
        MissingRawDataRegistry registry = MissingRawDataRegistry.load(nonExistent, "BTCUSDT");
        assertThat(registry.getSymbol()).isEqualTo("BTCUSDT");
        assertThat(registry.getMissingPolymarketIntervals()).isEmpty();
        assertThat(registry.getMissingFuturesOrderBookHours()).isEmpty();
    }
}
