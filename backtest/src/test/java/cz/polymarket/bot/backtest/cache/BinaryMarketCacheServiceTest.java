package cz.polymarket.bot.backtest.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinaryMarketCacheServiceTest {

    @Test
    @DisplayName("Should write and memory-map read indicator cache files correctly")
    void shouldWriteAndReadIndicatorCache(@TempDir Path tempDir) throws IOException {
        BinaryMarketCacheService cacheService = new BinaryMarketCacheService(tempDir.toString());

        String symbol = "BTCUSDT";
        String indicatorId = "twap";
        String month = "2026-10";
        List<String> keys = List.of("twap_open", "twap_close");

        long t1 = 1790812800L;
        long t2 = 1790813700L;
        Map<Long, Map<String, Double>> records = Map.of(
                t1, Map.of("twap_open", 83620.5, "twap_close", 83710.0),
                t2, Map.of("twap_open", 83710.0, "twap_close", 83695.2)
        );

        assertThat(cacheService.hasIndicatorCache(symbol, indicatorId, month)).isFalse();
        cacheService.writeIndicatorCache(symbol, indicatorId, month, keys, records);
        assertThat(cacheService.hasIndicatorCache(symbol, indicatorId, month)).isTrue();

        Map<Long, Map<String, Double>> read = cacheService.readIndicatorCache(symbol, indicatorId, month);
        assertThat(read).hasSize(2);
        assertThat(read.get(t1).get("twap_open")).isEqualTo(83620.5);
        assertThat(read.get(t1).get("twap_close")).isEqualTo(83710.0);
        assertThat(read.get(t2).get("twap_open")).isEqualTo(83710.0);
        assertThat(read.get(t2).get("twap_close")).isEqualTo(83695.2);
    }

    @Test
    @DisplayName("Should write and memory-map read market rows cache files correctly")
    void shouldWriteAndReadMarketRowsCache(@TempDir Path tempDir) throws IOException {
        BinaryMarketCacheService cacheService = new BinaryMarketCacheService(tempDir.toString());

        String symbol = "BTCUSDT";
        String month = "2026-10";

        CachedMarketRow row1 = new CachedMarketRow(
                1790812800L, 1790813700L,
                83600.0, 83750.0, 83580.0, 83720.0, 150.5, 12500000.0, 25.4,
                83610.0, 83760.0, 83590.0, 83730.0, 320.0, 26800000.0, 40.1,
                true,
                0.52, 0.51, 0.55, 0.54, 0.60, 0.59, 0.65, 0.64,
                0.53, 0.015, 0.48, 0.015,
                0.70, 0.45, 450.0, 520.0
        );

        CachedMarketRow row2 = new CachedMarketRow(
                1790813700L, 1790814600L,
                83720.0, 83800.0, 83690.0, 83700.0, 110.2, 9200000.0, -12.3,
                83730.0, 83810.0, 83700.0, 83710.0, 280.0, 23400000.0, -18.7,
                false,
                0.51, 0.50, 0.49, 0.48, 0.45, 0.44, 0.42, 0.41,
                0.52, 0.014, 0.50, 0.014,
                0.55, 0.38, 380.0, 410.0
        );

        assertThat(cacheService.hasMarketRowsCache(symbol, month)).isFalse();
        cacheService.writeMarketRowsCache(symbol, month, List.of(row1, row2));
        assertThat(cacheService.hasMarketRowsCache(symbol, month)).isTrue();

        List<CachedMarketRow> readRows = cacheService.readMarketRowsCache(symbol, month);
        assertThat(readRows).hasSize(2);

        CachedMarketRow r1 = readRows.get(0);
        assertThat(r1.tStart()).isEqualTo(1790812800L);
        assertThat(r1.tEnd()).isEqualTo(1790813700L);
        assertThat(r1.sOpen()).isEqualTo(83600.0);
        assertThat(r1.sClose()).isEqualTo(83720.0);
        assertThat(r1.fOpen()).isEqualTo(83610.0);
        assertThat(r1.actualOutcomeUp()).isTrue();
        assertThat(r1.pmAsk0()).isEqualTo(0.52);
        assertThat(r1.pmBid0()).isEqualTo(0.51);
        assertThat(r1.pmDepth1cUp()).isEqualTo(450.0);

        CachedMarketRow r2 = readRows.get(1);
        assertThat(r2.tStart()).isEqualTo(1790813700L);
        assertThat(r2.actualOutcomeUp()).isFalse();
    }

    @Test
    @DisplayName("Should detect corrupt magic in indicator cache")
    void shouldDetectCorruptMagic(@TempDir Path tempDir) throws IOException {
        BinaryMarketCacheService cacheService = new BinaryMarketCacheService(tempDir.toString());
        Path file = tempDir.resolve("ind_BTCUSDT_twap_2026-10.bin");
        java.nio.file.Files.write(file, new byte[]{0x01, 0x02, 0x03, 0x04, 0x00, 0x01});

        assertThatThrownBy(() -> cacheService.readIndicatorCache("BTCUSDT", "twap", "2026-10"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid indicator binary cache magic");
    }
}
