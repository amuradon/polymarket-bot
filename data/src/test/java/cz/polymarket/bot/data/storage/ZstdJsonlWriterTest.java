package cz.polymarket.bot.data.storage;

import com.github.luben.zstd.ZstdInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ZstdJsonlWriter} verifying Zstandard streaming compression, frame flushing,
 * and concatenated frame appending across restarts.
 */
class ZstdJsonlWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldWriteAndDecompressStreamingZstdJsonlLinesAndSupportAppendAcrossRestarts() throws Exception {
        Path file = tempDir.resolve("test-stream.jsonl.zst");

        try (ZstdJsonlWriter writer = new ZstdJsonlWriter(file)) {
            writer.writeBinanceEvent(1789041600123L, "{\"e\":\"aggTrade\",\"p\":\"78000.50\"}");
            writer.writePolymarketEvent(1789041600456L, "twap_60s", "{\"value\":78001.25}");
            writer.flush();
            assertThat(writer.getRecordsWritten()).isEqualTo(2L);
        }

        // Simulate restart appending a new Zstandard frame to the same file
        try (ZstdJsonlWriter writer2 = new ZstdJsonlWriter(file)) {
            writer2.writePolymarketEvent(1789041601789L, "trade", "{\"price\":\"0.54\",\"size\":\"100\"}");
        }

        List<String> lines = readAllZstdLines(file);
        assertThat(lines).containsExactly(
                "{\"ts\":1789041600123,\"payload\":{\"e\":\"aggTrade\",\"p\":\"78000.50\"}}",
                "{\"ts\":1789041600456,\"type\":\"twap_60s\",\"payload\":{\"value\":78001.25}}",
                "{\"ts\":1789041601789,\"type\":\"trade\",\"payload\":{\"price\":\"0.54\",\"size\":\"100\"}}"
        );
    }

    static List<String> readAllZstdLines(Path file) throws Exception {
        List<String> lines = new ArrayList<>();
        try (ZstdInputStream zis = new ZstdInputStream(Files.newInputStream(file));
             BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }
}
