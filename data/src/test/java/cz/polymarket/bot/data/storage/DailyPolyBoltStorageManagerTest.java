package cz.polymarket.bot.data.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DailyPolyBoltStorageManager} verifying separate daily file creation and automatic
 * UTC midnight rotation for both PolyBolt {@code price.crypto} and {@code price.crypto.twap} (60s) streams.
 */
class DailyPolyBoltStorageManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldWritePolyBoltCryptoPriceAndTwapStreamsToSeparateDailyFilesAndRotateAtUtcMidnight() throws Exception {
        DailyPolyBoltStorageManager manager = new DailyPolyBoltStorageManager(tempDir.toString(), "BTCUSDT");

        long day1Ts = Instant.parse("2026-10-10T23:59:59.500Z").toEpochMilli();
        long day2Ts = Instant.parse("2026-10-11T00:00:00.100Z").toEpochMilli();

        manager.recordCryptoPrice("{\"v\":1,\"channel\":\"price.crypto\",\"seq\":1,\"payload\":{\"symbol\":\"btcusd\",\"value\":78100.0}}", day1Ts);
        manager.recordCryptoPrice("{\"v\":1,\"channel\":\"price.crypto\",\"seq\":2,\"payload\":{\"symbol\":\"btcusd\",\"value\":78200.0}}", day2Ts);

        manager.recordTwap("{\"v\":1,\"channel\":\"price.crypto.twap\",\"seq\":1,\"payload\":{\"symbol\":\"btcusd\",\"value\":78150.0,\"window_seconds\":60}}", day1Ts);
        manager.recordTwap("{\"v\":1,\"channel\":\"price.crypto.twap\",\"seq\":2,\"payload\":{\"symbol\":\"btcusd\",\"value\":78250.0,\"window_seconds\":60}}", day2Ts);

        manager.closeAll();

        Path cryptoDay1File = tempDir.resolve("polymarket/price_crypto/btcusd/btcusd-price_crypto-2026-10-10.jsonl.zst");
        Path cryptoDay2File = tempDir.resolve("polymarket/price_crypto/btcusd/btcusd-price_crypto-2026-10-11.jsonl.zst");
        Path twapDay1File = tempDir.resolve("polymarket/price_crypto_twap_60s/btcusd/btcusd-price_crypto_twap_60s-2026-10-10.jsonl.zst");
        Path twapDay2File = tempDir.resolve("polymarket/price_crypto_twap_60s/btcusd/btcusd-price_crypto_twap_60s-2026-10-11.jsonl.zst");

        assertThat(Files.exists(cryptoDay1File)).isTrue();
        assertThat(Files.exists(cryptoDay2File)).isTrue();
        assertThat(Files.exists(twapDay1File)).isTrue();
        assertThat(Files.exists(twapDay2File)).isTrue();

        List<String> cryptoDay1Lines = ZstdJsonlWriterTest.readAllZstdLines(cryptoDay1File);
        assertThat(cryptoDay1Lines).hasSize(1);
        assertThat(cryptoDay1Lines.get(0)).contains("78100.0");

        List<String> twapDay1Lines = ZstdJsonlWriterTest.readAllZstdLines(twapDay1File);
        assertThat(twapDay1Lines).hasSize(1);
        assertThat(twapDay1Lines.get(0)).contains("78150.0");

        List<String> twapDay2Lines = ZstdJsonlWriterTest.readAllZstdLines(twapDay2File);
        assertThat(twapDay2Lines).hasSize(1);
        assertThat(twapDay2Lines.get(0)).contains("78250.0");
    }
}
