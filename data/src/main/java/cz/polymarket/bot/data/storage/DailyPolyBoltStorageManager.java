package cz.polymarket.bot.data.storage;

import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages separate daily Zstandard streaming JSONL files ({@code .jsonl.zst}) for Polymarket PolyBolt
 * {@code price.crypto} and {@code price.crypto.twap} (60s) streams, automatically rotating files at UTC midnight
 * with per-stream lock isolation and zero-decode {@code byte[]} writing.
 */
@ApplicationScoped
public class DailyPolyBoltStorageManager {

    private static final DateTimeFormatter UTC_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final String STREAM_PRICE_CRYPTO = "price_crypto";
    private static final String STREAM_TWAP_60S = "price_crypto_twap_60s";

    private final Path baseDataDir;
    private final String polyBoltSymbol;
    private final Object cryptoLock = new Object();
    private final Object twapLock = new Object();
    private final AtomicLong totalRecordsWritten = new AtomicLong(0L);

    private LocalDate activeCryptoDate;
    private ZstdJsonlWriter activeCryptoWriter;
    private LocalDate activeTwapDate;
    private ZstdJsonlWriter activeTwapWriter;

    /**
     * Constructs the daily PolyBolt storage manager.
     *
     * @param dataDirStr root collector data directory path
     * @param symbol     configured trading symbol (e.g. BTCUSDT), normalized to PolyBolt symbol (e.g. btcusd)
     */
    @Inject
    public DailyPolyBoltStorageManager(
            @ConfigProperty(name = "polymarket.collector.data-dir", defaultValue = "data/collector")
            String dataDirStr,
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol) {
        this.baseDataDir = Path.of(dataDirStr);
        this.polyBoltSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(symbol);
    }

    /**
     * Records a raw PolyBolt {@code price.crypto} stream message into its UTC daily {@code .jsonl.zst} file.
     *
     * @param rawJson         unmodified raw WebSocket JSON payload
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void recordCryptoPrice(String rawJson, long recvTimestampMs) {
        if (!isJsonObject(rawJson)) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        synchronized (cryptoLock) {
            try {
                ZstdJsonlWriter writer = getOrRotateCryptoWriter(utcDate);
                writer.writeBinanceEvent(recvTimestampMs, rawJson);
                totalRecordsWritten.incrementAndGet();
            } catch (IOException e) {
                Log.errorf(e, "Failed to write PolyBolt price.crypto event to daily Zstd file");
            }
        }
    }

    /**
     * Records a raw UTF-8 {@code byte[]} PolyBolt {@code price.crypto} stream payload directly into its
     * UTC daily {@code .jsonl.zst} file without allocating a {@link String}.
     *
     * @param rawPayloadBytes unmodified raw UTF-8 JSON bytes from the WebSocket frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void recordCryptoPriceBytes(byte[] rawPayloadBytes, long recvTimestampMs) {
        if (!isJsonObjectBytes(rawPayloadBytes)) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        synchronized (cryptoLock) {
            try {
                ZstdJsonlWriter writer = getOrRotateCryptoWriter(utcDate);
                writer.writeBinanceEvent(recvTimestampMs, rawPayloadBytes);
                totalRecordsWritten.incrementAndGet();
            } catch (IOException e) {
                Log.errorf(e, "Failed to write PolyBolt price.crypto byte event to daily Zstd file");
            }
        }
    }

    /**
     * Records a raw PolyBolt {@code price.crypto.twap} (60s) stream message into its UTC daily {@code .jsonl.zst} file.
     *
     * @param rawJson         unmodified raw WebSocket JSON payload
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void recordTwap(String rawJson, long recvTimestampMs) {
        if (!isJsonObject(rawJson)) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        synchronized (twapLock) {
            try {
                ZstdJsonlWriter writer = getOrRotateTwapWriter(utcDate);
                writer.writeBinanceEvent(recvTimestampMs, rawJson);
                totalRecordsWritten.incrementAndGet();
            } catch (IOException e) {
                Log.errorf(e, "Failed to write PolyBolt price.crypto.twap event to daily Zstd file");
            }
        }
    }

    /**
     * Records a raw UTF-8 {@code byte[]} PolyBolt {@code price.crypto.twap} (60s) stream payload directly into its
     * UTC daily {@code .jsonl.zst} file without allocating a {@link String}.
     *
     * @param rawPayloadBytes unmodified raw UTF-8 JSON bytes from the WebSocket frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void recordTwapBytes(byte[] rawPayloadBytes, long recvTimestampMs) {
        if (!isJsonObjectBytes(rawPayloadBytes)) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        synchronized (twapLock) {
            try {
                ZstdJsonlWriter writer = getOrRotateTwapWriter(utcDate);
                writer.writeBinanceEvent(recvTimestampMs, rawPayloadBytes);
                totalRecordsWritten.incrementAndGet();
            } catch (IOException e) {
                Log.errorf(e, "Failed to write PolyBolt price.crypto.twap byte event to daily Zstd file");
            }
        }
    }

    /**
     * Flushes all currently open daily PolyBolt writers to disk.
     */
    public void flushAll() {
        synchronized (cryptoLock) {
            flushWriter(activeCryptoWriter, STREAM_PRICE_CRYPTO);
        }
        synchronized (twapLock) {
            flushWriter(activeTwapWriter, STREAM_TWAP_60S);
        }
    }

    /**
     * Closes all currently open daily PolyBolt writers cleanly.
     */
    public void closeAll() {
        synchronized (cryptoLock) {
            closeWriter(activeCryptoWriter, STREAM_PRICE_CRYPTO);
            activeCryptoWriter = null;
            activeCryptoDate = null;
        }
        synchronized (twapLock) {
            closeWriter(activeTwapWriter, STREAM_TWAP_60S);
            activeTwapWriter = null;
            activeTwapDate = null;
        }
    }

    /**
     * Resolves the daily file path for a PolyBolt stream folder and UTC date.
     *
     * @param streamFolder stream folder name ({@code price_crypto} or {@code price_crypto_twap_60s})
     * @param utcDate      UTC date
     * @return path to the {@code .jsonl.zst} file
     */
    public Path resolveDailyFilePath(String streamFolder, LocalDate utcDate) {
        String dateStr = utcDate.format(UTC_DATE_FORMATTER);
        String filename = polyBoltSymbol + "-" + streamFolder + "-" + dateStr + ".jsonl.zst";
        return baseDataDir
                .resolve("polymarket")
                .resolve(streamFolder)
                .resolve(polyBoltSymbol)
                .resolve(filename);
    }

    /**
     * Returns the total number of PolyBolt records written across both daily streams since startup.
     *
     * @return total records written
     */
    public long getTotalRecordsWritten() {
        return totalRecordsWritten.get();
    }

    private ZstdJsonlWriter getOrRotateCryptoWriter(LocalDate utcDate) throws IOException {
        if (activeCryptoWriter != null && utcDate.equals(activeCryptoDate)) {
            return activeCryptoWriter;
        }
        closeWriter(activeCryptoWriter, STREAM_PRICE_CRYPTO);
        Path targetPath = resolveDailyFilePath(STREAM_PRICE_CRYPTO, utcDate);
        activeCryptoWriter = new ZstdJsonlWriter(targetPath);
        activeCryptoDate = utcDate;
        Log.infof("Opened PolyBolt %s daily stream file: %s", STREAM_PRICE_CRYPTO, targetPath);
        return activeCryptoWriter;
    }

    private ZstdJsonlWriter getOrRotateTwapWriter(LocalDate utcDate) throws IOException {
        if (activeTwapWriter != null && utcDate.equals(activeTwapDate)) {
            return activeTwapWriter;
        }
        closeWriter(activeTwapWriter, STREAM_TWAP_60S);
        Path targetPath = resolveDailyFilePath(STREAM_TWAP_60S, utcDate);
        activeTwapWriter = new ZstdJsonlWriter(targetPath);
        activeTwapDate = utcDate;
        Log.infof("Opened PolyBolt %s daily stream file: %s", STREAM_TWAP_60S, targetPath);
        return activeTwapWriter;
    }

    private static void flushWriter(ZstdJsonlWriter writer, String label) {
        if (writer != null) {
            try {
                writer.flush();
            } catch (IOException e) {
                Log.warnf("Failed to flush PolyBolt %s writer: %s", label, e.getMessage());
            }
        }
    }

    private static void closeWriter(ZstdJsonlWriter writer, String label) {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                Log.warnf("Failed to close PolyBolt %s writer: %s", label, e.getMessage());
            }
        }
    }

    private static boolean isJsonObject(String rawJson) {
        return rawJson != null && !rawJson.isBlank() && rawJson.stripLeading().charAt(0) == '{';
    }

    private static boolean isJsonObjectBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return false;
        }
        for (byte b : bytes) {
            int u = b & 0xFF;
            if (u > ' ') {
                return u == '{';
            }
        }
        return false;
    }
}
