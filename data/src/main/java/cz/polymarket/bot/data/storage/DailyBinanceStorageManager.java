package cz.polymarket.bot.data.storage;

import cz.polymarket.bot.connector.binance.BinanceStreamConnector;
import cz.polymarket.bot.connector.binance.BinanceStreamType;
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
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages daily Zstandard streaming JSONL files ({@code .jsonl.zst}) for each of the 5 Binance streams,
 * automatically rotating files at UTC midnight with per-stream lock isolation and zero-decode {@code byte[]} writing.
 */
@ApplicationScoped
public class DailyBinanceStorageManager {

    private static final DateTimeFormatter UTC_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final Path baseDataDir;
    private final String symbol;
    private final Map<BinanceStreamType, Object> streamLocks = new EnumMap<>(BinanceStreamType.class);
    private final Map<BinanceStreamType, ActiveDailyWriter> activeWriters = new ConcurrentHashMap<>();
    private final AtomicLong totalRecordsWritten = new AtomicLong(0L);

    private record ActiveDailyWriter(LocalDate utcDate, ZstdJsonlWriter writer) {
    }

    /**
     * Constructs the daily Binance storage manager.
     *
     * @param dataDirStr root collector data directory path
     * @param symbol     configured trading symbol (e.g. BTCUSDT)
     */
    @Inject
    public DailyBinanceStorageManager(
            @ConfigProperty(name = "polymarket.collector.data-dir", defaultValue = "data/collector")
            String dataDirStr,
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol) {
        this.baseDataDir = Path.of(dataDirStr);
        this.symbol = BinanceStreamConnector.normalizeSymbol(symbol);
        for (BinanceStreamType type : BinanceStreamType.values()) {
            this.streamLocks.put(type, new Object());
        }
    }

    /**
     * Records a raw Binance stream message into the stream's corresponding UTC daily {@code .jsonl.zst} file.
     * Automatically rotates to a new file when the UTC date advances.
     *
     * @param streamType      Binance stream type
     * @param rawJson         unmodified raw WebSocket JSON payload
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void record(BinanceStreamType streamType, String rawJson, long recvTimestampMs) {
        if (streamType == null || rawJson == null || rawJson.isBlank()) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        synchronized (streamLocks.get(streamType)) {
            try {
                ZstdJsonlWriter writer = getOrRotateWriter(streamType, utcDate);
                writer.writeBinanceEvent(recvTimestampMs, rawJson);
                totalRecordsWritten.incrementAndGet();
            } catch (IOException e) {
                Log.errorf(e, "Failed to write Binance %s event to daily Zstd file", streamType);
            }
        }
    }

    /**
     * Records a raw UTF-8 {@code byte[]} Binance stream payload directly into the stream's corresponding
     * UTC daily {@code .jsonl.zst} file without allocating a {@link String}.
     *
     * @param streamType      Binance stream type
     * @param rawPayloadBytes unmodified raw UTF-8 JSON bytes from the WebSocket frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void recordBytes(BinanceStreamType streamType, byte[] rawPayloadBytes, long recvTimestampMs) {
        if (streamType == null || rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        synchronized (streamLocks.get(streamType)) {
            try {
                ZstdJsonlWriter writer = getOrRotateWriter(streamType, utcDate);
                writer.writeBinanceEvent(recvTimestampMs, rawPayloadBytes);
                totalRecordsWritten.incrementAndGet();
            } catch (IOException e) {
                Log.errorf(e, "Failed to write Binance %s byte event to daily Zstd file", streamType);
            }
        }
    }

    /**
     * Flushes all currently open daily Binance stream writers to disk.
     */
    public void flushAll() {
        for (BinanceStreamType type : BinanceStreamType.values()) {
            synchronized (streamLocks.get(type)) {
                ActiveDailyWriter active = activeWriters.get(type);
                if (active != null) {
                    try {
                        active.writer().flush();
                    } catch (IOException e) {
                        Log.warnf("Failed to flush Binance %s writer: %s", type, e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Closes all open daily Binance stream writers cleanly.
     */
    public void closeAll() {
        for (BinanceStreamType type : BinanceStreamType.values()) {
            synchronized (streamLocks.get(type)) {
                ActiveDailyWriter active = activeWriters.remove(type);
                if (active != null) {
                    try {
                        active.writer().close();
                    } catch (IOException e) {
                        Log.warnf("Failed to close Binance %s writer: %s", type, e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Resolves the daily file path for a specific Binance stream type and UTC date.
     *
     * @param streamType Binance stream type
     * @param utcDate    UTC date
     * @return path to the {@code .jsonl.zst} file
     */
    public Path resolveDailyFilePath(BinanceStreamType streamType, LocalDate utcDate) {
        String dateStr = utcDate.format(UTC_DATE_FORMATTER);
        String filename = symbol + "-" + streamType.getStreamFolder() + "-" + dateStr + ".jsonl.zst";
        return baseDataDir
                .resolve("binance")
                .resolve(streamType.getMarketCategory())
                .resolve(symbol)
                .resolve(streamType.getStreamFolder())
                .resolve(filename);
    }

    /**
     * Returns the total number of Binance stream records written since startup.
     *
     * @return total records written
     */
    public long getTotalRecordsWritten() {
        return totalRecordsWritten.get();
    }

    private ZstdJsonlWriter getOrRotateWriter(BinanceStreamType streamType, LocalDate utcDate) throws IOException {
        ActiveDailyWriter current = activeWriters.get(streamType);
        if (current != null && current.utcDate().equals(utcDate)) {
            return current.writer();
        }
        if (current != null) {
            try {
                current.writer().close();
            } catch (IOException e) {
                Log.warnf("Error closing previous day writer for %s: %s", streamType, e.getMessage());
            }
        }
        Path targetPath = resolveDailyFilePath(streamType, utcDate);
        ZstdJsonlWriter newWriter = new ZstdJsonlWriter(targetPath);
        activeWriters.put(streamType, new ActiveDailyWriter(utcDate, newWriter));
        Log.infof("Opened Binance %s daily stream file: %s", streamType, targetPath);
        return newWriter;
    }
}
