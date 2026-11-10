package cz.polymarket.bot.data.storage;

import cz.polymarket.bot.connector.deribit.DeribitStreamConnector;
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
 * Manages daily Zstandard streaming JSONL files ({@code .jsonl.zst}) for Deribit Futures trade streams,
 * automatically rotating files at UTC midnight and supporting zero-decode {@code byte[]} writing.
 * File path format: {@code deribit/futures/{instrument}/trades/{instrument}-trades-{YYYY-MM-DD}.jsonl.zst}.
 */
@ApplicationScoped
public class DailyDeribitStorageManager {

    private static final DateTimeFormatter UTC_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final Path baseDataDir;
    private final String instrument;
    private final AtomicLong totalRecordsWritten = new AtomicLong(0L);

    private LocalDate activeDate;
    private ZstdJsonlWriter activeWriter;

    /**
     * Constructs the daily Deribit storage manager.
     *
     * @param dataDirStr root collector data directory path
     * @param symbol     configured trading symbol (e.g. BTCUSDT, normalized to Deribit instrument like BTC-PERPETUAL)
     */
    @Inject
    public DailyDeribitStorageManager(
            @ConfigProperty(name = "polymarket.collector.data-dir", defaultValue = "data/collector")
            String dataDirStr,
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol) {
        this.baseDataDir = Path.of(dataDirStr);
        this.instrument = DeribitStreamConnector.normalizeInstrument(symbol);
    }

    /**
     * Records a raw Deribit Futures trade stream message into the UTC daily {@code .jsonl.zst} file.
     * Automatically rotates to a new file when the UTC date advances.
     *
     * @param rawJson         unmodified raw WebSocket JSON payload
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public synchronized void record(String rawJson, long recvTimestampMs) {
        if (rawJson == null || rawJson.isBlank()) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        try {
            ZstdJsonlWriter writer = getOrRotateWriter(utcDate);
            writer.writeDeribitEvent(recvTimestampMs, rawJson);
            totalRecordsWritten.incrementAndGet();
        } catch (IOException e) {
            Log.errorf(e, "Failed to write Deribit trade event to daily Zstd file");
        }
    }

    /**
     * Records a raw UTF-8 {@code byte[]} Deribit Futures trade payload directly into the UTC daily
     * {@code .jsonl.zst} file without allocating a {@link String}.
     *
     * @param rawPayloadBytes unmodified raw UTF-8 JSON bytes from the WebSocket frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public synchronized void recordBytes(byte[] rawPayloadBytes, long recvTimestampMs) {
        if (rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        try {
            ZstdJsonlWriter writer = getOrRotateWriter(utcDate);
            writer.writeDeribitEvent(recvTimestampMs, rawPayloadBytes);
            totalRecordsWritten.incrementAndGet();
        } catch (IOException e) {
            Log.errorf(e, "Failed to write Deribit byte trade event to daily Zstd file");
        }
    }

    /**
     * Flushes the currently open daily Deribit trade stream writer to disk.
     */
    public synchronized void flushAll() {
        if (activeWriter != null) {
            try {
                activeWriter.flush();
            } catch (IOException e) {
                Log.warnf("Failed to flush Deribit writer: %s", e.getMessage());
            }
        }
    }

    /**
     * Closes the open daily Deribit trade stream writer cleanly.
     */
    public synchronized void closeAll() {
        if (activeWriter != null) {
            try {
                activeWriter.close();
            } catch (IOException e) {
                Log.warnf("Failed to close Deribit writer: %s", e.getMessage());
            }
            activeWriter = null;
            activeDate = null;
        }
    }

    /**
     * Resolves the daily file path for Deribit Futures trades for the given UTC date.
     *
     * @param utcDate UTC date
     * @return path to the {@code .jsonl.zst} file
     */
    public Path resolveDailyFilePath(LocalDate utcDate) {
        String dateStr = utcDate.format(UTC_DATE_FORMATTER);
        String filename = instrument + "-trades-" + dateStr + ".jsonl.zst";
        return baseDataDir
                .resolve("deribit")
                .resolve("futures")
                .resolve(instrument)
                .resolve("trades")
                .resolve(filename);
    }

    /**
     * Returns the total number of Deribit stream records written since startup.
     *
     * @return total records written
     */
    public long getTotalRecordsWritten() {
        return totalRecordsWritten.get();
    }

    /**
     * Returns the active Deribit instrument name (e.g. BTC-PERPETUAL).
     *
     * @return instrument name
     */
    public String getInstrument() {
        return instrument;
    }

    private ZstdJsonlWriter getOrRotateWriter(LocalDate utcDate) throws IOException {
        if (activeWriter != null && utcDate.equals(activeDate)) {
            return activeWriter;
        }
        if (activeWriter != null) {
            try {
                activeWriter.close();
            } catch (IOException e) {
                Log.warnf("Error closing previous day writer for Deribit: %s", e.getMessage());
            }
        }
        Path targetPath = resolveDailyFilePath(utcDate);
        activeWriter = new ZstdJsonlWriter(targetPath);
        activeDate = utcDate;
        Log.infof("Opened Deribit Futures daily stream file: %s", targetPath);
        return activeWriter;
    }
}
