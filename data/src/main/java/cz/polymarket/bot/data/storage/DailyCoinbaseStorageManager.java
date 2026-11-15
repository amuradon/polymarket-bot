package cz.polymarket.bot.data.storage;

import cz.polymarket.bot.connector.coinbase.CoinbaseStreamConnector;
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
 * Manages daily Zstandard streaming JSONL files ({@code .jsonl.zst}) for Coinbase Exchange (Coinbase Pro)
 * Spot trade streams (e.g. {@code BTCUSD}, {@code ETHUSD}), automatically rotating files at UTC midnight
 * and supporting zero-decode {@code byte[]} writing.
 * File path format: {@code coinbase/spot/{SYMBOL}/trades/{SYMBOL}-trades-{YYYY-MM-DD}.jsonl.zst}.
 */
@ApplicationScoped
public class DailyCoinbaseStorageManager {

    private static final DateTimeFormatter UTC_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final Path baseDataDir;
    private final String symbol;
    private final AtomicLong totalRecordsWritten = new AtomicLong(0L);

    private LocalDate activeDate;
    private ZstdJsonlWriter activeWriter;

    /**
     * Constructs the daily Coinbase Spot storage manager.
     *
     * @param dataDirStr root collector data directory path
     * @param symbol     configured trading symbol (e.g. BTCUSDT, normalized to BTCUSD)
     */
    @Inject
    public DailyCoinbaseStorageManager(
            @ConfigProperty(name = "polymarket.collector.data-dir", defaultValue = "data/collector")
            String dataDirStr,
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol) {
        this.baseDataDir = Path.of(dataDirStr);
        this.symbol = CoinbaseStreamConnector.normalizeSymbol(symbol);
    }

    /**
     * Records a raw Coinbase Spot trade stream message into the UTC daily {@code .jsonl.zst} file.
     * Automatically rotates to a new file when the UTC date advances.
     *
     * @param streamSymbol    Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawJson         unmodified raw WebSocket JSON payload
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public synchronized void record(String streamSymbol, String rawJson, long recvTimestampMs) {
        if (rawJson == null || rawJson.isBlank()) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        try {
            ZstdJsonlWriter writer = getOrRotateWriter(utcDate);
            writer.writeBinanceEvent(recvTimestampMs, rawJson);
            totalRecordsWritten.incrementAndGet();
        } catch (IOException e) {
            Log.errorf(e, "Failed to write Coinbase %s trade event to daily Zstd file", symbol);
        }
    }

    /**
     * Records a raw UTF-8 {@code byte[]} Coinbase Spot trade payload directly into the UTC daily
     * {@code .jsonl.zst} file without allocating a {@link String}.
     *
     * @param streamSymbol    Coinbase stream symbol (e.g. {@code "BTCUSD"})
     * @param rawPayloadBytes unmodified raw UTF-8 JSON bytes from the WebSocket frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public synchronized void recordBytes(String streamSymbol, byte[] rawPayloadBytes, long recvTimestampMs) {
        if (rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        try {
            ZstdJsonlWriter writer = getOrRotateWriter(utcDate);
            writer.writeBinanceEvent(recvTimestampMs, rawPayloadBytes);
            totalRecordsWritten.incrementAndGet();
        } catch (IOException e) {
            Log.errorf(e, "Failed to write Coinbase %s byte trade event to daily Zstd file", symbol);
        }
    }

    /**
     * Flushes the currently open daily Coinbase trade stream writer to disk.
     */
    public synchronized void flushAll() {
        if (activeWriter != null) {
            try {
                activeWriter.flush();
            } catch (IOException e) {
                Log.warnf("Failed to flush Coinbase %s writer: %s", symbol, e.getMessage());
            }
        }
    }

    /**
     * Closes the open daily Coinbase trade stream writer cleanly.
     */
    public synchronized void closeAll() {
        if (activeWriter != null) {
            try {
                activeWriter.close();
            } catch (IOException e) {
                Log.warnf("Failed to close Coinbase %s writer: %s", symbol, e.getMessage());
            }
            activeWriter = null;
            activeDate = null;
        }
    }

    /**
     * Resolves the daily file path for Coinbase Spot trades for the given UTC date.
     *
     * @param utcDate UTC date
     * @return path to the {@code .jsonl.zst} file
     */
    public Path resolveDailyFilePath(LocalDate utcDate) {
        String dateStr = utcDate.format(UTC_DATE_FORMATTER);
        String filename = symbol + "-trades-" + dateStr + ".jsonl.zst";
        return baseDataDir
                .resolve("coinbase")
                .resolve("spot")
                .resolve(symbol)
                .resolve("trades")
                .resolve(filename);
    }

    /**
     * Returns the active Coinbase folder/file symbol (e.g. {@code "BTCUSD"}).
     *
     * @return normalized symbol
     */
    public String getSymbol() {
        return symbol;
    }

    /**
     * Returns the total number of Coinbase stream records written since startup.
     *
     * @return total records written
     */
    public long getTotalRecordsWritten() {
        return totalRecordsWritten.get();
    }

    private ZstdJsonlWriter getOrRotateWriter(LocalDate utcDate) throws IOException {
        if (activeWriter != null && utcDate.equals(activeDate)) {
            return activeWriter;
        }
        if (activeWriter != null) {
            try {
                activeWriter.close();
            } catch (IOException e) {
                Log.warnf("Error closing previous day writer for Coinbase %s: %s", symbol, e.getMessage());
            }
        }
        Path targetPath = resolveDailyFilePath(utcDate);
        activeWriter = new ZstdJsonlWriter(targetPath);
        activeDate = utcDate;
        Log.infof("Opened Coinbase Spot %s daily trades stream file: %s", symbol, targetPath);
        return activeWriter;
    }
}
