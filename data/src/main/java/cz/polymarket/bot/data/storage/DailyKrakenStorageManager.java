package cz.polymarket.bot.data.storage;

import cz.polymarket.bot.connector.kraken.KrakenStreamConnector;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages daily Zstandard streaming JSONL files ({@code .jsonl.zst}) for Kraken Spot trade streams
 * (e.g. {@code USDTUSD} and the configured symbol such as {@code BTCUSD} or {@code ETHUSD}),
 * automatically rotating files at UTC midnight with per-symbol lock isolation and zero-decode {@code byte[]} writing.
 * File path format: {@code kraken/spot/{SYMBOL}/trades/{SYMBOL}-trades-{YYYY-MM-DD}.jsonl.zst}.
 */
@ApplicationScoped
public class DailyKrakenStorageManager {

    private static final DateTimeFormatter UTC_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final Path baseDataDir;
    private final String primarySymbol;
    private final Map<String, Object> symbolLocks = new ConcurrentHashMap<>();
    private final Map<String, ActiveDailyWriter> activeWriters = new ConcurrentHashMap<>();
    private final AtomicLong totalRecordsWritten = new AtomicLong(0L);

    private record ActiveDailyWriter(LocalDate utcDate, ZstdJsonlWriter writer) {
    }

    /**
     * Constructs the daily Kraken Spot storage manager.
     *
     * @param dataDirStr root collector data directory path
     * @param symbol     configured trading symbol (e.g. BTCUSDT, normalized to BTCUSD)
     */
    @Inject
    public DailyKrakenStorageManager(
            @ConfigProperty(name = "polymarket.collector.data-dir", defaultValue = "data/collector")
            String dataDirStr,
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol) {
        this.baseDataDir = Path.of(dataDirStr);
        this.primarySymbol = KrakenStreamConnector.normalizeSymbol(symbol);
        this.symbolLocks.put(KrakenStreamConnector.USDTUSD_SYMBOL, new Object());
        this.symbolLocks.put(this.primarySymbol, new Object());
    }

    /**
     * Records a raw Kraken Spot trade stream message into the symbol's corresponding UTC daily {@code .jsonl.zst} file.
     * Automatically rotates to a new file when the UTC date advances.
     *
     * @param symbol          Kraken symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawJson         unmodified raw WebSocket JSON payload
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void record(String symbol, String rawJson, long recvTimestampMs) {
        if (symbol == null || symbol.isBlank() || rawJson == null || rawJson.isBlank()) {
            return;
        }
        String normSym = KrakenStreamConnector.normalizeSymbol(symbol);
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        Object lock = symbolLocks.computeIfAbsent(normSym, k -> new Object());
        synchronized (lock) {
            try {
                ZstdJsonlWriter writer = getOrRotateWriter(normSym, utcDate);
                writer.writeBinanceEvent(recvTimestampMs, rawJson);
                totalRecordsWritten.incrementAndGet();
            } catch (IOException e) {
                Log.errorf(e, "Failed to write Kraken %s trade event to daily Zstd file", normSym);
            }
        }
    }

    /**
     * Records a raw UTF-8 {@code byte[]} Kraken Spot trade payload directly into the symbol's corresponding
     * UTC daily {@code .jsonl.zst} file without allocating a {@link String}.
     *
     * @param symbol          Kraken symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param rawPayloadBytes unmodified raw UTF-8 JSON bytes from the WebSocket frame
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     */
    public void recordBytes(String symbol, byte[] rawPayloadBytes, long recvTimestampMs) {
        if (symbol == null || symbol.isBlank() || rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        String normSym = KrakenStreamConnector.normalizeSymbol(symbol);
        LocalDate utcDate = Instant.ofEpochMilli(recvTimestampMs).atZone(ZoneOffset.UTC).toLocalDate();
        Object lock = symbolLocks.computeIfAbsent(normSym, k -> new Object());
        synchronized (lock) {
            try {
                ZstdJsonlWriter writer = getOrRotateWriter(normSym, utcDate);
                writer.writeBinanceEvent(recvTimestampMs, rawPayloadBytes);
                totalRecordsWritten.incrementAndGet();
            } catch (IOException e) {
                Log.errorf(e, "Failed to write Kraken %s byte trade event to daily Zstd file", normSym);
            }
        }
    }

    /**
     * Flushes all currently open daily Kraken stream writers to disk.
     */
    public void flushAll() {
        for (Map.Entry<String, Object> entry : symbolLocks.entrySet()) {
            String sym = entry.getKey();
            synchronized (entry.getValue()) {
                ActiveDailyWriter active = activeWriters.get(sym);
                if (active != null) {
                    try {
                        active.writer().flush();
                    } catch (IOException e) {
                        Log.warnf("Failed to flush Kraken %s writer: %s", sym, e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Closes all open daily Kraken stream writers cleanly.
     */
    public void closeAll() {
        for (Map.Entry<String, Object> entry : symbolLocks.entrySet()) {
            String sym = entry.getKey();
            synchronized (entry.getValue()) {
                ActiveDailyWriter active = activeWriters.remove(sym);
                if (active != null) {
                    try {
                        active.writer().close();
                    } catch (IOException e) {
                        Log.warnf("Failed to close Kraken %s writer: %s", sym, e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Resolves the daily file path for a specific Kraken Spot symbol and UTC date.
     *
     * @param symbol  normalized Kraken symbol (e.g. {@code "USDTUSD"}, {@code "BTCUSD"})
     * @param utcDate UTC date
     * @return path to the {@code .jsonl.zst} file
     */
    public Path resolveDailyFilePath(String symbol, LocalDate utcDate) {
        String normSym = KrakenStreamConnector.normalizeSymbol(symbol);
        String dateStr = utcDate.format(UTC_DATE_FORMATTER);
        String filename = normSym + "-trades-" + dateStr + ".jsonl.zst";
        return baseDataDir
                .resolve("kraken")
                .resolve("spot")
                .resolve(normSym)
                .resolve("trades")
                .resolve(filename);
    }

    /**
     * Returns the normalized primary symbol configured for this storage manager (e.g. {@code "BTCUSD"}).
     *
     * @return primary symbol
     */
    public String getPrimarySymbol() {
        return primarySymbol;
    }

    /**
     * Returns the total number of Kraken stream records written since startup.
     *
     * @return total records written
     */
    public long getTotalRecordsWritten() {
        return totalRecordsWritten.get();
    }

    private ZstdJsonlWriter getOrRotateWriter(String normSym, LocalDate utcDate) throws IOException {
        ActiveDailyWriter current = activeWriters.get(normSym);
        if (current != null && current.utcDate().equals(utcDate)) {
            return current.writer();
        }
        if (current != null) {
            try {
                current.writer().close();
            } catch (IOException e) {
                Log.warnf("Error closing previous day writer for Kraken %s: %s", normSym, e.getMessage());
            }
        }
        Path targetPath = resolveDailyFilePath(normSym, utcDate);
        ZstdJsonlWriter newWriter = new ZstdJsonlWriter(targetPath);
        activeWriters.put(normSym, new ActiveDailyWriter(utcDate, newWriter));
        Log.infof("Opened Kraken Spot %s daily trades stream file: %s", normSym, targetPath);
        return newWriter;
    }
}
