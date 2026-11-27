package cz.polymarket.bot.collector.storage;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages per-market Zstandard streaming JSONL files ({@code .jsonl.zst}) for Polymarket markets,
 * partitioning archives dynamically by market series and UTC date
 * ({@code polymarket/<asset>-updown-<tf>/<yyyy-MM-dd>/<slug>.jsonl.zst})
 * and keeping each market's file open across its entire lifecycle (from discovery through resolution)
 * with per-market lock isolation and zero-decode {@code byte[]} writing.
 */
@ApplicationScoped
public class PolymarketMarketStorageManager {

    private static final DateTimeFormatter UTC_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final Path polymarketRootDir;
    private final Map<String, ZstdJsonlWriter> marketWriters = new ConcurrentHashMap<>();
    private final AtomicLong totalRecordsWritten = new AtomicLong(0L);

    /**
     * Constructs the Polymarket per-market storage manager.
     *
     * @param dataDirStr root collector data directory path
     */
    @Inject
    public PolymarketMarketStorageManager(
            @ConfigProperty(name = "polymarket.collector.data-dir", defaultValue = "data/collector")
            String dataDirStr) {
        this.polymarketRootDir = Path.of(dataDirStr).resolve("polymarket");
    }

    /**
     * Checks whether a non-empty {@code .jsonl.zst} archive already exists on disk for {@code marketSlug}
     * (used across application restarts to avoid writing duplicate {@code market_discovered} headers).
     *
     * @param marketSlug Polymarket market slug
     * @return {@code true} if the archive already exists and is non-empty
     */
    public boolean marketArchiveExists(String marketSlug) {
        if (marketSlug == null || marketSlug.isBlank()) {
            return false;
        }
        Path file = resolveMarketFilePath(marketSlug);
        try {
            return Files.exists(file) && Files.size(file) > 0L;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Appends an event to the specified market's {@code .jsonl.zst} file, opening the writer if not already open.
     *
     * @param marketSlug      Polymarket market slug (e.g. btc-updown-15m-1789041600)
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param eventType       event classification ("market_discovered", "clob")
     * @param rawJson         raw JSON payload
     */
    public void recordMarketEvent(
            String marketSlug,
            long recvTimestampMs,
            String eventType,
            String rawJson) {
        if (marketSlug == null || marketSlug.isBlank() || rawJson == null || rawJson.isBlank()) {
            return;
        }
        try {
            ZstdJsonlWriter writer = getOrCreateWriter(marketSlug);
            writer.writePolymarketEvent(recvTimestampMs, eventType, rawJson);
            totalRecordsWritten.incrementAndGet();
        } catch (IOException e) {
            Log.errorf(e, "Failed to write Polymarket %s event for market %s", eventType, marketSlug);
        }
    }

    /**
     * Appends a raw UTF-8 {@code byte[]} event payload directly to the specified market's {@code .jsonl.zst} file
     * without allocating a {@link String}.
     *
     * @param marketSlug      Polymarket market slug (e.g. btc-updown-15m-1789041600)
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param eventType       event classification ("market_discovered", "clob")
     * @param rawPayloadBytes raw UTF-8 JSON bytes
     */
    public void recordMarketEventBytes(
            String marketSlug,
            long recvTimestampMs,
            String eventType,
            byte[] rawPayloadBytes) {
        if (marketSlug == null || marketSlug.isBlank() || rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        try {
            ZstdJsonlWriter writer = getOrCreateWriter(marketSlug);
            writer.writePolymarketEvent(recvTimestampMs, eventType, rawPayloadBytes);
            totalRecordsWritten.incrementAndGet();
        } catch (IOException e) {
            Log.errorf(e, "Failed to write Polymarket %s byte event for market %s", eventType, marketSlug);
        }
    }

    /**
     * Closes and removes the writer for a completed and resolved market.
     *
     * @param marketSlug market slug to close
     */
    public synchronized void closeMarketWriter(String marketSlug) {
        ZstdJsonlWriter writer = marketWriters.remove(marketSlug);
        if (writer != null) {
            try {
                writer.close();
                Log.infof("Closed Polymarket market file for %s (%d records)", marketSlug, writer.getRecordsWritten());
            } catch (IOException e) {
                Log.warnf("Failed to close Polymarket writer for %s: %s", marketSlug, e.getMessage());
            }
        }
    }

    /**
     * Flushes all currently open market writers to disk.
     */
    public void flushAll() {
        for (Map.Entry<String, ZstdJsonlWriter> entry : marketWriters.entrySet()) {
            try {
                entry.getValue().flush();
            } catch (IOException e) {
                Log.warnf("Failed to flush Polymarket writer for %s: %s", entry.getKey(), e.getMessage());
            }
        }
    }

    /**
     * Closes all open market writers cleanly on shutdown.
     */
    public synchronized void closeAll() {
        for (String slug : Set.copyOf(marketWriters.keySet())) {
            closeMarketWriter(slug);
        }
    }

    /**
     * Resolves the series and UTC daily subdirectory file path for a given market slug
     * ({@code <polymarketRootDir>/<series>/<yyyy-MM-dd>/<marketSlug>.jsonl.zst}).
     *
     * @param marketSlug market slug formatted as {@code <series>-<epochSeconds>}
     * @return path to the market's {@code .jsonl.zst} file
     */
    public Path resolveMarketFilePath(String marketSlug) {
        String seriesDir = extractMarketSeriesFromSlug(marketSlug);
        String dateDir = extractUtcDateFromSlug(marketSlug);
        return polymarketRootDir.resolve(seriesDir).resolve(dateDir).resolve(marketSlug + ".jsonl.zst");
    }

    /**
     * Extracts the market series directory name from a market slug by stripping its trailing
     * {@code -<epochSeconds>} suffix (e.g. {@code btc-updown-5m-1789041600} -> {@code "btc-updown-5m"}).
     *
     * @param marketSlug market slug formatted as {@code <series>-<epochSeconds>}
     * @return market series directory name
     */
    public static String extractMarketSeriesFromSlug(String marketSlug) {
        if (marketSlug == null || marketSlug.isBlank()) {
            throw new IllegalArgumentException("Market slug cannot be null or blank");
        }
        int lastDash = marketSlug.lastIndexOf('-');
        if (lastDash <= 0 || lastDash == marketSlug.length() - 1) {
            throw new IllegalArgumentException("Market slug must end with '-<epochSeconds>': " + marketSlug);
        }
        return marketSlug.substring(0, lastDash);
    }

    /**
     * Extracts the UTC {@code yyyy-MM-dd} date string from a market slug's trailing epoch timestamp
     * (e.g. {@code btc-updown-15m-1789041600} -> {@code "2026-09-10"}).
     *
     * @param marketSlug market slug ending with {@code -<epochSeconds>}
     * @return UTC date formatted as {@code yyyy-MM-dd}
     */
    public static String extractUtcDateFromSlug(String marketSlug) {
        if (marketSlug == null || marketSlug.isBlank()) {
            throw new IllegalArgumentException("Market slug cannot be null or blank");
        }
        int lastDash = marketSlug.lastIndexOf('-');
        if (lastDash < 0 || lastDash == marketSlug.length() - 1) {
            throw new IllegalArgumentException("Market slug must end with '-<epochSeconds>': " + marketSlug);
        }
        String epochPart = marketSlug.substring(lastDash + 1);
        final long startSec;
        try {
            startSec = Long.parseLong(epochPart);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Market slug must end with numeric epoch seconds: " + marketSlug, e);
        }
        LocalDate utcDate = Instant.ofEpochSecond(startSec).atZone(ZoneOffset.UTC).toLocalDate();
        return utcDate.format(UTC_DATE_FORMATTER);
    }

    /**
     * Returns the total number of Polymarket records written across all markets since startup.
     *
     * @return total records written
     */
    public long getTotalRecordsWritten() {
        return totalRecordsWritten.get();
    }

    private ZstdJsonlWriter getOrCreateWriter(String marketSlug) throws IOException {
        ZstdJsonlWriter existing = marketWriters.get(marketSlug);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = marketWriters.get(marketSlug);
            if (existing != null) {
                return existing;
            }
            Path filePath = resolveMarketFilePath(marketSlug);
            ZstdJsonlWriter created = new ZstdJsonlWriter(filePath);
            marketWriters.put(marketSlug, created);
            Log.infof("Opened Polymarket market stream file: %s", filePath);
            return created;
        }
    }
}
