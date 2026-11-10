package cz.polymarket.bot.data.storage;

import cz.polymarket.bot.connector.polymarket.PolymarketMarketInfo;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages per-market Zstandard streaming JSONL files ({@code .jsonl.zst}) for Polymarket markets,
 * keeping each market's file open across its entire lifecycle (from creation ~7h before open through resolution)
 * with per-market lock isolation and zero-decode {@code byte[]} writing.
 */
@ApplicationScoped
public class PolymarketMarketStorageManager {

    private final Path polymarketDir;
    private final Map<String, ZstdJsonlWriter> marketWriters = new ConcurrentHashMap<>();
    private final AtomicLong totalRecordsWritten = new AtomicLong(0L);

    /**
     * Constructs the Polymarket per-market storage manager.
     *
     * @param dataDirStr root collector data directory path
     * @param symbol     configured asset symbol (e.g. BTCUSDT)
     * @param timeframe  configured Polymarket timeframe (e.g. 15m)
     */
    @Inject
    public PolymarketMarketStorageManager(
            @ConfigProperty(name = "polymarket.collector.data-dir", defaultValue = "data/collector")
            String dataDirStr,
            @ConfigProperty(name = "polymarket.collector.symbol", defaultValue = "BTCUSDT")
            String symbol,
            @ConfigProperty(name = "polymarket.collector.timeframe", defaultValue = "15m")
            String timeframe) {
        String asset = PolymarketMarketInfo.normalizeAssetSlugPrefix(symbol);
        String tf = (timeframe == null || timeframe.isBlank()) ? "15m" : timeframe.trim().toLowerCase();
        this.polymarketDir = Path.of(dataDirStr)
                .resolve("polymarket")
                .resolve(asset + "-updown-" + tf);
    }

    /**
     * Appends an event to the specified market's {@code .jsonl.zst} file, opening the writer if not already open.
     *
     * @param marketSlug      Polymarket market slug (e.g. btc-updown-15m-1789041600)
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param eventType       event classification ("market_discovered", "clob", "market_resolved")
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
     * @param eventType       event classification ("market_discovered", "clob", "market_resolved")
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
     * Resolves the file path for a given market slug.
     *
     * @param marketSlug market slug
     * @return path to the market's {@code .jsonl.zst} file
     */
    public Path resolveMarketFilePath(String marketSlug) {
        return polymarketDir.resolve(marketSlug + ".jsonl.zst");
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
