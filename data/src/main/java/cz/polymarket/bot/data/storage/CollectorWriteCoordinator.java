package cz.polymarket.bot.data.storage;

import cz.polymarket.bot.connector.binance.BinanceStreamType;
import cz.polymarket.bot.data.web.CutoverActivateResponseDto;
import cz.polymarket.bot.data.web.CutoverFreezeResponseDto;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;

/**
 * Coordinates zero-loss, zero-duplicate Blue-Green deployment cutover across all Data Collector storage managers.
 * <p>
 * In {@link CollectorWriteMode#STANDBY}, incoming WebSocket frames are held in a time-windowed in-memory queue
 * without touching {@code .jsonl.zst} files on disk while the previous instance is still writing.
 * Upon {@link #activate(long)}, buffered frames with {@code recvTimestampMs > afterTimestampMs} are replayed to disk
 * and the coordinator switches to direct lock-free {@link CollectorWriteMode#ACTIVE} writing.
 */
@ApplicationScoped
public class CollectorWriteCoordinator {

    private enum EventSource {
        BINANCE,
        POLYBOLT_CRYPTO_PRICE,
        POLYBOLT_TWAP,
        POLYMARKET_MARKET
    }

    private record BufferedEvent(
            EventSource source,
            BinanceStreamType binanceStreamType,
            String key,
            String eventType,
            byte[] rawPayloadBytes,
            long recvTimestampMs
    ) {
    }

    private final DailyBinanceStorageManager binanceStorage;
    private final DailyPolyBoltStorageManager polyBoltDailyStorage;
    private final PolymarketMarketStorageManager polymarketStorage;
    private final long standbyBufferRetentionMs;

    private final Object transitionLock = new Object();
    private final Deque<BufferedEvent> standbyBuffer = new ArrayDeque<>(4096);

    private volatile CollectorWriteMode mode;
    private volatile long activationCutoffTimestampMs = Long.MIN_VALUE;
    private volatile long freezeCutoffTimestampMs = Long.MAX_VALUE;
    private volatile long standbyBufferedCount = 0L;

    /**
     * Constructs the write coordinator.
     *
     * @param binanceStorage           daily Binance {@code bookTicker} storage manager
     * @param polyBoltDailyStorage     daily PolyBolt storage manager
     * @param polymarketStorage        per-market Polymarket storage manager
     * @param initialWriteMode         initial write mode ({@code "ACTIVE"} or {@code "STANDBY"})
     * @param standbyBufferRetentionMs retention window in milliseconds for buffered standby events (default 30000)
     */
    @Inject
    public CollectorWriteCoordinator(
            DailyBinanceStorageManager binanceStorage,
            DailyPolyBoltStorageManager polyBoltDailyStorage,
            PolymarketMarketStorageManager polymarketStorage,
            @ConfigProperty(name = "polymarket.collector.initial-write-mode", defaultValue = "ACTIVE")
            String initialWriteMode,
            @ConfigProperty(name = "polymarket.collector.standby-buffer-retention-ms", defaultValue = "30000")
            long standbyBufferRetentionMs) {
        this.binanceStorage = binanceStorage;
        this.polyBoltDailyStorage = polyBoltDailyStorage;
        this.polymarketStorage = polymarketStorage;
        this.standbyBufferRetentionMs = Math.max(1_000L, standbyBufferRetentionMs);
        this.mode = parseWriteMode(initialWriteMode);
    }

    private static CollectorWriteMode parseWriteMode(String rawMode) {
        if (rawMode == null || rawMode.isBlank()) {
            return CollectorWriteMode.ACTIVE;
        }
        String normalized = rawMode.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "STANDBY" -> CollectorWriteMode.STANDBY;
            case "FROZEN" -> CollectorWriteMode.FROZEN;
            default -> CollectorWriteMode.ACTIVE;
        };
    }

    /**
     * Returns the current write mode ({@link CollectorWriteMode#STANDBY}, {@link CollectorWriteMode#ACTIVE}, or {@link CollectorWriteMode#FROZEN}).
     *
     * @return current write mode
     */
    public CollectorWriteMode getWriteMode() {
        return mode;
    }

    /**
     * Returns the number of records currently buffered in memory while in {@link CollectorWriteMode#STANDBY}.
     *
     * @return buffered standby record count
     */
    public long getStandbyBufferedRecords() {
        return standbyBufferedCount;
    }

    /**
     * Returns the total number of records written across all storage managers.
     *
     * @return total records written to disk
     */
    public long getTotalRecordsWritten() {
        return binanceStorage.getTotalRecordsWritten()
                + polyBoltDailyStorage.getTotalRecordsWritten()
                + polymarketStorage.getTotalRecordsWritten();
    }

    /**
     * Records a Binance {@code bookTicker} stream message (UTF-8 {@link String}) or buffers it if in
     * {@link CollectorWriteMode#STANDBY}.
     *
     * @param streamType      Binance stream type ({@code SPOT_BOOK_TICKER} or {@code FUTURES_BOOK_TICKER})
     * @param rawJson         raw JSON payload
     * @param recvTimestampMs receipt timestamp in epoch milliseconds
     */
    public void recordBinance(BinanceStreamType streamType, String rawJson, long recvTimestampMs) {
        DailyBinanceStorageManager.validateSupportedStream(streamType);
        if (rawJson == null || rawJson.isBlank()) {
            return;
        }
        recordBinanceBytes(streamType, rawJson.getBytes(StandardCharsets.UTF_8), recvTimestampMs);
    }

    /**
     * Records a Binance {@code bookTicker} stream message ({@code byte[]}) or buffers it if in
     * {@link CollectorWriteMode#STANDBY}.
     *
     * @param streamType      Binance stream type ({@code SPOT_BOOK_TICKER} or {@code FUTURES_BOOK_TICKER})
     * @param rawPayloadBytes raw UTF-8 JSON bytes
     * @param recvTimestampMs receipt timestamp in epoch milliseconds
     */
    public void recordBinanceBytes(BinanceStreamType streamType, byte[] rawPayloadBytes, long recvTimestampMs) {
        DailyBinanceStorageManager.validateSupportedStream(streamType);
        if (rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        CollectorWriteMode currentMode = this.mode;
        if (currentMode == CollectorWriteMode.ACTIVE) {
            if (recvTimestampMs > activationCutoffTimestampMs && recvTimestampMs <= freezeCutoffTimestampMs) {
                binanceStorage.recordBytes(streamType, rawPayloadBytes, recvTimestampMs);
            }
            return;
        }
        if (currentMode == CollectorWriteMode.FROZEN) {
            return;
        }
        enqueueOrWriteStandby(new BufferedEvent(EventSource.BINANCE, streamType, null, null, rawPayloadBytes.clone(), recvTimestampMs));
    }

    /**
     * Records a PolyBolt {@code price.crypto} message (UTF-8 {@link String}) or buffers it if in {@link CollectorWriteMode#STANDBY}.
     *
     * @param rawJson         raw JSON payload
     * @param recvTimestampMs receipt timestamp in epoch milliseconds
     */
    public void recordPolyBoltCryptoPrice(String rawJson, long recvTimestampMs) {
        if (rawJson == null || rawJson.isBlank()) {
            return;
        }
        recordPolyBoltCryptoPriceBytes(rawJson.getBytes(StandardCharsets.UTF_8), recvTimestampMs);
    }

    /**
     * Records a PolyBolt {@code price.crypto} message ({@code byte[]}) or buffers it if in {@link CollectorWriteMode#STANDBY}.
     *
     * @param rawPayloadBytes raw UTF-8 JSON bytes
     * @param recvTimestampMs receipt timestamp in epoch milliseconds
     */
    public void recordPolyBoltCryptoPriceBytes(byte[] rawPayloadBytes, long recvTimestampMs) {
        if (rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        CollectorWriteMode currentMode = this.mode;
        if (currentMode == CollectorWriteMode.ACTIVE) {
            if (recvTimestampMs > activationCutoffTimestampMs && recvTimestampMs <= freezeCutoffTimestampMs) {
                polyBoltDailyStorage.recordCryptoPriceBytes(rawPayloadBytes, recvTimestampMs);
            }
            return;
        }
        if (currentMode == CollectorWriteMode.FROZEN) {
            return;
        }
        enqueueOrWriteStandby(new BufferedEvent(EventSource.POLYBOLT_CRYPTO_PRICE, null, null, null, rawPayloadBytes.clone(), recvTimestampMs));
    }

    /**
     * Records a PolyBolt {@code price.crypto.twap} message (UTF-8 {@link String}) or buffers it if in {@link CollectorWriteMode#STANDBY}.
     *
     * @param rawJson         raw JSON payload
     * @param recvTimestampMs receipt timestamp in epoch milliseconds
     */
    public void recordPolyBoltTwap(String rawJson, long recvTimestampMs) {
        if (rawJson == null || rawJson.isBlank()) {
            return;
        }
        recordPolyBoltTwapBytes(rawJson.getBytes(StandardCharsets.UTF_8), recvTimestampMs);
    }

    /**
     * Records a PolyBolt {@code price.crypto.twap} message ({@code byte[]}) or buffers it if in {@link CollectorWriteMode#STANDBY}.
     *
     * @param rawPayloadBytes raw UTF-8 JSON bytes
     * @param recvTimestampMs receipt timestamp in epoch milliseconds
     */
    public void recordPolyBoltTwapBytes(byte[] rawPayloadBytes, long recvTimestampMs) {
        if (rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        CollectorWriteMode currentMode = this.mode;
        if (currentMode == CollectorWriteMode.ACTIVE) {
            if (recvTimestampMs > activationCutoffTimestampMs && recvTimestampMs <= freezeCutoffTimestampMs) {
                polyBoltDailyStorage.recordTwapBytes(rawPayloadBytes, recvTimestampMs);
            }
            return;
        }
        if (currentMode == CollectorWriteMode.FROZEN) {
            return;
        }
        enqueueOrWriteStandby(new BufferedEvent(EventSource.POLYBOLT_TWAP, null, null, null, rawPayloadBytes.clone(), recvTimestampMs));
    }

    /**
     * Records a Polymarket per-market event (UTF-8 {@link String}) or buffers it if in {@link CollectorWriteMode#STANDBY}.
     *
     * @param marketSlug      Polymarket market slug
     * @param recvTimestampMs receipt timestamp in epoch milliseconds
     * @param eventType       event classification ({@code "market_discovered"}, {@code "clob"})
     * @param rawJson         raw JSON payload
     */
    public void recordPolymarketEvent(String marketSlug, long recvTimestampMs, String eventType, String rawJson) {
        if (marketSlug == null || marketSlug.isBlank() || eventType == null || rawJson == null || rawJson.isBlank()) {
            return;
        }
        recordPolymarketEventBytes(marketSlug, recvTimestampMs, eventType, rawJson.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Records a Polymarket per-market event ({@code byte[]}) or buffers it if in {@link CollectorWriteMode#STANDBY}.
     *
     * @param marketSlug      Polymarket market slug
     * @param recvTimestampMs receipt timestamp in epoch milliseconds
     * @param eventType       event classification ({@code "market_discovered"}, {@code "clob"})
     * @param rawPayloadBytes raw UTF-8 JSON bytes
     */
    public void recordPolymarketEventBytes(String marketSlug, long recvTimestampMs, String eventType, byte[] rawPayloadBytes) {
        if (marketSlug == null || marketSlug.isBlank() || eventType == null || rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        CollectorWriteMode currentMode = this.mode;
        if (currentMode == CollectorWriteMode.ACTIVE) {
            if (recvTimestampMs > activationCutoffTimestampMs && recvTimestampMs <= freezeCutoffTimestampMs) {
                polymarketStorage.recordMarketEventBytes(marketSlug, recvTimestampMs, eventType, rawPayloadBytes);
            }
            return;
        }
        if (currentMode == CollectorWriteMode.FROZEN) {
            return;
        }
        enqueueOrWriteStandby(new BufferedEvent(EventSource.POLYMARKET_MARKET, null, marketSlug, eventType, rawPayloadBytes.clone(), recvTimestampMs));
    }

    /**
     * Closes the Polymarket market writer for a completed market if currently in {@link CollectorWriteMode#ACTIVE}.
     *
     * @param marketSlug market slug to close
     */
    public void closePolymarketMarketWriter(String marketSlug) {
        if (this.mode == CollectorWriteMode.ACTIVE) {
            polymarketStorage.closeMarketWriter(marketSlug);
        }
    }

    /**
     * Flushes all open Zstandard writers to disk when in {@link CollectorWriteMode#ACTIVE}.
     * No-op when in {@link CollectorWriteMode#STANDBY} or {@link CollectorWriteMode#FROZEN}.
     */
    public void flushAllIfActive() {
        if (this.mode != CollectorWriteMode.ACTIVE) {
            return;
        }
        binanceStorage.flushAll();
        polyBoltDailyStorage.flushAll();
        polymarketStorage.flushAll();
    }

    /**
     * Closes all open Zstandard writers across all storage managers.
     */
    public void closeAll() {
        synchronized (transitionLock) {
            standbyBuffer.clear();
            standbyBufferedCount = 0L;
            binanceStorage.closeAll();
            polyBoltDailyStorage.closeAll();
            polymarketStorage.closeAll();
        }
    }

    /**
     * Atomically freezes this collector instance at {@code cutoverTimestampMs}, rejecting any subsequent writes
     * with {@code recvTimestampMs > cutoverTimestampMs}, and flushes and closes all open {@code .jsonl.zst} files.
     *
     * @param cutoverTimestampMs epoch millisecond cutoff timestamp
     * @return freeze result summary
     */
    public CutoverFreezeResponseDto freeze(long cutoverTimestampMs) {
        synchronized (transitionLock) {
            this.freezeCutoffTimestampMs = cutoverTimestampMs;
            this.mode = CollectorWriteMode.FROZEN;
            standbyBuffer.clear();
            standbyBufferedCount = 0L;
            binanceStorage.flushAll();
            polyBoltDailyStorage.flushAll();
            polymarketStorage.flushAll();
            binanceStorage.closeAll();
            polyBoltDailyStorage.closeAll();
            polymarketStorage.closeAll();
            long totalWritten = getTotalRecordsWritten();
            Log.infof("Data Collector FROZEN at cutoverTimestampMs=%d (totalRecordsWritten=%d). All Zstd frames flushed and closed.",
                    cutoverTimestampMs, totalWritten);
            return new CutoverFreezeResponseDto(CollectorWriteMode.FROZEN.name(), cutoverTimestampMs, totalWritten);
        }
    }

    /**
     * Atomically activates this collector instance from {@link CollectorWriteMode#STANDBY} (or {@link CollectorWriteMode#FROZEN})
     * into {@link CollectorWriteMode#ACTIVE}, replaying buffered events with {@code recvTimestampMs > afterTimestampMs}
     * and discarding earlier overlapping events already persisted by the previous slot.
     *
     * @param afterTimestampMs cutover timestamp returned by the freezing slot
     * @return activation result summary
     */
    public CutoverActivateResponseDto activate(long afterTimestampMs) {
        synchronized (transitionLock) {
            this.activationCutoffTimestampMs = afterTimestampMs;
            this.freezeCutoffTimestampMs = Long.MAX_VALUE;
            long replayed = 0L;
            long dropped = 0L;

            while (!standbyBuffer.isEmpty()) {
                BufferedEvent ev = standbyBuffer.pollFirst();
                if (ev.source() == EventSource.POLYMARKET_MARKET && "market_discovered".equals(ev.eventType())) {
                    Path marketFile = polymarketStorage.resolveMarketFilePath(ev.key());
                    if (!Files.exists(marketFile) || ev.recvTimestampMs() > afterTimestampMs) {
                        writeEventDirect(ev);
                        replayed++;
                    } else {
                        dropped++;
                    }
                } else if (ev.recvTimestampMs() > afterTimestampMs) {
                    writeEventDirect(ev);
                    replayed++;
                } else {
                    dropped++;
                }
            }

            this.standbyBufferedCount = 0L;
            this.mode = CollectorWriteMode.ACTIVE;
            Log.infof("Data Collector ACTIVATED afterTimestampMs=%d (replayedRecords=%d, droppedDuplicateRecords=%d).",
                    afterTimestampMs, replayed, dropped);
            return new CutoverActivateResponseDto(CollectorWriteMode.ACTIVE.name(), afterTimestampMs, replayed, dropped);
        }
    }

    private void enqueueOrWriteStandby(BufferedEvent event) {
        synchronized (transitionLock) {
            CollectorWriteMode currentMode = this.mode;
            if (currentMode == CollectorWriteMode.ACTIVE) {
                if (event.recvTimestampMs() > activationCutoffTimestampMs && event.recvTimestampMs() <= freezeCutoffTimestampMs) {
                    writeEventDirect(event);
                }
                return;
            }
            if (currentMode == CollectorWriteMode.FROZEN) {
                return;
            }
            standbyBuffer.addLast(event);
            long minAllowedTs = event.recvTimestampMs() - standbyBufferRetentionMs;
            while (!standbyBuffer.isEmpty() && standbyBuffer.peekFirst().recvTimestampMs() < minAllowedTs) {
                standbyBuffer.pollFirst();
            }
            standbyBufferedCount = standbyBuffer.size();
        }
    }

    private void writeEventDirect(BufferedEvent ev) {
        switch (ev.source()) {
            case BINANCE -> binanceStorage.recordBytes(ev.binanceStreamType(), ev.rawPayloadBytes(), ev.recvTimestampMs());
            case POLYBOLT_CRYPTO_PRICE -> polyBoltDailyStorage.recordCryptoPriceBytes(ev.rawPayloadBytes(), ev.recvTimestampMs());
            case POLYBOLT_TWAP -> polyBoltDailyStorage.recordTwapBytes(ev.rawPayloadBytes(), ev.recvTimestampMs());
            case POLYMARKET_MARKET -> polymarketStorage.recordMarketEventBytes(ev.key(), ev.recvTimestampMs(), ev.eventType(), ev.rawPayloadBytes());
        }
    }
}
