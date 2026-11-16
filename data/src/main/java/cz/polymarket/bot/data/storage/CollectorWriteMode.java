package cz.polymarket.bot.data.storage;

/**
 * Operational write mode of the Market Data Collector during Blue-Green deployment cutover.
 */
public enum CollectorWriteMode {

    /**
     * Warm-up standby mode: all WebSocket streams are connected and incoming events are buffered
     * in a bounded time-windowed in-memory ring without opening or modifying {@code .jsonl.zst} files on disk.
     */
    STANDBY,

    /**
     * Active recording mode: incoming WebSocket events are written directly to Zstandard {@code .jsonl.zst} files on disk.
     */
    ACTIVE,

    /**
     * Frozen cutover mode: all open {@code .jsonl.zst} writers have been flushed and closed at the cutover timestamp,
     * and any subsequent events are ignored.
     */
    FROZEN
}
