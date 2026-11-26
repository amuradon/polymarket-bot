package cz.polymarket.bot.connector.polymarket.stream;

/**
 * Zero-allocation primitive callback sink receiving parsed Polymarket CLOB Level 2 orderbook
 * events on a 1-bps integer tick grid ({@code 0..10_000} representing {@code $0.0000..$1.0000}).
 * <p>
 * Keeps {@code connector} completely free of internal dependencies while streaming snapshots,
 * price-level deltas, crossed-book BBO pruning hints, one-way latency samples, and per-token
 * monotonic server timestamps directly into the shared {@code PolymarketOrderBook} in {@code common}.
 */
public interface ClobBookSink {

    /**
     * Returns pre-encoded UTF-8 bytes of the active UP ("Yes") conditional token ID for fast
     * byte-level dual-book snapshot detection, or an empty array if not set.
     *
     * @return UTF-8 bytes of UP token ID
     */
    byte[] getUpTokenBytes();

    /**
     * Returns pre-encoded UTF-8 bytes of the active DOWN ("No") conditional token ID for fast
     * byte-level dual-book snapshot detection, or an empty array if not set.
     *
     * @return UTF-8 bytes of DOWN token ID
     */
    byte[] getDownTokenBytes();

    /**
     * Maps a raw Polymarket {@code asset_id} string to {@code 1} for the UP token, {@code 0} for
     * the DOWN token, or {@code -1} if unrecognized.
     *
     * @param assetId Polymarket conditional token ID
     * @return {@code 1} for UP, {@code 0} for DOWN, or {@code -1} if unknown
     */
    int resolveTokenSide(String assetId);

    /**
     * Validates per-side monotonic exchange server timestamps so delayed frames from a slower
     * redundant WebSocket leg cannot regress the same token side, while never dropping frames
     * belonging to the opposite binary token.
     *
     * @param isUp              {@code true} for UP token, {@code false} for DOWN token
     * @param serverTimestampMs exchange event timestamp in epoch milliseconds ({@code <= 0} bypasses check)
     * @return {@code true} if the update should be applied
     */
    boolean tryAcceptServerTimestamp(boolean isUp, long serverTimestampMs);

    /**
     * Records an observed one-way latency sample ({@code receiptTimestampMs - serverTimestampMs}).
     *
     * @param receiptTimestampMs local frame receipt timestamp in epoch milliseconds UTC
     * @param serverTimestampMs  exchange server timestamp in epoch milliseconds UTC
     */
    void updateObservedLatency(long receiptTimestampMs, long serverTimestampMs);

    /**
     * Clears active raw levels on {@code isUp} side prior to streaming a {@code book} snapshot.
     *
     * @param isUp {@code true} for UP book, {@code false} for DOWN book
     */
    void onBookSnapshotStart(boolean isUp);

    /**
     * Populates a single raw {@code book} snapshot level at 1-bps {@code tick} in {@code [0, 10_000]}.
     *
     * @param isUp  {@code true} for UP book, {@code false} for DOWN book
     * @param isBid {@code true} for bid level, {@code false} for ask level
     * @param tick  1-bps price tick in {@code [0, 10_000]}
     * @param size  share depth at this tick
     */
    void onBookSnapshotLevel(boolean isUp, boolean isBid, int tick, double size);

    /**
     * Finalizes a {@code book} snapshot on {@code isUp} side at {@code recvTimestampMs}.
     *
     * @param isUp            {@code true} for UP book, {@code false} for DOWN book
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds UTC
     */
    void onBookSnapshotEnd(boolean isUp, long recvTimestampMs);

    /**
     * Applies an incremental {@code price_change} level delta at 1-bps {@code tick} in {@code [0, 10_000]}.
     *
     * @param isUp            {@code true} for UP book, {@code false} for DOWN book
     * @param isBid           {@code true} for bid level, {@code false} for ask level
     * @param tick            1-bps price tick in {@code [0, 10_000]}
     * @param size            new share depth ({@code 0.0} removes level)
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds UTC
     */
    void onPriceChangeLevel(boolean isUp, boolean isBid, int tick, double size, long recvTimestampMs);

    /**
     * Applies top-of-book crossed-level pruning hints ({@code -1} when absent) without fabricating share depth.
     *
     * @param isUp            {@code true} for UP book, {@code false} for DOWN book
     * @param bestBidTick     exchange-reported best bid tick in {@code [0, 10_000]}, or {@code -1}
     * @param bestAskTick     exchange-reported best ask tick in {@code [0, 10_000]}, or {@code -1}
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds UTC
     */
    void onBboHint(boolean isUp, int bestBidTick, int bestAskTick, long recvTimestampMs);
}
