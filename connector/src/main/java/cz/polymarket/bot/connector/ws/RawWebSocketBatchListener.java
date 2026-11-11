package cz.polymarket.bot.connector.ws;

import java.util.List;

/**
 * Callback listener invoked with a batch of raw WebSocket text frames drained in a single pass
 * from the lock-free inbound queue. Enables downstream consumers (such as the CLOB orderbook updater)
 * to coalesce bursts and skip superseded intermediate snapshots before emitting a quote.
 */
@FunctionalInterface
public interface RawWebSocketBatchListener {

    /**
     * Handles a chronological batch of raw WebSocket frames drained from the inbound queue.
     *
     * @param batch non-empty list of frames ordered from oldest to newest within the burst
     */
    void onMessageBatch(List<TimestampedFrame> batch);
}
