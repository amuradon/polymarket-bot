package cz.polymarket.bot.data.storage;

import com.github.luben.zstd.ZstdOutputStream;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-efficiency Zstandard streaming JSONL writer ({@code .jsonl.zst}).
 * Writes raw WebSocket JSON payloads paired with local receipt timestamps directly as UTF-8 bytes
 * without re-parsing or re-serializing the JSON payload tree.
 */
public class ZstdJsonlWriter implements Closeable, Flushable {

    private static final int BUFFER_SIZE_BYTES = 64 * 1024;
    private static final byte[] PREFIX_TS = "{\"ts\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INFIX_PAYLOAD = ",\"payload\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INFIX_TYPE = ",\"type\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INFIX_TYPE_PAYLOAD = "\",\"payload\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SUFFIX_LINE = "}\n".getBytes(StandardCharsets.UTF_8);

    private final Path filePath;
    private final ZstdOutputStream zstdOut;
    private final AtomicLong recordsWritten = new AtomicLong(0L);
    private volatile boolean closed = false;

    /**
     * Opens or appends to a Zstandard streaming JSONL file at the given path.
     *
     * @param filePath target {@code .jsonl.zst} file path
     * @throws IOException if parent directories or the output stream cannot be opened
     */
    public ZstdJsonlWriter(Path filePath) throws IOException {
        this.filePath = filePath;
        if (filePath.getParent() != null) {
            Files.createDirectories(filePath.getParent());
        }
        OutputStream fileOut = Files.newOutputStream(
                filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
        BufferedOutputStream bufferedOut = new BufferedOutputStream(fileOut, BUFFER_SIZE_BYTES);
        this.zstdOut = new ZstdOutputStream(bufferedOut);
        this.zstdOut.setCloseFrameOnFlush(true);
    }

    /**
     * Writes a raw Binance WebSocket JSON payload with its local receipt timestamp as a single JSONL line:
     * {@code {"ts":<recvTimestampMs>,"payload":<rawPayloadJson>}}
     *
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param rawPayloadJson  unmodified raw JSON payload from the WebSocket
     * @throws IOException if writing to the underlying stream fails
     */
    public synchronized void writeBinanceEvent(long recvTimestampMs, String rawPayloadJson) throws IOException {
        if (closed || rawPayloadJson == null || rawPayloadJson.isBlank()) {
            return;
        }
        zstdOut.write(PREFIX_TS);
        zstdOut.write(Long.toString(recvTimestampMs).getBytes(StandardCharsets.UTF_8));
        zstdOut.write(INFIX_PAYLOAD);
        zstdOut.write(rawPayloadJson.strip().getBytes(StandardCharsets.UTF_8));
        zstdOut.write(SUFFIX_LINE);
        recordsWritten.incrementAndGet();
    }

    /**
     * Writes a raw Deribit Futures trade payload with its local receipt timestamp as a single JSONL line:
     * {@code {"ts":<recvTimestampMs>,"payload":<rawPayloadJson>}}
     *
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param rawPayloadJson  unmodified raw JSON payload from the WebSocket
     * @throws IOException if writing to the underlying stream fails
     */
    public synchronized void writeDeribitEvent(long recvTimestampMs, String rawPayloadJson) throws IOException {
        writeBinanceEvent(recvTimestampMs, rawPayloadJson);
    }


    /**
     * Writes a raw Polymarket event payload with its local receipt timestamp and event type as a single JSONL line:
     * {@code {"ts":<recvTimestampMs>,"type":"<eventType>","payload":<rawPayloadJson>}}
     *
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param eventType       Polymarket event classification (e.g. "market_discovered", "twap_60s", "target_price", "resolution_price", "book", "price_change", "last_trade_price")
     * @param rawPayloadJson  raw JSON payload string
     * @throws IOException if writing to the underlying stream fails
     */
    public synchronized void writePolymarketEvent(long recvTimestampMs, String eventType, String rawPayloadJson) throws IOException {
        if (closed || rawPayloadJson == null || rawPayloadJson.isBlank()) {
            return;
        }
        zstdOut.write(PREFIX_TS);
        zstdOut.write(Long.toString(recvTimestampMs).getBytes(StandardCharsets.UTF_8));
        zstdOut.write(INFIX_TYPE);
        zstdOut.write(eventType.getBytes(StandardCharsets.UTF_8));
        zstdOut.write(INFIX_TYPE_PAYLOAD);
        zstdOut.write(rawPayloadJson.strip().getBytes(StandardCharsets.UTF_8));
        zstdOut.write(SUFFIX_LINE);
        recordsWritten.incrementAndGet();
    }

    /**
     * Flushes the current Zstandard frame to disk so all buffered records are persisted and readable.
     *
     * @throws IOException if flushing fails
     */
    @Override
    public synchronized void flush() throws IOException {
        if (!closed) {
            zstdOut.flush();
        }
    }

    /**
     * Closes the Zstandard stream and underlying file output stream.
     *
     * @throws IOException if closing the stream fails
     */
    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            zstdOut.close();
        }
    }

    /**
     * Returns the path of the underlying {@code .jsonl.zst} file.
     *
     * @return file path
     */
    public Path getFilePath() {
        return filePath;
    }

    /**
     * Returns the total number of JSONL records written by this writer instance.
     *
     * @return number of records written
     */
    public long getRecordsWritten() {
        return recordsWritten.get();
    }
}
