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
 * without re-parsing, re-serializing, or allocating intermediate {@link String} or {@code byte[]} objects.
 */
public class ZstdJsonlWriter implements Closeable, Flushable {

    private static final int BUFFER_SIZE_BYTES = 64 * 1024;
    private static final byte[] PREFIX_TS = "{\"ts\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INFIX_PAYLOAD = ",\"payload\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INFIX_TYPE = ",\"type\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INFIX_TYPE_PAYLOAD = "\",\"payload\":".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SUFFIX_LINE = "}\n".getBytes(StandardCharsets.UTF_8);

    private final Path filePath;
    private final OutputStream underlyingOut;
    private final ZstdOutputStream zstdOut;
    private final byte[] timestampAsciiScratch = new byte[20];
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
        this.underlyingOut = bufferedOut;
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
        writeBinanceEvent(recvTimestampMs, rawPayloadJson.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Writes a raw UTF-8 {@code byte[]} payload with its local receipt timestamp directly into the Zstandard stream
     * without allocating a {@link String} or temporary {@code byte[]} copy:
     * {@code {"ts":<recvTimestampMs>,"payload":<rawPayloadBytes>}}
     *
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param rawPayloadBytes unmodified raw UTF-8 JSON bytes from the WebSocket frame
     * @throws IOException if writing to the underlying stream fails
     */
    public synchronized void writeBinanceEvent(long recvTimestampMs, byte[] rawPayloadBytes) throws IOException {
        if (closed || rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        int start = firstNonWhitespaceIndex(rawPayloadBytes);
        int end = lastNonWhitespaceEndIndex(rawPayloadBytes, start);
        if (start >= end) {
            return;
        }
        zstdOut.write(PREFIX_TS);
        writeTimestampAscii(recvTimestampMs);
        zstdOut.write(INFIX_PAYLOAD);
        zstdOut.write(rawPayloadBytes, start, end - start);
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
     * Writes a raw Deribit Futures trade UTF-8 {@code byte[]} payload with its local receipt timestamp
     * directly into the Zstandard stream with zero heap allocations:
     * {@code {"ts":<recvTimestampMs>,"payload":<rawPayloadBytes>}}
     *
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param rawPayloadBytes unmodified raw UTF-8 JSON bytes from the WebSocket frame
     * @throws IOException if writing to the underlying stream fails
     */
    public synchronized void writeDeribitEvent(long recvTimestampMs, byte[] rawPayloadBytes) throws IOException {
        writeBinanceEvent(recvTimestampMs, rawPayloadBytes);
    }

    /**
     * Writes a raw Polymarket event payload with its local receipt timestamp and event type as a single JSONL line:
     * {@code {"ts":<recvTimestampMs>,"type":"<eventType>","payload":<rawPayloadJson>}}
     *
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param eventType       Polymarket event classification (e.g. "market_discovered", "clob", "market_resolved")
     * @param rawPayloadJson  raw JSON payload string
     * @throws IOException if writing to the underlying stream fails
     */
    public synchronized void writePolymarketEvent(long recvTimestampMs, String eventType, String rawPayloadJson) throws IOException {
        if (closed || rawPayloadJson == null || rawPayloadJson.isBlank()) {
            return;
        }
        writePolymarketEvent(recvTimestampMs, eventType, rawPayloadJson.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Writes a raw Polymarket UTF-8 {@code byte[]} event payload with its local receipt timestamp and event type
     * directly into the Zstandard stream without {@link String} allocation:
     * {@code {"ts":<recvTimestampMs>,"type":"<eventType>","payload":<rawPayloadBytes>}}
     *
     * @param recvTimestampMs local receipt timestamp in epoch milliseconds
     * @param eventType       Polymarket event classification (e.g. "market_discovered", "clob", "market_resolved")
     * @param rawPayloadBytes raw UTF-8 JSON payload bytes
     * @throws IOException if writing to the underlying stream fails
     */
    public synchronized void writePolymarketEvent(long recvTimestampMs, String eventType, byte[] rawPayloadBytes) throws IOException {
        if (closed || eventType == null || rawPayloadBytes == null || rawPayloadBytes.length == 0) {
            return;
        }
        int start = firstNonWhitespaceIndex(rawPayloadBytes);
        int end = lastNonWhitespaceEndIndex(rawPayloadBytes, start);
        if (start >= end) {
            return;
        }
        zstdOut.write(PREFIX_TS);
        writeTimestampAscii(recvTimestampMs);
        zstdOut.write(INFIX_TYPE);
        for (int i = 0, n = eventType.length(); i < n; i++) {
            zstdOut.write((byte) eventType.charAt(i));
        }
        zstdOut.write(INFIX_TYPE_PAYLOAD);
        zstdOut.write(rawPayloadBytes, start, end - start);
        zstdOut.write(SUFFIX_LINE);
        recordsWritten.incrementAndGet();
    }

    private void writeTimestampAscii(long value) throws IOException {
        if (value == 0L) {
            timestampAsciiScratch[0] = '0';
            zstdOut.write(timestampAsciiScratch, 0, 1);
            return;
        }
        boolean negative = value < 0L;
        long abs = negative ? -value : value;
        int cursor = timestampAsciiScratch.length;
        while (abs > 0L) {
            long q = abs / 10L;
            int digit = (int) (abs - (q * 10L));
            timestampAsciiScratch[--cursor] = (byte) ('0' + digit);
            abs = q;
        }
        if (negative) {
            timestampAsciiScratch[--cursor] = '-';
        }
        zstdOut.write(timestampAsciiScratch, cursor, timestampAsciiScratch.length - cursor);
    }

    private static int firstNonWhitespaceIndex(byte[] bytes) {
        int len = bytes.length;
        int i = 0;
        while (i < len && (bytes[i] & 0xFF) <= ' ') {
            i++;
        }
        return i;
    }

    private static int lastNonWhitespaceEndIndex(byte[] bytes, int start) {
        int end = bytes.length;
        while (end > start && (bytes[end - 1] & 0xFF) <= ' ') {
            end--;
        }
        return end;
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
            underlyingOut.flush();
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
