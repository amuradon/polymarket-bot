package cz.polymarket.bot.connector.ws;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * WebSocket text frame paired with its local epoch millisecond receipt timestamp
 * captured immediately upon socket arrival on the I/O reader thread.
 * <p>
 * Supports construction from raw UTF-8 {@code byte[]} so the Netty I/O reader thread
 * never performs UTF-8 validation or {@link String} allocation on the hot network path;
 * UTF-8 decoding to {@link String} is deferred lazily to the downstream worker thread.
 */
public final class TimestampedFrame {

    private final byte[] rawUtf8Bytes;
    private String rawText;
    private final long recvTs;

    /**
     * Constructs a timestamped frame from an already decoded UTF-8 string.
     *
     * @param rawText raw UTF-8 JSON text payload received from the WebSocket
     * @param recvTs  local receipt timestamp in epoch milliseconds
     */
    public TimestampedFrame(String rawText, long recvTs) {
        this.rawText = rawText;
        this.rawUtf8Bytes = null;
        this.recvTs = recvTs;
    }

    /**
     * Constructs a timestamped frame from raw UTF-8 bytes without decoding on the I/O thread.
     *
     * @param rawUtf8Bytes raw UTF-8 bytes extracted directly from the socket buffer
     * @param recvTs       local receipt timestamp in epoch milliseconds
     */
    public TimestampedFrame(byte[] rawUtf8Bytes, long recvTs) {
        this.rawUtf8Bytes = rawUtf8Bytes;
        this.rawText = null;
        this.recvTs = recvTs;
    }

    /**
     * Returns the decoded UTF-8 JSON text payload, lazily decoding from {@code rawUtf8Bytes}
     * on the calling worker thread if constructed from raw bytes.
     *
     * @return decoded UTF-8 text payload
     */
    public String rawText() {
        String cached = this.rawText;
        if (cached == null && this.rawUtf8Bytes != null) {
            cached = new String(this.rawUtf8Bytes, StandardCharsets.UTF_8);
            this.rawText = cached;
        }
        return cached;
    }

    /**
     * Returns the raw UTF-8 byte array if constructed from bytes, or encodes {@link #rawText()}.
     *
     * @return UTF-8 bytes
     */
    public byte[] rawUtf8Bytes() {
        if (this.rawUtf8Bytes != null) {
            return this.rawUtf8Bytes;
        }
        return this.rawText != null ? this.rawText.getBytes(StandardCharsets.UTF_8) : new byte[0];
    }

    /**
     * Returns the local receipt timestamp in epoch milliseconds captured on the I/O thread.
     *
     * @return receipt timestamp in epoch milliseconds
     */
    public long recvTs() {
        return recvTs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TimestampedFrame that)) {
            return false;
        }
        return recvTs == that.recvTs && Objects.equals(rawText(), that.rawText());
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawText(), recvTs);
    }

    @Override
    public String toString() {
        return "TimestampedFrame[rawText=" + rawText() + ", recvTs=" + recvTs + "]";
    }
}
