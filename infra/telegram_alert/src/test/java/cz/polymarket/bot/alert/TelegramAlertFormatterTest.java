package cz.polymarket.bot.alert;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Synthetic unit tests for {@link TelegramAlertFormatter} formatting HTML messages
 * with entity escaping and length truncation.
 */
class TelegramAlertFormatterTest {

    private final TelegramAlertFormatter formatter = new TelegramAlertFormatter();

    @Test
    @DisplayName("Should format message with escaped HTML characters and metadata")
    void shouldFormatMessageWithEscapedHtmlAndMetadata() {
        String logGroup = "/polymarket-bot/live-trading";
        String logStream = "i-0123456789abcdef0/polymarket-live";
        long timestampMs = 1762336800000L; // 2025-11-05T10:00:00Z
        String rawMessage = "ERROR: Failed order where price < 0.50 & size > 100 with \"BAD_SIG\"";

        String formatted = formatter.format(logGroup, logStream, timestampMs, rawMessage);

        assertThat(formatted).contains("<b>[ERROR: APPLICATION FAILURE]</b>");
        assertThat(formatted).contains("<b>Log Group:</b> <code>/polymarket-bot/live-trading</code>");
        assertThat(formatted).contains("<b>Instance / Stream:</b> <code>i-0123456789abcdef0/polymarket-live</code>");
        assertThat(formatted).contains("<code>2025-11-05 10:00:00 UTC</code>");
        assertThat(formatted).contains("&lt; 0.50 &amp; size &gt; 100 with &quot;BAD_SIG&quot;");
        assertThat(formatted).startsWith("<b>");
        assertThat(formatted).endsWith("</code></pre>");
    }

    @Test
    @DisplayName("Should truncate extremely long log messages exceeding maximum threshold")
    void shouldTruncateExtremelyLongLogMessages() {
        String logGroup = "/test-log-group";
        String logStream = "test-stream";
        long timestampMs = 1762336800000L;
        String rawMessage = "A".repeat(2000);

        String formatted = formatter.format(logGroup, logStream, timestampMs, rawMessage);

        assertThat(formatted).contains("... [truncated]");
        assertThat(formatted.length()).isLessThan(2500);
    }
}
