package cz.polymarket.bot.alert;

import cz.polymarket.bot.alert.model.AlertSummary;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Formats log events and extracted summaries into Telegram-compatible HTML messages.
 */
public class TelegramAlertFormatter {

    private static final int MAX_RAW_MESSAGE_LENGTH = 1200;

    private static final DateTimeFormatter UTC_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
            .withZone(ZoneOffset.UTC);

    private final AlertSummaryExtractor summaryExtractor;

    /**
     * Constructs a formatter with the default {@link AlertSummaryExtractor}.
     */
    public TelegramAlertFormatter() {
        this(new AlertSummaryExtractor());
    }

    /**
     * Constructs a formatter with a custom {@link AlertSummaryExtractor}.
     *
     * @param summaryExtractor extractor for badges and summaries
     */
    public TelegramAlertFormatter(AlertSummaryExtractor summaryExtractor) {
        this.summaryExtractor = summaryExtractor != null ? summaryExtractor : new AlertSummaryExtractor();
    }

    /**
     * Formats an HTML alert string for Telegram dispatch.
     *
     * @param logGroup    CloudWatch Log Group name
     * @param logStream   CloudWatch Log Stream / instance name
     * @param timestampMs event timestamp in epoch milliseconds
     * @param rawMessage  raw log message text
     * @return HTML-formatted message string
     */
    public String format(String logGroup, String logStream, long timestampMs, String rawMessage) {
        AlertSummary alertSummary = summaryExtractor.extract(rawMessage);

        String formattedTime;
        try {
            formattedTime = UTC_FORMATTER.format(Instant.ofEpochMilli(timestampMs));
        } catch (Exception e) {
            formattedTime = "Timestamp: " + timestampMs + " ms";
        }

        String cleanedRaw = rawMessage != null ? rawMessage.strip() : "";
        if (cleanedRaw.length() > MAX_RAW_MESSAGE_LENGTH) {
            cleanedRaw = cleanedRaw.substring(0, MAX_RAW_MESSAGE_LENGTH) + "\n... [truncated]";
        }

        String escapedBadge = escapeHtml(alertSummary.badge());
        String escapedSummary = escapeHtml(alertSummary.summary());
        String escapedLogGroup = escapeHtml(logGroup != null ? logGroup : "unknown-log-group");
        String escapedLogStream = escapeHtml(logStream != null ? logStream : "unknown-log-stream");
        String escapedRaw = escapeHtml(cleanedRaw);

        return "<b>" + escapedBadge + "</b>\n\n"
                + "<b>Summary:</b> " + escapedSummary + "\n"
                + "<b>Time:</b> <code>" + formattedTime + "</code>\n"
                + "<b>Log Group:</b> <code>" + escapedLogGroup + "</code>\n"
                + "<b>Instance / Stream:</b> <code>" + escapedLogStream + "</code>\n\n"
                + "<b>Raw Log Event:</b>\n"
                + "<pre><code>" + escapedRaw + "</code></pre>";
    }

    /**
     * Escapes characters with special meaning in Telegram HTML parse mode.
     *
     * @param text unescaped plain text
     * @return HTML-escaped string
     */
    public static String escapeHtml(String text) {
        if (text == null) {
            return "";
        }
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
