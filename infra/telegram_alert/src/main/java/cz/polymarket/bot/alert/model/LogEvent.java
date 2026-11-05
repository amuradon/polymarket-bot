package cz.polymarket.bot.alert.model;

/**
 * Individual log event extracted from CloudWatch Logs.
 *
 * @param id        unique CloudWatch log event identifier
 * @param timestamp epoch timestamp in milliseconds when the event was recorded
 * @param message   raw log line emitted by the application
 */
public record LogEvent(String id, long timestamp, String message) {
    public LogEvent {
        if (id == null) {
            id = "";
        }
        if (message == null) {
            message = "";
        }
    }
}
