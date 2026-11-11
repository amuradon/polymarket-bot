package cz.polymarket.bot.alert.model;

/**
 * High-level summary of an alert event containing a severity badge and descriptive summary text.
 *
 * @param badge   severity classification badge (e.g. {@code [CRITICAL: JVM OUT OF MEMORY]})
 * @param summary human-readable extracted summary of the root cause or error
 */
public record AlertSummary(String badge, String summary) {
    public AlertSummary {
        if (badge == null) {
            badge = "[ERROR: APPLICATION FAILURE]";
        }
        if (summary == null) {
            summary = "Application reported an error";
        }
    }
}
