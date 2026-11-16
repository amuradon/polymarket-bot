package cz.polymarket.bot.alert;

import java.util.regex.Pattern;

/**
 * Filter evaluating whether an individual CloudWatch log message qualifies for dispatching
 * an alert to Telegram.
 * <p>
 * Matches log events containing strictly:
 * <ul>
 *   <li>Bracketed log level markers: {@code [FATAL]}, {@code [ERROR]}, {@code [WARN]}</li>
 *   <li>Any Java Exception class or token ending with {@code *Exception} (e.g. {@code NullPointerException},
 *       {@code TimeoutException}, {@code PolymarketClobException}, {@code Exception})</li>
 * </ul>
 */
public class LogEventMatcher {

    private static final Pattern BRACKETED_LEVEL_PATTERN = Pattern.compile(
            "\\[(?:FATAL|ERROR|WARN)\\]"
    );

    private static final Pattern JAVA_EXCEPTION_PATTERN = Pattern.compile(
            "\\b[A-Za-z0-9_$]*Exception\\b"
    );

    /**
     * Determines whether the given log message qualifies for a Telegram alert.
     *
     * @param message raw log message string
     * @return {@code true} if an alert should be dispatched, {@code false} otherwise
     */
    public boolean shouldAlert(String message) {
        if (message == null || message.isBlank()) {
            return false;
        }

        if (BRACKETED_LEVEL_PATTERN.matcher(message).find()) {
            return true;
        }

        return JAVA_EXCEPTION_PATTERN.matcher(message).find();
    }
}
