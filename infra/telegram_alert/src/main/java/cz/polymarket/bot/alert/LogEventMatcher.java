package cz.polymarket.bot.alert;

import java.util.regex.Pattern;

/**
 * Filter evaluating whether an individual CloudWatch log message qualifies for dispatching
 * an alert to Telegram.
 * <p>
 * Matches log events containing:
 * <ul>
 *   <li>Log level markers: {@code WARN}, {@code WARNING}, {@code ERROR}, {@code FATAL} (case-insensitive)</li>
 *   <li>Any Java Exception class or token ending with {@code *Exception} (e.g. {@code NullPointerException},
 *       {@code TimeoutException}, {@code PolymarketClobException}) or {@code Exception:}</li>
 *   <li>Any Java Error class or token ending with {@code *Error} (e.g. {@code OutOfMemoryError},
 *       {@code AssertionError}, {@code StackOverflowError}) or general {@code error} markers</li>
 * </ul>
 */
public class LogEventMatcher {

    private static final Pattern LEVEL_OR_ERROR_PATTERN = Pattern.compile(
            "(?i)\\b(?:WARN(?:ING)?|ERROR|FATAL)\\b"
    );

    private static final Pattern JAVA_EXCEPTION_PATTERN = Pattern.compile(
            "\\b[A-Za-z0-9_$]*Exception\\b|Exception:",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern JAVA_ERROR_PATTERN = Pattern.compile(
            "\\b[A-Za-z0-9_$]*Error\\b|Error:",
            Pattern.CASE_INSENSITIVE
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

        if (LEVEL_OR_ERROR_PATTERN.matcher(message).find()) {
            return true;
        }

        if (JAVA_EXCEPTION_PATTERN.matcher(message).find()) {
            return true;
        }

        return JAVA_ERROR_PATTERN.matcher(message).find();
    }
}
