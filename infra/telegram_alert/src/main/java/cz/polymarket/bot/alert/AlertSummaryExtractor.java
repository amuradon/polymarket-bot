package cz.polymarket.bot.alert;

import cz.polymarket.bot.alert.model.AlertSummary;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts a high-level severity badge and concise human-readable summary from
 * an application log event.
 */
public class AlertSummaryExtractor {

    private static final Pattern JSON_ERROR_PATTERN = Pattern.compile(
            "\\{.*\"error\"\\s*:\\s*\"?([^\"\\}]+)\"?.*\\}",
            Pattern.DOTALL
    );

    private static final Pattern EXCEPTION_PATTERN = Pattern.compile(
            "([A-Za-z0-9_$.]+(?:Exception|Error))(?::\\s*([^\\n]+))?"
    );

    /**
     * Analyzes raw log message and produces an {@link AlertSummary}.
     *
     * @param message raw log event text
     * @return structured {@link AlertSummary}
     */
    public AlertSummary extract(String message) {
        if (message == null || message.isBlank()) {
            return new AlertSummary("[ERROR: APPLICATION FAILURE]", "Empty or null log event received");
        }

        String clean = message.strip();
        String lower = clean.toLowerCase(Locale.ROOT);

        // 1. JVM Out of Memory or fatal JVM error
        if (lower.contains("outofmemoryerror") || lower.contains("out of memory")) {
            return new AlertSummary(
                    "[CRITICAL: JVM OUT OF MEMORY / CRASH]",
                    extractExceptionDetail(clean, "JVM OutOfMemoryError detected; process memory exhausted")
            );
        }

        // 2. HTTP 400 Bad Request
        if (lower.contains("status 400") || lower.contains("400 bad request")) {
            Matcher jsonMatcher = JSON_ERROR_PATTERN.matcher(clean);
            if (jsonMatcher.find()) {
                return new AlertSummary(
                        "[PERMANENT ERROR: HTTP 400 BAD REQUEST]",
                        "Server rejected request payload: " + jsonMatcher.group(0).trim()
                );
            }
            return new AlertSummary(
                    "[PERMANENT ERROR: HTTP 400 BAD REQUEST]",
                    "CLOB API or server rejected request with HTTP 400 Bad Request"
            );
        }

        // 3. HTTP 401 / 403 Authentication / Access Denied
        if (lower.contains("status 401") || lower.contains("status 403")
                || lower.contains("unauthorized") || lower.contains("access denied")
                || lower.contains("authentication failed")) {
            return new AlertSummary(
                    "[FATAL: AUTHENTICATION / ACCESS DENIED]",
                    "Authentication rejected or API keys/credentials invalid"
            );
        }

        // 4. Mandatory configuration or secret failure
        if (lower.contains("mandatory environment variable")
                || lower.contains("startup aborted")
                || lower.contains("missing credentials")) {
            return new AlertSummary(
                    "[FATAL: CONFIGURATION / SECRETS]",
                    "Critical configuration or Secrets Manager credential retrieval failure"
            );
        }

        // 5. Background stream task termination
        if (lower.contains("background stream task")
                || lower.contains("stream terminated")
                || lower.contains("stream connection halted")) {
            return new AlertSummary(
                    "[FATAL: BACKGROUND STREAM TERMINATED]",
                    "Critical market data stream connection halted; failing fast"
            );
        }

        // 6. Uncaught exception
        if (lower.contains("uncaught") || lower.contains("panic")) {
            return new AlertSummary(
                    "[FATAL: UNCAUGHT EXCEPTION]",
                    extractExceptionDetail(clean, "Unhandled exception or panic crashed thread")
            );
        }

        // 7. General WARN / WARNING
        if (clean.contains("WARN") || clean.contains("WARNING") || lower.startsWith("warn")) {
            return new AlertSummary(
                    "[WARNING: APPLICATION WARNING]",
                    extractFirstMeaningfulLine(clean)
            );
        }

        // 8. General ERROR / Exception fallback
        String fallbackSummary = extractExceptionDetail(clean, extractFirstMeaningfulLine(clean));
        return new AlertSummary("[ERROR: APPLICATION FAILURE]", fallbackSummary);
    }

    private static String extractExceptionDetail(String clean, String fallback) {
        Matcher matcher = EXCEPTION_PATTERN.matcher(clean);
        if (matcher.find()) {
            String exName = matcher.group(1);
            int lastDot = exName.lastIndexOf('.');
            String simpleName = lastDot >= 0 ? exName.substring(lastDot + 1) : exName;
            String msg = matcher.group(2);
            if (msg != null && !msg.isBlank()) {
                return simpleName + ": " + msg.trim();
            }
            return simpleName + " thrown";
        }
        return fallback;
    }

    private static String extractFirstMeaningfulLine(String clean) {
        String firstLine = clean.lines()
                .filter(l -> !l.isBlank())
                .findFirst()
                .orElse(clean);
        if (firstLine.length() > 200) {
            return firstLine.substring(0, 197) + "...";
        }
        return firstLine;
    }
}
