package cz.polymarket.bot.alert;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Synthetic unit tests validating log event matching for WARN, ERROR, FATAL,
 * Java *Exception, and Java *Error occurrences.
 */
class LogEventMatcherTest {

    private final LogEventMatcher matcher = new LogEventMatcher();

    @Test
    @DisplayName("Should not alert on null or empty message")
    void shouldNotAlertOnNullOrEmptyMessage() {
        assertThat(matcher.shouldAlert(null)).isFalse();
        assertThat(matcher.shouldAlert("")).isFalse();
        assertThat(matcher.shouldAlert("   ")).isFalse();
    }

    @ParameterizedTest(name = "Should alert on log level: {0}")
    @ValueSource(strings = {
            "2026-11-05 10:00:00 WARN  [cz.polymarket.bot] High memory usage detected",
            "2026-11-05 10:00:00 WARNING [cz.polymarket.bot] Approaching rate limit",
            "2026-11-05 10:00:00 ERROR [cz.polymarket.bot] Failed to submit signed order",
            "2026-11-05 10:00:00 FATAL [cz.polymarket.bot] Critical engine shutdown",
            "warn: degraded websocket connection",
            "error: response status 500",
            "fatal runtime failure in engine"
    })
    void shouldAlertOnLogLevelMarkers(String message) {
        assertThat(matcher.shouldAlert(message)).isTrue();
    }

    @ParameterizedTest(name = "Should alert on Java Exception: {0}")
    @ValueSource(strings = {
            "java.lang.NullPointerException: Cannot invoke method on null object",
            "java.util.concurrent.TimeoutException: Timed out waiting for response",
            "cz.polymarket.bot.connector.PolymarketClobException: Order rejected with 400",
            "java.lang.IllegalStateException: State engine corrupted",
            "Caused by: java.io.IOException: Broken pipe",
            "Exception: Connection refused by remote host",
            "Uncaught RuntimeException in Vert.x event loop"
    })
    void shouldAlertOnJavaExceptions(String message) {
        assertThat(matcher.shouldAlert(message)).isTrue();
    }

    @ParameterizedTest(name = "Should alert on Java Error: {0}")
    @ValueSource(strings = {
            "java.lang.OutOfMemoryError: Java heap space",
            "java.lang.StackOverflowError: Recursion depth exceeded",
            "java.lang.AssertionError: Invariant violated",
            "java.lang.NoClassDefFoundError: Could not initialize class",
            "java.lang.InternalError: Unexpected JVM failure",
            "Error: Unrecoverable hardware failure"
    })
    void shouldAlertOnJavaErrors(String message) {
        assertThat(matcher.shouldAlert(message)).isTrue();
    }

    @ParameterizedTest(name = "Should NOT alert on normal informational log lines: {0}")
    @ValueSource(strings = {
            "2026-11-05 10:00:00 INFO  [cz.polymarket.bot] WebSocket connected to Polymarket",
            "2026-11-05 10:00:01 DEBUG [cz.polymarket.bot] Received order book update: bids=10, asks=12",
            "2026-11-05 10:00:02 INFO  [cz.polymarket.bot] Seeded 150 historical 15m candles",
            "Order executed successfully at price 0.52",
            "System metrics healthy; 0 dropped packets; load average 0.12"
    })
    void shouldNotAlertOnNormalLogs(String message) {
        assertThat(matcher.shouldAlert(message)).isFalse();
    }
}
