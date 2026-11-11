package cz.polymarket.bot.alert;

import cz.polymarket.bot.alert.model.AlertSummary;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Synthetic unit tests for {@link AlertSummaryExtractor} classifying severity badges
 * and extracting human-readable summary text.
 */
class AlertSummaryExtractorTest {

    private final AlertSummaryExtractor extractor = new AlertSummaryExtractor();

    @Test
    @DisplayName("Should detect JVM OutOfMemoryError as critical crash")
    void shouldDetectOutOfMemoryError() {
        String msg = "java.lang.OutOfMemoryError: Java heap space\n\tat java.base/java.util.Arrays.copyOf(Arrays.java:3537)";
        AlertSummary summary = extractor.extract(msg);

        assertThat(summary.badge()).isEqualTo("[CRITICAL: JVM OUT OF MEMORY / CRASH]");
        assertThat(summary.summary()).contains("OutOfMemoryError");
    }

    @Test
    @DisplayName("Should detect HTTP 400 Bad Request and extract error payload")
    void shouldDetectHttp400BadRequest() {
        String msg = "Polymarket CLOB API returned status 400: {\"error\":\"invalid signature or nonce expired\"}";
        AlertSummary summary = extractor.extract(msg);

        assertThat(summary.badge()).isEqualTo("[PERMANENT ERROR: HTTP 400 BAD REQUEST]");
        assertThat(summary.summary()).contains("invalid signature or nonce expired");
    }

    @Test
    @DisplayName("Should detect authentication or access denied as fatal")
    void shouldDetectAuthenticationFailure() {
        String msg = "HTTP status 401: Unauthorized access to Polymarket private endpoint";
        AlertSummary summary = extractor.extract(msg);

        assertThat(summary.badge()).isEqualTo("[FATAL: AUTHENTICATION / ACCESS DENIED]");
        assertThat(summary.summary()).contains("Authentication rejected or API keys/credentials invalid");
    }

    @Test
    @DisplayName("Should detect configuration or secret failure as fatal")
    void shouldDetectConfigurationFailure() {
        String msg = "Mandatory environment variable 'TELEGRAM_SECRET_ID' is not set; startup aborted";
        AlertSummary summary = extractor.extract(msg);

        assertThat(summary.badge()).isEqualTo("[FATAL: CONFIGURATION / SECRETS]");
        assertThat(summary.summary()).contains("Critical configuration or Secrets Manager credential retrieval failure");
    }

    @Test
    @DisplayName("Should detect background stream termination")
    void shouldDetectStreamTermination() {
        String msg = "WebSocket background stream task terminated abnormally for btcusd";
        AlertSummary summary = extractor.extract(msg);

        assertThat(summary.badge()).isEqualTo("[FATAL: BACKGROUND STREAM TERMINATED]");
        assertThat(summary.summary()).contains("Critical market data stream connection halted");
    }

    @Test
    @DisplayName("Should detect uncaught exception with clean exception detail")
    void shouldDetectUncaughtException() {
        String msg = "Uncaught java.lang.NullPointerException: Cannot invoke 'Order.price()' because 'order' is null\n\tat cz.polymarket.bot.trading.OrderRouter.route";
        AlertSummary summary = extractor.extract(msg);

        assertThat(summary.badge()).isEqualTo("[FATAL: UNCAUGHT EXCEPTION]");
        assertThat(summary.summary()).contains("NullPointerException");
    }

    @Test
    @DisplayName("Should classify general warning as warning badge")
    void shouldClassifyGeneralWarning() {
        String msg = "2026-11-05 10:00:00 WARN  [cz.polymarket.bot.TwapEngine] Stale price detected; falling back to last good mark";
        AlertSummary summary = extractor.extract(msg);

        assertThat(summary.badge()).isEqualTo("[WARNING: APPLICATION WARNING]");
        assertThat(summary.summary()).contains("Stale price detected");
    }

    @Test
    @DisplayName("Should classify general error as application failure badge")
    void shouldClassifyGeneralError() {
        String msg = "2026-11-05 10:00:00 ERROR [cz.polymarket.bot.OrderManager] Order placement failed unexpectedly";
        AlertSummary summary = extractor.extract(msg);

        assertThat(summary.badge()).isEqualTo("[ERROR: APPLICATION FAILURE]");
        assertThat(summary.summary()).contains("Order placement failed unexpectedly");
    }
}
