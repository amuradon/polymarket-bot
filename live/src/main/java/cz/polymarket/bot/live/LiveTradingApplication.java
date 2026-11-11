package cz.polymarket.bot.live;

import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Lifecycle startup observer for the standalone Live Trading Quarkus application.
 */
@ApplicationScoped
public class LiveTradingApplication {

    private final int httpPort;

    /**
     * Constructs the Live Trading application startup observer.
     *
     * @param httpPort configured HTTP server port
     */
    @Inject
    public LiveTradingApplication(
            @ConfigProperty(name = "quarkus.http.port", defaultValue = "8080") int httpPort) {
        this.httpPort = httpPort;
    }

    void onStart(@Observes StartupEvent event) {
        Log.infof("Starting Polymarket Bot in LIVE TRADING mode on port %d...", httpPort);
    }
}
