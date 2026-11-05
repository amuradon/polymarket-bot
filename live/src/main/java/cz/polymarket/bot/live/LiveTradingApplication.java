package cz.polymarket.bot.live;

import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

/**
 * Lifecycle startup observer for the standalone Live Trading Quarkus application.
 */
@ApplicationScoped
public class LiveTradingApplication {

    void onStart(@Observes StartupEvent event) {
        Log.info("Starting Polymarket Bot in LIVE TRADING mode on port 8080...");
    }
}
