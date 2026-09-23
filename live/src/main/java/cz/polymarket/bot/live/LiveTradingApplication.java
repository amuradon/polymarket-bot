package cz.polymarket.bot.live;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

@ApplicationScoped
public class LiveTradingApplication {
    private static final Logger LOG = Logger.getLogger(LiveTradingApplication.class);

    void onStart(@Observes StartupEvent event) {
        LOG.info("Starting Polymarket Bot in LIVE TRADING mode on port 8080...");
    }
}
