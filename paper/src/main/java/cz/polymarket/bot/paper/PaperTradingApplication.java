package cz.polymarket.bot.paper;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PaperTradingApplication {
    private static final Logger LOG = Logger.getLogger(PaperTradingApplication.class);

    void onStart(@Observes StartupEvent event) {
        LOG.info("Starting Polymarket Bot in PAPER TRADING mode on port 8082...");
    }
}
