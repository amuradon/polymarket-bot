package cz.polymarket.bot.backtest;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

/**
 * Main application entry point for the backtest module.
 * Can be run via Quarkus dev mode (quarkus:dev -pl backtest) or executed directly
 * via standard Java main method.
 */
@QuarkusMain
@ApplicationScoped
public class BacktestApplication {
    private static final Logger LOG = Logger.getLogger(BacktestApplication.class);

    void onStart(@Observes StartupEvent event) {
        LOG.info("Starting Polymarket Bot in BACKTEST mode on port 8083...");
    }

    public static void main(String... args) {
        Quarkus.run(args);
    }
}
