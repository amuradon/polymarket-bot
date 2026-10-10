package cz.polymarket.bot.data;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.jboss.logging.Logger;

/**
 * Main entry point for the standalone Polymarket & Binance Data Collector Quarkus application.
 */
@QuarkusMain
public class DataCollectorApplication {

    private static final Logger LOG = Logger.getLogger(DataCollectorApplication.class);

    /**
     * Launches the Quarkus Data Collector daemon.
     *
     * @param args command line arguments
     */
    public static void main(String... args) {
        LOG.info("Starting Polymarket & Binance Data Collector Application...");
        Quarkus.run(args);
    }
}
