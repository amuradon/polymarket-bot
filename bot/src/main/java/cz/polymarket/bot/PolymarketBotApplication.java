package cz.polymarket.bot;

import io.quarkus.logging.Log;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

/**
 * Main entry point for the unified Polymarket Bot Quarkus application
 * (Data Collector + Live/Paper/Off Strategy Runtime).
 */
@QuarkusMain
public class PolymarketBotApplication {

    /**
     * Launches the Quarkus Polymarket Bot daemon.
     *
     * @param args command line arguments
     */
    public static void main(String... args) {
        Log.info("Starting Polymarket Bot Application...");
        Quarkus.run(args);
    }
}
