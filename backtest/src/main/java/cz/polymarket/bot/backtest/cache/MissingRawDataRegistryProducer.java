package cz.polymarket.bot.backtest.cache;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.file.Path;

/**
 * CDI Producer providing application-scoped singleton of {@link MissingRawDataRegistry}.
 * Loads existing tracked missing raw data from disk on startup.
 */
@ApplicationScoped
public class MissingRawDataRegistryProducer {

    /**
     * Produces the application-wide MissingRawDataRegistry instance for BTCUSDT.
     *
     * @param cacheDir path to backtesting cache directory
     * @return loaded or freshly initialized MissingRawDataRegistry
     */
    @Produces
    @ApplicationScoped
    public MissingRawDataRegistry produceRegistry(
            @ConfigProperty(name = "polymarket.backtest.cache-dir", defaultValue = "D:/Crypto/data/Polymarket/backtesting/cache")
            String cacheDir) {
        Path jsonPath = Path.of(cacheDir, "missing_raw_data_BTCUSDT.json");
        return MissingRawDataRegistry.load(jsonPath, "BTCUSDT");
    }
}
