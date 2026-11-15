package cz.polymarket.bot.backtest;

import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * Main application entry point for the backtest module.
 * <p>
 * Supports two execution modes controlled by {@code polymarket.backtest.batchMode}:
 * <ul>
 *     <li><b>Batch mode ({@code true})</b>: Immediately executes the configured backtest on startup,
 *     exports the JSON report identical to {@code /api/backtest/run}, and terminates the application.</li>
 *     <li><b>Server mode ({@code false})</b>: Runs continuously as a REST server on port 8083.</li>
 * </ul>
 */
@QuarkusMain
@ApplicationScoped
public class BacktestApplication implements QuarkusApplication {

    private final BacktestEngine backtestEngine;
    private final boolean batchMode;
    private final String strategyName;
    private final String symbol;
    private final String startDate;
    private final Optional<String> endDate;
    private final double initialCapital;
    private final String outputDir;
    private final double latencyFactor;

    /**
     * Constructs a new {@link BacktestApplication} with injected engine and native Quarkus configuration properties.
     *
     * @param backtestEngine simulation engine executing the backtest
     * @param batchMode {@code true} to execute backtest on startup and exit; {@code false} for REST server mode
     * @param strategyName trading strategy name (default {@code UtterParameciumStrategy})
     * @param symbol trading symbol (default {@code BTCUSDT})
     * @param startDate inclusive start date in {@code yyyy-MM-dd} format (default {@code 2026-10-12})
     * @param endDate optional inclusive end date in {@code yyyy-MM-dd} format (defaults to current UTC date if omitted)
     * @param initialCapital initial account balance in USD (default {@code 10000.0})
     * @param outputDir directory path for exported JSON reports (default {@code D:/Crypto/data/Polymarket/backtesting})
     * @param latencyFactor simulated order execution latency factor (default {@code 1.0})
     */
    @Inject
    public BacktestApplication(
            BacktestEngine backtestEngine,
            @ConfigProperty(name = "polymarket.backtest.batchMode", defaultValue = "false")
            boolean batchMode,
            @ConfigProperty(name = "polymarket.backtest.strategy", defaultValue = "UtterParameciumStrategy")
            String strategyName,
            @ConfigProperty(name = "polymarket.backtest.symbol", defaultValue = "BTCUSDT")
            String symbol,
            @ConfigProperty(name = "polymarket.backtest.from", defaultValue = "2026-10-12")
            String startDate,
            @ConfigProperty(name = "polymarket.backtest.to")
            Optional<String> endDate,
            @ConfigProperty(name = "polymarket.backtest.initial-capital", defaultValue = "10000.0")
            double initialCapital,
            @ConfigProperty(name = "polymarket.backtest.output-dir", defaultValue = "D:/Crypto/data/Polymarket/backtesting")
            String outputDir,
            @ConfigProperty(name = "polymarket.backtest.latency-factor", defaultValue = "1.0")
            double latencyFactor) {
        if (backtestEngine == null) {
            Log.fatal("Fatal initialization error in BacktestApplication: backtestEngine must not be null");
            throw new IllegalArgumentException("backtestEngine must not be null");
        }
        this.backtestEngine = backtestEngine;
        this.batchMode = batchMode;
        this.strategyName = strategyName;
        this.symbol = symbol;
        this.startDate = startDate;
        this.endDate = endDate != null ? endDate : Optional.empty();
        this.initialCapital = initialCapital;
        this.outputDir = outputDir;
        this.latencyFactor = latencyFactor;
    }

    void onStart(@Observes StartupEvent event) {
        if (Log.isInfoEnabled()) {
            Log.infof("Starting Polymarket Bot in BACKTEST mode (batchMode=%s)...", batchMode);
        }
    }

    /**
     * Executes the application lifecycle according to {@code polymarket.backtest.batchMode}.
     *
     * @param args command line arguments
     * @return process exit code ({@code 0} on success, {@code 1} on batch execution failure)
     */
    @Override
    public int run(String... args) {
        if (!batchMode) {
            if (Log.isInfoEnabled()) {
                Log.info("Backtest application running in SERVER mode on port 8083. Waiting for requests...");
            }
            Quarkus.waitForExit();
            return 0;
        }

        String resolvedEndDate = endDate
                .filter(s -> !s.isBlank())
                .map(String::trim)
                .orElseGet(() -> LocalDate.now(ZoneOffset.UTC).toString());

        if (Log.isInfoEnabled()) {
            Log.infof("Executing startup batch backtest: strategy=%s, symbol=%s, from=%s, to=%s, capital=%.2f, dir=%s",
                    strategyName, symbol, startDate, resolvedEndDate, initialCapital, outputDir);
        }

        try {
            BacktestResult result = backtestEngine.runBacktest(
                    strategyName,
                    symbol,
                    startDate,
                    resolvedEndDate,
                    initialCapital,
                    outputDir,
                    latencyFactor
            );
            if (Log.isInfoEnabled()) {
                Log.infof("Startup batch backtest finished: strategy=%s, markets=%d, trades=%d, report=%s. Shutting down...",
                        result.strategyName(),
                        result.totalMarkets(),
                        result.trades() != null ? result.trades().size() : 0,
                        result.jsonFilePath());
            }
            return 0;
        } catch (Throwable t) {
            Log.errorf(t, "Startup batch backtest failed: %s", t.getMessage());
            return 1;
        }
    }

    /**
     * Standard Java main entry point launching the Quarkus runtime.
     *
     * @param args command line arguments
     */
    public static void main(String... args) {
        Quarkus.run(BacktestApplication.class, args);
    }
}
