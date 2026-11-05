package cz.polymarket.bot.live.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.TradeRecord;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Crash-resilient, append-only repository for live trading executions and continuous performance metrics.
 * Persists completed trades to {@code trades.jsonl} and maintains atomic metrics snapshots in {@code metrics.json}.
 */
@ApplicationScoped
public class LiveTradeRepository {

    private static final String TRADES_FILENAME = "trades.jsonl";
    private static final String METRICS_FILENAME = "metrics.json";

    private final Path dataDirectory;
    private final double initialCapital;
    private final ObjectMapper objectMapper;
    private final PerformanceMetricsCalculator metricsCalculator;

    private final List<TradeRecord> trades = new CopyOnWriteArrayList<>();
    private volatile PerformanceMetrics currentMetrics;
    private volatile double currentBalance;

    /**
     * Constructs the live trade repository.
     *
     * @param dataDir           directory path for persistent JSONL trade storage
     * @param initialCapital    initial live trading bankroll in USD
     * @param objectMapper      Jackson mapper
     * @param metricsCalculator performance metrics calculator
     */
    @Inject
    public LiveTradeRepository(
            @ConfigProperty(name = "polymarket.live.data-dir", defaultValue = "data/live")
            String dataDir,
            @ConfigProperty(name = "polymarket.live.initial-capital", defaultValue = "100.0")
            double initialCapital,
            ObjectMapper objectMapper,
            PerformanceMetricsCalculator metricsCalculator) {
        if (dataDir == null || dataDir.isBlank()) {
            throw new IllegalArgumentException("dataDir cannot be null or blank");
        }
        if (initialCapital <= 0.0) {
            throw new IllegalArgumentException("initialCapital must be positive");
        }
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper cannot be null");
        }
        if (metricsCalculator == null) {
            throw new IllegalArgumentException("metricsCalculator cannot be null");
        }

        Path path = Path.of(dataDir);
        if (!path.isAbsolute()) {
            Path workCandidate = Path.of("/work").resolve(dataDir);
            if (Files.exists(workCandidate)) {
                path = workCandidate;
            } else {
                path = path.toAbsolutePath();
            }
        }
        this.dataDirectory = path;
        this.initialCapital = initialCapital;
        this.objectMapper = objectMapper;
        this.metricsCalculator = metricsCalculator;

        initialize();
    }

    private synchronized void initialize() {
        try {
            Files.createDirectories(dataDirectory);
            Log.infof("Initializing live trade storage in directory: %s", dataDirectory.toAbsolutePath());

            Path tradesFile = dataDirectory.resolve(TRADES_FILENAME);
            if (Files.exists(tradesFile)) {
                List<TradeRecord> loaded = new ArrayList<>();
                try (BufferedReader reader = Files.newBufferedReader(tradesFile)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) {
                            continue;
                        }
                        try {
                            TradeRecord record = objectMapper.readValue(line, TradeRecord.class);
                            loaded.add(record);
                        } catch (Exception e) {
                            Log.warnf("Skipping malformed live trade line during replay: %s (%s)", line, e.getMessage());
                        }
                    }
                }
                trades.addAll(loaded);
                Log.infof("Restored %d live trades from %s", trades.size(), tradesFile.toAbsolutePath());
            } else {
                Log.infof("No live trades file found at %s - starting with initial capital $%.2f",
                        tradesFile.toAbsolutePath(), initialCapital);
            }

            if (trades.isEmpty()) {
                this.currentBalance = initialCapital;
                this.currentMetrics = PerformanceMetrics.empty(initialCapital);
            } else {
                this.currentBalance = trades.getLast().balanceAfterTrade();
                this.currentMetrics = metricsCalculator.calculate(trades, initialCapital);
            }
        } catch (IOException e) {
            Log.errorf("Failed to initialize live trade storage in %s: %s", dataDirectory, e.getMessage());
            throw new RuntimeException("Could not initialize LiveTradeRepository", e);
        }
    }

    /**
     * Appends a settled live trade to {@code trades.jsonl} and updates {@code metrics.json}.
     *
     * @param trade settled trade record
     */
    public synchronized void recordTrade(TradeRecord trade) {
        if (trade == null) {
            throw new IllegalArgumentException("Trade record cannot be null");
        }

        trades.add(trade);
        this.currentBalance = trade.balanceAfterTrade();
        this.currentMetrics = metricsCalculator.calculate(trades, initialCapital);

        Path tradesFile = dataDirectory.resolve(TRADES_FILENAME);
        try {
            String jsonLine = objectMapper.writeValueAsString(trade) + System.lineSeparator();
            Files.writeString(tradesFile, jsonLine,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            Log.errorf("Failed to append live trade to %s: %s", tradesFile, e.getMessage());
            throw new RuntimeException("Failed to persist live trade to " + tradesFile, e);
        }

        Path metricsFile = dataDirectory.resolve(METRICS_FILENAME);
        Path tempFile = dataDirectory.resolve(METRICS_FILENAME + ".tmp");
        try {
            String metricsJson = objectMapper.writeValueAsString(currentMetrics);
            Files.writeString(tempFile, metricsJson,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            Files.move(tempFile, metricsFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            try {
                String metricsJson = objectMapper.writeValueAsString(currentMetrics);
                Files.writeString(metricsFile, metricsJson,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);
            } catch (IOException ex) {
                Log.warnf("Failed to write live metrics snapshot: %s", ex.getMessage());
            }
        }
    }

    /**
     * Resets stored trade history and restores balance to {@code initialCapital}.
     */
    public synchronized void reset() {
        trades.clear();
        this.currentBalance = initialCapital;
        this.currentMetrics = PerformanceMetrics.empty(initialCapital);

        Path tradesFile = dataDirectory.resolve(TRADES_FILENAME);
        try {
            Files.deleteIfExists(tradesFile);
        } catch (IOException e) {
            Log.warnf("Failed to delete %s during reset: %s", tradesFile, e.getMessage());
        }

        Path metricsFile = dataDirectory.resolve(METRICS_FILENAME);
        try {
            String metricsJson = objectMapper.writeValueAsString(currentMetrics);
            Files.writeString(metricsFile, metricsJson,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            Log.warnf("Failed to write metrics.json during reset: %s", e.getMessage());
        }
        Log.infof("Reset live trade storage to initial capital: $%.2f", initialCapital);
    }

    /**
     * Returns an unmodifiable list of all recorded live trades.
     *
     * @return recorded trades
     */
    public List<TradeRecord> getTrades() {
        return Collections.unmodifiableList(trades);
    }

    /**
     * Returns the current aggregated performance metrics.
     *
     * @return current {@link PerformanceMetrics}
     */
    public PerformanceMetrics getMetrics() {
        return currentMetrics;
    }

    /**
     * Returns the current live trading balance in USD.
     *
     * @return current balance
     */
    public double getCurrentBalance() {
        return currentBalance;
    }

    /**
     * Returns the configured initial capital in USD.
     *
     * @return initial capital
     */
    public double getInitialCapital() {
        return initialCapital;
    }
}
