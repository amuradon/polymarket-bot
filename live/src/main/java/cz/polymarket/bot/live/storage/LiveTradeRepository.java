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
    private final ObjectMapper objectMapper;
    private final PerformanceMetricsCalculator metricsCalculator;

    private final List<TradeRecord> trades = new CopyOnWriteArrayList<>();
    private volatile double initialCapital;
    private volatile PerformanceMetrics currentMetrics;
    private volatile double currentBalance;

    /**
     * Constructs the live trade repository.
     *
     * @param dataDir           directory path for persistent JSONL trade storage
     * @param objectMapper      Jackson mapper
     * @param metricsCalculator performance metrics calculator
     */
    @Inject
    public LiveTradeRepository(
            @ConfigProperty(name = "polymarket.live.data-dir", defaultValue = "data/live")
            String dataDir,
            ObjectMapper objectMapper,
            PerformanceMetricsCalculator metricsCalculator) {
        if (dataDir == null || dataDir.isBlank()) {
            throw new IllegalArgumentException("dataDir cannot be null or blank");
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
        this.objectMapper = objectMapper;
        this.metricsCalculator = metricsCalculator;

        initialize();
    }

    private synchronized void initialize() {
        try {
            Files.createDirectories(dataDirectory);
            Log.infof("Initializing live trade storage in directory: %s", dataDirectory.toAbsolutePath());

            Path metricsFile = dataDirectory.resolve(METRICS_FILENAME);
            if (Files.exists(metricsFile)) {
                try {
                    PerformanceMetrics persistedMetrics = objectMapper.readValue(metricsFile.toFile(), PerformanceMetrics.class);
                    if (persistedMetrics != null && persistedMetrics.initialCapital() > 0.0) {
                        this.initialCapital = persistedMetrics.initialCapital();
                    }
                } catch (Exception e) {
                    Log.warnf("Skipping malformed live metrics file during startup: %s", e.getMessage());
                }
            }

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
                Log.infof("No live trades file found at %s - awaiting live account balance synchronization",
                        tradesFile.toAbsolutePath());
            }

            if (trades.isEmpty()) {
                this.currentBalance = this.initialCapital;
                this.currentMetrics = PerformanceMetrics.empty(this.initialCapital);
            } else {
                this.currentBalance = trades.getLast().balanceAfterTrade();
                if (this.initialCapital <= 0.0) {
                    TradeRecord first = trades.getFirst();
                    this.initialCapital = Math.max(0.0, first.balanceAfterTrade() - first.netPnl());
                }
                this.currentMetrics = metricsCalculator.calculate(trades, this.initialCapital);
            }
        } catch (IOException e) {
            Log.fatalf("Fatal storage error: failed to initialize live trade storage in %s: %s", dataDirectory, e.getMessage());
            throw new RuntimeException("Could not initialize LiveTradeRepository", e);
        }
    }

    /**
     * Synchronizes the repository balance with the authoritative USDC collateral balance read from the
     * Polymarket CLOB account. When no trades have been recorded yet (or {@code initialCapital} was not yet set),
     * also initializes {@code initialCapital} to the account balance.
     *
     * @param accountBalanceUsd available Polymarket account USDC collateral balance in USD
     */
    public synchronized void syncAccountBalance(double accountBalanceUsd) {
        if (!Double.isFinite(accountBalanceUsd) || accountBalanceUsd < 0.0) {
            throw new IllegalArgumentException("accountBalanceUsd must be finite and non-negative");
        }
        this.currentBalance = accountBalanceUsd;
        if (this.initialCapital <= 0.0 || trades.isEmpty()) {
            this.initialCapital = accountBalanceUsd;
        }
        this.currentMetrics = trades.isEmpty()
                ? PerformanceMetrics.empty(this.initialCapital)
                : metricsCalculator.calculate(trades, this.initialCapital);
        persistMetricsSnapshot();
    }

    /**
     * Immediately debits the cost of a newly opened live BUY position ({@code sizeUsd + entryFee}) from
     * the in-memory balance on the hot path so subsequent batch orders within the same candle cannot overspend.
     *
     * @param costUsd total position cost in USD ({@code sizeUsd + entryFee})
     */
    public synchronized void debitOpenPositionCost(double costUsd) {
        if (costUsd > 0.0 && Double.isFinite(costUsd)) {
            this.currentBalance = Math.max(0.0, this.currentBalance - costUsd);
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
        if (this.initialCapital <= 0.0 && trades.size() == 1) {
            this.initialCapital = Math.max(0.0, trade.balanceAfterTrade() - trade.netPnl());
        }
        this.currentMetrics = metricsCalculator.calculate(trades, this.initialCapital);

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

        persistMetricsSnapshot();
        if (Log.isTraceEnabled()) {
            Log.tracef("Persisted live trade record and metrics snapshot to %s (totalTrades=%d, balance=$%.2f)",
                    dataDirectory, trades.size(), currentBalance);
        }
    }

    /**
     * Resets stored trade history and sets the current account balance as the new baseline {@code initialCapital}.
     */
    public synchronized void reset() {
        trades.clear();
        if (this.currentBalance > 0.0) {
            this.initialCapital = this.currentBalance;
        } else {
            this.currentBalance = this.initialCapital;
        }
        this.currentMetrics = PerformanceMetrics.empty(this.initialCapital);

        Path tradesFile = dataDirectory.resolve(TRADES_FILENAME);
        try {
            Files.deleteIfExists(tradesFile);
        } catch (IOException e) {
            Log.errorf("Failed to delete %s during reset: %s", tradesFile, e.getMessage());
        }

        persistMetricsSnapshot();
        Log.infof("Reset live trade storage to account capital: $%.2f", initialCapital);
    }

    private void persistMetricsSnapshot() {
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
                Log.errorf("Failed to write live metrics snapshot: %s", ex.getMessage());
            }
        }
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
