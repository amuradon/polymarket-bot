package cz.polymarket.bot.paper.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.TradeRecord;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

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
 * Crash-resilient, append-only repository for paper trading trades and continuous metrics.
 * Persists trade executions to line-delimited JSON (trades.jsonl) and maintains atomic metrics snapshots (metrics.json).
 * Restores full trading history and performance statistics upon container restarts or upgrades.
 */
@ApplicationScoped
public class PaperTradeRepository {

    private static final Logger LOG = Logger.getLogger(PaperTradeRepository.class);
    private static final String TRADES_FILENAME = "trades.jsonl";
    private static final String METRICS_FILENAME = "metrics.json";

    private final Path dataDirectory;
    private final double initialCapital;
    private final ObjectMapper objectMapper;
    private final PerformanceMetricsCalculator metricsCalculator;

    private final List<TradeRecord> trades = new CopyOnWriteArrayList<>();
    private volatile PerformanceMetrics currentMetrics;
    private volatile double currentBalance;

    @Inject
    public PaperTradeRepository(
            @ConfigProperty(name = "polymarket.paper.data-dir", defaultValue = "data/paper") String dataDir,
            @ConfigProperty(name = "polymarket.paper.initial-capital", defaultValue = "10000.0") double initialCapital,
            ObjectMapper objectMapper,
            PerformanceMetricsCalculator metricsCalculator) {
        if (dataDir == null || dataDir.isBlank()) {
            throw new IllegalArgumentException("dataDir cannot be null or blank");
        }
        if (initialCapital <= 0) {
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
            LOG.infof("Initializing paper trade storage in directory: %s", dataDirectory.toAbsolutePath());
            try (var stream = Files.list(dataDirectory)) {
                List<String> files = stream.map(p -> p.getFileName().toString()).toList();
                LOG.infof("Existing files in %s: %s", dataDirectory.toAbsolutePath(), files);
            } catch (Exception e) {
                LOG.warnf("Could not list files in %s: %s", dataDirectory, e.getMessage());
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
                            LOG.warnf("Skipping malformed trade line during replay: %s (%s)", line, e.getMessage());
                        }
                    }
                }
                trades.addAll(loaded);
                LOG.infof("Restored %d trades from %s", trades.size(), tradesFile.toAbsolutePath());
            } else {
                LOG.infof("No trades file found at %s - starting with fresh balance $%.2f", tradesFile.toAbsolutePath(), initialCapital);
            }

            if (trades.isEmpty()) {
                this.currentBalance = initialCapital;
                this.currentMetrics = PerformanceMetrics.empty(initialCapital);
            } else {
                this.currentBalance = trades.get(trades.size() - 1).balanceAfterTrade();
                this.currentMetrics = metricsCalculator.calculate(trades, initialCapital);
            }
        } catch (IOException e) {
            LOG.errorf("Failed to initialize paper trade storage in %s: %s", dataDirectory, e.getMessage());
            throw new RuntimeException("Could not initialize PaperTradeRepository", e);
        }
    }

    /**
     * Appends an executed trade to the persistent JSON lines log and atomically updates metrics.
     *
     * @param trade the executed trade record
     */
    public synchronized void recordTrade(TradeRecord trade) {
        if (trade == null) {
            throw new IllegalArgumentException("Trade record cannot be null");
        }

        trades.add(trade);
        this.currentBalance = trade.balanceAfterTrade();
        this.currentMetrics = metricsCalculator.calculate(trades, initialCapital);

        // 1. Append to trades.jsonl
        Path tradesFile = dataDirectory.resolve(TRADES_FILENAME);
        try {
            String jsonLine = objectMapper.writeValueAsString(trade) + System.lineSeparator();
            Files.writeString(tradesFile, jsonLine,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            LOG.errorf("Failed to append trade to %s: %s", tradesFile, e.getMessage());
            throw new RuntimeException("Failed to persist trade to " + tradesFile, e);
        }

        // 2. Write metrics.json snapshot (using REPLACE_EXISTING with direct write fallback for Cloud Storage / GCSFuse)
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
                LOG.warnf("Failed to write metrics snapshot: %s", ex.getMessage());
            }
        }
    }

    public synchronized void reset() {
        trades.clear();
        this.currentBalance = initialCapital;
        this.currentMetrics = PerformanceMetrics.empty(initialCapital);

        Path tradesFile = dataDirectory.resolve(TRADES_FILENAME);
        try {
            Files.deleteIfExists(tradesFile);
        } catch (IOException e) {
            LOG.warnf("Failed to delete %s during reset: %s", tradesFile, e.getMessage());
        }

        Path metricsFile = dataDirectory.resolve(METRICS_FILENAME);
        try {
            String metricsJson = objectMapper.writeValueAsString(currentMetrics);
            Files.writeString(metricsFile, metricsJson,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            LOG.warnf("Failed to write metrics.json during reset: %s", e.getMessage());
        }
        LOG.infof("Reset paper trade storage to initial capital: $%.2f", initialCapital);
    }

    public List<TradeRecord> getTrades() {
        return Collections.unmodifiableList(trades);
    }

    public PerformanceMetrics getMetrics() {
        return currentMetrics;
    }

    public double getCurrentBalance() {
        return currentBalance;
    }

    public double getInitialCapital() {
        return initialCapital;
    }
}
