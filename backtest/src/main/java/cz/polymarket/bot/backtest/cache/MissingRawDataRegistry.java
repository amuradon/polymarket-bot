package cz.polymarket.bot.backtest.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks missing raw data files for Binance Futures orderbook and Polymarket events
 * to avoid repeated failed disk lookups or re-computation.
 */
public class MissingRawDataRegistry {

    private static final Logger LOG = Logger.getLogger(MissingRawDataRegistry.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String symbol;
    private final Set<Long> missingPolymarketIntervals;
    private final Set<String> missingFuturesOrderBookHours;

    /**
     * Initializes a fresh missing raw data registry for the specified symbol.
     *
     * @param symbol trading asset pair (e.g. BTCUSDT)
     */
    public MissingRawDataRegistry(String symbol) {
        this.symbol = symbol != null ? symbol.toUpperCase() : "BTCUSDT";
        this.missingPolymarketIntervals = ConcurrentHashMap.newKeySet();
        this.missingFuturesOrderBookHours = ConcurrentHashMap.newKeySet();
    }

    /**
     * JSON deserialization constructor restoring missing interval and hourly sets.
     *
     * @param symbol asset symbol
     * @param missingPolymarketIntervals set of missing Polymarket 15m interval timestamps
     * @param missingFuturesOrderBookHours set of missing hourly orderbook filenames/keys
     */
    @JsonCreator
    public MissingRawDataRegistry(
            @JsonProperty("symbol") String symbol,
            @JsonProperty("missingPolymarketIntervals") Set<Long> missingPolymarketIntervals,
            @JsonProperty("missingFuturesOrderBookHours") Set<String> missingFuturesOrderBookHours) {
        this.symbol = symbol != null ? symbol.toUpperCase() : "BTCUSDT";
        this.missingPolymarketIntervals = ConcurrentHashMap.newKeySet();
        if (missingPolymarketIntervals != null) {
            this.missingPolymarketIntervals.addAll(missingPolymarketIntervals);
        }
        this.missingFuturesOrderBookHours = ConcurrentHashMap.newKeySet();
        if (missingFuturesOrderBookHours != null) {
            this.missingFuturesOrderBookHours.addAll(missingFuturesOrderBookHours);
        }
    }

    /**
     * Returns the trading asset symbol tracked by this registry.
     */
    public String getSymbol() {
        return symbol;
    }

    /**
     * Returns the unmodifiable set of missing Polymarket candle timestamps in seconds.
     */
    public Set<Long> getMissingPolymarketIntervals() {
        return Collections.unmodifiableSet(missingPolymarketIntervals);
    }

    /**
     * Returns the unmodifiable set of missing Binance Futures orderbook date-hour strings.
     */
    public Set<String> getMissingFuturesOrderBookHours() {
        return Collections.unmodifiableSet(missingFuturesOrderBookHours);
    }

    /**
     * Checks if a Polymarket 15m parquet file is registered as missing for the given start timestamp.
     *
     * @param tStart start timestamp in epoch seconds
     * @return true if marked missing
     */
    public boolean isPolymarketMissing(long tStart) {
        return missingPolymarketIntervals.contains(tStart);
    }

    /**
     * Records a missing Polymarket 15m parquet file.
     *
     * @param tStart start timestamp in epoch seconds
     */
    public void recordMissingPolymarket(long tStart) {
        missingPolymarketIntervals.add(tStart);
    }

    /**
     * Bulk-records a collection of missing Polymarket 15m timestamps.
     *
     * @param tStarts collection of timestamps in epoch seconds
     */
    public void recordMissingPolymarketAll(Collection<Long> tStarts) {
        if (tStarts != null) {
            missingPolymarketIntervals.addAll(tStarts);
        }
    }

    /**
     * Checks if a Binance Futures orderbook parquet file is registered as missing for a date-hour.
     *
     * @param dateHour formatted date-hour string (e.g. "2026-10-01-14")
     * @return true if marked missing
     */
    public boolean isFuturesOrderBookMissing(String dateHour) {
        return missingFuturesOrderBookHours.contains(dateHour);
    }

    /**
     * Records a missing Binance Futures orderbook date-hour.
     *
     * @param dateHour formatted date-hour string
     */
    public void recordMissingFuturesOrderBook(String dateHour) {
        if (dateHour != null && !dateHour.isBlank()) {
            missingFuturesOrderBookHours.add(dateHour);
        }
    }

    /**
     * Persists the missing data registry to a JSON file.
     *
     * @param filePath target file path
     * @throws IOException if writing fails
     */
    public synchronized void save(Path filePath) throws IOException {
        if (filePath.getParent() != null) {
            Files.createDirectories(filePath.getParent());
        }
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(filePath.toFile(), this);
        LOG.debugf("Saved missing raw data registry for %s to %s", symbol, filePath);
    }

    /**
     * Loads the missing raw data registry from a JSON file, or creates an empty one on failure.
     *
     * @param filePath path to JSON file
     * @param symbol default asset symbol
     * @return loaded or new MissingRawDataRegistry instance
     */
    public static MissingRawDataRegistry load(Path filePath, String symbol) {
        if (filePath != null && Files.exists(filePath) && Files.isRegularFile(filePath)) {
            try {
                return MAPPER.readValue(filePath.toFile(), MissingRawDataRegistry.class);
            } catch (IOException e) {
                LOG.warnf("Failed to read missing raw data registry from %s: %s. Initializing fresh registry.", filePath, e.getMessage());
            }
        }
        return new MissingRawDataRegistry(symbol);
    }
}
