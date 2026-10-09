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

    public MissingRawDataRegistry(String symbol) {
        this.symbol = symbol != null ? symbol.toUpperCase() : "BTCUSDT";
        this.missingPolymarketIntervals = ConcurrentHashMap.newKeySet();
        this.missingFuturesOrderBookHours = ConcurrentHashMap.newKeySet();
    }

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

    public String getSymbol() {
        return symbol;
    }

    public Set<Long> getMissingPolymarketIntervals() {
        return Collections.unmodifiableSet(missingPolymarketIntervals);
    }

    public Set<String> getMissingFuturesOrderBookHours() {
        return Collections.unmodifiableSet(missingFuturesOrderBookHours);
    }

    public boolean isPolymarketMissing(long tStart) {
        return missingPolymarketIntervals.contains(tStart);
    }

    public void recordMissingPolymarket(long tStart) {
        missingPolymarketIntervals.add(tStart);
    }

    public void recordMissingPolymarketAll(Collection<Long> tStarts) {
        if (tStarts != null) {
            missingPolymarketIntervals.addAll(tStarts);
        }
    }

    public boolean isFuturesOrderBookMissing(String dateHour) {
        return missingFuturesOrderBookHours.contains(dateHour);
    }

    public void recordMissingFuturesOrderBook(String dateHour) {
        if (dateHour != null && !dateHour.isBlank()) {
            missingFuturesOrderBookHours.add(dateHour);
        }
    }

    public synchronized void save(Path filePath) throws IOException {
        if (filePath.getParent() != null) {
            Files.createDirectories(filePath.getParent());
        }
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(filePath.toFile(), this);
        LOG.debugf("Saved missing raw data registry for %s to %s", symbol, filePath);
    }

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
