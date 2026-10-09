package cz.polymarket.bot.backtest.cache;

import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import cz.polymarket.bot.backtest.data.ParquetDatasetLoader;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * High-level market data service for the backtest engine.
 * Transparently checks memory-mapped binary cache, dynamically computes missing data
 * from raw Binance and Polymarket datasets, and merges technical indicators into BacktestMarketRow models.
 */
@ApplicationScoped
public class BacktestDataCacheService {

    private static final Logger LOG = Logger.getLogger(BacktestDataCacheService.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final BinaryMarketCacheService cacheService;
    private final RawMarketDataProcessor processor;
    private final ParquetDatasetLoader parquetDatasetLoader;
    private final MissingRawDataRegistry missingRegistry;
    private final String defaultPrecomputedParquet;

    @Inject
    public BacktestDataCacheService(
            BinaryMarketCacheService cacheService,
            RawMarketDataProcessor processor,
            ParquetDatasetLoader parquetDatasetLoader,
            MissingRawDataRegistry missingRegistry,
            @ConfigProperty(name = "polymarket.backtest.dataset-path", defaultValue = "D:/Polymarket/btc_nextCandle/unified_market_data.parquet")
            String defaultPrecomputedParquet) {
        this.cacheService = cacheService;
        this.processor = processor;
        this.parquetDatasetLoader = parquetDatasetLoader;
        this.missingRegistry = missingRegistry;
        this.defaultPrecomputedParquet = defaultPrecomputedParquet;
    }

    public BacktestDataCacheService(
            BinaryMarketCacheService cacheService,
            RawMarketDataProcessor processor,
            ParquetDatasetLoader parquetDatasetLoader,
            MissingRawDataRegistry missingRegistry) {
        this(cacheService, processor, parquetDatasetLoader, missingRegistry, "D:/Polymarket/btc_nextCandle/unified_market_data.parquet");
    }

    public List<BacktestMarketRow> loadMarketData(String symbol, String startDateStr, String endDateStr, String overrideDatasetPath) {
        String sym = (symbol != null && !symbol.isBlank()) ? symbol.toUpperCase() : "BTCUSDT";

        // 1. If explicit datasetPath is provided, validate and load
        if (overrideDatasetPath != null && !overrideDatasetPath.isBlank()) {
            File f = new File(overrideDatasetPath);
            if (!f.exists() || !f.isFile()) {
                throw new IllegalArgumentException("Specified dataset file does not exist: " + overrideDatasetPath);
            }
            LOG.infof("Loading market rows from explicit dataset path: %s", overrideDatasetPath);
            return parquetDatasetLoader.loadDataset(overrideDatasetPath);
        }

        // 2. Determine target months
        List<String> targetMonths = resolveTargetMonths(sym, startDateStr, endDateStr);
        LOG.infof("Resolved target months for %s: %s", sym, targetMonths);

        // 3. For each month, ensure it exists in binary cache
        for (String month : targetMonths) {
            if (!cacheService.hasMarketRowsCache(sym, month)) {
                LOG.infof("Cache miss for %s %s. Attempting to ingest...", sym, month);
                ensureMonthCached(sym, month);
            }
        }

        // 4. Read cached rows and indicators, then merge
        List<BacktestMarketRow> result = new ArrayList<>();
        LocalDate filterStart = (startDateStr != null && !startDateStr.isBlank()) ? LocalDate.parse(startDateStr, DATE_FORMATTER) : null;
        LocalDate filterEnd = (endDateStr != null && !endDateStr.isBlank()) ? LocalDate.parse(endDateStr, DATE_FORMATTER) : null;

        for (String month : targetMonths) {
            if (!cacheService.hasMarketRowsCache(sym, month)) {
                continue;
            }

            List<CachedMarketRow> cachedRows = cacheService.readMarketRowsCache(sym, month);
            Map<Long, Map<String, Double>> twapInd = cacheService.readIndicatorCache(sym, "twap", month);
            Map<Long, Map<String, Double>> basisInd = cacheService.readIndicatorCache(sym, "basis", month);

            for (CachedMarketRow r : cachedRows) {
                Instant inst = Instant.ofEpochSecond(r.tStart());
                LocalDate rowDate = inst.atZone(ZoneOffset.UTC).toLocalDate();

                if (filterStart != null && rowDate.isBefore(filterStart)) {
                    continue;
                }
                if (filterEnd != null && rowDate.isAfter(filterEnd)) {
                    continue;
                }

                Map<String, Double> twapVals = twapInd.get(r.tStart());
                double twapOpen = (twapVals != null && twapVals.containsKey("twap_open")) ? twapVals.get("twap_open") : r.sOpen();
                double twapClose = (twapVals != null && twapVals.containsKey("twap_close")) ? twapVals.get("twap_close") : r.sClose();

                Map<String, Double> basisVals = basisInd.get(r.tStart());
                double basisOpen = (basisVals != null && basisVals.containsKey("basis_open_bps")) ? basisVals.get("basis_open_bps") : 0.0;
                double basisClose = (basisVals != null && basisVals.containsKey("basis_close_bps")) ? basisVals.get("basis_close_bps") : 0.0;

                String dateStr = rowDate.format(DATE_FORMATTER);
                String datetimeUtc = DateTimeFormatter.ISO_INSTANT.format(inst);
                String actualOutcome = r.actualOutcomeUp() ? "UP" : "DOWN";

                result.add(new BacktestMarketRow(
                        r.tStart(), r.tEnd(), datetimeUtc, dateStr,
                        twapOpen, twapClose, actualOutcome,
                        r.sOpen(), r.sHigh(), r.sLow(), r.sClose(),
                        r.sVolBtc(), r.sVolUsd(), r.sDeltaBtc(),
                        r.fOpen(), r.fClose(),
                        r.fVolBtc(), r.fVolUsd(), r.fDeltaBtc(),
                        basisOpen, basisClose,
                        r.pmAsk0(), r.pmBid0(),
                        r.pmAsk60(), r.pmBid60(),
                        r.pmAsk180(), r.pmBid180(),
                        r.pmAsk300(), r.pmBid300(),
                        r.pmFill100Up(), r.pmFee100Up(),
                        r.pmFill100Down(), r.pmFee100Down(),
                        r.pmMaxPrice(), r.pmMinPrice(),
                        r.pmDepth1cUp(), r.pmDepth1cDown()
                ));
            }
        }

        result.sort(Comparator.comparingLong(BacktestMarketRow::tStart));
        LOG.infof("Loaded %d market rows for %s across months %s", result.size(), sym, targetMonths);
        return Collections.unmodifiableList(result);
    }

    private void ensureMonthCached(String symbol, String month) {
        // If unified_market_data.parquet is available and covers this month (e.g. 2026-08 or 2026-09), import from it
        File defaultParquetFile = new File(defaultPrecomputedParquet);
        if (defaultParquetFile.exists() && ("2026-08".equals(month) || "2026-09".equals(month))) {
            LOG.infof("Bootstrapping cache for %s from precomputed parquet %s...", month, defaultPrecomputedParquet);
            List<BacktestMarketRow> rows = parquetDatasetLoader.loadDataset(defaultPrecomputedParquet);
            List<BacktestMarketRow> monthRows = new ArrayList<>();
            for (BacktestMarketRow r : rows) {
                if (r.dateStr() != null && r.dateStr().startsWith(month)) {
                    monthRows.add(r);
                }
            }
            if (!monthRows.isEmpty()) {
                processor.importMarketRows(symbol, month, monthRows);
                LOG.infof("Imported %d rows for %s into cache", monthRows.size(), month);
                return;
            }
        }

        // Otherwise process raw data directly
        LOG.infof("Processing raw datasets for %s %s...", symbol, month);
        processor.processRawDataForMonth(symbol, month);
    }

    private List<String> resolveTargetMonths(String symbol, String startDateStr, String endDateStr) {
        List<String> months = new ArrayList<>();
        if (startDateStr != null && !startDateStr.isBlank() && endDateStr != null && !endDateStr.isBlank()) {
            LocalDate start = LocalDate.parse(startDateStr, DATE_FORMATTER);
            LocalDate end = LocalDate.parse(endDateStr, DATE_FORMATTER);
            LocalDate curr = start.withDayOfMonth(1);
            while (!curr.isAfter(end)) {
                String m = String.format("%04d-%02d", curr.getYear(), curr.getMonthValue());
                if (!months.contains(m)) {
                    months.add(m);
                }
                curr = curr.plusMonths(1);
            }
            return months;
        }

        // Check already cached months
        List<String> cached = cacheService.getAvailableMonths(symbol);
        if (!cached.isEmpty()) {
            months.addAll(cached);
        }

        // Also check if raw data files exist for known months
        Path spotDir = processor.getBinanceSpotAggTradesDir(symbol);
        if (Files.exists(spotDir)) {
            File[] files = spotDir.toFile().listFiles((dir, name) -> name.endsWith(".csv"));
            if (files != null) {
                for (File f : files) {
                    String name = f.getName();
                    // BTCUSDT-aggTrades-YYYY-MM-DD.csv
                    int idx = name.indexOf("aggTrades-");
                    if (idx >= 0 && name.length() >= idx + 17) {
                        String m = name.substring(idx + 10, idx + 17);
                        if (!months.contains(m)) {
                            months.add(m);
                        }
                    }
                }
            }
        }

        if (months.isEmpty()) {
            // Default to current target period
            months.add("2026-08");
            months.add("2026-09");
            months.add("2026-10");
        }

        Collections.sort(months);
        return months;
    }
}
