package cz.polymarket.bot.backtest.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Ingests, processes, and reconstructs multi-source raw market data
 * (Binance Spot CSV, Binance Futures CSV, Binance Futures OrderBook Parquet, Polymarket 15m Parquet)
 * and technical indicators, caching them in memory-mapped binary files.
 */
@ApplicationScoped
public class RawMarketDataProcessor {

    private static final Logger LOG = Logger.getLogger(RawMarketDataProcessor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            DriverManager.registerDriver(new org.duckdb.DuckDBDriver());
        } catch (Exception ignored) {
        }
    }

    private final BinaryMarketCacheService cacheService;
    private final MissingRawDataRegistry missingRegistry;
    private final Path binanceBaseDir;
    private final Path polymarketBaseDir;

    /**
     * Constructs the processor injecting cache service, missing registry, and base file paths.
     *
     * @param cacheService binary market cache service
     * @param missingRegistry registry tracking missing parquet/csv files
     * @param binanceBaseDirStr path to Binance raw data root directory
     * @param polymarketBaseDirStr path to Polymarket raw data root directory
     */
    @Inject
    public RawMarketDataProcessor(
            BinaryMarketCacheService cacheService,
            MissingRawDataRegistry missingRegistry,
            @ConfigProperty(name = "polymarket.data.base-dir", defaultValue = "D:/Crypto/data/Polymarket/Binance")
            String binanceBaseDirStr,
            @ConfigProperty(name = "polymarket.backtest.polymarket-dir", defaultValue = "D:/Crypto/data/Polymarket")
            String polymarketBaseDirStr) {
        this.cacheService = cacheService;
        this.missingRegistry = missingRegistry;
        this.binanceBaseDir = Path.of(binanceBaseDirStr);
        this.polymarketBaseDir = Path.of(polymarketBaseDirStr);
    }

    /**
     * Returns the underlying binary cache service.
     */
    public BinaryMarketCacheService getCacheService() {
        return cacheService;
    }

    /**
     * Returns the missing raw data registry.
     */
    public MissingRawDataRegistry getMissingRegistry() {
        return missingRegistry;
    }

    /**
     * Resolves the Polymarket raw Parquet directory for the given symbol (e.g. btc-up-or-down-15m).
     *
     * @param symbol asset symbol (e.g. BTCUSDT)
     * @return Path to directory containing raw Polymarket 15m Parquet files
     */
    public Path getPolymarketDir(String symbol) {
        // e.g. D:/Crypto/data/Polymarket/btc-up-or-down-15m
        if (symbol != null && symbol.toUpperCase().startsWith("BTC")) {
            Path btcDir = polymarketBaseDir.resolve("btc-up-or-down-15m");
            if (Files.exists(btcDir)) {
                return btcDir;
            }
        }
        String symLower = symbol != null ? symbol.toLowerCase().replace("usdt", "") : "btc";
        Path candidate = polymarketBaseDir.resolve(symLower + "-up-or-down-15m");
        if (Files.exists(candidate)) {
            return candidate;
        }
        return polymarketBaseDir.resolve("btc-up-or-down-15m");
    }

    /**
     * Resolves the Binance Futures OrderBook Parquet directory.
     *
     * @param symbol asset symbol
     * @return Path to Binance Futures orderbook directory
     */
    public Path getBinanceFuturesOrderBookDir(String symbol) {
        // baseBinanceDir may be D:/Crypto/data/Polymarket/Binance or D:/Crypto/data/Polymarket
        Path direct = binanceBaseDir.resolve("futures").resolve(symbol.toUpperCase()).resolve("orderBook");
        if (Files.exists(direct)) {
            return direct;
        }
        Path alt = binanceBaseDir.resolve("Binance").resolve("futures").resolve(symbol.toUpperCase()).resolve("orderBook");
        if (Files.exists(alt)) {
            return alt;
        }
        return direct;
    }

    /**
     * Resolves the Binance Spot aggTrades CSV directory.
     *
     * @param symbol asset symbol
     * @return Path to Binance Spot aggTrades directory
     */
    public Path getBinanceSpotAggTradesDir(String symbol) {
        Path direct = binanceBaseDir.resolve("spot").resolve(symbol.toUpperCase()).resolve("aggTrades");
        if (Files.exists(direct)) {
            return direct;
        }
        Path alt = binanceBaseDir.resolve("Binance").resolve("spot").resolve(symbol.toUpperCase()).resolve("aggTrades");
        if (Files.exists(alt)) {
            return alt;
        }
        return direct;
    }

    /**
     * Resolves the Binance Futures aggTrades CSV directory.
     *
     * @param symbol asset symbol
     * @return Path to Binance Futures aggTrades directory
     */
    public Path getBinanceFuturesAggTradesDir(String symbol) {
        Path direct = binanceBaseDir.resolve("futures").resolve(symbol.toUpperCase()).resolve("aggTrades");
        if (Files.exists(direct)) {
            return direct;
        }
        Path alt = binanceBaseDir.resolve("Binance").resolve("futures").resolve(symbol.toUpperCase()).resolve("aggTrades");
        if (Files.exists(alt)) {
            return alt;
        }
        return direct;
    }

    /**
     * Checks for presence of raw Polymarket and OrderBook files and registers any missing entries.
     *
     * @param symbol asset symbol
     * @param tStart interval start timestamp in seconds
     * @param dateHour formatted date-hour string
     */
    public void checkAndRecordMissingRawFiles(String symbol, long tStart, String dateHour) {
        Path pmFile = getPolymarketDir(symbol).resolve(tStart + ".parquet");
        if (!Files.exists(pmFile)) {
            missingRegistry.recordMissingPolymarket(tStart);
        }

        if (dateHour != null && !dateHour.isBlank()) {
            String filename = symbol.toUpperCase() + "-orderbook-" + dateHour + ".parquet";
            Path obFile = getBinanceFuturesOrderBookDir(symbol).resolve(filename);
            if (!Files.exists(obFile)) {
                missingRegistry.recordMissingFuturesOrderBook(dateHour);
            }
        }
    }

    /**
     * Imports market rows into memory-mapped binary cache files, splitting indicators into separate binary files.
     *
     * @param symbol trading asset pair (e.g. BTCUSDT)
     * @param month target month in YYYY-MM format
     * @param rows list of aggregated BacktestMarketRow instances
     */
    public void importMarketRows(String symbol, String month, List<BacktestMarketRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return;
        }

        List<CachedMarketRow> cachedRows = new ArrayList<>(rows.size());
        Map<Long, Map<String, Double>> twapRecords = new LinkedHashMap<>();
        Map<Long, Map<String, Double>> basisRecords = new LinkedHashMap<>();

        for (BacktestMarketRow r : rows) {
            boolean actualOutcomeUp = "UP".equalsIgnoreCase(r.actualOutcome());
            cachedRows.add(new CachedMarketRow(
                    r.tStart(), r.tEnd(),
                    r.sOpen(), r.sHigh(), r.sLow(), r.sClose(),
                    r.sVolBtc(), r.sVolUsd(), r.sDeltaBtc(),
                    r.fOpen(), Math.max(r.fOpen(), r.fClose()), Math.min(r.fOpen(), r.fClose()), r.fClose(),
                    r.fVolBtc(), r.fVolUsd(), r.fDeltaBtc(),
                    actualOutcomeUp,
                    r.pmAsk0(), r.pmBid0(),
                    r.pmAsk60(), r.pmBid60(),
                    r.pmAsk180(), r.pmBid180(),
                    r.pmAsk300(), r.pmBid300(),
                    r.pmFill100Up(), r.pmFee100Up(),
                    r.pmFill100Down(), r.pmFee100Down(),
                    r.pmMaxPrice(), r.pmMinPrice(),
                    r.pmDepth1cUp(), r.pmDepth1cDown()
            ));

            twapRecords.put(r.tStart(), Map.of(
                    "twap_open", r.twapOpen(),
                    "twap_close", r.twapClose()
            ));

            basisRecords.put(r.tStart(), Map.of(
                    "basis_open_bps", r.basisOpenBps(),
                    "basis_close_bps", r.basisCloseBps()
            ));
        }

        // 1. Write base market rows
        cacheService.writeMarketRowsCache(symbol, month, cachedRows);

        // 2. Write TWAP indicator cache
        cacheService.writeIndicatorCache(symbol, "twap", month, List.of("twap_open", "twap_close"), twapRecords);

        // 3. Write Basis indicator cache
        cacheService.writeIndicatorCache(symbol, "basis", month, List.of("basis_open_bps", "basis_close_bps"), basisRecords);

        // 4. Compute and write technical indicators (VWAP, CVD, Volatility)
        Map<String, Map<Long, Map<String, Double>>> techInds = computeIndicators(cachedRows);
        for (Map.Entry<String, Map<Long, Map<String, Double>>> entry : techInds.entrySet()) {
            String indId = entry.getKey();
            Map<Long, Map<String, Double>> indRecs = entry.getValue();
            if (!indRecs.isEmpty()) {
                List<String> keys = new ArrayList<>(indRecs.values().iterator().next().keySet());
                Collections.sort(keys);
                cacheService.writeIndicatorCache(symbol, indId, month, keys, indRecs);
            }
        }
    }

    /**
     * Computes technical indicators across sequential market intervals:
     * - Rolling VWAP and Z-score
     * - Cumulative Volume Delta (15m, 1h, 4h) and order absorption divergence
     * - 4-hour realized volatility
     *
     * @param rows sequence of cached market intervals
     * @return map of indicator identifier to map of interval start epoch seconds to metric key-value pairs
     */
    public Map<String, Map<Long, Map<String, Double>>> computeIndicators(List<CachedMarketRow> rows) {
        Map<String, Map<Long, Map<String, Double>>> result = new HashMap<>();

        Map<Long, Map<String, Double>> vwapMap = new LinkedHashMap<>();
        Map<Long, Map<String, Double>> cvdMap = new LinkedHashMap<>();
        Map<Long, Map<String, Double>> volMap = new LinkedHashMap<>();

        List<CachedMarketRow> history = new ArrayList<>();

        for (CachedMarketRow row : rows) {
            long t = row.tStart();
            history.add(row);

            // 1. VWAP (computed across available history)
            double totUsd = 0.0;
            double totBtc = 0.0;
            for (CachedMarketRow h : history) {
                totUsd += h.sVolUsd();
                totBtc += h.sVolBtc();
            }
            double vwap = (totBtc > 0.0) ? (totUsd / totBtc) : row.sClose();
            double weightedVar = 0.0;
            if (totBtc > 0.0) {
                for (CachedMarketRow h : history) {
                    double diff = h.sClose() - vwap;
                    weightedVar += h.sVolBtc() * diff * diff;
                }
                weightedVar /= totBtc;
            }
            double stdDev = (weightedVar > 0.0) ? Math.sqrt(weightedVar) : 1.0;
            double zScore = (stdDev > 0.0) ? ((row.sClose() - vwap) / stdDev) : 0.0;

            vwapMap.put(t, Map.of(
                    "vwap", vwap,
                    "vwap_upper", vwap + stdDev,
                    "vwap_lower", vwap - stdDev,
                    "std_dev", stdDev,
                    "z_score", zScore
            ));

            // 2. CVD metrics
            double cvd15m = row.sDeltaBtc();
            int h1Start = Math.max(0, history.size() - 4);
            double cvd1h = 0.0;
            for (int i = h1Start; i < history.size(); i++) {
                cvd1h += history.get(i).sDeltaBtc();
            }
            int h4Start = Math.max(0, history.size() - 16);
            double cvd4h = 0.0;
            for (int i = h4Start; i < history.size(); i++) {
                cvd4h += history.get(i).sDeltaBtc();
            }
            double priceChange = row.sClose() - row.sOpen();
            double divergence = 0.0;
            if (priceChange < 0.0 && cvd15m > 0.0) {
                divergence = 1.0; // Bullish absorption
            } else if (priceChange > 0.0 && cvd15m < 0.0) {
                divergence = -1.0; // Bearish exhaustion
            }

            cvdMap.put(t, Map.of(
                    "cvd_15m", cvd15m,
                    "cvd_1h", cvd1h,
                    "cvd_4h", cvd4h,
                    "divergence", divergence
            ));

            // 3. Volatility (4h realized volatility over last 16 intervals)
            double vol4h = 0.001;
            if (history.size() >= 4) {
                int volStart = Math.max(0, history.size() - 16);
                List<Double> returns = new ArrayList<>();
                double sumRet = 0.0;
                for (int i = volStart; i < history.size(); i++) {
                    CachedMarketRow curr = history.get(i);
                    double ret = (curr.sOpen() > 0.0) ? ((curr.sClose() - curr.sOpen()) / curr.sOpen()) : 0.0;
                    returns.add(ret);
                    sumRet += ret;
                }
                double meanRet = sumRet / returns.size();
                double sumSq = 0.0;
                for (Double r : returns) {
                    sumSq += (r - meanRet) * (r - meanRet);
                }
                vol4h = Math.sqrt(sumSq / returns.size());
            }

            volMap.put(t, Map.of("vol_4h", vol4h));
        }

        result.put("vwap", vwapMap);
        result.put("cvd", cvdMap);
        result.put("volatility", volMap);

        return result;
    }

    /**
     * Ingests, processes, aggregates, and caches multi-source raw market data for a given month.
     * Merges Binance Spot CSVs, Binance Futures CSVs, Binance Futures OrderBook Parquets,
     * and Polymarket 15m Parquet books.
     *
     * @param symbol trading asset pair (e.g. BTCUSDT)
     * @param month target month string (YYYY-MM)
     */
    public void processRawDataForMonth(String symbol, String month) {
        LOG.infof("Starting raw data processing for symbol %s month %s...", symbol, month);

        Path spotDir = getBinanceSpotAggTradesDir(symbol);
        Path futDir = getBinanceFuturesAggTradesDir(symbol);
        Path pmDir = getPolymarketDir(symbol);
        Path obDir = getBinanceFuturesOrderBookDir(symbol);

        if (!Files.exists(spotDir) || !Files.exists(futDir)) {
            LOG.warnf("Raw aggTrades directories do not exist for %s (spot: %s, fut: %s)", symbol, spotDir, futDir);
            return;
        }

        File[] spotFiles = spotDir.toFile().listFiles((dir, name) ->
                name.startsWith(symbol.toUpperCase() + "-aggTrades-" + month) && name.endsWith(".csv"));

        if (spotFiles == null || spotFiles.length == 0) {
            LOG.infof("No raw spot CSV files found for %s month %s", symbol, month);
            return;
        }

        List<File> sortedSpotFiles = new ArrayList<>(List.of(spotFiles));
        sortedSpotFiles.sort((a, b) -> a.getName().compareTo(b.getName()));

        List<BacktestMarketRow> allMonthRows = new ArrayList<>();
        Map<Long, Map<String, Double>> binanceObiRecords = new LinkedHashMap<>();

        // Keep tail of previous day for 60s TWAP calculation
        List<TradeRecordSimple> prevSpotTail = new ArrayList<>();

        for (File sFile : sortedSpotFiles) {
            String dateStr = sFile.getName().replace(symbol.toUpperCase() + "-aggTrades-", "").replace(".csv", "");
            File fFile = futDir.resolve(symbol.toUpperCase() + "-aggTrades-" + dateStr + ".csv").toFile();
            if (!fFile.exists()) {
                LOG.warnf("Futures file missing for %s date %s, skipping", symbol, dateStr);
                continue;
            }

            LOG.infof("Processing raw date %s for %s...", dateStr, symbol);

            // Read Spot trades: transact_time in microseconds
            List<TradeRecordSimple> spotTrades = readSpotTrades(sFile);
            // Read Futures trades: transact_time in milliseconds
            List<TradeRecordSimple> futTrades = readFuturesTrades(fFile);

            // Combine previous day tail with today's spot trades for TWAP
            List<TradeRecordSimple> combinedSpot = new ArrayList<>(prevSpotTail);
            combinedSpot.addAll(spotTrades);

            if (!spotTrades.isEmpty()) {
                double lastT = spotTrades.get(spotTrades.size() - 1).tSec();
                prevSpotTail.clear();
                for (TradeRecordSimple t : spotTrades) {
                    if (t.tSec() >= lastT - 120.0) {
                        prevSpotTail.add(t);
                    }
                }
            }

            // Aggregate 15m intervals
            Map<Long, IntervalCandle> s15m = aggregate15m(spotTrades);
            Map<Long, IntervalCandle> f15m = aggregate15m(futTrades);

            List<Long> intervalStarts = new ArrayList<>(s15m.keySet());
            Collections.sort(intervalStarts);

            for (Long tStart : intervalStarts) {
                long tEnd = tStart + 900L;
                IntervalCandle sCandle = s15m.get(tStart);
                IntervalCandle fCandle = f15m.getOrDefault(tStart, sCandle);

                // Check Polymarket raw parquet
                Path pmFile = pmDir.resolve(tStart + ".parquet");
                PolymarketExtractedData pmData = null;
                if (!Files.exists(pmFile)) {
                    missingRegistry.recordMissingPolymarket(tStart);
                } else {
                    pmData = extractPolymarketData(pmFile, tStart);
                }

                // If Polymarket file missing, synthesize default reference quotes
                if (pmData == null) {
                    pmData = new PolymarketExtractedData(
                            0.50, 0.49, 0.50, 0.49, 0.50, 0.49, 0.50, 0.49,
                            0.50, 0.015, 0.51, 0.015, 0.50, 0.49, 300.0, 300.0
                    );
                }

                // Check Binance Futures OrderBook for corresponding hour
                Instant inst = Instant.ofEpochSecond(tStart);
                int hour = inst.atZone(ZoneOffset.UTC).getHour();
                String hourStr = String.format("%02d", hour);
                String dateHour = dateStr + "-" + hourStr;
                Path obFile = obDir.resolve(symbol.toUpperCase() + "-orderbook-" + dateHour + ".parquet");
                if (!Files.exists(obFile)) {
                    missingRegistry.recordMissingFuturesOrderBook(dateHour);
                    binanceObiRecords.put(tStart, Map.of(
                            "obi", Double.NaN,
                            "mid_price", Double.NaN,
                            "depth_bids", Double.NaN,
                            "depth_asks", Double.NaN
                    ));
                } else {
                    // Extract OBI at tStart + 60s
                    BinanceObiExtractedData obiData = extractBinanceObi(obFile, tStart + 60L);
                    binanceObiRecords.put(tStart, Map.of(
                            "obi", obiData.obi(),
                            "mid_price", obiData.midPrice(),
                            "depth_bids", obiData.depthBids(),
                            "depth_asks", obiData.depthAsks()
                    ));
                }

                double twapOpen = computeTwap(combinedSpot, tStart, 60.0);
                if (twapOpen <= 0.0) {
                    twapOpen = sCandle.open();
                }
                double twapClose = computeTwap(combinedSpot, tEnd, 60.0);
                if (twapClose <= 0.0) {
                    twapClose = sCandle.close();
                }
                String actualOutcome = (twapClose >= twapOpen) ? "UP" : "DOWN";

                double basisOpenBps = (sCandle.open() > 0.0) ? (((fCandle.open() - sCandle.open()) / sCandle.open()) * 10000.0) : 0.0;
                double basisCloseBps = (sCandle.close() > 0.0) ? (((fCandle.close() - sCandle.close()) / sCandle.close()) * 10000.0) : 0.0;

                String datetimeUtc = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(tStart));

                allMonthRows.add(new BacktestMarketRow(
                        tStart, tEnd, datetimeUtc, dateStr,
                        twapOpen, twapClose, actualOutcome,
                        sCandle.open(), sCandle.high(), sCandle.low(), sCandle.close(),
                        sCandle.volBtc(), sCandle.volUsd(), sCandle.deltaBtc(),
                        fCandle.open(), fCandle.close(),
                        fCandle.volBtc(), fCandle.volUsd(), fCandle.deltaBtc(),
                        basisOpenBps, basisCloseBps,
                        pmData.ask0(), pmData.bid0(),
                        pmData.ask60(), pmData.bid60(),
                        pmData.ask180(), pmData.bid180(),
                        pmData.ask300(), pmData.bid300(),
                        pmData.fill100Up(), pmData.fee100Up(),
                        pmData.fill100Down(), pmData.fee100Down(),
                        pmData.maxPrice(), pmData.minPrice(),
                        pmData.depth1cUp(), pmData.depth1cDown()
                ));
            }
        }

        if (!allMonthRows.isEmpty()) {
            LOG.infof("Writing %d processed intervals to cache for %s month %s...", allMonthRows.size(), symbol, month);
            importMarketRows(symbol, month, allMonthRows);

            if (!binanceObiRecords.isEmpty()) {
                cacheService.writeIndicatorCache(symbol, "binance_obi", month,
                        List.of("obi", "mid_price", "depth_bids", "depth_asks"), binanceObiRecords);
            }

            // Persist updated missing registry
            Path missingJson = cacheService.getCacheDir().resolve("missing_raw_data_" + symbol.toUpperCase() + ".json");
            try {
                missingRegistry.save(missingJson);
            } catch (IOException e) {
                LOG.warnf("Failed to persist missing raw data registry: %s", e.getMessage());
            }
        }
    }

    /**
     * Reconstructs Polymarket orderbook and trade activity for a 15-minute interval from Parquet.
     * Extracts best quotes at t=0s, 60s, 180s, 300s, simulated fills for $100 orders, and depth.
     *
     * @param pmFile Path to Polymarket parquet file for the interval
     * @param tStartSec interval start timestamp in seconds
     * @return PolymarketExtractedData containing snapshot quotes and execution benchmarks
     */
    public PolymarketExtractedData extractPolymarketData(Path pmFile, long tStartSec) {
        String normPath = pmFile.toAbsolutePath().toString().replace('\\', '/');
        String query = "SELECT event_type, t, price, size, side, bids, asks FROM read_parquet('" + normPath + "') ORDER BY t ASC";

        double pmMax = Double.NaN;
        double pmMin = Double.NaN;
        double pmAsk0 = 0.50, pmBid0 = 0.49;
        double pmAsk60 = 0.50, pmBid60 = 0.49;
        double pmAsk180 = 0.50, pmBid180 = 0.49;
        double pmAsk300 = 0.50, pmBid300 = 0.49;
        double pmDepth1cUp = 300.0, pmDepth1cDown = 300.0;
        double pmFill100Up = 0.50, pmFee100Up = 0.015;
        double pmFill100Down = 0.51, pmFee100Down = 0.015;

        long t0Ms = tStartSec * 1000L + 10L;
        long t60Ms = tStartSec * 1000L + 60_000L;
        long t180Ms = tStartSec * 1000L + 180_000L;
        long t300Ms = tStartSec * 1000L + 300_000L;

        boolean captured0 = false;
        boolean captured60 = false;
        boolean captured180 = false;
        boolean captured300 = false;

        Map<Double, Double> bids = new HashMap<>();
        Map<Double, Double> asks = new HashMap<>();

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String et = rs.getString(1);
                long tMs = rs.getLong(2);
                double price = rs.getDouble(3);
                double size = rs.getDouble(4);
                String side = rs.getString(5);
                String bidsJson = rs.getString(6);
                String asksJson = rs.getString(7);

                if ("trade".equalsIgnoreCase(et) && !rs.wasNull()) {
                    if (Double.isNaN(pmMax) || price > pmMax) pmMax = price;
                    if (Double.isNaN(pmMin) || price < pmMin) pmMin = price;
                }

                if (!captured0 && tMs >= t0Ms) {
                    pmAsk0 = getBestAsk(asks, 0.50);
                    pmBid0 = getBestBid(bids, 0.49);
                    pmDepth1cUp = getDepthWithin1c(asks, pmAsk0, true);
                    pmDepth1cDown = getDepthWithin1c(bids, pmBid0, false);
                    FillFeeResult ffUp = calculateFillAndFee(asks, 100.0, true);
                    FillFeeResult ffDown = calculateFillAndFee(bids, 100.0, false);
                    pmFill100Up = ffUp.avgPrice();
                    pmFee100Up = ffUp.fee();
                    pmFill100Down = ffDown.avgPrice();
                    pmFee100Down = ffDown.fee();
                    captured0 = true;
                }

                if (!captured60 && tMs >= t60Ms) {
                    pmAsk60 = getBestAsk(asks, pmAsk0);
                    pmBid60 = getBestBid(bids, pmBid0);
                    captured60 = true;
                }

                if (!captured180 && tMs >= t180Ms) {
                    pmAsk180 = getBestAsk(asks, pmAsk60);
                    pmBid180 = getBestBid(bids, pmBid60);
                    captured180 = true;
                }

                if (!captured300 && tMs >= t300Ms) {
                    pmAsk300 = getBestAsk(asks, pmAsk180);
                    pmBid300 = getBestBid(bids, pmBid180);
                    captured300 = true;
                    break;
                }

                if ("snapshot".equalsIgnoreCase(et)) {
                    parseBookJson(bidsJson, bids);
                    parseBookJson(asksJson, asks);
                } else if ("delta".equalsIgnoreCase(et)) {
                    if ("BUY".equalsIgnoreCase(side)) {
                        if (size <= 0.0) bids.remove(price);
                        else bids.put(price, size);
                    } else if ("SELL".equalsIgnoreCase(side)) {
                        if (size <= 0.0) asks.remove(price);
                        else asks.put(price, size);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.warnf("DuckDB error reading Polymarket parquet %s: %s", pmFile, e.getMessage());
        }

        if (Double.isNaN(pmMax)) pmMax = pmAsk0;
        if (Double.isNaN(pmMin)) pmMin = pmBid0;

        return new PolymarketExtractedData(
                pmAsk0, pmBid0, pmAsk60, pmBid60,
                pmAsk180, pmBid180, pmAsk300, pmBid300,
                pmFill100Up, pmFee100Up, pmFill100Down, pmFee100Down,
                pmMax, pmMin, pmDepth1cUp, pmDepth1cDown
        );
    }

    /**
     * Extracts Binance Futures Order Book Imbalance (OBI) at a target timestamp from hourly Parquet.
     * Computes imbalance across top 5 bid and ask depth levels.
     *
     * @param obFile Path to Binance Futures orderbook Parquet file
     * @param targetSec target epoch second
     * @return BinanceObiExtractedData containing OBI ratio, mid price, and depth sums
     */
    public BinanceObiExtractedData extractBinanceObi(Path obFile, long targetSec) {
        String normPath = obFile.toAbsolutePath().toString().replace('\\', '/');
        long targetMs = targetSec * 1000L;
        // Read recent 500 events up to targetMs
        String query = "SELECT side, price, quantity FROM read_parquet('" + normPath + "') " +
                "WHERE event_time <= " + targetMs + " ORDER BY event_time DESC LIMIT 2000";

        NavigableMap<Double, Double> bids = new TreeMap<>(Collections.reverseOrder());
        NavigableMap<Double, Double> asks = new TreeMap<>();

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String side = rs.getString(1);
                String pStr = rs.getString(2);
                String qStr = rs.getString(3);
                if (pStr != null && qStr != null) {
                    try {
                        double p = Double.parseDouble(pStr);
                        double q = Double.parseDouble(qStr);
                        if ("bid".equalsIgnoreCase(side)) {
                            if (!bids.containsKey(p) && q > 0.0) bids.put(p, q);
                        } else if ("ask".equalsIgnoreCase(side)) {
                            if (!asks.containsKey(p) && q > 0.0) asks.put(p, q);
                        }
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        } catch (SQLException e) {
            LOG.warnf("DuckDB error reading Binance orderbook %s: %s", obFile, e.getMessage());
            return new BinanceObiExtractedData(Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        }

        if (bids.isEmpty() || asks.isEmpty()) {
            return new BinanceObiExtractedData(Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        }

        double vb = 0.0;
        int countB = 0;
        for (Double q : bids.values()) {
            vb += q;
            if (++countB >= 5) break;
        }

        double va = 0.0;
        int countA = 0;
        for (Double q : asks.values()) {
            va += q;
            if (++countA >= 5) break;
        }

        double obi = (vb + va > 0.0) ? ((vb - va) / (vb + va)) : 0.0;
        double bestBid = bids.firstKey();
        double bestAsk = asks.firstKey();
        double mid = (bestBid + bestAsk) / 2.0;

        return new BinanceObiExtractedData(obi, mid, vb, va);
    }

    /**
     * Parses JSON representation of book price levels into the destination map.
     */
    private void parseBookJson(String json, Map<Double, Double> target) {
        target.clear();
        if (json == null || json.isBlank()) return;
        try {
            JsonNode array = MAPPER.readTree(json);
            if (array.isArray()) {
                for (JsonNode item : array) {
                    double p = item.path("price").asDouble();
                    double s = item.path("size").asDouble();
                    if (s > 0.0) {
                        target.put(p, s);
                    }
                }
            }
        } catch (Exception ignored) {
        }
    }

    /**
     * Returns best ask price from asks map, or fallback if empty.
     */
    private double getBestAsk(Map<Double, Double> asks, double fallback) {
        if (asks == null || asks.isEmpty()) return fallback;
        double min = Double.MAX_VALUE;
        for (Double p : asks.keySet()) {
            if (p < min && p > 0.0) min = p;
        }
        return (min != Double.MAX_VALUE) ? min : fallback;
    }

    /**
     * Returns best bid price from bids map, or fallback if empty.
     */
    private double getBestBid(Map<Double, Double> bids, double fallback) {
        if (bids == null || bids.isEmpty()) return fallback;
        double max = -1.0;
        for (Double p : bids.keySet()) {
            if (p > max) max = p;
        }
        return (max > 0.0) ? max : fallback;
    }

    /**
     * Calculates cumulative liquidity depth within 1 cent of best price.
     */
    private double getDepthWithin1c(Map<Double, Double> book, double bestPrice, boolean isAsk) {
        if (book == null || book.isEmpty()) return 300.0;
        double depth = 0.0;
        for (Map.Entry<Double, Double> e : book.entrySet()) {
            double p = e.getKey();
            double s = e.getValue();
            if (isAsk && p <= bestPrice + 0.01) {
                depth += s;
            } else if (!isAsk && p >= bestPrice - 0.01) {
                depth += s;
            }
        }
        return depth > 0.0 ? depth : 300.0;
    }

    /**
     * Simulates walking the book levels for a trade size, calculating volume-weighted fill price and taker fee.
     */
    private FillFeeResult calculateFillAndFee(Map<Double, Double> levels, double sizeUsd, boolean isUp) {
        if (levels == null || levels.isEmpty()) {
            return new FillFeeResult(0.50, 0.015);
        }

        List<Map.Entry<Double, Double>> sorted = new ArrayList<>(levels.entrySet());
        if (isUp) {
            sorted.sort(Map.Entry.comparingByKey());
        } else {
            // For DOWN, bids on UP sorted descending -> effective price = 1.0 - p
            sorted.sort((a, b) -> Double.compare(b.getKey(), a.getKey()));
        }

        double remaining = sizeUsd;
        double totShares = 0.0;
        double totCost = 0.0;

        for (Map.Entry<Double, Double> e : sorted) {
            double rawP = e.getKey();
            double s = e.getValue();
            double p = isUp ? rawP : Math.round((1.0 - rawP) * 10000.0) / 10000.0;
            if (p <= 0.0 || p >= 1.0) continue;

            double lvlCost = p * s;
            if (remaining <= lvlCost) {
                totShares += remaining / p;
                totCost += remaining;
                remaining = 0.0;
                break;
            } else {
                totShares += s;
                totCost += lvlCost;
                remaining -= lvlCost;
            }
        }

        if (totShares > 0.0 && totCost > 0.0) {
            double avgP = totCost / totShares;
            double fee = totShares * 0.07 * avgP * (1.0 - avgP);
            return new FillFeeResult(avgP, fee);
        }

        return new FillFeeResult(0.50, 0.015);
    }

    /**
     * Reads Binance Spot aggTrades from CSV file (transact_time in microseconds).
     */
    private List<TradeRecordSimple> readSpotTrades(File csvFile) {
        List<TradeRecordSimple> trades = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] cols = line.split(",");
                if (cols.length >= 7) {
                    double p = Double.parseDouble(cols[1]);
                    double q = Double.parseDouble(cols[2]);
                    long tMicro = Long.parseLong(cols[5]);
                    boolean isBuyerMaker = Boolean.parseBoolean(cols[6]);
                    double tSec = tMicro / 1_000_000.0;
                    trades.add(new TradeRecordSimple(tSec, p, q, isBuyerMaker));
                }
            }
        } catch (Exception e) {
            LOG.warnf("Error reading spot CSV %s: %s", csvFile, e.getMessage());
        }
        return trades;
    }

    /**
     * Reads Binance Futures aggTrades from CSV file (transact_time in milliseconds).
     */
    private List<TradeRecordSimple> readFuturesTrades(File csvFile) {
        List<TradeRecordSimple> trades = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line = br.readLine(); // skip header
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] cols = line.split(",");
                if (cols.length >= 7) {
                    double p = Double.parseDouble(cols[1]);
                    double q = Double.parseDouble(cols[2]);
                    long tMilli = Long.parseLong(cols[5]);
                    boolean isBuyerMaker = Boolean.parseBoolean(cols[6]);
                    double tSec = tMilli / 1000.0;
                    trades.add(new TradeRecordSimple(tSec, p, q, isBuyerMaker));
                }
            }
        } catch (Exception e) {
            LOG.warnf("Error reading futures CSV %s: %s", csvFile, e.getMessage());
        }
        return trades;
    }

    /**
     * Aggregates fine-grained trades into 15-minute interval OHLCV and CVD candles.
     */
    private Map<Long, IntervalCandle> aggregate15m(List<TradeRecordSimple> trades) {
        Map<Long, List<TradeRecordSimple>> grouped = new LinkedHashMap<>();
        for (TradeRecordSimple t : trades) {
            long intervalStart = ((long) (t.tSec() / 900.0)) * 900L;
            grouped.computeIfAbsent(intervalStart, k -> new ArrayList<>()).add(t);
        }

        Map<Long, IntervalCandle> candles = new LinkedHashMap<>();
        for (Map.Entry<Long, List<TradeRecordSimple>> e : grouped.entrySet()) {
            long tStart = e.getKey();
            List<TradeRecordSimple> list = e.getValue();
            if (list.isEmpty()) continue;

            double open = list.get(0).price();
            double close = list.get(list.size() - 1).price();
            double high = open;
            double low = open;
            double volBtc = 0.0;
            double volUsd = 0.0;
            double deltaBtc = 0.0;

            for (TradeRecordSimple t : list) {
                if (t.price() > high) high = t.price();
                if (t.price() < low) low = t.price();
                volBtc += t.qty();
                volUsd += t.price() * t.qty();
                deltaBtc += t.isBuyerMaker() ? -t.qty() : t.qty();
            }

            candles.put(tStart, new IntervalCandle(open, high, low, close, volBtc, volUsd, deltaBtc));
        }

        return candles;
    }

    /**
     * Computes volume-weighted average price (TWAP approximation) over a trailing window in seconds.
     */
    private double computeTwap(List<TradeRecordSimple> trades, long tEndSec, double windowSec) {
        double tStartSec = tEndSec - windowSec;
        double volSum = 0.0;
        double usdSum = 0.0;
        for (TradeRecordSimple t : trades) {
            if (t.tSec() >= tStartSec && t.tSec() <= tEndSec) {
                volSum += t.qty();
                usdSum += t.price() * t.qty();
            }
        }
        return (volSum > 0.0) ? (usdSum / volSum) : 0.0;
    }

    /**
     * Minimal trade record holding execution second, price, volume, and aggressor side.
     */
    public record TradeRecordSimple(double tSec, double price, double qty, boolean isBuyerMaker) {}

    /**
     * Summary candle for a 15-minute interval holding OHLC, volume, and cumulative delta.
     */
    public record IntervalCandle(double open, double high, double low, double close, double volBtc, double volUsd, double deltaBtc) {}

    /**
     * Reconstructed quotes and simulated execution results extracted from Polymarket Parquet files.
     */
    public record PolymarketExtractedData(
            double ask0, double bid0,
            double ask60, double bid60,
            double ask180, double bid180,
            double ask300, double bid300,
            double fill100Up, double fee100Up,
            double fill100Down, double fee100Down,
            double maxPrice, double minPrice,
            double depth1cUp, double depth1cDown
    ) {}

    /**
     * Order Book Imbalance metrics extracted from Binance Futures orderbook depth.
     */
    public record BinanceObiExtractedData(double obi, double midPrice, double depthBids, double depthAsks) {}

    /**
     * Execution outcome for simulated depth consumption holding average fill price and fee.
     */
    public record FillFeeResult(double avgPrice, double fee) {}
}
