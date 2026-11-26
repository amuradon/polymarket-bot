package cz.polymarket.bot.backtest.indicator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import cz.polymarket.bot.backtest.data.ExchangeDelays;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.indicators.SpotTwapIndicator;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Service managing deterministic Parquet caching of indicator values for backtesting in
 * {@code /data/polymarket/cache/<indicator>/<hash>}.
 * <p>
 * For each UTC day in a backtest run:
 * <ul>
 *   <li>If the daily cache Parquet file exists for an indicator and its configuration hash,
 *       the precomputed values are loaded directly from the cache without reading raw exchange archives.</li>
 *   <li>If one or more indicators lack a daily cache file for that UTC day, the raw exchange Parquet
 *       archives ({@code Binance}, {@code Coinbase}, {@code Kraken}) are streamed <b>strictly once</b>
 *       in a single chronological pass, simultaneously computing and caching all missing indicators.</li>
 * </ul>
 */
@ApplicationScoped
public class IndicatorCacheService {

    /** Number of lowercase hexadecimal characters (64 bits) used from the SHA-256 config digest. */
    private static final int HASH_HEX_LENGTH = 16;

    /** Standard filename for the indicator configuration descriptor inside each {@code <hash>} directory. */
    public static final String CONFIG_JSON_FILENAME = "config.json";

    private static final ObjectMapper CANONICAL_HASH_MAPPER = JsonMapper.builder()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .build();

    private final Path cacheRootDir;
    private final ObjectMapper prettyJsonMapper;
    private final List<BacktestIndicator> indicators;
    private final ExchangeDelays defaultExchangeDelays;

    /**
     * CDI constructor injecting configured cache root directory, Jackson mapper, discovered
     * {@link BacktestIndicator} beans, and simulated exchange stream delays.
     *
     * @param cacheDir root directory for indicator Parquet caches
     * @param objectMapper Jackson object mapper
     * @param indicatorInstances CDI iterable of discovered {@link BacktestIndicator} beans
     * @param binanceDelayMs simulated Binance one-way feed delay in milliseconds
     * @param krakenDelayMs simulated Kraken one-way feed delay in milliseconds
     * @param coinbaseDelayMs simulated Coinbase one-way feed delay in milliseconds
     */
    @Inject
    public IndicatorCacheService(
            @ConfigProperty(name = "polymarket.backtest.cache-dir", defaultValue = "/data/polymarket/cache")
            String cacheDir,
            ObjectMapper objectMapper,
            Instance<BacktestIndicator> indicatorInstances,
            @ConfigProperty(name = "backtest.binance.delay-ms", defaultValue = "100")
            long binanceDelayMs,
            @ConfigProperty(name = "backtest.kraken.delay-ms", defaultValue = "50")
            long krakenDelayMs,
            @ConfigProperty(name = "backtest.coinbase.delay-ms", defaultValue = "50")
            long coinbaseDelayMs) {
        this(
                validateCacheDirString(cacheDir),
                objectMapper,
                collectIndicators(indicatorInstances),
                binanceDelayMs,
                krakenDelayMs,
                coinbaseDelayMs
        );
    }

    /**
     * Programmatic constructor for explicit indicator lists and custom cache directories.
     *
     * @param cacheRootDir root directory path for indicator Parquet caches
     * @param objectMapper Jackson object mapper for writing {@code config.json}
     * @param indicators list of backtest indicators to manage
     * @param binanceDelayMs simulated Binance one-way feed delay in milliseconds
     * @param krakenDelayMs simulated Kraken one-way feed delay in milliseconds
     * @param coinbaseDelayMs simulated Coinbase one-way feed delay in milliseconds
     */
    public IndicatorCacheService(
            Path cacheRootDir,
            ObjectMapper objectMapper,
            List<BacktestIndicator> indicators,
            long binanceDelayMs,
            long krakenDelayMs,
            long coinbaseDelayMs) {
        if (cacheRootDir == null) {
            throw new IllegalArgumentException("cacheRootDir cannot be null");
        }
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper cannot be null");
        }
        if (indicators == null) {
            throw new IllegalArgumentException("indicators cannot be null");
        }
        if (binanceDelayMs < 0L) {
            throw new IllegalArgumentException("binanceDelayMs must be non-negative");
        }
        if (krakenDelayMs < 0L) {
            throw new IllegalArgumentException("krakenDelayMs must be non-negative");
        }
        if (coinbaseDelayMs < 0L) {
            throw new IllegalArgumentException("coinbaseDelayMs must be non-negative");
        }

        this.cacheRootDir = cacheRootDir;
        this.prettyJsonMapper = objectMapper.copy().enable(SerializationFeature.INDENT_OUTPUT);

        List<BacktestIndicator> sorted = new ArrayList<>(indicators);
        sorted.sort(Comparator.comparing(BacktestIndicator::getName));
        this.indicators = Collections.unmodifiableList(sorted);

        this.defaultExchangeDelays = new ExchangeDelays(binanceDelayMs, krakenDelayMs, coinbaseDelayMs);
    }

    private static Path validateCacheDirString(String cacheDir) {
        if (cacheDir == null || cacheDir.isBlank()) {
            throw new IllegalArgumentException("cacheDir cannot be null or blank");
        }
        return Path.of(cacheDir.trim());
    }

    private static List<BacktestIndicator> collectIndicators(Instance<BacktestIndicator> instances) {
        List<BacktestIndicator> list = new ArrayList<>();
        if (instances != null) {
            for (BacktestIndicator indicator : instances) {
                if (indicator != null) {
                    list.add(indicator);
                }
            }
        }
        return list;
    }

    /**
     * Returns the configured root directory for indicator caches.
     *
     * @return cache root {@link Path}
     */
    public Path getCacheRootDir() {
        return cacheRootDir;
    }

    /**
     * Returns the immutable list of registered {@link BacktestIndicator} instances.
     *
     * @return registered indicators
     */
    public List<BacktestIndicator> getIndicators() {
        return indicators;
    }

    /**
     * Returns the configured default exchange feed delays used when a run does not specify overrides.
     *
     * @return default {@link ExchangeDelays}
     */
    public ExchangeDelays getDefaultExchangeDelays() {
        return defaultExchangeDelays;
    }

    /**
     * Computes a deterministic, repeatable hexadecimal hash over the indicator's canonical name,
     * configuration parameters, and the provided (or default) exchange stream delays.
     *
     * @param indicator indicator to hash
     * @param exchangeDelays optional per-run exchange feed delays (defaults to configured delays when null)
     * @return deterministic 16-character lowercase hex SHA-256 hash
     */
    public String computeIndicatorHash(BacktestIndicator indicator, ExchangeDelays exchangeDelays) {
        if (indicator == null) {
            throw new IllegalArgumentException("indicator cannot be null");
        }
        ExchangeDelays resolvedDelays = exchangeDelays != null ? exchangeDelays : defaultExchangeDelays;
        Map<String, Object> sortedParams = new TreeMap<>();
        Map<String, Object> rawParams = indicator.getConfigurationParameters();
        if (rawParams != null) {
            sortedParams.putAll(rawParams);
        }

        Map<String, Object> canonicalPayload = new LinkedHashMap<>();
        canonicalPayload.put("indicator", indicator.getName());
        canonicalPayload.put("exchangeDelaysMs", resolvedDelays.toSortedMap());
        canonicalPayload.put("parameters", sortedParams);

        try {
            String canonicalJson = CANONICAL_HASH_MAPPER.writeValueAsString(canonicalPayload);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(canonicalJson.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hashBytes).substring(0, HASH_HEX_LENGTH);
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to compute deterministic indicator hash for " + indicator.getName(), e);
        }
    }

    /**
     * Builds the serializable {@link IndicatorCacheConfigDescriptor} for the given indicator and delays.
     *
     * @param indicator target indicator
     * @param exchangeDelays optional per-run exchange feed delays (defaults to configured delays when null)
     * @return immutable {@link IndicatorCacheConfigDescriptor}
     */
    public IndicatorCacheConfigDescriptor buildConfigDescriptor(BacktestIndicator indicator, ExchangeDelays exchangeDelays) {
        ExchangeDelays resolvedDelays = exchangeDelays != null ? exchangeDelays : defaultExchangeDelays;
        String hash = computeIndicatorHash(indicator, resolvedDelays);
        Map<String, Object> sortedParams = new TreeMap<>();
        if (indicator.getConfigurationParameters() != null) {
            sortedParams.putAll(indicator.getConfigurationParameters());
        }
        return new IndicatorCacheConfigDescriptor(
                indicator.getName(),
                hash,
                resolvedDelays.toSortedMap(),
                Collections.unmodifiableMap(new LinkedHashMap<>(sortedParams))
        );
    }

    /**
     * Resolves the cache directory {@code <cacheRootDir>/<indicator>/<hash>} for the specified indicator and delays.
     *
     * @param indicator target indicator
     * @param exchangeDelays optional per-run exchange feed delays (defaults to configured delays when null)
     * @return directory path
     */
    public Path resolveIndicatorHashDir(BacktestIndicator indicator, ExchangeDelays exchangeDelays) {
        if (indicator == null) {
            throw new IllegalArgumentException("indicator cannot be null");
        }
        return cacheRootDir.resolve(indicator.getName()).resolve(computeIndicatorHash(indicator, exchangeDelays));
    }

    /**
     * Resolves the {@code config.json} metadata file path inside {@code <cacheRootDir>/<indicator>/<hash>}.
     *
     * @param indicator target indicator
     * @param exchangeDelays optional per-run exchange feed delays (defaults to configured delays when null)
     * @return {@code config.json} file path
     */
    public Path resolveConfigJsonFile(BacktestIndicator indicator, ExchangeDelays exchangeDelays) {
        return resolveIndicatorHashDir(indicator, exchangeDelays).resolve(CONFIG_JSON_FILENAME);
    }

    /**
     * Resolves the daily Parquet cache file path {@code <cacheRootDir>/<indicator>/<hash>/<cryptoSymbol>-<dateStr>.parquet}
     * for the given indicator, asset symbol, UTC date string, and exchange delays.
     *
     * @param indicator target indicator
     * @param symbol trading symbol (e.g. {@code "BTCUSDT"} or {@code "btcusd"})
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @param exchangeDelays optional per-run exchange feed delays (defaults to configured delays when null)
     * @return daily Parquet cache file path
     */
    public Path resolveDailyCacheFile(
            BacktestIndicator indicator,
            String symbol,
            String dateStr,
            ExchangeDelays exchangeDelays) {
        if (dateStr == null || dateStr.isBlank()) {
            throw new IllegalArgumentException("dateStr cannot be null or blank");
        }
        String cryptoSymbol = CollectorParquetDatasetService.extractBaseAsset(symbol).toLowerCase(Locale.ROOT) + "usd";
        return resolveIndicatorHashDir(indicator, exchangeDelays).resolve(cryptoSymbol + "-" + dateStr.trim() + ".parquet");
    }

    /**
     * Loads or computes the daily indicator tracks for registered {@link BacktestIndicator}s consumed
     * by {@code strategy} on a single UTC day using the specified (or default) exchange feed delays.
     * <p>
     * Indicators not consumed by {@code strategy} are skipped completely (neither read from cache nor computed).
     * Consumed indicators with an existing daily Parquet cache file are loaded directly from disk without touching
     * raw exchange archives; any consumed indicators missing a cache file for {@code dateStr} are computed together
     * in a single chronological streaming pass over the raw exchange Parquet files and saved to the cache.
     *
     * @param datasetService collector Parquet dataset service
     * @param symbol trading asset symbol (e.g. {@code "BTCUSDT"})
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @param strategy active {@link TradingStrategy} determining which indicators are required
     * @param exchangeDelays optional per-run exchange feed delays (defaults to configured delays when null)
     * @return ordered list of {@link DailyIndicatorTrack}s for indicators consumed by {@code strategy} on {@code dateStr}
     */
    public List<DailyIndicatorTrack> loadOrComputeDailyIndicators(
            CollectorParquetDatasetService datasetService,
            String symbol,
            String dateStr,
            TradingStrategy strategy,
            ExchangeDelays exchangeDelays) {
        if (strategy == null) {
            throw new IllegalArgumentException("strategy cannot be null");
        }
        if (datasetService == null || dateStr == null || dateStr.isBlank() || indicators.isEmpty()) {
            return List.of();
        }
        ExchangeDelays resolvedDelays = exchangeDelays != null ? exchangeDelays : defaultExchangeDelays;

        List<BacktestIndicator> activeIndicators = new ArrayList<>(indicators.size());
        for (int i = 0, n = indicators.size(); i < n; i++) {
            BacktestIndicator indicator = indicators.get(i);
            if (indicator.isConsumedBy(strategy)) {
                activeIndicators.add(indicator);
            }
        }
        if (activeIndicators.isEmpty()) {
            return List.of();
        }

        String trimmedDate = dateStr.trim();
        String cryptoSymbol = CollectorParquetDatasetService.extractBaseAsset(symbol).toLowerCase(Locale.ROOT) + "usd";
        int indicatorCount = activeIndicators.size();
        DailyIndicatorTrack[] resolvedTracks = new DailyIndicatorTrack[indicatorCount];

        List<IndicatorSlot> missingSlots = new ArrayList<>(indicatorCount);
        List<IndicatorSlot> hitSlots = new ArrayList<>(indicatorCount);

        for (int i = 0; i < indicatorCount; i++) {
            BacktestIndicator indicator = activeIndicators.get(i);
            Path cacheFile = resolveDailyCacheFile(indicator, symbol, trimmedDate, resolvedDelays);
            if (Files.isRegularFile(cacheFile)) {
                hitSlots.add(new IndicatorSlot(i, indicator, cacheFile));
            } else {
                missingSlots.add(new IndicatorSlot(i, indicator, cacheFile));
            }
        }

        DuckDbSessionManager sessionManager = datasetService.getDuckDbSessionManager();

        // 1. Read all cache hits directly from their cached daily Parquet files
        if (!hitSlots.isEmpty()) {
            try (Connection conn = sessionManager.openSession()) {
                for (IndicatorSlot hit : hitSlots) {
                    ensureConfigJsonExists(hit.indicator(), resolvedDelays);
                    DailyIndicatorTrack track = hit.indicator().readCachedTrack(conn, hit.cacheFile(), cryptoSymbol);
                    resolvedTracks[hit.index()] = track;
                    if (Log.isDebugEnabled()) {
                        Log.debugf("Loaded %d cached indicator events for %s on %s from %s",
                                track.size(), hit.indicator().getName(), trimmedDate, hit.cacheFile());
                    }
                }
            } catch (SQLException e) {
                Log.warnf("Failed reading cached indicator Parquet file on %s: %s", trimmedDate, e.getMessage());
            }
        }

        // 2. If any indicators missed the cache, stream raw exchange data strictly ONCE for all missing indicators
        if (!missingSlots.isEmpty()) {
            computeAndCacheMissingIndicators(
                    datasetService,
                    sessionManager,
                    symbol,
                    cryptoSymbol,
                    trimmedDate,
                    resolvedDelays,
                    missingSlots,
                    resolvedTracks
            );
        }

        List<DailyIndicatorTrack> result = new ArrayList<>(indicatorCount);
        for (int i = 0; i < indicatorCount; i++) {
            if (resolvedTracks[i] != null && !resolvedTracks[i].isEmpty()) {
                result.add(resolvedTracks[i]);
            }
        }
        return result;
    }

    private record IndicatorSlot(int index, BacktestIndicator indicator, Path cacheFile) {}

    private void computeAndCacheMissingIndicators(
            CollectorParquetDatasetService datasetService,
            DuckDbSessionManager sessionManager,
            String symbol,
            String cryptoSymbol,
            String dateStr,
            ExchangeDelays resolvedDelays,
            List<IndicatorSlot> missingSlots,
            DailyIndicatorTrack[] resolvedTracks) {

        long binanceDelayMs = resolvedDelays.binanceDelayMs();
        long krakenDelayMs = resolvedDelays.krakenDelayMs();
        long coinbaseDelayMs = resolvedDelays.coinbaseDelayMs();

        String prevDateStr;
        long dayStartMs;
        long dayEndMs;
        try {
            LocalDate parsedDate = LocalDate.parse(dateStr);
            prevDateStr = parsedDate.minusDays(1).toString();
            dayStartMs = parsedDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            dayEndMs = parsedDate.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (Exception e) {
            Log.warnf("Invalid dateStr '%s' for indicator cache computation: %s", dateStr, e.getMessage());
            return;
        }

        Path curBinanceFile = datasetService.getBinanceSpotAggTradesFile(symbol, dateStr);
        Path prevBinanceFile = prevDateStr != null ? datasetService.getBinanceSpotAggTradesFile(symbol, prevDateStr) : null;
        String binanceReadExpr = buildParquetReadExpression(prevBinanceFile, curBinanceFile);

        Path curCoinbaseFile = datasetService.getCoinbaseSpotTradesFile(symbol, dateStr);
        Path prevCoinbaseFile = prevDateStr != null ? datasetService.getCoinbaseSpotTradesFile(symbol, prevDateStr) : null;
        String coinbaseReadExpr = buildParquetReadExpression(prevCoinbaseFile, curCoinbaseFile);

        Path curKrakenSpotFile = datasetService.getKrakenSpotTradesFile(symbol, dateStr);
        Path prevKrakenSpotFile = prevDateStr != null ? datasetService.getKrakenSpotTradesFile(symbol, prevDateStr) : null;
        String krakenSpotReadExpr = buildParquetReadExpression(prevKrakenSpotFile, curKrakenSpotFile);

        if (binanceReadExpr == null && coinbaseReadExpr == null && krakenSpotReadExpr == null) {
            if (Log.isDebugEnabled()) {
                Log.debugf("No raw exchange Parquet files found for symbol=%s on date=%s; skipping indicator cache creation",
                        symbol, dateStr);
            }
            return;
        }

        Path curKrakenUsdtFile = datasetService.getKrakenSpotUsdtUsdTradesFile(dateStr);
        Path prevKrakenUsdtFile = prevDateStr != null ? datasetService.getKrakenSpotUsdtUsdTradesFile(prevDateStr) : null;
        String krakenUsdtReadExpr = buildParquetReadExpression(prevKrakenUsdtFile, curKrakenUsdtFile);

        int missingCount = missingSlots.size();
        int[] targetIndices = new int[missingCount];
        BacktestIndicator[] missingIndicators = new BacktestIndicator[missingCount];
        Path[] targetCacheFiles = new Path[missingCount];
        BacktestIndicator.ComputationSession[] sessions = new BacktestIndicator.ComputationSession[missingCount];
        long[] spotWarmupStartsMs = new long[missingCount];
        long[] usdtWarmupStartsMs = new long[missingCount];

        long minSpotWarmupStartMs = dayStartMs;
        long minUsdtWarmupStartMs = dayStartMs;

        for (int i = 0; i < missingCount; i++) {
            IndicatorSlot slot = missingSlots.get(i);
            BacktestIndicator ind = slot.indicator();

            targetIndices[i] = slot.index();
            missingIndicators[i] = ind;
            targetCacheFiles[i] = slot.cacheFile();
            sessions[i] = ind.createComputationSession(cryptoSymbol);

            long spotWarmupStart = Math.max(0L, dayStartMs - Math.max(0L, ind.getSpotWarmupMs()));
            long usdtWarmupStart = Math.max(0L, dayStartMs - Math.max(0L, ind.getUsdtPegWarmupMs()));
            spotWarmupStartsMs[i] = spotWarmupStart;
            usdtWarmupStartsMs[i] = usdtWarmupStart;
            if (spotWarmupStart < minSpotWarmupStartMs) {
                minSpotWarmupStartMs = spotWarmupStart;
            }
            if (usdtWarmupStart < minUsdtWarmupStartMs) {
                minUsdtWarmupStartMs = usdtWarmupStart;
            }
        }

        if (Log.isInfoEnabled()) {
            Log.infof("Computing and caching %d missing indicator(s) from raw exchange streams for symbol=%s on UTC day %s",
                    missingCount, symbol, dateStr);
        }

        SpotVenueCursor[] venueCursors = {
                new SpotVenueCursor(SpotTwapIndicator.SpotVenue.BINANCE, sessionManager),
                new SpotVenueCursor(SpotTwapIndicator.SpotVenue.COINBASE, sessionManager),
                new SpotVenueCursor(SpotTwapIndicator.SpotVenue.KRAKEN, sessionManager)
        };
        Connection krakenUsdtConn = null;
        Statement krakenUsdtStmt = null;
        ResultSet krakenUsdtRs = null;

        try {
            boolean hasNextKrakenUsdt = false;
            long nextKrakenUsdtTsMs = 0L;
            double nextKrakenUsdtPrice = 1.0;

            if (krakenUsdtReadExpr != null && dayEndMs >= krakenDelayMs) {
                long minKrakenUsdtServerMs = Math.max(0L, minUsdtWarmupStartMs - krakenDelayMs);
                long maxKrakenUsdtServerMs = dayEndMs - krakenDelayMs;
                String krakenUsdtSql = "SELECT (CAST(trade_timestamp AS BIGINT) + " + krakenDelayMs + ") AS eff_ts, price FROM " +
                        krakenUsdtReadExpr +
                        " WHERE trade_timestamp >= " + minKrakenUsdtServerMs +
                        " AND trade_timestamp <= " + maxKrakenUsdtServerMs +
                        " AND price IS NOT NULL" +
                        " ORDER BY eff_ts ASC, trade_id ASC";
                krakenUsdtConn = sessionManager.openSession();
                krakenUsdtStmt = krakenUsdtConn.createStatement();
                krakenUsdtRs = krakenUsdtStmt.executeQuery(krakenUsdtSql);
                if (krakenUsdtRs.next()) {
                    nextKrakenUsdtTsMs = krakenUsdtRs.getLong(1);
                    nextKrakenUsdtPrice = krakenUsdtRs.getDouble(2);
                    hasNextKrakenUsdt = true;
                }
            }

            String binanceSql = buildSpotTradesSql(
                    binanceReadExpr, "COALESCE(event_time, trade_time)", "COALESCE(quantity, 1.0)",
                    "NOT COALESCE(is_buyer_maker, false)",
                    "aggregate_trade_id", minSpotWarmupStartMs, dayEndMs, binanceDelayMs);
            if (binanceSql != null) {
                venueCursors[0].open(binanceSql);
            }

            String coinbaseSql = buildSpotTradesSql(
                    coinbaseReadExpr, "trade_timestamp", "COALESCE(qty, 1.0)",
                    "COALESCE(side = 'sell', true)",
                    "trade_id", minSpotWarmupStartMs, dayEndMs, coinbaseDelayMs);
            if (coinbaseSql != null) {
                venueCursors[1].open(coinbaseSql);
            }

            String krakenSpotSql = buildSpotTradesSql(
                    krakenSpotReadExpr, "trade_timestamp", "COALESCE(qty, 1.0)",
                    "COALESCE(side = 'buy', true)",
                    "trade_id", minSpotWarmupStartMs, dayEndMs, krakenDelayMs);
            if (krakenSpotSql != null) {
                venueCursors[2].open(krakenSpotSql);
            }

            // Single chronological streaming pass feeding all missing indicators simultaneously
            while (true) {
                SpotVenueCursor bestCursor = pickEarliestVenueCursor(venueCursors);
                if (bestCursor == null) {
                    break;
                }
                long ts = bestCursor.nextTsMs;
                double price = bestCursor.nextPrice;
                double qty = bestCursor.nextQty;
                boolean isTakerBuy = bestCursor.nextIsTakerBuy;
                SpotTwapIndicator.SpotVenue venue = bestCursor.venue;
                bestCursor.advance();

                while (hasNextKrakenUsdt && nextKrakenUsdtTsMs <= ts) {
                    for (int i = 0; i < missingCount; i++) {
                        if (nextKrakenUsdtTsMs >= usdtWarmupStartsMs[i]) {
                            sessions[i].onUsdtUsdTrade(nextKrakenUsdtTsMs, nextKrakenUsdtPrice);
                        }
                    }
                    if (krakenUsdtRs.next()) {
                        nextKrakenUsdtTsMs = krakenUsdtRs.getLong(1);
                        nextKrakenUsdtPrice = krakenUsdtRs.getDouble(2);
                    } else {
                        hasNextKrakenUsdt = false;
                    }
                }

                boolean inTargetDay = ts >= dayStartMs && ts <= dayEndMs;
                for (int i = 0; i < missingCount; i++) {
                    if (ts >= spotWarmupStartsMs[i]) {
                        sessions[i].onSpotVenueTrade(venue, ts, price, qty, isTakerBuy, inTargetDay);
                    }
                }
            }
        } catch (SQLException e) {
            Log.warnf("Failed streaming raw exchange trades for indicator cache on %s (%s): %s",
                    symbol, dateStr, e.getMessage());
            return;
        } finally {
            for (SpotVenueCursor cursor : venueCursors) {
                cursor.close();
            }
            if (krakenUsdtRs != null) {
                try {
                    krakenUsdtRs.close();
                } catch (SQLException ignored) {
                }
            }
            if (krakenUsdtStmt != null) {
                try {
                    krakenUsdtStmt.close();
                } catch (SQLException ignored) {
                }
            }
            if (krakenUsdtConn != null) {
                try {
                    krakenUsdtConn.close();
                } catch (SQLException ignored) {
                }
            }
        }

        // Persist config.json and daily Parquet cache file for each computed indicator
        try (Connection writeConn = sessionManager.openSession()) {
            for (int i = 0; i < missingCount; i++) {
                BacktestIndicator indicator = missingIndicators[i];
                Path cacheFile = targetCacheFiles[i];
                try {
                    ensureConfigJsonExists(indicator, resolvedDelays);
                    DailyIndicatorTrack track = sessions[i].writeParquetAndBuildTrack(writeConn, cacheFile);
                    resolvedTracks[targetIndices[i]] = track;
                    if (Log.isDebugEnabled()) {
                        Log.debugf("Cached %d computed events for indicator %s on %s to %s",
                                track.size(), indicator.getName(), dateStr, cacheFile);
                    }
                } catch (IOException | SQLException e) {
                    Log.warnf("Failed persisting indicator cache file %s for %s: %s",
                            cacheFile, indicator.getName(), e.getMessage());
                }
            }
        } catch (SQLException e) {
            Log.warnf("Failed opening DuckDB connection to write indicator cache on %s: %s", dateStr, e.getMessage());
        }
    }

    private void ensureConfigJsonExists(BacktestIndicator indicator, ExchangeDelays resolvedDelays) {
        Path configJsonFile = resolveConfigJsonFile(indicator, resolvedDelays);
        if (Files.isRegularFile(configJsonFile)) {
            return;
        }
        try {
            Path hashDir = configJsonFile.getParent();
            if (hashDir != null) {
                Files.createDirectories(hashDir);
            }
            IndicatorCacheConfigDescriptor descriptor = buildConfigDescriptor(indicator, resolvedDelays);
            Path tempJson = configJsonFile.resolveSibling(CONFIG_JSON_FILENAME + ".tmp." + System.nanoTime());
            try {
                prettyJsonMapper.writeValue(tempJson.toFile(), descriptor);
                Files.move(tempJson, configJsonFile, StandardCopyOption.REPLACE_EXISTING);
            } finally {
                Files.deleteIfExists(tempJson);
            }
        } catch (IOException e) {
            Log.warnf("Failed writing indicator config JSON to %s: %s", configJsonFile, e.getMessage());
        }
    }

    private static SpotVenueCursor pickEarliestVenueCursor(SpotVenueCursor[] cursors) {
        SpotVenueCursor best = null;
        for (int i = 0; i < cursors.length; i++) {
            SpotVenueCursor c = cursors[i];
            if (c.hasNextRow && (best == null || c.nextTsMs < best.nextTsMs)) {
                best = c;
            }
        }
        return best;
    }

    private static String buildSpotTradesSql(
            String readExpr,
            String serverTsExpr,
            String qtyExpr,
            String isTakerBuyExpr,
            String tieBreakerCol,
            long warmupStartMs,
            long dayEndMs,
            long delayMs) {
        if (readExpr == null || dayEndMs < delayMs) {
            return null;
        }
        long minServerMs = Math.max(0L, warmupStartMs - delayMs);
        long maxServerMs = dayEndMs - delayMs;
        return "SELECT (CAST(" + serverTsExpr + " AS BIGINT) + " + delayMs + ") AS eff_ts, " +
                "price, " + qtyExpr + ", " + isTakerBuyExpr + " FROM " + readExpr +
                " WHERE " + serverTsExpr + " >= " + minServerMs +
                " AND " + serverTsExpr + " <= " + maxServerMs +
                " AND price IS NOT NULL" +
                " ORDER BY eff_ts ASC, " + tieBreakerCol + " ASC";
    }

    private static String buildParquetReadExpression(Path prevFile, Path curFile) {
        boolean hasPrev = prevFile != null && Files.exists(prevFile);
        boolean hasCur = curFile != null && Files.exists(curFile);
        if (!hasPrev && !hasCur) {
            return null;
        }
        if (hasPrev && hasCur) {
            String p1 = prevFile.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
            String p2 = curFile.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
            return "read_parquet(['" + p1 + "', '" + p2 + "'])";
        }
        Path single = hasCur ? curFile : prevFile;
        String norm = single.toAbsolutePath().toString().replace('\\', '/').replace("'", "''");
        return "read_parquet('" + norm + "')";
    }

    private static final class SpotVenueCursor implements AutoCloseable {
        final SpotTwapIndicator.SpotVenue venue;
        final DuckDbSessionManager sessionManager;
        Connection connection;
        Statement statement;
        ResultSet resultSet;
        boolean hasNextRow;
        long nextTsMs;
        double nextPrice;
        double nextQty;
        boolean nextIsTakerBuy;

        SpotVenueCursor(SpotTwapIndicator.SpotVenue venue, DuckDbSessionManager sessionManager) {
            this.venue = venue;
            this.sessionManager = sessionManager;
        }

        void open(String sql) throws SQLException {
            connection = sessionManager.openSession();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            advance();
        }

        void advance() throws SQLException {
            if (resultSet != null && resultSet.next()) {
                nextTsMs = resultSet.getLong(1);
                nextPrice = resultSet.getDouble(2);
                nextQty = resultSet.getDouble(3);
                nextIsTakerBuy = resultSet.getBoolean(4);
                hasNextRow = true;
            } else {
                hasNextRow = false;
            }
        }

        @Override
        public void close() {
            hasNextRow = false;
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException ignored) {
                }
                resultSet = null;
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ignored) {
                }
                statement = null;
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
                connection = null;
            }
        }
    }
}
