package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.stream.PolymarketOrderBook;
import cz.polymarket.bot.strategy.TradeDirection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Service discovering and resolving historical Parquet archives captured by the data collector
 * in {@code D:\Crypto\data\Polymarket\collector}.
 * <p>
 * Mirrors the exact subdirectory hierarchy produced by the {@code data} collector module and
 * {@code scripts/convert-to-parquet.ps1}.
 */
@ApplicationScoped
public class CollectorParquetDatasetService {

    private static final Logger LOG = Logger.getLogger(CollectorParquetDatasetService.class);
    private static final long CANDLE_DURATION_SEC = 900L;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC);

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            LOG.warn("DuckDB JDBC driver not found on classpath", e);
        }
    }

    private final Path collectorRootDir;

    /**
     * Constructs the dataset service with the configured collector root directory.
     *
     * @param collectorDir root path of converted collector Parquet files
     */
    @Inject
    public CollectorParquetDatasetService(
            @ConfigProperty(name = "polymarket.backtest.collector-dir", defaultValue = "D:/Crypto/data/Polymarket/collector")
            String collectorDir) {
        if (collectorDir == null || collectorDir.isBlank()) {
            throw new IllegalArgumentException("collectorDir cannot be null or blank");
        }
        this.collectorRootDir = Path.of(collectorDir);
    }

    /**
     * Extracts the uppercase base asset code (e.g. {@code "BTC"}, {@code "ETH"}, {@code "SOL"})
     * from a trading pair or symbol string.
     *
     * @param symbol trading symbol such as {@code "BTCUSDT"}, {@code "btcusd"}, or {@code "BTC"}
     * @return uppercase base asset code
     */
    public static String extractBaseAsset(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "BTC";
        }
        String upper = symbol.trim().toUpperCase(Locale.ROOT);
        if (upper.endsWith("-PERPETUAL")) {
            return upper.substring(0, upper.length() - "-PERPETUAL".length());
        }
        if (upper.endsWith("USDT") && upper.length() > 4) {
            return upper.substring(0, upper.length() - 4);
        }
        if (upper.endsWith("USD") && upper.length() > 3) {
            return upper.substring(0, upper.length() - 3);
        }
        return upper;
    }

    /**
     * Resolves the Polymarket 15-minute market directory for the given symbol
     * (e.g. {@code <collectorDir>/polymarket/btc-updown-15m}).
     *
     * @param symbol trading symbol
     * @return directory path
     */
    public Path getPolymarket15mDir(String symbol) {
        String assetLower = extractBaseAsset(symbol).toLowerCase(Locale.ROOT);
        return collectorRootDir.resolve("polymarket").resolve(assetLower + "-updown-15m");
    }

    /**
     * Resolves the {@code polymarket/price_crypto} directory for the given symbol
     * (e.g. {@code <collectorDir>/polymarket/price_crypto/btcusd}).
     *
     * @param symbol trading symbol
     * @return directory path
     */
    public Path getPolymarketCryptoPriceDir(String symbol) {
        String cryptoSym = extractBaseAsset(symbol).toLowerCase(Locale.ROOT) + "usd";
        return collectorRootDir.resolve("polymarket").resolve("price_crypto").resolve(cryptoSym);
    }

    /**
     * Resolves the daily {@code polymarket/price_crypto} Parquet file for the given symbol and UTC date.
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getPolymarketCryptoPriceFile(String symbol, String dateStr) {
        String cryptoSym = extractBaseAsset(symbol).toLowerCase(Locale.ROOT) + "usd";
        return getPolymarketCryptoPriceDir(symbol).resolve(cryptoSym + "-price_crypto-" + dateStr + ".parquet");
    }

    /**
     * Resolves the {@code polymarket/price_crypto_twap_60s} directory for the given symbol
     * (e.g. {@code <collectorDir>/polymarket/price_crypto_twap_60s/btcusd}).
     *
     * @param symbol trading symbol
     * @return directory path
     */
    public Path getPolymarketCryptoTwap60sDir(String symbol) {
        String cryptoSym = extractBaseAsset(symbol).toLowerCase(Locale.ROOT) + "usd";
        return collectorRootDir.resolve("polymarket").resolve("price_crypto_twap_60s").resolve(cryptoSym);
    }

    /**
     * Resolves the daily {@code polymarket/price_crypto_twap_60s} Parquet file for the given symbol and UTC date.
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getPolymarketCryptoTwap60sFile(String symbol, String dateStr) {
        String cryptoSym = extractBaseAsset(symbol).toLowerCase(Locale.ROOT) + "usd";
        return getPolymarketCryptoTwap60sDir(symbol).resolve(cryptoSym + "-price_crypto_twap_60s-" + dateStr + ".parquet");
    }

    /**
     * Resolves the daily Binance Spot {@code aggTrades} Parquet file for the given symbol and UTC date.
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getBinanceSpotAggTradesFile(String symbol, String dateStr) {
        String pair = extractBaseAsset(symbol) + "USDT";
        return collectorRootDir.resolve("binance").resolve("spot").resolve(pair)
                .resolve("aggTrades").resolve(pair + "-aggTrades-" + dateStr + ".parquet");
    }

    /**
     * Resolves the daily Binance Futures {@code aggTrades} Parquet file for the given symbol and UTC date.
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getBinanceFuturesAggTradesFile(String symbol, String dateStr) {
        String pair = extractBaseAsset(symbol) + "USDT";
        return collectorRootDir.resolve("binance").resolve("futures").resolve(pair)
                .resolve("aggTrades").resolve(pair + "-aggTrades-" + dateStr + ".parquet");
    }

    /**
     * Resolves the daily Binance Futures {@code depthPartial} Parquet file for the given symbol and UTC date.
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getBinanceFuturesDepthPartialFile(String symbol, String dateStr) {
        String pair = extractBaseAsset(symbol) + "USDT";
        return collectorRootDir.resolve("binance").resolve("futures").resolve(pair)
                .resolve("depthPartial").resolve(pair + "-depthPartial-" + dateStr + ".parquet");
    }

    /**
     * Resolves the daily Binance Futures {@code depthDiff} Parquet file for the given symbol and UTC date.
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getBinanceFuturesDepthDiffFile(String symbol, String dateStr) {
        String pair = extractBaseAsset(symbol) + "USDT";
        return collectorRootDir.resolve("binance").resolve("futures").resolve(pair)
                .resolve("depthDiff").resolve(pair + "-depthDiff-" + dateStr + ".parquet");
    }

    /**
     * Resolves the daily Binance Futures {@code forceOrder} Parquet file for the given symbol and UTC date.
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getBinanceFuturesForceOrderFile(String symbol, String dateStr) {
        String pair = extractBaseAsset(symbol) + "USDT";
        return collectorRootDir.resolve("binance").resolve("futures").resolve(pair)
                .resolve("forceOrder").resolve(pair + "-forceOrder-" + dateStr + ".parquet");
    }

    /**
     * Resolves the daily Deribit Futures {@code trades} Parquet file for the given symbol and UTC date.
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getDeribitFuturesTradesFile(String symbol, String dateStr) {
        String instrument = extractBaseAsset(symbol) + "-PERPETUAL";
        return collectorRootDir.resolve("deribit").resolve("futures").resolve(instrument)
                .resolve("trades").resolve(instrument + "-trades-" + dateStr + ".parquet");
    }

    /**
     * Discovers all 15-minute Polymarket market archives for the requested symbol and date range,
     * sorted chronologically by candle start timestamp {@code tStart} ascending.
     *
     * @param symbol trading symbol (e.g. {@code "BTCUSDT"})
     * @param startDate optional start date filter ({@code yyyy-MM-dd}, inclusive)
     * @param endDate optional end date filter ({@code yyyy-MM-dd}, inclusive)
     * @return chronologically ordered list of discovered market metadata descriptors
     */
    public List<CollectorMarketMetadata> discoverMarkets(String symbol, String startDate, String endDate) {
        Path marketDir = getPolymarket15mDir(symbol);
        if (!Files.isDirectory(marketDir)) {
            return List.of();
        }

        String assetLower = extractBaseAsset(symbol).toLowerCase(Locale.ROOT);
        String filePrefix = assetLower + "-updown-15m-";
        String fileSuffix = ".parquet";

        long minStartEpoch = (startDate != null && !startDate.isBlank())
                ? LocalDate.parse(startDate.trim()).atStartOfDay(ZoneOffset.UTC).toEpochSecond()
                : Long.MIN_VALUE;
        long maxStartEpoch = (endDate != null && !endDate.isBlank())
                ? LocalDate.parse(endDate.trim()).plusDays(1).atStartOfDay(ZoneOffset.UTC).toEpochSecond() - 1L
                : Long.MAX_VALUE;

        record CandidateFile(long tStart, Path path) {}
        List<CandidateFile> candidates = new ArrayList<>();

        try (Stream<Path> stream = Files.list(marketDir)) {
            stream.filter(Files::isRegularFile).forEach(path -> {
                String name = path.getFileName().toString();
                if (name.startsWith(filePrefix) && name.endsWith(fileSuffix)) {
                    String epochPart = name.substring(filePrefix.length(), name.length() - fileSuffix.length());
                    try {
                        long tStart = Long.parseLong(epochPart);
                        if (tStart >= minStartEpoch && tStart <= maxStartEpoch) {
                            candidates.add(new CandidateFile(tStart, path));
                        }
                    } catch (NumberFormatException ignored) {
                    }
                }
            });
        } catch (IOException e) {
            LOG.warnf("Failed to list collector market directory %s: %s", marketDir, e.getMessage());
            return List.of();
        }

        if (candidates.isEmpty()) {
            return List.of();
        }

        candidates.sort(Comparator.comparingLong(CandidateFile::tStart));

        List<CollectorMarketMetadata> result = new ArrayList<>(candidates.size());
        Map<String, TimeSeriesIndex> dailyPriceIndexCache = new HashMap<>();

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            for (CandidateFile candidate : candidates) {
                long tStart = candidate.tStart();
                long tEnd = tStart + CANDLE_DURATION_SEC;
                Instant startInstant = Instant.ofEpochSecond(tStart);
                String datetimeUtc = startInstant.toString();
                String dateStr = DATE_FORMATTER.format(startInstant);

                TimeSeriesIndex priceIdx = dailyPriceIndexCache.computeIfAbsent(
                        dateStr, d -> loadDailyReferencePriceIndex(stmt, symbol, d));

                double twapOpen = priceIdx.findNearestPrice(tStart * 1000L);
                double twapClose = priceIdx.findNearestPrice(tEnd * 1000L);

                ParsedMarketHeader header = readMarketHeader(stmt, candidate.path());

                TradeDirection outcome;
                if (header.resolvedOutcome() != null) {
                    outcome = header.resolvedOutcome();
                } else if (!Double.isNaN(twapOpen) && !Double.isNaN(twapClose)) {
                    outcome = (twapClose >= twapOpen) ? TradeDirection.UP : TradeDirection.DOWN;
                } else {
                    outcome = TradeDirection.UP;
                }

                result.add(new CollectorMarketMetadata(
                        tStart,
                        tEnd,
                        datetimeUtc,
                        dateStr,
                        candidate.path(),
                        header.upTokenId(),
                        header.downTokenId(),
                        twapOpen,
                        twapClose,
                        outcome
                ));
            }
        } catch (SQLException e) {
            LOG.warnf("DuckDB error while discovering market metadata: %s", e.getMessage());
        }

        return result;
    }

    private record ParsedMarketHeader(String upTokenId, String downTokenId, TradeDirection resolvedOutcome) {}

    private ParsedMarketHeader readMarketHeader(Statement stmt, Path parquetFile) {
        String upToken = "BTC-UP";
        String downToken = "BTC-DOWN";
        TradeDirection resolvedOutcome = null;

        String normPath = parquetFile.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT event_type, CAST(payload AS VARCHAR) FROM read_parquet('" + normPath + "') " +
                "WHERE event_type IN ('market_discovered', 'market_resolved')";

        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String eventType = rs.getString(1);
                String payload = rs.getString(2);
                if (payload == null) {
                    continue;
                }
                if ("market_discovered".equals(eventType)) {
                    String[] tokens = extractClobTokenIds(payload);
                    if (tokens != null) {
                        upToken = tokens[0];
                        downToken = tokens[1];
                    }
                } else if ("market_resolved".equals(eventType)) {
                    resolvedOutcome = extractResolvedOutcome(payload);
                }
            }
        } catch (SQLException e) {
            LOG.debugf("Could not read metadata rows from %s: %s", parquetFile, e.getMessage());
        }

        return new ParsedMarketHeader(upToken, downToken, resolvedOutcome);
    }

    static String[] extractClobTokenIds(String payload) {
        int keyIdx = payload.indexOf("\"clobTokenIds\"");
        if (keyIdx < 0) {
            return null;
        }
        int bracketStart = payload.indexOf('[', keyIdx + 14);
        if (bracketStart < 0) {
            return null;
        }
        int bracketEnd = payload.indexOf(']', bracketStart + 1);
        if (bracketEnd < 0) {
            return null;
        }

        List<String> ids = extractQuotedStrings(payload, bracketStart + 1, bracketEnd);
        if (ids.size() >= 2) {
            return new String[]{ids.get(0), ids.get(1)};
        }
        return null;
    }

    static TradeDirection extractResolvedOutcome(String payload) {
        int keyIdx = payload.indexOf("\"outcomePrices\"");
        if (keyIdx < 0) {
            return null;
        }
        int bracketStart = payload.indexOf('[', keyIdx + 15);
        if (bracketStart < 0) {
            return null;
        }
        int bracketEnd = payload.indexOf(']', bracketStart + 1);
        if (bracketEnd < 0) {
            return null;
        }
        List<String> prices = extractQuotedStrings(payload, bracketStart + 1, bracketEnd);
        if (prices.size() >= 2) {
            double p0 = PolymarketOrderBook.parseFastDouble(prices.get(0), 0, prices.get(0).length());
            double p1 = PolymarketOrderBook.parseFastDouble(prices.get(1), 0, prices.get(1).length());
            if (!Double.isNaN(p0) && !Double.isNaN(p1)) {
                return p0 >= p1 ? TradeDirection.UP : TradeDirection.DOWN;
            }
        }
        return null;
    }

    private static List<String> extractQuotedStrings(String s, int start, int end) {
        List<String> list = new ArrayList<>(2);
        int i = start;
        while (i < end) {
            int q1 = s.indexOf('"', i);
            if (q1 < 0 || q1 >= end) {
                break;
            }
            while (q1 > start && s.charAt(q1 - 1) == '\\') {
                // Skip escaped quote token if outer string is JSON-encoded string
                break;
            }
            int q2 = q1 + 1;
            while (q2 < end) {
                char c = s.charAt(q2);
                if (c == '"') {
                    break;
                }
                if (c == '\\' && q2 + 1 < end && s.charAt(q2 + 1) == '"') {
                    break;
                }
                q2++;
            }
            if (q2 >= end) {
                break;
            }
            String token = s.substring(q1 + 1, q2).replace("\\", "").trim();
            if (!token.isEmpty() && !token.equals(",")) {
                list.add(token);
            }
            i = q2 + 1;
            if (i < end && s.charAt(i) == '"') {
                i++;
            }
        }
        return list;
    }

    private TimeSeriesIndex loadDailyReferencePriceIndex(Statement stmt, String symbol, String dateStr) {
        Path twapFile = getPolymarketCryptoTwap60sFile(symbol, dateStr);
        if (Files.exists(twapFile)) {
            TimeSeriesIndex idx = queryTimeSeries(stmt, twapFile, "ts", "twap_price");
            if (!idx.isEmpty()) {
                return idx;
            }
        }
        return TimeSeriesIndex.EMPTY;
    }

    private TimeSeriesIndex queryTimeSeries(Statement stmt, Path file, String tsCol, String priceCol) {
        String norm = file.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT " + tsCol + ", " + priceCol + " FROM read_parquet('" + norm + "') " +
                "WHERE " + priceCol + " IS NOT NULL ORDER BY " + tsCol + " ASC";
        long[] timestamps = new long[1024];
        double[] prices = new double[1024];
        int size = 0;

        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                if (size == timestamps.length) {
                    int newCap = timestamps.length << 1;
                    timestamps = Arrays.copyOf(timestamps, newCap);
                    prices = Arrays.copyOf(prices, newCap);
                }
                timestamps[size] = rs.getLong(1);
                prices[size] = rs.getDouble(2);
                size++;
            }
        } catch (SQLException e) {
            LOG.debugf("Could not load reference price series from %s: %s", file, e.getMessage());
            return TimeSeriesIndex.EMPTY;
        }

        if (size == 0) {
            return TimeSeriesIndex.EMPTY;
        }
        return new TimeSeriesIndex(Arrays.copyOf(timestamps, size), Arrays.copyOf(prices, size));
    }

    private record TimeSeriesIndex(long[] timestamps, double[] prices) {
        static final TimeSeriesIndex EMPTY = new TimeSeriesIndex(new long[0], new double[0]);
        private static final long MAX_TWAP_SAMPLE_DRIFT_MS = 10_000L;

        boolean isEmpty() {
            return timestamps.length == 0;
        }

        double findNearestPrice(long targetTsMs) {
            if (timestamps.length == 0) {
                return Double.NaN;
            }
            int idx = Arrays.binarySearch(timestamps, targetTsMs);
            if (idx >= 0) {
                return prices[idx];
            }
            int insertPoint = -idx - 1;
            int bestIdx;
            if (insertPoint == 0) {
                bestIdx = 0;
            } else if (insertPoint >= timestamps.length) {
                bestIdx = timestamps.length - 1;
            } else {
                long diffBefore = targetTsMs - timestamps[insertPoint - 1];
                long diffAfter = timestamps[insertPoint] - targetTsMs;
                bestIdx = (diffBefore <= diffAfter) ? (insertPoint - 1) : insertPoint;
            }
            if (Math.abs(timestamps[bestIdx] - targetTsMs) > MAX_TWAP_SAMPLE_DRIFT_MS) {
                return Double.NaN;
            }
            return prices[bestIdx];
        }
    }
}
