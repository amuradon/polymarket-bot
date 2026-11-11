package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.stream.BacktestEvent;
import cz.polymarket.bot.backtest.stream.ChronologicalEventStreamer;
import cz.polymarket.bot.backtest.stream.PolymarketOrderBook;
import cz.polymarket.bot.strategy.TradeDirection;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
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

    private static final long CANDLE_DURATION_SEC = 900L;
    private static final long MAX_TWAP_SAMPLE_DRIFT_MS = 10_000L;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC);

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            Log.fatal("Fatal error: DuckDB JDBC driver not found on classpath; backtesting engine cannot run", e);
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
            Log.error("Invalid configuration in CollectorParquetDatasetService: collectorDir cannot be null or blank");
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
     * Resolves the daily Kraken Spot {@code USDTUSD} {@code trades} Parquet file for the given UTC date.
     *
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getKrakenSpotUsdtUsdTradesFile(String dateStr) {
        return collectorRootDir.resolve("kraken").resolve("spot").resolve("USDTUSD")
                .resolve("trades").resolve("USDTUSD-trades-" + dateStr + ".parquet");
    }

    /**
     * Resolves the daily Kraken Spot {@code <ASSET>USD} {@code trades} Parquet file for the given symbol and UTC date
     * (e.g. {@code <collectorDir>/kraken/spot/BTCUSD/trades/BTCUSD-trades-<dateStr>.parquet}).
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getKrakenSpotTradesFile(String symbol, String dateStr) {
        String pair = extractBaseAsset(symbol) + "USD";
        return collectorRootDir.resolve("kraken").resolve("spot").resolve(pair)
                .resolve("trades").resolve(pair + "-trades-" + dateStr + ".parquet");
    }

    /**
     * Resolves the daily Coinbase Spot {@code <ASSET>USD} {@code trades} Parquet file for the given symbol and UTC date
     * (e.g. {@code <collectorDir>/coinbase/spot/BTCUSD/trades/BTCUSD-trades-<dateStr>.parquet}).
     *
     * @param symbol trading symbol
     * @param dateStr UTC date string ({@code yyyy-MM-dd})
     * @return Parquet file path
     */
    public Path getCoinbaseSpotTradesFile(String symbol, String dateStr) {
        String pair = extractBaseAsset(symbol) + "USD";
        return collectorRootDir.resolve("coinbase").resolve("spot").resolve(pair)
                .resolve("trades").resolve(pair + "-trades-" + dateStr + ".parquet");
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
     * <p>
     * Discovery relies strictly on the {@code <asset>-updown-15m-<timestamp>.parquet} filename
     * without reading any Parquet metadata. Market metadata is read lazily via
     * {@link #loadMarketMetadata(CollectorMarketMetadata, BacktestEvent[])} when each market is processed.
     *
     * @param symbol trading symbol (e.g. {@code "BTCUSDT"})
     * @param startDate optional start date filter ({@code yyyy-MM-dd}, inclusive)
     * @param endDate optional end date filter ({@code yyyy-MM-dd}, inclusive)
     * @return chronologically ordered list of discovered market descriptors
     */
    public List<CollectorMarketMetadata> discoverMarkets(String symbol, String startDate, String endDate) {
        Path marketDir = getPolymarket15mDir(symbol);
        if (!Files.isDirectory(marketDir)) {
            Log.warnf("Collector market directory does not exist for symbol %s: %s", symbol, marketDir);
            return List.of();
        }

        if (Log.isInfoEnabled()) {
            Log.infof("Discovering markets for symbol=%s in %s (startDate=%s, endDate=%s)",
                    symbol, marketDir, startDate != null ? startDate : "ALL", endDate != null ? endDate : "ALL");
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
                    } catch (NumberFormatException e) {
                        Log.warnf("Skipping market file with invalid epoch timestamp in filename: %s", path);
                    }
                }
            });
        } catch (IOException e) {
            Log.warnf("Failed to list collector market directory %s: %s", marketDir, e.getMessage());
            return List.of();
        }

        if (candidates.isEmpty()) {
            Log.warnf("No candidate market files found for symbol %s in date range [%s, %s]",
                    symbol, startDate, endDate);
            return List.of();
        }

        candidates.sort(Comparator.comparingLong(CandidateFile::tStart));

        List<CollectorMarketMetadata> result = new ArrayList<>(candidates.size());
        for (CandidateFile candidate : candidates) {
            long tStart = candidate.tStart();
            long tEnd = tStart + CANDLE_DURATION_SEC;
            Instant startInstant = Instant.ofEpochSecond(tStart);
            String datetimeUtc = startInstant.toString();
            String dateStr = DATE_FORMATTER.format(startInstant);

            result.add(CollectorMarketMetadata.discovered(
                    tStart,
                    tEnd,
                    datetimeUtc,
                    dateStr,
                    candidate.path()
            ));
        }

        if (Log.isInfoEnabled()) {
            Log.infof("Discovered %d markets for symbol=%s", result.size(), symbol);
        }
        return result;
    }

    /**
     * Lazily loads and validates the metadata for a single discovered market when the market is processed.
     * <p>
     * Extracts {@code twapOpen} and {@code twapClose} from the pre-loaded daily stream events and queries
     * the market Parquet file via DuckDB for {@code market_discovered} ({@code clobTokenIds}) and
     * {@code market_resolved} ({@code outcomePrices}). If any required data is missing or unreadable,
     * logs a warning and returns {@link Optional#empty()} so the market file is skipped.
     *
     * @param discoveredMarket initial market descriptor returned by {@link #discoverMarkets}
     * @param sharedDailyEvents pre-loaded sorted daily stream events for the market's UTC day
     * @return resolved {@link CollectorMarketMetadata}, or {@link Optional#empty()} if metadata is incomplete
     */
    public Optional<CollectorMarketMetadata> loadMarketMetadata(
            CollectorMarketMetadata discoveredMarket,
            BacktestEvent[] sharedDailyEvents) {

        if (discoveredMarket == null || discoveredMarket.marketParquetFile() == null) {
            Log.warn("Skipping market: discovered market descriptor or file path is null");
            return Optional.empty();
        }

        long tStart = discoveredMarket.tStart();
        long tEnd = discoveredMarket.tEnd();
        String datetimeUtc = discoveredMarket.datetimeUtc();
        String dateStr = discoveredMarket.dateStr();
        Path parquetFile = discoveredMarket.marketParquetFile();

        if (!Files.isRegularFile(parquetFile)) {
            Log.warnf("Skipping market at %d (%s): market Parquet file does not exist: %s",
                    tStart, datetimeUtc, parquetFile);
            return Optional.empty();
        }

        double twapOpen = findNearestTwap60sPrice(sharedDailyEvents, tStart * 1000L);
        if (Double.isNaN(twapOpen) || twapOpen <= 0.0) {
            Log.warnf("Skipping market at %d (%s): target price is missing from PolyBolt 60s TWAP stream for %s",
                    tStart, datetimeUtc, parquetFile);
            return Optional.empty();
        }

        double twapClose = findNearestTwap60sPrice(sharedDailyEvents, tEnd * 1000L);

        ParsedMarketHeader header = readMarketHeader(parquetFile);
        if (header == null
                || header.upTokenId() == null || header.upTokenId().isBlank()
                || header.downTokenId() == null || header.downTokenId().isBlank()) {
            Log.warnf("Skipping market at %d (%s): missing CLOB token IDs (market_discovered) in %s",
                    tStart, datetimeUtc, parquetFile);
            return Optional.empty();
        }

        TradeDirection outcome;
        if (header.resolvedOutcome() != null) {
            outcome = header.resolvedOutcome();
        } else if (!Double.isNaN(twapClose) && twapClose > 0.0) {
            outcome = (twapClose >= twapOpen) ? TradeDirection.UP : TradeDirection.DOWN;
        } else {
            Log.warnf("Skipping market at %d (%s): market resolution and closing 60s TWAP price are both missing for %s",
                    tStart, datetimeUtc, parquetFile);
            return Optional.empty();
        }

        return Optional.of(new CollectorMarketMetadata(
                tStart,
                tEnd,
                datetimeUtc,
                dateStr,
                parquetFile,
                header.upTokenId(),
                header.downTokenId(),
                twapOpen,
                twapClose,
                outcome
        ));
    }

    private record ParsedMarketHeader(String upTokenId, String downTokenId, TradeDirection resolvedOutcome) {}

    private ParsedMarketHeader readMarketHeader(Path parquetFile) {
        String upToken = null;
        String downToken = null;
        TradeDirection resolvedOutcome = null;

        String normPath = parquetFile.toAbsolutePath().toString().replace('\\', '/');
        String sql = "SELECT event_type, CAST(payload AS VARCHAR) FROM read_parquet('" + normPath + "') " +
                "WHERE msg_type IN ('market_discovered', 'market_resolved', 'market_lifecycle') " +
                "AND event_type IN ('market_discovered', 'market_resolved')";

        try (Connection conn = ChronologicalEventStreamer.openStreamingConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
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
            Log.warnf("Could not read metadata rows from %s: %s", parquetFile, e.getMessage());
            return null;
        }

        return new ParsedMarketHeader(upToken, downToken, resolvedOutcome);
    }

    private static double findNearestTwap60sPrice(BacktestEvent[] sharedDailyEvents, long targetTsMs) {
        if (sharedDailyEvents == null || sharedDailyEvents.length == 0) {
            return Double.NaN;
        }

        int insertPoint = findFirstIndexAtOrAfter(sharedDailyEvents, targetTsMs);

        long beforeTs = Long.MIN_VALUE;
        double beforePrice = Double.NaN;
        for (int i = insertPoint - 1; i >= 0; i--) {
            BacktestEvent ev = sharedDailyEvents[i];
            if (targetTsMs - ev.timestampMs() > MAX_TWAP_SAMPLE_DRIFT_MS) {
                break;
            }
            if (ev instanceof BacktestEvent.PolymarketCryptoTwap60sEvent twapEv) {
                beforeTs = twapEv.timestampMs();
                beforePrice = twapEv.twapPrice();
                break;
            }
        }

        long afterTs = Long.MAX_VALUE;
        double afterPrice = Double.NaN;
        for (int i = insertPoint; i < sharedDailyEvents.length; i++) {
            BacktestEvent ev = sharedDailyEvents[i];
            if (ev.timestampMs() - targetTsMs > MAX_TWAP_SAMPLE_DRIFT_MS) {
                break;
            }
            if (ev instanceof BacktestEvent.PolymarketCryptoTwap60sEvent twapEv) {
                afterTs = twapEv.timestampMs();
                afterPrice = twapEv.twapPrice();
                break;
            }
        }

        boolean hasBefore = !Double.isNaN(beforePrice);
        boolean hasAfter = !Double.isNaN(afterPrice);
        if (!hasBefore && !hasAfter) {
            return Double.NaN;
        }
        if (hasBefore && !hasAfter) {
            return beforePrice;
        }
        if (!hasBefore) {
            return afterPrice;
        }
        long diffBefore = targetTsMs - beforeTs;
        long diffAfter = afterTs - targetTsMs;
        return (diffBefore <= diffAfter) ? beforePrice : afterPrice;
    }

    private static int findFirstIndexAtOrAfter(BacktestEvent[] events, long targetTsMs) {
        int low = 0;
        int high = events.length - 1;
        int result = events.length;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (events[mid].timestampMs() >= targetTsMs) {
                result = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return result;
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
}
