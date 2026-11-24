package cz.polymarket.bot.backtest.indicator;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.backtest.stream.BacktestEvent;
import cz.polymarket.bot.indicators.FastSpotMicroIndicatorConfig;
import cz.polymarket.bot.indicators.SpotTwapIndicator;
import cz.polymarket.bot.indicators.SpotTwapIndicatorConfig;
import cz.polymarket.bot.strategy.OraclePulseLockStrategy;
import cz.polymarket.bot.strategy.OraclePulseLockStrategyConfig;
import cz.polymarket.bot.strategy.TradingStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategyConfig;
import cz.polymarket.bot.strategy.UtterParameciumStrategyV0;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit and integration tests for {@link IndicatorCacheService}, {@link SpotTwapBacktestIndicator},
 * and {@link DailyIndicatorTrack}.
 */
class IndicatorCacheServiceTest {

    private static final long BASE_START_SEC = 1785542400L; // 2026-08-01 00:00:00 UTC
    private static final long BASE_START_MS = BASE_START_SEC * 1000L;
    private static final String DATE_STR = "2026-08-01";

    @Test
    @DisplayName("Should generate identical hash for identical indicator config and exchange delays, and distinct hash when any parameter or delay changes")
    void shouldGenerateDeterministicAndUniqueHash(@TempDir Path tempDir) {
        ObjectMapper mapper = new ObjectMapper();
        SpotTwapBacktestIndicator defaultInd1 = new SpotTwapBacktestIndicator(SpotTwapIndicatorConfig.defaults());
        SpotTwapBacktestIndicator defaultInd2 = new SpotTwapBacktestIndicator(SpotTwapIndicatorConfig.defaults());

        SpotTwapIndicatorConfig customWindowCfg = new SpotTwapIndicatorConfig(
                0.45, 0.35, 0.20,
                5000.0, 250.0, 10000.0, 5000.0,
                300000L, 1e-4, 0.70, 0.30, 0.0010,
                30_000L, // changed twapWindowMs from 60000 to 30000
                65000L, 1800000L
        );
        SpotTwapBacktestIndicator customInd = new SpotTwapBacktestIndicator(customWindowCfg);

        IndicatorCacheService service1 = new IndicatorCacheService(
                tempDir, mapper, List.of(defaultInd1), 100L, 50L, 50L);
        IndicatorCacheService service2 = new IndicatorCacheService(
                tempDir, mapper, List.of(defaultInd2), 100L, 50L, 50L);
        IndicatorCacheService diffBinanceDelayService = new IndicatorCacheService(
                tempDir, mapper, List.of(defaultInd1), 120L, 50L, 50L);
        IndicatorCacheService diffKrakenDelayService = new IndicatorCacheService(
                tempDir, mapper, List.of(defaultInd1), 100L, 65L, 50L);
        IndicatorCacheService diffCoinbaseDelayService = new IndicatorCacheService(
                tempDir, mapper, List.of(defaultInd1), 100L, 50L, 75L);

        String hash1 = service1.computeIndicatorHash(defaultInd1);
        String hash2 = service2.computeIndicatorHash(defaultInd2);
        String customParamHash = service1.computeIndicatorHash(customInd);
        String diffBinanceHash = diffBinanceDelayService.computeIndicatorHash(defaultInd1);
        String diffKrakenHash = diffKrakenDelayService.computeIndicatorHash(defaultInd1);
        String diffCoinbaseHash = diffCoinbaseDelayService.computeIndicatorHash(defaultInd1);

        assertThat(hash1)
                .hasSize(16)
                .matches("[0-9a-f]{16}")
                .isEqualTo(hash2);
        assertThat(customParamHash).isNotEqualTo(hash1);
        assertThat(diffBinanceHash).isNotEqualTo(hash1);
        assertThat(diffKrakenHash).isNotEqualTo(hash1);
        assertThat(diffCoinbaseHash).isNotEqualTo(hash1);
    }

    @Test
    @DisplayName("Should compute indicator values on cache miss, write daily Parquet and config.json, and reuse cache without reading raw exchange files on cache hit")
    void shouldWriteParquetAndConfigOnCacheMissAndReuseCacheWithoutRawFilesOnHit(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB")) {
            Path collectorDir = tempDir.resolve("collector");
            Path cacheDir = tempDir.resolve("cache");
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), duckDb);

            Path binanceSpotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", DATE_STR);
            Path coinbaseSpotFile = datasetService.getCoinbaseSpotTradesFile("BTCUSDT", DATE_STR);
            Path krakenSpotFile = datasetService.getKrakenSpotTradesFile("BTCUSDT", DATE_STR);
            Path krakenUsdtFile = datasetService.getKrakenSpotUsdtUsdTradesFile(DATE_STR);

            writeSyntheticRawExchangeFiles(duckDb, binanceSpotFile, coinbaseSpotFile, krakenSpotFile, krakenUsdtFile);

            ObjectMapper mapper = new ObjectMapper();
            SpotTwapBacktestIndicator indicator = new SpotTwapBacktestIndicator();
            IndicatorCacheService cacheService = new IndicatorCacheService(
                    cacheDir, mapper, List.of(indicator), 100L, 50L, 50L);

            String expectedHash = cacheService.computeIndicatorHash(indicator);
            Path expectedHashDir = cacheDir.resolve("SpotTwapIndicator").resolve(expectedHash);
            Path expectedConfigJson = expectedHashDir.resolve("config.json");
            Path expectedParquetFile = expectedHashDir.resolve("btcusd-" + DATE_STR + ".parquet");

            assertThat(Files.exists(expectedParquetFile)).isFalse();

            TradingStrategy spotTwapStrategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());

            // 1. First run (Cache Miss): streams raw exchange data and creates config.json + daily Parquet file
            List<DailyIndicatorTrack> firstTracks = cacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", DATE_STR, spotTwapStrategy);

            assertThat(Files.isRegularFile(expectedConfigJson)).isTrue();
            assertThat(Files.isRegularFile(expectedParquetFile)).isTrue();

            IndicatorCacheConfigDescriptor descriptor = mapper.readValue(
                    expectedConfigJson.toFile(), IndicatorCacheConfigDescriptor.class);
            assertThat(descriptor.indicator()).isEqualTo("SpotTwapIndicator");
            assertThat(descriptor.hash()).isEqualTo(expectedHash);
            assertThat(descriptor.exchangeDelaysMs())
                    .containsEntry("binanceDelayMs", 100L)
                    .containsEntry("krakenDelayMs", 50L)
                    .containsEntry("coinbaseDelayMs", 50L);
            assertThat(descriptor.parameters())
                    .containsEntry("binanceBaseWeight", 0.45)
                    .containsKey("twapWindowMs");

            assertThat(firstTracks).hasSize(1);
            DailyIndicatorTrack track1 = firstTracks.getFirst();
            assertThat(track1.size()).isEqualTo(3);

            // 2. Delete ALL raw exchange Parquet files to prove second run does not read them
            Files.deleteIfExists(binanceSpotFile);
            Files.deleteIfExists(coinbaseSpotFile);
            Files.deleteIfExists(krakenSpotFile);
            Files.deleteIfExists(krakenUsdtFile);

            // 3. Second run (Cache Hit): loads directly from cached Parquet file
            List<DailyIndicatorTrack> secondTracks = cacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", DATE_STR, spotTwapStrategy);

            assertThat(secondTracks).hasSize(1);
            DailyIndicatorTrack track2 = secondTracks.getFirst();
            assertThat(track2.size()).isEqualTo(track1.size());

            for (int i = 0; i < track1.size(); i++) {
                BacktestEvent.CalculatedSpotTwapEvent ev1 = (BacktestEvent.CalculatedSpotTwapEvent) track1.eventAt(i);
                BacktestEvent.CalculatedSpotTwapEvent ev2 = (BacktestEvent.CalculatedSpotTwapEvent) track2.eventAt(i);
                assertThat(ev2.timestampMs()).isEqualTo(ev1.timestampMs());
                assertThat(ev2.sequenceNumber()).isEqualTo(ev1.sequenceNumber());
                assertThat(ev2.symbol()).isEqualTo(ev1.symbol());
                assertThat(ev2.spotPrice()).isCloseTo(ev1.spotPrice(), within(1e-9));
                assertThat(ev2.twap60sPrice()).isCloseTo(ev1.twap60sPrice(), within(1e-9));
            }
        }
    }

    @Test
    @DisplayName("Should stream raw exchange files once to compute multiple missing indicators simultaneously, and compute only missing ones on partial cache hit")
    void shouldComputeMultipleMissingIndicatorsInSinglePassAndSupportPartialCacheHit(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB")) {
            Path collectorDir = tempDir.resolve("collector");
            Path cacheDir = tempDir.resolve("cache");
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), duckDb);

            Path binanceSpotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", DATE_STR);
            Path coinbaseSpotFile = datasetService.getCoinbaseSpotTradesFile("BTCUSDT", DATE_STR);
            Path krakenSpotFile = datasetService.getKrakenSpotTradesFile("BTCUSDT", DATE_STR);
            Path krakenUsdtFile = datasetService.getKrakenSpotUsdtUsdTradesFile(DATE_STR);

            writeSyntheticRawExchangeFiles(duckDb, binanceSpotFile, coinbaseSpotFile, krakenSpotFile, krakenUsdtFile);

            AtomicInteger ind1ComputeSessions = new AtomicInteger(0);
            AtomicInteger ind2ComputeSessions = new AtomicInteger(0);

            BacktestIndicator indicator1 = new CountingBacktestIndicator(
                    "SpotTwapFast30s",
                    new SpotTwapIndicatorConfig(
                            0.45, 0.35, 0.20, 5000.0, 250.0, 10000.0, 5000.0,
                            300000L, 1e-4, 0.70, 0.30, 0.0010, 30_000L, 65000L, 1800000L),
                    ind1ComputeSessions
            );
            BacktestIndicator indicator2 = new CountingBacktestIndicator(
                    "SpotTwapStandard60s",
                    SpotTwapIndicatorConfig.defaults(),
                    ind2ComputeSessions
            );

            ObjectMapper mapper = new ObjectMapper();
            IndicatorCacheService cacheService = new IndicatorCacheService(
                    cacheDir, mapper, List.of(indicator1, indicator2), 100L, 50L, 50L);

            Path ind1Parquet = cacheService.resolveDailyCacheFile(indicator1, "BTCUSDT", DATE_STR);
            Path ind2Parquet = cacheService.resolveDailyCacheFile(indicator2, "BTCUSDT", DATE_STR);

            TradingStrategy spotTwapStrategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());

            // 1. Both indicators missing -> single pass computes both
            List<DailyIndicatorTrack> initialTracks = cacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", DATE_STR, spotTwapStrategy);

            assertThat(initialTracks).hasSize(2);
            assertThat(ind1ComputeSessions.get()).isEqualTo(1);
            assertThat(ind2ComputeSessions.get()).isEqualTo(1);
            assertThat(Files.isRegularFile(ind1Parquet)).isTrue();
            assertThat(Files.isRegularFile(ind2Parquet)).isTrue();
            assertThat(Files.isRegularFile(cacheService.resolveConfigJsonFile(indicator1))).isTrue();
            assertThat(Files.isRegularFile(cacheService.resolveConfigJsonFile(indicator2))).isTrue();

            // 2. Delete only indicator2's cache file -> indicator1 must be loaded from cache (0 new sessions), indicator2 recomputed
            Files.delete(ind2Parquet);
            List<DailyIndicatorTrack> partialHitTracks = cacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", DATE_STR, spotTwapStrategy);

            assertThat(partialHitTracks).hasSize(2);
            assertThat(ind1ComputeSessions.get()).isEqualTo(1); // NOT recomputed!
            assertThat(ind2ComputeSessions.get()).isEqualTo(2); // Recomputed!
            assertThat(Files.isRegularFile(ind2Parquet)).isTrue();

            // 3. Both indicators now cached -> delete raw exchange files and verify both load from cache with 0 new sessions
            Files.deleteIfExists(binanceSpotFile);
            Files.deleteIfExists(coinbaseSpotFile);
            Files.deleteIfExists(krakenSpotFile);
            Files.deleteIfExists(krakenUsdtFile);

            List<DailyIndicatorTrack> fullHitTracks = cacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", DATE_STR, spotTwapStrategy);

            assertThat(fullHitTracks).hasSize(2);
            assertThat(ind1ComputeSessions.get()).isEqualTo(1);
            assertThat(ind2ComputeSessions.get()).isEqualTo(2);
        }
    }

    @Test
    @DisplayName("Should cache FastSpotMicroBacktestIndicator with verified Coinbase maker-side polarity and round-trip from Parquet")
    void shouldCacheAndRoundTripFastSpotMicroWithExactVenuePolarity(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB")) {
            Path collectorDir = tempDir.resolve("collector");
            Path cacheDir = tempDir.resolve("cache");
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), duckDb);

            Path binanceSpotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", DATE_STR);
            Path coinbaseSpotFile = datasetService.getCoinbaseSpotTradesFile("BTCUSDT", DATE_STR);
            Path krakenSpotFile = datasetService.getKrakenSpotTradesFile("BTCUSDT", DATE_STR);
            Path krakenUsdtFile = datasetService.getKrakenSpotUsdtUsdTradesFile(DATE_STR);
            Files.createDirectories(binanceSpotFile.getParent());
            Files.createDirectories(coinbaseSpotFile.getParent());
            Files.createDirectories(krakenSpotFile.getParent());
            Files.createDirectories(krakenUsdtFile.getParent());

            // All 3 trades are Taker BUY: Binance is_buyer_maker=false, Coinbase maker side='sell', Kraken side='buy'
            try (Connection conn = duckDb.openSession();
                 Statement stmt = conn.createStatement()) {
                String normUsdt = krakenUsdtFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (BASE_START_MS + 50L) + "::UBIGINT, 'USDT/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'limit'::VARCHAR, 1.0::DOUBLE, 500.0::DOUBLE, '2026-08-01T00:00:00.050Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normUsdt + "' (FORMAT PARQUET)");

                String normBinance = binanceSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (BASE_START_MS + 1000L) + "::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 70010.0::DOUBLE, 2.0::DOUBLE, 1::BIGINT, 1::BIGINT, " + (BASE_START_MS + 1000L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                        ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normBinance + "' (FORMAT PARQUET)");

                String normCoinbase = coinbaseSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (BASE_START_MS + 1200L) + "::UBIGINT, 'BTC-USD'::VARCHAR, 1::BIGINT, 'sell'::VARCHAR, 70020.0::DOUBLE, 3.0::DOUBLE, '2026-08-01T00:00:01.200Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, price, qty, timestamp)) TO '" + normCoinbase + "' (FORMAT PARQUET)");

                String normKraken = krakenSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (BASE_START_MS + 1300L) + "::UBIGINT, 'BTC/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'market'::VARCHAR, 70030.0::DOUBLE, 1.0::DOUBLE, '2026-08-01T00:00:01.300Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normKraken + "' (FORMAT PARQUET)");
            }

            FastSpotMicroBacktestIndicator fastInd = new FastSpotMicroBacktestIndicator(FastSpotMicroIndicatorConfig.defaults());
            IndicatorCacheService cacheService = new IndicatorCacheService(
                    cacheDir, new ObjectMapper(), List.of(fastInd), 100L, 50L, 50L);

            TradingStrategy fastSpotStrategy = new OraclePulseLockStrategy(OraclePulseLockStrategyConfig.defaults());
            List<DailyIndicatorTrack> missTracks = cacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", DATE_STR, fastSpotStrategy);
            assertThat(missTracks).hasSize(1);
            DailyIndicatorTrack t1 = missTracks.getFirst();
            assertThat(t1.size()).isEqualTo(3);

            BacktestEvent.CalculatedFastSpotMicroEvent lastEv =
                    (BacktestEvent.CalculatedFastSpotMicroEvent) t1.eventAt(t1.size() - 1);
            // Since all 3 venues had large Taker BUY polarity (>350k USD notional), signedOfi1s must exceed +0.97!
            assertThat(lastEv.signedOfi1s()).isBetween(0.97, 1.0);
            assertThat(lastEv.fastDirectUsd()).isBetween(70_020.0, 70_030.0);

            // Delete raw files and verify exact Parquet cache round-trip
            Files.deleteIfExists(binanceSpotFile);
            Files.deleteIfExists(coinbaseSpotFile);
            Files.deleteIfExists(krakenSpotFile);
            Files.deleteIfExists(krakenUsdtFile);

            List<DailyIndicatorTrack> hitTracks = cacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", DATE_STR, fastSpotStrategy);
            DailyIndicatorTrack t2 = hitTracks.getFirst();
            assertThat(t2.size()).isEqualTo(3);
            BacktestEvent.CalculatedFastSpotMicroEvent cachedLast =
                    (BacktestEvent.CalculatedFastSpotMicroEvent) t2.eventAt(2);
            assertThat(cachedLast.fastDirectUsd()).isCloseTo(lastEv.fastDirectUsd(), within(1e-9));
            assertThat(cachedLast.signedOfi1s()).isCloseTo(lastEv.signedOfi1s(), within(1e-9));
        }
    }

    @Test
    @DisplayName("Should compute and cache only indicators consumed by the active strategy")
    void shouldComputeAndCacheOnlyIndicatorsConsumedByActiveStrategy(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB")) {
            Path collectorDir = tempDir.resolve("collector");
            Path cacheDir = tempDir.resolve("cache");
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), duckDb);

            Path binanceSpotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", DATE_STR);
            Path coinbaseSpotFile = datasetService.getCoinbaseSpotTradesFile("BTCUSDT", DATE_STR);
            Path krakenSpotFile = datasetService.getKrakenSpotTradesFile("BTCUSDT", DATE_STR);
            Path krakenUsdtFile = datasetService.getKrakenSpotUsdtUsdTradesFile(DATE_STR);
            writeSyntheticRawExchangeFiles(duckDb, binanceSpotFile, coinbaseSpotFile, krakenSpotFile, krakenUsdtFile);

            SpotTwapBacktestIndicator spotTwapIndicator = new SpotTwapBacktestIndicator();
            FastSpotMicroBacktestIndicator fastSpotIndicator = new FastSpotMicroBacktestIndicator(FastSpotMicroIndicatorConfig.defaults());
            IndicatorCacheService cacheService = new IndicatorCacheService(
                    cacheDir, new ObjectMapper(), List.of(spotTwapIndicator, fastSpotIndicator), 100L, 50L, 50L);

            Path spotTwapCacheFile = cacheService.resolveDailyCacheFile(spotTwapIndicator, "BTCUSDT", DATE_STR);
            Path fastSpotCacheFile = cacheService.resolveDailyCacheFile(fastSpotIndicator, "BTCUSDT", DATE_STR);

            // 1. Strategy consuming neither indicator -> returns empty and creates no cache files
            TradingStrategy noIndicatorStrategy = new UtterParameciumStrategyV0(UtterParameciumStrategyV0.Config.defaultConfig());
            assertThat(cacheService.loadOrComputeDailyIndicators(datasetService, "BTCUSDT", DATE_STR, noIndicatorStrategy)).isEmpty();
            assertThat(Files.exists(spotTwapCacheFile)).isFalse();
            assertThat(Files.exists(fastSpotCacheFile)).isFalse();

            // 2. OraclePulseLockStrategy -> computes & caches ONLY FastSpotMicroIndicator
            TradingStrategy oracleStrategy = new OraclePulseLockStrategy(OraclePulseLockStrategyConfig.defaults());
            List<DailyIndicatorTrack> oracleTracks = cacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", DATE_STR, oracleStrategy);
            assertThat(oracleTracks).hasSize(1);
            assertThat(oracleTracks.getFirst().indicatorName()).isEqualTo(fastSpotIndicator.getName());
            assertThat(Files.isRegularFile(fastSpotCacheFile)).isTrue();
            assertThat(Files.exists(spotTwapCacheFile)).isFalse();

            // 3. UtterParameciumStrategy -> computes & caches ONLY SpotTwapIndicator
            TradingStrategy utterStrategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
            List<DailyIndicatorTrack> utterTracks = cacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", DATE_STR, utterStrategy);
            assertThat(utterTracks).hasSize(1);
            assertThat(utterTracks.getFirst().indicatorName()).isEqualTo(spotTwapIndicator.getName());
            assertThat(Files.isRegularFile(spotTwapCacheFile)).isTrue();
        }
    }

    private static void writeSyntheticRawExchangeFiles(
            DuckDbSessionManager duckDb,
            Path binanceSpotFile,
            Path coinbaseSpotFile,
            Path krakenSpotFile,
            Path krakenUsdtFile) throws Exception {
        Files.createDirectories(binanceSpotFile.getParent());
        Files.createDirectories(coinbaseSpotFile.getParent());
        Files.createDirectories(krakenSpotFile.getParent());
        Files.createDirectories(krakenUsdtFile.getParent());

        try (Connection conn = duckDb.openSession();
             Statement stmt = conn.createStatement()) {
            String normUsdt = krakenUsdtFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (BASE_START_MS + 100L) + "::UBIGINT, 'USDT/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'limit'::VARCHAR, 1.0000::DOUBLE, 1000.0::DOUBLE, '2026-08-01T00:00:00.100000Z'::VARCHAR)" +
                    ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normUsdt + "' (FORMAT PARQUET)");

            String normBinance = binanceSpotFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 70000.0::DOUBLE, 1.0::DOUBLE, 1::BIGINT, 1::BIGINT, " + (BASE_START_MS + 1000L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                    ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normBinance + "' (FORMAT PARQUET)");

            String normCoinbase = coinbaseSpotFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (BASE_START_MS + 2000L) + "::UBIGINT, 'BTC-USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 70020.0::DOUBLE, 1.0::DOUBLE, '2026-08-01T00:00:02.000000Z'::VARCHAR)" +
                    ") AS t(trade_timestamp, symbol, trade_id, side, price, qty, timestamp)) TO '" + normCoinbase + "' (FORMAT PARQUET)");

            String normKraken = krakenSpotFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (BASE_START_MS + 3000L) + "::UBIGINT, 'BTC/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'market'::VARCHAR, 70040.0::DOUBLE, 1.0::DOUBLE, '2026-08-01T00:00:03.000000Z'::VARCHAR)" +
                    ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normKraken + "' (FORMAT PARQUET)");
        }
    }

    private static final class CountingBacktestIndicator implements BacktestIndicator {
        private final String name;
        private final SpotTwapBacktestIndicator delegate;
        private final AtomicInteger sessionCounter;

        CountingBacktestIndicator(String name, SpotTwapIndicatorConfig config, AtomicInteger sessionCounter) {
            this.name = name;
            this.delegate = new SpotTwapBacktestIndicator(config);
            this.sessionCounter = sessionCounter;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Map<String, Object> getConfigurationParameters() {
            return delegate.getConfigurationParameters();
        }

        @Override
        public boolean isConsumedBy(TradingStrategy strategy) {
            return delegate.isConsumedBy(strategy);
        }

        @Override
        public long getSpotWarmupMs() {
            return delegate.getSpotWarmupMs();
        }

        @Override
        public long getUsdtPegWarmupMs() {
            return delegate.getUsdtPegWarmupMs();
        }

        @Override
        public ComputationSession createComputationSession(String cryptoSymbol) {
            sessionCounter.incrementAndGet();
            return delegate.createComputationSession(cryptoSymbol);
        }

        @Override
        public DailyIndicatorTrack readCachedTrack(Connection connection, Path parquetFile, String cryptoSymbol)
                throws java.sql.SQLException {
            return delegate.readCachedTrack(connection, parquetFile, cryptoSymbol);
        }
    }
}
