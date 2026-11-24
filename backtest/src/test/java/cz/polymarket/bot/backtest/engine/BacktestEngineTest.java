package cz.polymarket.bot.backtest.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper.ClobRow;
import cz.polymarket.bot.backtest.export.BacktestJsonExporter;
import cz.polymarket.bot.backtest.indicator.IndicatorCacheService;
import cz.polymarket.bot.backtest.indicator.SpotTwapBacktestIndicator;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import cz.polymarket.bot.strategy.OraclePulseLockStrategy;
import cz.polymarket.bot.strategy.OraclePulseLockStrategyConfig;
import cz.polymarket.bot.strategy.UtterParameciumStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategyConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BacktestEngineTest {

    private DuckDbSessionManager duckDbSessionManager;
    private BacktestEngine backtestEngine;
    private final AtomicInteger polymarketCryptoPriceCount = new AtomicInteger();
    private final AtomicInteger polymarketTwap60sCount = new AtomicInteger();

    private static final long BASE_START = 1785542400L; // 2026-08-01 00:00:00 UTC

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        duckDbSessionManager = new DuckDbSessionManager("256MB");
        Path collectorDir = tempDir.resolve("collector");
        CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), duckDbSessionManager);

        long c1Start = BASE_START;
        long c2Start = BASE_START + 900L;
        long c3Start = BASE_START + 1800L;

        Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
        Path cryptoPriceFile = datasetService.getPolymarketCryptoPriceFile("BTCUSDT", "2026-08-01");
        Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");
        Path spotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", "2026-08-01");

        Files.createDirectories(pmDir);
        Files.createDirectories(cryptoPriceFile.getParent());
        Files.createDirectories(twap60sFile.getParent());
        Files.createDirectories(spotFile.getParent());

        Path pmFile1 = pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet");
        Path pmFile2 = pmDir.resolve("btc-updown-15m-" + c2Start + ".parquet");
        Path pmFile3 = pmDir.resolve("btc-updown-15m-" + c3Start + ".parquet");

        try (Connection conn = duckDbSessionManager.openSession();
             Statement stmt = conn.createStatement()) {

            // 1. Polymarket price_crypto_twap_60s
            String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                    "(" + (c2Start * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c2Start * 1000L) + "::UBIGINT), " +
                    "(" + (c3Start * 1000L) + "::UBIGINT, 3::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60150.0::DOUBLE, '60150.0'::VARCHAR, 60::INT, " + (c3Start * 1000L) + "::UBIGINT), " +
                    "(" + ((c3Start + 900L) * 1000L) + "::UBIGINT, 4::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 59850.0::DOUBLE, '59850.0'::VARCHAR, 60::INT, " + ((c3Start + 900L) * 1000L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

            // 2. Polymarket price_crypto
            String normCrypto = cryptoPriceFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + ((c1Start + 60L) * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, " + ((c1Start + 60L) * 1000L - 5L) + "::UBIGINT), " +
                    "(" + ((c2Start + 60L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60150.0::DOUBLE, '60150.0'::VARCHAR, " + ((c2Start + 60L) * 1000L - 5L) + "::UBIGINT), " +
                    "(" + ((c3Start + 60L) * 1000L) + "::UBIGINT, 3::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 59850.0::DOUBLE, '59850.0'::VARCHAR, " + ((c3Start + 60L) * 1000L - 5L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, price, full_accuracy_price, price_timestamp)) TO '" + normCrypto + "' (FORMAT PARQUET)");

            // 3. Binance Spot aggTrades (new schema without ts: event_time + trade_time)
            String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 5.0::DOUBLE, 1::BIGINT, 1::BIGINT, " + (c1Start * 1000L + 100L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 2::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 5.0::DOUBLE, 2::BIGINT, 2::BIGINT, " + ((c1Start + 60L) * 1000L) + "::UBIGINT, true::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 3::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 5.0::DOUBLE, 3::BIGINT, 3::BIGINT, " + (c2Start * 1000L + 100L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 4::BIGINT, 'BTCUSDT'::VARCHAR, 60150.0::DOUBLE, 15.0::DOUBLE, 4::BIGINT, 4::BIGINT, " + ((c2Start + 60L) * 1000L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 5::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 5.0::DOUBLE, 5::BIGINT, 5::BIGINT, " + (c3Start * 1000L + 100L) + "::UBIGINT, true::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 6::BIGINT, 'BTCUSDT'::VARCHAR, 59850.0::DOUBLE, 20.0::DOUBLE, 6::BIGINT, 6::BIGINT, " + ((c3Start + 60L) * 1000L) + "::UBIGINT, true::BOOLEAN, true::BOOLEAN)" +
                    ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normSpot + "' (FORMAT PARQUET)");

            // 4. Polymarket 15m typed CLOB market files
            TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, List.of(
                    ClobRow.discovered(c1Start * 1000L - 5000L, "101", "102"),
                    ClobRow.bookLevel((c1Start + 60L) * 1000L + 10L, (c1Start + 60L) * 1000L, 4, true, true, 4900, 300.0),
                    ClobRow.bookLevel((c1Start + 60L) * 1000L + 10L, (c1Start + 60L) * 1000L, 4, true, false, 5000, 300.0)
            ));

            TypedClobParquetTestHelper.writeParquet(stmt, pmFile2, List.of(
                    ClobRow.discovered(c2Start * 1000L - 5000L, "201", "202"),
                    ClobRow.bookLevel((c2Start + 60L) * 1000L + 10L, (c2Start + 60L) * 1000L, 4, true, true, 5000, 300.0),
                    ClobRow.bookLevel((c2Start + 60L) * 1000L + 10L, (c2Start + 60L) * 1000L, 4, true, false, 5200, 300.0)
            ));

            TypedClobParquetTestHelper.writeParquet(stmt, pmFile3, List.of(
                    ClobRow.discovered(c3Start * 1000L - 5000L, "301", "302"),
                    ClobRow.bookLevel((c3Start + 60L) * 1000L + 10L, (c3Start + 60L) * 1000L, 4, true, true, 5000, 300.0),
                    ClobRow.bookLevel((c3Start + 60L) * 1000L + 10L, (c3Start + 60L) * 1000L, 4, true, false, 5200, 300.0)
            ));
        }

        PerformanceMetricsCalculator metricsCalculator = new PerformanceMetricsCalculator();
        BacktestJsonExporter jsonExporter = new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export").toString());

        class PolymarketOnlyBatchStrategy implements TradingStrategy {
            private StrategyContext ctx;
            private boolean orderedInCandle = false;

            @Override
            public String getName() {
                return "PolymarketOnlyStrategy";
            }

            @Override
            public TradingStrategy createInstance() {
                return new PolymarketOnlyBatchStrategy();
            }

            @Override
            public void init(StrategyContext context) {
                this.ctx = context;
            }

            @Override
            public void reset() {
                this.orderedInCandle = false;
            }

            @Override
            public void onPolymarketCryptoPrice(long timestampMs, String symbol, String source, double price, long priceTimestampMs) {
                polymarketCryptoPriceCount.incrementAndGet();
            }

            @Override
            public void onPolymarketCryptoTwap60s(long timestampMs, String symbol, String source, double twapPrice, int windowSeconds, long twapTimestampMs) {
                polymarketTwap60sCount.incrementAndGet();
            }

            @Override
            public void onOrderBookQuote(OrderBookQuote quote) {
                if (!orderedInCandle && !Double.isNaN(quote.bestAskUp())) {
                    orderedInCandle = true;
                    ctx.getExecutionRouter().submitOrder(new OrderCommand(
                            "pm-only-1", "mkt", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                            "BUY", BigDecimal.valueOf(quote.bestAskUp()), BigDecimal.valueOf(25.0)
                    ));
                    ctx.getExecutionRouter().submitOrder(new OrderCommand(
                            "pm-only-2", "mkt", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                            "BUY", BigDecimal.valueOf(quote.bestAskUp()), BigDecimal.valueOf(25.0)
                    ));
                }
            }

            @Override
            public void onCandleResolution(TradeDirection actualOutcome) {
                orderedInCandle = false;
            }

            @Override
            public void onExecutionReport(ExecutionReport report) {
            }
        }
        TradingStrategy polymarketOnlyStrategy = new PolymarketOnlyBatchStrategy();
        UtterParameciumStrategy utterStrategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        OraclePulseLockStrategy oraclePulseLockStrategy = new OraclePulseLockStrategy(OraclePulseLockStrategyConfig.defaults());

        StrategyRegistry strategyRegistry = new StrategyRegistry(Map.of(
                polymarketOnlyStrategy.getName(), polymarketOnlyStrategy,
                utterStrategy.getName(), utterStrategy,
                oraclePulseLockStrategy.getName(), oraclePulseLockStrategy
        ));
        IndicatorCacheService indicatorCacheService = new IndicatorCacheService(
                tempDir.resolve("cache"),
                new ObjectMapper(),
                List.of(new SpotTwapBacktestIndicator()),
                100L, 50L, 50L
        );

        backtestEngine = new BacktestEngine(
                datasetService,
                indicatorCacheService,
                strategyRegistry,
                metricsCalculator,
                jsonExporter,
                "OraclePulseLockStrategy",
                "2026-08-01",
                tempDir.resolve("export").toString(),
                10000.0,
                1.0,
                2,
                100L,
                50L,
                50L
        );
    }

    @AfterEach
    void tearDown() {
        if (backtestEngine != null) {
            backtestEngine.shutdown();
        }
        if (duckDbSessionManager != null) {
            duckDbSessionManager.close();
        }
    }

    @Test
    @DisplayName("Should run backtest over collector Parquet archives with time-varying latency and latencyFactor")
    void shouldRunBacktestOverCollectorParquetArchives(@TempDir Path tempDir) {
        BacktestResult result = backtestEngine.runBacktest(
                "PolymarketOnlyStrategy",
                "BTCUSDT",
                "2026-08-01",
                "2026-08-01",
                10000.0,
                tempDir.toString(),
                1.5
        );

        assertThat(result).isNotNull();
        assertThat(result.strategyName()).isEqualTo("PolymarketOnlyStrategy");
        assertThat(result.totalMarkets()).isEqualTo(3);

        assertThat(result.trades()).isNotEmpty();
        assertThat(result.metrics().totalTrades()).isEqualTo(result.trades().size());
        assertThat(result.metrics().totalNetPnl()).isGreaterThan(0.0);

        assertThat(result.jsonFilePath()).isNotNull();
        Path jsonPath = Path.of(result.jsonFilePath());
        assertThat(Files.exists(jsonPath)).isTrue();
        assertThat(Files.exists(tempDir.resolve("latest.json"))).isTrue();
    }

    @Test
    @DisplayName("Should execute Polymarket-only batch strategy across multiple markets")
    void shouldExecutePolymarketOnlyBatchStrategy(@TempDir Path tempDir) {
        polymarketCryptoPriceCount.set(0);
        polymarketTwap60sCount.set(0);

        BacktestResult result = backtestEngine.runBacktest(
                "PolymarketOnlyStrategy",
                "BTCUSDT",
                "2026-08-01",
                "2026-08-01",
                10000.0,
                tempDir.toString(),
                1.0
        );

        assertThat(result).isNotNull();
        assertThat(result.totalMarkets()).isEqualTo(3);
        assertThat(result.trades()).hasSize(6);
        assertThat(polymarketCryptoPriceCount.get()).isEqualTo(3);
        assertThat(polymarketTwap60sCount.get()).isGreaterThanOrEqualTo(3);
    }

    @Test
    @DisplayName("Should execute UtterParameciumStrategy final-minute TWAP lock-in sniping via normalized name")
    void shouldExecuteUtterParameciumStrategyInFinalMinuteWindow(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager localDuckDb = new DuckDbSessionManager("256MB")) {
            Path collectorDir = tempDir.resolve("collector_utter");
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), localDuckDb);

            long c1Start = BASE_START;
            Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
            Path cryptoPriceFile = datasetService.getPolymarketCryptoPriceFile("BTCUSDT", "2026-08-01");
            Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");

            Files.createDirectories(pmDir);
            Files.createDirectories(cryptoPriceFile.getParent());
            Files.createDirectories(twap60sFile.getParent());

            Path pmFile1 = pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet");
            long c2MissingTwapStart = BASE_START + 1800L;
            Path pmFile2 = pmDir.resolve("btc-updown-15m-" + c2MissingTwapStart + ".parquet");

            try (Connection conn = localDuckDb.openSession();
                 Statement stmt = conn.createStatement()) {

                String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                        "(" + ((c1Start + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60050.0::DOUBLE, '60050.0'::VARCHAR, 60::INT, " + ((c1Start + 900L) * 1000L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

                String normCrypto = cryptoPriceFile.toAbsolutePath().toString().replace('\\', '/');
                StringBuilder cryptoValues = new StringBuilder();
                for (int sec = 835; sec <= 885; sec++) {
                    if (cryptoValues.length() > 0) {
                        cryptoValues.append(", ");
                    }
                    long tsMs = (c1Start + sec) * 1000L;
                    double price = 60050.0 + (sec % 2 == 0 ? 1.0 : -1.0);
                    cryptoValues.append("(")
                            .append(tsMs).append("::UBIGINT, ")
                            .append(sec).append("::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, ")
                            .append(price).append("::DOUBLE, '").append(price).append("'::VARCHAR, ")
                            .append(tsMs).append("::UBIGINT)");
                }
                stmt.execute("COPY (SELECT * FROM (VALUES " + cryptoValues +
                        ") AS t(ts, seq, symbol, source, price, full_accuracy_price, price_timestamp)) TO '" + normCrypto + "' (FORMAT PARQUET)");

                long bookTs1 = (c1Start + 845L) * 1000L;
                long bookTs2 = (c1Start + 860L) * 1000L;
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, List.of(
                        ClobRow.discovered(c1Start * 1000L - 5000L, "101", "102"),
                        ClobRow.bookLevel(bookTs1, bookTs1 - 12L, 4, true, true, 5300, 300.0),
                        ClobRow.bookLevel(bookTs1, bookTs1 - 12L, 4, true, false, 5500, 300.0),
                        ClobRow.bookLevel(bookTs2, bookTs2 - 10L, 8, true, true, 5400, 300.0),
                        ClobRow.bookLevel(bookTs2, bookTs2 - 10L, 8, true, false, 5600, 300.0)
                ));

                long m2BookTs = (c2MissingTwapStart + 850L) * 1000L;
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile2, List.of(
                        ClobRow.discovered(c2MissingTwapStart * 1000L - 5000L, "201", "202"),
                        ClobRow.bookLevel(m2BookTs, m2BookTs - 10L, 4, true, true, 5300, 300.0),
                        ClobRow.bookLevel(m2BookTs, m2BookTs - 10L, 4, true, false, 5500, 300.0)
                ));
            }

            UtterParameciumStrategy utterStrategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
            StrategyRegistry localRegistry = new StrategyRegistry(Map.of(utterStrategy.getName(), utterStrategy));
            IndicatorCacheService localCacheService = new IndicatorCacheService(
                    tempDir.resolve("cache_utter"),
                    new ObjectMapper(),
                    List.of(new SpotTwapBacktestIndicator()),
                    100L, 50L, 50L
            );
            BacktestEngine utterEngine = new BacktestEngine(
                    datasetService,
                    localCacheService,
                    localRegistry,
                    new PerformanceMetricsCalculator(),
                    new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export_utter").toString()),
                    "UtterParameciumStrategy",
                    "2026-08-01",
                    tempDir.resolve("export_utter").toString(),
                    10000.0,
                    1.0,
                    2,
                    100L,
                    50L,
                    50L
            );

            try {
                BacktestResult result = utterEngine.runBacktest(
                        "utter_paramecium",
                        "BTCUSDT",
                        "2026-08-01",
                        "2026-08-01",
                        10000.0,
                        tempDir.resolve("export_utter").toString(),
                        1.0
                );

                assertThat(result).isNotNull();
                assertThat(result.totalMarkets()).isEqualTo(2);
                assertThat(result.trades()).isNotEmpty();
                assertThat(result.trades()).allSatisfy(trade -> {
                    assertThat(trade.intervalStartSec()).isEqualTo(c1Start);
                    assertThat(trade.isWin()).isTrue();
                    assertThat(trade.exitPrice()).isEqualTo(1.0);
                    assertThat(trade.netPnl()).isGreaterThan(0.0);
                });
            } finally {
                utterEngine.shutdown();
            }
        }
    }

    @Test
    @DisplayName("Zero-Discrepancy: Bounded 76s CLOB lookback window must produce 100% identical trades, PnL, and Brier score as full 15m CLOB replay across multiple Parquet row groups")
    void shouldProduceZeroDiscrepancyBetweenBoundedClobWindowAndFullCandleReplay(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager localDuckDb = new DuckDbSessionManager("256MB")) {
            Path collectorDir = tempDir.resolve("collector_equivalence");
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), localDuckDb);

            long c1Start = BASE_START;
            Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
            Path cryptoPriceFile = datasetService.getPolymarketCryptoPriceFile("BTCUSDT", "2026-08-01");
            Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");

            Files.createDirectories(pmDir);
            Files.createDirectories(cryptoPriceFile.getParent());
            Files.createDirectories(twap60sFile.getParent());

            Path pmFile1 = pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet");

            try (Connection conn = localDuckDb.openSession();
                 Statement stmt = conn.createStatement()) {

                // Boundary TWAP arrives 300ms BEFORE c1Start to also verify boundary target-price lock-in!
                String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L - 300L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L - 300L) + "::UBIGINT), " +
                        "(" + ((c1Start + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60060.0::DOUBLE, '60060.0'::VARCHAR, 60::INT, " + ((c1Start + 900L) * 1000L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

                // Full 15-minute Chainlink price_crypto stream (every 5s early, every 1s in final 120s)
                String normCrypto = cryptoPriceFile.toAbsolutePath().toString().replace('\\', '/');
                StringBuilder cryptoValues = new StringBuilder();
                int seq = 0;
                for (int sec = 0; sec <= 895; sec += (sec < 780 ? 5 : 1)) {
                    if (seq > 0) {
                        cryptoValues.append(", ");
                    }
                    seq++;
                    long tsMs = (c1Start + sec) * 1000L;
                    double price = sec < 820 ? 60005.0 + (sec % 3) : 60060.0 + (sec % 2 == 0 ? 1.0 : -1.0);
                    cryptoValues.append("(")
                            .append(tsMs).append("::UBIGINT, ")
                            .append(seq).append("::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, ")
                            .append(price).append("::DOUBLE, '").append(price).append("'::VARCHAR, ")
                            .append(tsMs).append("::UBIGINT)");
                }
                stmt.execute("COPY (SELECT * FROM (VALUES " + cryptoValues +
                        ") AS t(ts, seq, symbol, source, price, full_accuracy_price, price_timestamp)) TO '" + normCrypto + "' (FORMAT PARQUET)");

                // Dense 15m CLOB market spanning multiple Parquet row groups (rowGroupSize = 25):
                // periodic book snapshots every 30s + deltas every 2s + pre-window snapshots at +816s/+818s + deltas at +821s..+880s
                List<ClobRow> clobRows = new ArrayList<>();
                clobRows.add(ClobRow.discovered(c1Start * 1000L - 5000L, "101", "102"));
                int frameId = 4;
                for (int sec = 0; sec <= 885; sec += 3) {
                    long ts = (c1Start + sec) * 1000L;
                    long srvTs = ts - (11L + (sec % 7));
                    if (sec % 30 == 0 || sec == 816) {
                        clobRows.add(ClobRow.bookLevel(ts, srvTs, frameId, true, true, 5200, 250.0));
                        clobRows.add(ClobRow.bookLevel(ts, srvTs, frameId, true, false, 5400, 250.0));
                        frameId += 4;
                        clobRows.add(ClobRow.bookLevel(ts + 5L, srvTs + 5L, frameId, false, true, 4600, 250.0));
                        clobRows.add(ClobRow.bookLevel(ts + 5L, srvTs + 5L, frameId, false, false, 4800, 250.0));
                        frameId += 4;
                    } else {
                        int askTick = sec >= 820 ? 5500 : 5400 + (sec % 20);
                        clobRows.add(ClobRow.priceChange(ts, srvTs, frameId, true, false, askTick, 320.0));
                        clobRows.add(ClobRow.priceChange(ts, srvTs, frameId, false, true, 10_000 - askTick, 320.0));
                        frameId += 4;
                    }
                }
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, clobRows, 25);
            }

            UtterParameciumStrategyConfig cfg = UtterParameciumStrategyConfig.defaultModelV1();
            UtterParameciumStrategy windowedStrategy = new UtterParameciumStrategy(cfg);
            UtterParameciumStrategy fullReplayStrategy = new UtterParameciumStrategy(cfg) {
                @Override
                public String getName() {
                    return "UtterParameciumFullReplay";
                }

                @Override
                public long getRequiredClobLookbackMs() {
                    return Long.MAX_VALUE;
                }

                @Override
                public TradingStrategy createInstance() {
                    UtterParameciumStrategy delegate = this;
                    return new UtterParameciumStrategy(cfg) {
                        @Override
                        public String getName() {
                            return delegate.getName();
                        }

                        @Override
                        public long getRequiredClobLookbackMs() {
                            return Long.MAX_VALUE;
                        }
                    };
                }
            };

            StrategyRegistry registry = new StrategyRegistry(Map.of(
                    windowedStrategy.getName(), windowedStrategy,
                    fullReplayStrategy.getName(), fullReplayStrategy
            ));
            IndicatorCacheService eqCacheService = new IndicatorCacheService(
                    tempDir.resolve("cache_eq"),
                    new ObjectMapper(),
                    List.of(new SpotTwapBacktestIndicator()),
                    100L, 50L, 50L
            );
            BacktestEngine engine = new BacktestEngine(
                    datasetService,
                    eqCacheService,
                    registry,
                    new PerformanceMetricsCalculator(),
                    new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export_eq").toString()),
                    "UtterParameciumStrategy",
                    "2026-08-01",
                    tempDir.resolve("export_eq").toString(),
                    10000.0,
                    1.0,
                    2,
                    100L,
                    50L,
                    50L
            );

            try {
                BacktestResult windowedRes = engine.runBacktest(
                        windowedStrategy.getName(), "BTCUSDT", "2026-08-01", "2026-08-01",
                        10000.0, tempDir.resolve("export_eq_win").toString(), 1.0);
                BacktestResult fullRes = engine.runBacktest(
                        fullReplayStrategy.getName(), "BTCUSDT", "2026-08-01", "2026-08-01",
                        10000.0, tempDir.resolve("export_eq_full").toString(), 1.0);

                assertThat(windowedRes.trades()).isNotEmpty();
                assertThat(windowedRes.trades()).hasSameSizeAs(fullRes.trades());
                for (int i = 0; i < windowedRes.trades().size(); i++) {
                    assertThat(windowedRes.trades().get(i)).isEqualTo(fullRes.trades().get(i));
                }
                assertThat(windowedRes.metrics()).isEqualTo(fullRes.metrics());
            } finally {
                engine.shutdown();
            }
        }
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when unknown strategy requested")
    void shouldThrowWhenUnknownStrategy() {
        assertThatThrownBy(() -> backtestEngine.runBacktest("UnknownStrategy", "BTCUSDT", null, null, 10000.0, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown strategy");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when no market rows available")
    void shouldThrowWhenNoMarketRowsAvailable() {
        assertThatThrownBy(() -> backtestEngine.runBacktest("UtterParameciumStrategy", "UNKNOWN_SYMBOL", null, null, 10000.0, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No market rows available");
    }

    @Test
    @DisplayName("Should resolve default strategy, default startDate, current UTC endDate, default capital, and default outputDir when omitted")
    void shouldResolveDefaultParametersWhenOmitted(@TempDir Path tempDir) {
        BacktestResult result = backtestEngine.runBacktest(
                null,
                "BTCUSDT",
                null,
                null,
                null,
                tempDir.toString(),
                null
        );

        assertThat(result).isNotNull();
        assertThat(result.strategyName()).isEqualTo("OraclePulseLockStrategy");
        assertThat(result.totalMarkets()).isEqualTo(3);
        assertThat(result.metrics().initialCapital()).isEqualTo(10000.0);
        assertThat(result.jsonFilePath()).isNotBlank();
        assertThat(Files.exists(Path.of(result.jsonFilePath()))).isTrue();
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when startDate is after endDate")
    void shouldThrowWhenStartDateIsAfterEndDate() {
        assertThatThrownBy(() -> backtestEngine.runBacktest(
                "UtterParameciumStrategy", "BTCUSDT", "2026-08-10", "2026-08-01", 10000.0, null, 1.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("startDate must not be after endDate");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when any configured exchange delay is negative")
    void shouldThrowWhenExchangeDelayIsNegative(@TempDir Path tempDir) {
        CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(tempDir.toString(), duckDbSessionManager);
        IndicatorCacheService cacheService = new IndicatorCacheService(
                tempDir.resolve("cache"), new ObjectMapper(), List.of(new SpotTwapBacktestIndicator()), 100L, 50L, 50L);
        StrategyRegistry registry = new StrategyRegistry(Map.of());
        PerformanceMetricsCalculator calc = new PerformanceMetricsCalculator();
        BacktestJsonExporter exporter = new BacktestJsonExporter(new ObjectMapper(), tempDir.toString());

        assertThatThrownBy(() -> new BacktestEngine(
                datasetService, cacheService, registry, calc, exporter,
                "UtterParameciumStrategy", "2026-08-01", tempDir.toString(), 10000.0, 1.0, 2,
                -1L, 50L, 50L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("binanceDelayMs");

        assertThatThrownBy(() -> new BacktestEngine(
                datasetService, cacheService, registry, calc, exporter,
                "UtterParameciumStrategy", "2026-08-01", tempDir.toString(), 10000.0, 1.0, 2,
                100L, -1L, 50L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("krakenDelayMs");

        assertThatThrownBy(() -> new BacktestEngine(
                datasetService, cacheService, registry, calc, exporter,
                "UtterParameciumStrategy", "2026-08-01", tempDir.toString(), 10000.0, 1.0, 2,
                100L, 50L, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("coinbaseDelayMs");
    }

    @Test
    @DisplayName("Should populate indicator Parquet cache and config.json on first backtest run and reuse cached indicator values after raw exchange files are deleted")
    void shouldPopulateAndReuseIndicatorCacheAcrossBacktestRuns(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager localDuckDb = new DuckDbSessionManager("256MB")) {
            Path collectorDir = tempDir.resolve("collector_cache_e2e");
            Path cacheDir = tempDir.resolve("cache_e2e");
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), localDuckDb);

            long c1Start = BASE_START;
            String dateStr = "2026-08-01";
            Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
            Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", dateStr);
            Path spotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", dateStr);

            Files.createDirectories(pmDir);
            Files.createDirectories(twap60sFile.getParent());
            Files.createDirectories(spotFile.getParent());

            Path pmFile1 = pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet");

            try (Connection conn = localDuckDb.openSession();
                 Statement stmt = conn.createStatement()) {
                String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                        "(" + ((c1Start + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60100.0::DOUBLE, '60100.0'::VARCHAR, 60::INT, " + ((c1Start + 900L) * 1000L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

                String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 60010.0::DOUBLE, 2.0::DOUBLE, 1::BIGINT, 1::BIGINT, " + (c1Start * 1000L + 500L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                        "(NULL::UBIGINT, 2::BIGINT, 'BTCUSDT'::VARCHAR, 60040.0::DOUBLE, 3.0::DOUBLE, 2::BIGINT, 2::BIGINT, " + (c1Start * 1000L + 1500L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                        ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normSpot + "' (FORMAT PARQUET)");

                TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, List.of(
                        ClobRow.discovered(c1Start * 1000L - 5000L, "101", "102"),
                        ClobRow.bookLevel(c1Start * 1000L + 2000L, c1Start * 1000L + 1990L, 4, true, true, 4900, 300.0),
                        ClobRow.bookLevel(c1Start * 1000L + 2000L, c1Start * 1000L + 1990L, 4, true, false, 5100, 300.0)
                ));
            }

            List<String> observedSpotTwapTicks = new ArrayList<>();
            class SpotTwapRecordingStrategy implements TradingStrategy, cz.polymarket.bot.indicators.SpotTwapListener {
                @Override
                public String getName() {
                    return "SpotTwapRecordingStrategy";
                }

                @Override
                public void init(StrategyContext context) {}

                @Override
                public void onSpotTwap(long timestampMs, String symbol, double spotPrice, double twap60sPrice) {
                    observedSpotTwapTicks.add(timestampMs + ":" + symbol + ":" + Math.round(spotPrice) + ":" + Math.round(twap60sPrice));
                }

                @Override
                public void onExecutionReport(ExecutionReport report) {}
            }

            SpotTwapRecordingStrategy recordingStrategy = new SpotTwapRecordingStrategy();
            SpotTwapBacktestIndicator spotTwapIndicator = new SpotTwapBacktestIndicator();
            IndicatorCacheService cacheService = new IndicatorCacheService(
                    cacheDir, new ObjectMapper(), List.of(spotTwapIndicator), 100L, 50L, 50L);
            StrategyRegistry localRegistry = new StrategyRegistry(Map.of(recordingStrategy.getName(), recordingStrategy));

            BacktestEngine localEngine = new BacktestEngine(
                    datasetService,
                    cacheService,
                    localRegistry,
                    new PerformanceMetricsCalculator(),
                    new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export_cache_e2e").toString()),
                    recordingStrategy.getName(),
                    dateStr,
                    tempDir.resolve("export_cache_e2e").toString(),
                    10000.0,
                    1.0,
                    1,
                    100L,
                    50L,
                    50L
            );

            try {
                // First run: Cache Miss -> computes and caches indicator values
                localEngine.runBacktest(recordingStrategy.getName(), "BTCUSDT", dateStr, dateStr, 10000.0,
                        tempDir.resolve("export_cache_e2e_1").toString(), 1.0);
                List<String> firstRunTicks = new ArrayList<>(observedSpotTwapTicks);
                assertThat(firstRunTicks).hasSize(2);

                Path cachedParquet = cacheService.resolveDailyCacheFile(spotTwapIndicator, "BTCUSDT", dateStr);
                Path configJson = cacheService.resolveConfigJsonFile(spotTwapIndicator);
                assertThat(Files.isRegularFile(cachedParquet)).isTrue();
                assertThat(Files.isRegularFile(configJson)).isTrue();

                // Delete raw exchange Parquet file and re-run -> must read purely from cache
                Files.delete(spotFile);
                observedSpotTwapTicks.clear();

                localEngine.runBacktest(recordingStrategy.getName(), "BTCUSDT", dateStr, dateStr, 10000.0,
                        tempDir.resolve("export_cache_e2e_2").toString(), 1.0);

                assertThat(observedSpotTwapTicks).isEqualTo(firstRunTicks);
            } finally {
                localEngine.shutdown();
            }
        }
    }

    @Test
    @DisplayName("BacktestEngine caches FastSpotMicroBacktestIndicator and dispatches FastSpotMicroListener events")
    void testFastSpotMicroCacheAndDispatch(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager localDuckDb = new DuckDbSessionManager("256MB")) {
            Path collectorDir = tempDir.resolve("collector_fast_micro");
            Path cacheDir = tempDir.resolve("cache_fast_micro");
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), localDuckDb);

            long c1Start = BASE_START;
            String dateStr = "2026-08-01";
            Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
            Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", dateStr);
            Path cbFile = datasetService.getCoinbaseSpotTradesFile("BTCUSDT", dateStr);
            Path krFile = datasetService.getKrakenSpotTradesFile("BTCUSDT", dateStr);
            Path bnFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", dateStr);

            Files.createDirectories(pmDir);
            Files.createDirectories(twap60sFile.getParent());
            Files.createDirectories(cbFile.getParent());
            Files.createDirectories(krFile.getParent());
            Files.createDirectories(bnFile.getParent());

            Path pmFile1 = pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet");

            try (Connection conn = localDuckDb.openSession();
                 Statement stmt = conn.createStatement()) {
                String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                        "(" + ((c1Start + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60120.0::DOUBLE, '60120.0'::VARCHAR, 60::INT, " + ((c1Start + 900L) * 1000L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

                String normCb = cbFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L + 500L) + "::UBIGINT, 'BTC-USD'::VARCHAR, 1::BIGINT, 'sell'::VARCHAR, 60050.0::DOUBLE, 2.0::DOUBLE, '2026-08-01T00:00:00.500000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, price, qty, timestamp)) TO '" + normCb + "' (FORMAT PARQUET)");

                String normKr = krFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L + 500L) + "::UBIGINT, 'BTC/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'market'::VARCHAR, 60060.0::DOUBLE, 1.5::DOUBLE, '2026-08-01T00:00:00.500000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normKr + "' (FORMAT PARQUET)");

                String normBn = bnFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 60040.0::DOUBLE, 2.5::DOUBLE, 1::BIGINT, 1::BIGINT, " + (c1Start * 1000L + 450L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                        ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normBn + "' (FORMAT PARQUET)");

                TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, List.of(
                        ClobRow.discovered(c1Start * 1000L - 5000L, "101", "102"),
                        ClobRow.bookLevel(c1Start * 1000L + 2000L, c1Start * 1000L + 1990L, 4, true, true, 4900, 300.0),
                        ClobRow.bookLevel(c1Start * 1000L + 2000L, c1Start * 1000L + 1990L, 4, true, false, 5100, 300.0)
                ));
            }

            List<double[]> observedFastTicks = new ArrayList<>();
            class FastMicroRecordingStrategy implements TradingStrategy, cz.polymarket.bot.indicators.FastSpotMicroListener {
                @Override
                public String getName() {
                    return "FastMicroRecordingStrategy";
                }

                @Override
                public void init(StrategyContext context) {}

                @Override
                public void onFastSpotMicro(long timestampMs, String symbol, double fastDirectUsd, double signedOfi1s, double syncMove500Usd) {
                    observedFastTicks.add(new double[]{timestampMs, fastDirectUsd, signedOfi1s, syncMove500Usd});
                }

                @Override
                public void onExecutionReport(ExecutionReport report) {}
            }

            FastMicroRecordingStrategy recordingStrategy = new FastMicroRecordingStrategy();
            SpotTwapBacktestIndicator spotTwapIndicator = new SpotTwapBacktestIndicator();
            cz.polymarket.bot.backtest.indicator.FastSpotMicroBacktestIndicator fastIndicator =
                    new cz.polymarket.bot.backtest.indicator.FastSpotMicroBacktestIndicator(
                            cz.polymarket.bot.indicators.FastSpotMicroIndicatorConfig.defaults());
            IndicatorCacheService cacheService = new IndicatorCacheService(
                    cacheDir, new ObjectMapper(), List.of(spotTwapIndicator, fastIndicator), 100L, 50L, 50L);
            StrategyRegistry localRegistry = new StrategyRegistry(Map.of(recordingStrategy.getName(), recordingStrategy));

            BacktestEngine localEngine = new BacktestEngine(
                    datasetService,
                    cacheService,
                    localRegistry,
                    new PerformanceMetricsCalculator(),
                    new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export_fast_e2e").toString()),
                    recordingStrategy.getName(),
                    dateStr,
                    tempDir.resolve("export_fast_e2e").toString(),
                    10000.0,
                    1.0,
                    1,
                    100L,
                    50L,
                    50L
            );

            try {
                localEngine.runBacktest(recordingStrategy.getName(), "BTCUSDT", dateStr, dateStr, 10000.0,
                        tempDir.resolve("export_fast_e2e_1").toString(), 1.0);
                assertThat(observedFastTicks).isNotEmpty();
                double[] tick = observedFastTicks.getLast();
                assertThat(tick[1]).isCloseTo(0.70 * 60050.0 + 0.30 * 60060.0, org.assertj.core.api.Assertions.within(1e-4));
                assertThat(tick[2]).isBetween(0.95, 1.0);
            } finally {
                localEngine.shutdown();
            }
        }
    }
}
