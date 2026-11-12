package cz.polymarket.bot.backtest.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper.ClobRow;
import cz.polymarket.bot.backtest.export.BacktestJsonExporter;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.calculator.KellyPositionSizer;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExternalStreamType;
import cz.polymarket.bot.strategy.IndicatorType;
import cz.polymarket.bot.strategy.NextCandleProbabilityModel;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategy;
import cz.polymarket.bot.strategy.TWAPArbitrageStrategyConfig;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BacktestEngineTest {

    private DuckDbSessionManager duckDbSessionManager;
    private BacktestEngine backtestEngine;
    private final AtomicInteger polymarketCryptoPriceCount = new AtomicInteger();
    private final AtomicInteger polymarketTwap60sCount = new AtomicInteger();
    private final AtomicInteger externalSpotTradeCount = new AtomicInteger();

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
        Path futFile = datasetService.getBinanceFuturesAggTradesFile("BTCUSDT", "2026-08-01");

        Files.createDirectories(pmDir);
        Files.createDirectories(cryptoPriceFile.getParent());
        Files.createDirectories(twap60sFile.getParent());
        Files.createDirectories(spotFile.getParent());
        Files.createDirectories(futFile.getParent());

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

            // 3. Binance Spot aggTrades
            String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L + 100L) + "::UBIGINT, 1::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, false::BOOLEAN), " +
                    "(" + ((c1Start + 60L) * 1000L) + "::UBIGINT, 2::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, true::BOOLEAN), " +
                    "(" + (c2Start * 1000L + 100L) + "::UBIGINT, 3::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, false::BOOLEAN), " +
                    "(" + ((c2Start + 60L) * 1000L) + "::UBIGINT, 4::BIGINT, 60150.0::DOUBLE, 15.0::DOUBLE, false::BOOLEAN), " +
                    "(" + (c3Start * 1000L + 100L) + "::UBIGINT, 5::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, true::BOOLEAN), " +
                    "(" + ((c3Start + 60L) * 1000L) + "::UBIGINT, 6::BIGINT, 59850.0::DOUBLE, 20.0::DOUBLE, true::BOOLEAN)" +
                    ") AS t(ts, aggregate_trade_id, price, quantity, is_buyer_maker)) TO '" + normSpot + "' (FORMAT PARQUET)");

            // 4. Binance Futures aggTrades
            String normFut = futFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L + 100L) + "::UBIGINT, 1::BIGINT, 60000.0::DOUBLE, 5.0::DOUBLE, false::BOOLEAN), " +
                    "(" + ((c2Start + 60L) * 1000L) + "::UBIGINT, 2::BIGINT, 60160.0::DOUBLE, 18.0::DOUBLE, false::BOOLEAN), " +
                    "(" + ((c3Start + 60L) * 1000L) + "::UBIGINT, 3::BIGINT, 59840.0::DOUBLE, 25.0::DOUBLE, true::BOOLEAN)" +
                    ") AS t(ts, aggregate_trade_id, price, quantity, is_buyer_maker)) TO '" + normFut + "' (FORMAT PARQUET)");

            // 5. Polymarket 15m typed CLOB market files
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

        TWAPArbitrageStrategyConfig config = TWAPArbitrageStrategyConfig.defaultIteration20();
        KellyPositionSizer positionSizer = new KellyPositionSizer(10000.0, 0.25, 50.0, 300.0);
        NextCandleProbabilityModel probabilityModel = new NextCandleProbabilityModel(positionSizer);
        TWAPArbitrageStrategy twapStrategy = new TWAPArbitrageStrategy(probabilityModel, config);

        class PolymarketOnlyBatchStrategy implements TradingStrategy {
            private StrategyContext ctx;
            private boolean orderedInCandle = false;

            @Override
            public String getName() {
                return "PolymarketOnlyStrategy";
            }

            @Override
            public Set<IndicatorType> getRequiredIndicators() {
                return Set.of(IndicatorType.VWAP);
            }

            @Override
            public Set<ExternalStreamType> getRequiredExternalStreams() {
                return Set.of();
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
                assertThat(ctx.getIndicatorValue(IndicatorType.VWAP)).isNaN();
            }

            @Override
            public void onPolymarketCryptoTwap60s(long timestampMs, String symbol, String source, double twapPrice, int windowSeconds, long twapTimestampMs) {
                polymarketTwap60sCount.incrementAndGet();
                assertThat(ctx.getIndicatorValue(IndicatorType.VWAP)).isNaN();
            }

            @Override
            public void onBinanceSpotTrade(long timestampMs, double price, double quantity, boolean isBuyerMaker) {
                externalSpotTradeCount.incrementAndGet();
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

        StrategyRegistry strategyRegistry = new StrategyRegistry(Map.of(
                twapStrategy.getName(), twapStrategy,
                polymarketOnlyStrategy.getName(), polymarketOnlyStrategy,
                utterStrategy.getName(), utterStrategy
        ));

        backtestEngine = new BacktestEngine(
                datasetService,
                strategyRegistry,
                metricsCalculator,
                jsonExporter,
                "UtterParameciumStrategy",
                "2026-08-01",
                tempDir.resolve("export").toString(),
                10000.0,
                1.0,
                2
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
                "TWAPArbitrageStrategy",
                "BTCUSDT",
                "2026-08-01",
                "2026-08-01",
                10000.0,
                tempDir.toString(),
                1.5
        );

        assertThat(result).isNotNull();
        assertThat(result.strategyName()).isEqualTo("TWAPArbitrageStrategy");
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
    @DisplayName("Should skip all Binance and Deribit external files when strategy requires no external streams")
    void shouldSkipExternalFilesForPolymarketOnlyStrategy(@TempDir Path tempDir) {
        polymarketCryptoPriceCount.set(0);
        polymarketTwap60sCount.set(0);
        externalSpotTradeCount.set(0);

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
        assertThat(externalSpotTradeCount.get()).isZero();
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
            BacktestEngine utterEngine = new BacktestEngine(
                    datasetService,
                    localRegistry,
                    new PerformanceMetricsCalculator(),
                    new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export_utter").toString()),
                    "UtterParameciumStrategy",
                    "2026-08-01",
                    tempDir.resolve("export_utter").toString(),
                    10000.0,
                    1.0,
                    2
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
            BacktestEngine engine = new BacktestEngine(
                    datasetService,
                    registry,
                    new PerformanceMetricsCalculator(),
                    new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export_eq").toString()),
                    "UtterParameciumStrategy",
                    "2026-08-01",
                    tempDir.resolve("export_eq").toString(),
                    10000.0,
                    1.0,
                    2
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
        assertThatThrownBy(() -> backtestEngine.runBacktest("TWAPArbitrageStrategy", "UNKNOWN_SYMBOL", null, null, 10000.0, null, null))
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
        assertThat(result.strategyName()).isEqualTo("UtterParameciumStrategy");
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
}
