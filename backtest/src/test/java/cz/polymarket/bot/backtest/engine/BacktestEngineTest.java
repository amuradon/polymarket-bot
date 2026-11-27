package cz.polymarket.bot.backtest.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper.ClobRow;
import cz.polymarket.bot.backtest.data.BacktestProgressListener;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.data.MarketEquityPoint;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.backtest.service.StrategyParameterService;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.engine.CoreTradingEngine;
import cz.polymarket.bot.engine.SimulatedClobExecutionEngine;
import cz.polymarket.bot.indicators.FastSpotMicroIndicator;
import cz.polymarket.bot.indicators.FastSpotMicroIndicatorConfig;
import cz.polymarket.bot.indicators.FastSpotMicroListener;
import cz.polymarket.bot.indicators.SpotTwapIndicator;
import cz.polymarket.bot.indicators.SpotTwapListener;
import cz.polymarket.bot.orderbook.PolymarketOrderBook;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.OraclePulseLockStrategy;
import cz.polymarket.bot.strategy.OraclePulseLockStrategyConfig;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.StrategyRegistry;
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
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class BacktestEngineTest {

    private DuckDbSessionManager duckDbSessionManager;
    private CollectorParquetDatasetService datasetService;
    private BacktestHistoryRepository historyRepository;
    private BacktestEngine backtestEngine;
    private final AtomicInteger polymarketCryptoPriceCount = new AtomicInteger();
    private final AtomicInteger polymarketTwap60sCount = new AtomicInteger();

    private static final long BASE_START = 1785542400L; // 2026-08-01 00:00:00 UTC

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        duckDbSessionManager = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp");
        Path collectorDir = tempDir.resolve("collector");
        datasetService = new CollectorParquetDatasetService(collectorDir.toString(), duckDbSessionManager);

        long c1Start = BASE_START;
        long c2Start = BASE_START + 900L;
        long c3Start = BASE_START + 1800L;

        Path pmFile1 = datasetService.getPolymarket15mFile("BTCUSDT", c1Start);
        Path pmFile2 = datasetService.getPolymarket15mFile("BTCUSDT", c2Start);
        Path pmFile3 = datasetService.getPolymarket15mFile("BTCUSDT", c3Start);
        Path cryptoPriceFile = datasetService.getPolymarketCryptoPriceFile("BTCUSDT", "2026-08-01");
        Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");
        Path spotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", "2026-08-01");

        Files.createDirectories(pmFile1.getParent());
        Files.createDirectories(cryptoPriceFile.getParent());
        Files.createDirectories(twap60sFile.getParent());
        Files.createDirectories(spotFile.getParent());

        try (Connection conn = duckDbSessionManager.openSession();
             Statement stmt = conn.createStatement()) {

            String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                    "(" + (c2Start * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c2Start * 1000L) + "::UBIGINT), " +
                    "(" + (c3Start * 1000L) + "::UBIGINT, 3::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60150.0::DOUBLE, '60150.0'::VARCHAR, 60::INT, " + (c3Start * 1000L) + "::UBIGINT), " +
                    "(" + ((c3Start + 900L) * 1000L) + "::UBIGINT, 4::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 59850.0::DOUBLE, '59850.0'::VARCHAR, 60::INT, " + ((c3Start + 900L) * 1000L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

            String normCrypto = cryptoPriceFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + ((c1Start + 60L) * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, " + ((c1Start + 60L) * 1000L - 5L) + "::UBIGINT), " +
                    "(" + ((c2Start + 60L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60150.0::DOUBLE, '60150.0'::VARCHAR, " + ((c2Start + 60L) * 1000L - 5L) + "::UBIGINT), " +
                    "(" + ((c3Start + 60L) * 1000L) + "::UBIGINT, 3::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 59850.0::DOUBLE, '59850.0'::VARCHAR, " + ((c3Start + 60L) * 1000L - 5L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, price, full_accuracy_price, price_timestamp)) TO '" + normCrypto + "' (FORMAT PARQUET)");

            String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 5.0::DOUBLE, 1::BIGINT, 1::BIGINT, " + (c1Start * 1000L + 100L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 2::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 5.0::DOUBLE, 2::BIGINT, 2::BIGINT, " + ((c1Start + 60L) * 1000L) + "::UBIGINT, true::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 3::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 5.0::DOUBLE, 3::BIGINT, 3::BIGINT, " + (c2Start * 1000L + 100L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 4::BIGINT, 'BTCUSDT'::VARCHAR, 60150.0::DOUBLE, 15.0::DOUBLE, 4::BIGINT, 4::BIGINT, " + ((c2Start + 60L) * 1000L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 5::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 5.0::DOUBLE, 5::BIGINT, 5::BIGINT, " + (c3Start * 1000L + 100L) + "::UBIGINT, true::BOOLEAN, true::BOOLEAN), " +
                    "(NULL::UBIGINT, 6::BIGINT, 'BTCUSDT'::VARCHAR, 59850.0::DOUBLE, 20.0::DOUBLE, 6::BIGINT, 6::BIGINT, " + ((c3Start + 60L) * 1000L) + "::UBIGINT, true::BOOLEAN, true::BOOLEAN)" +
                    ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normSpot + "' (FORMAT PARQUET)");

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
        historyRepository = new BacktestHistoryRepository(
                tempDir.resolve("history.duckdb"), new ObjectMapper(), metricsCalculator);

        class PolymarketOnlyBatchStrategy implements TradingStrategy {
            private StrategyContext ctx;
            private boolean orderedInCandle = false;
            private int seq = 0;

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
                this.seq = 0;
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
                            "pm-only-" + (++seq), "mkt", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                            "BUY", BigDecimal.valueOf(quote.bestAskUp()), BigDecimal.valueOf(25.0)
                    ));
                    ctx.getExecutionRouter().submitOrder(new OrderCommand(
                            "pm-only-" + (++seq), "mkt", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
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

        backtestEngine = new BacktestEngine(
                datasetService,
                new StrategyParameterService(strategyRegistry),
                metricsCalculator,
                historyRepository,
                "OraclePulseLockStrategy",
                "2026-08-01",
                10000.0,
                220L,
                100L,
                50L,
                50L
        );
    }

    @AfterEach
    void tearDown() {
        if (historyRepository != null) {
            historyRepository.close();
        }
        if (duckDbSessionManager != null) {
            duckDbSessionManager.close();
        }
    }

    @Test
    @DisplayName("Should run backtest over collector Parquet archives, record per-market equity points, and persist to DuckDB")
    void shouldRunBacktestOverCollectorParquetArchives() {
        BacktestRunRequest req = new BacktestRunRequest(
                "PM Only 330ms", "PolymarketOnlyStrategy", "BTCUSDT",
                "2026-08-01", "2026-08-01", false, 10000.0, 330L, null, null, null, null, false, false);
        BacktestResult result = backtestEngine.runBacktest(null, req, BacktestProgressListener.NOOP);

        assertThat(result).isNotNull();
        assertThat(result.strategyName()).isEqualTo("PolymarketOnlyStrategy");
        assertThat(result.totalMarkets()).isEqualTo(3);
        assertThat(result.marketPoints()).hasSize(3);
        assertThat(result.marketPoints()).extracting(MarketEquityPoint::evaluated).containsOnly(true);

        assertThat(result.trades()).isNotEmpty();
        assertThat(result.metrics().totalTrades()).isEqualTo(result.trades().size());
        assertThat(result.metrics().totalNetPnl()).isGreaterThan(0.0);
        assertThat(historyRepository.getRun(result.runId())).isPresent();
    }

    @Test
    @DisplayName("Should execute Polymarket-only strategy across multiple markets without inter-candle reset")
    void shouldExecutePolymarketOnlyBatchStrategy() {
        polymarketCryptoPriceCount.set(0);
        polymarketTwap60sCount.set(0);

        BacktestRunRequest req = new BacktestRunRequest(
                null, "PolymarketOnlyStrategy", "BTCUSDT",
                "2026-08-01", "2026-08-01", false, 10000.0, 220L, null, null, null, null, false, false);
        BacktestResult result = backtestEngine.runBacktest(null, req, BacktestProgressListener.NOOP);

        assertThat(result).isNotNull();
        assertThat(result.totalMarkets()).isEqualTo(3);
        assertThat(result.trades()).hasSize(6);
        assertThat(polymarketCryptoPriceCount.get()).isEqualTo(3);
        assertThat(polymarketTwap60sCount.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("Should execute UtterParameciumStrategy final-minute TWAP lock-in sniping via normalized name")
    void shouldExecuteUtterParameciumStrategyInFinalMinuteWindow(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager localDuckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp")) {
            Path collectorDir = tempDir.resolve("collector_utter");
            CollectorParquetDatasetService localDataset = new CollectorParquetDatasetService(collectorDir.toString(), localDuckDb);

            long c1Start = BASE_START;
            long c2MissingTwapStart = BASE_START + 1800L;
            Path pmFile1 = localDataset.getPolymarket15mFile("BTCUSDT", c1Start);
            Path pmFile2 = localDataset.getPolymarket15mFile("BTCUSDT", c2MissingTwapStart);
            Path cryptoPriceFile = localDataset.getPolymarketCryptoPriceFile("BTCUSDT", "2026-08-01");
            Path twap60sFile = localDataset.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");

            Files.createDirectories(pmFile1.getParent());
            Files.createDirectories(cryptoPriceFile.getParent());
            Files.createDirectories(twap60sFile.getParent());

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
            PerformanceMetricsCalculator localCalc = new PerformanceMetricsCalculator();
            try (BacktestHistoryRepository localHistory = new BacktestHistoryRepository(
                    tempDir.resolve("history_utter.duckdb"), new ObjectMapper(), localCalc)) {
                BacktestEngine utterEngine = new BacktestEngine(
                        localDataset,
                        new StrategyParameterService(localRegistry),
                        localCalc,
                        localHistory,
                        "UtterParameciumStrategy",
                        "2026-08-01",
                        10000.0,
                        220L,
                        100L,
                        50L,
                        50L
                );

                BacktestResult result = utterEngine.runBacktest(
                        null,
                        new BacktestRunRequest(
                                null, "utter_paramecium", "BTCUSDT", "2026-08-01", "2026-08-01",
                                false, 10000.0, 220L, null, null, null, null, false, false),
                        BacktestProgressListener.NOOP
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
            }
        }
    }

    @Test
    @DisplayName("Zero-Discrepancy: Bounded 76s CLOB lookback window must produce 100% identical trades, PnL, and Brier score as full 15m CLOB replay")
    void shouldProduceZeroDiscrepancyBetweenBoundedClobWindowAndFullCandleReplay(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager localDuckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp")) {
            Path collectorDir = tempDir.resolve("collector_equivalence");
            CollectorParquetDatasetService localDataset = new CollectorParquetDatasetService(collectorDir.toString(), localDuckDb);

            long c1Start = BASE_START;
            Path pmFile1 = localDataset.getPolymarket15mFile("BTCUSDT", c1Start);
            Path cryptoPriceFile = localDataset.getPolymarketCryptoPriceFile("BTCUSDT", "2026-08-01");
            Path twap60sFile = localDataset.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");

            Files.createDirectories(pmFile1.getParent());
            Files.createDirectories(cryptoPriceFile.getParent());
            Files.createDirectories(twap60sFile.getParent());

            try (Connection conn = localDuckDb.openSession();
                 Statement stmt = conn.createStatement()) {

                String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L - 300L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L - 300L) + "::UBIGINT), " +
                        "(" + ((c1Start + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60060.0::DOUBLE, '60060.0'::VARCHAR, 60::INT, " + ((c1Start + 900L) * 1000L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

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
            PerformanceMetricsCalculator eqCalc = new PerformanceMetricsCalculator();
            try (BacktestHistoryRepository eqHistory = new BacktestHistoryRepository(
                    tempDir.resolve("history_eq.duckdb"), new ObjectMapper(), eqCalc)) {
                BacktestEngine engine = new BacktestEngine(
                        localDataset,
                        new StrategyParameterService(registry),
                        eqCalc,
                        eqHistory,
                        "UtterParameciumStrategy",
                        "2026-08-01",
                        10000.0,
                        220L,
                        100L,
                        50L,
                        50L
                );

                BacktestResult windowedRes = engine.runBacktest(null, new BacktestRunRequest(
                        null, windowedStrategy.getName(), "BTCUSDT", "2026-08-01", "2026-08-01",
                        false, 10000.0, 220L, null, null, null, null, false, false), BacktestProgressListener.NOOP);
                BacktestResult fullRes = engine.runBacktest(null, new BacktestRunRequest(
                        null, fullReplayStrategy.getName(), "BTCUSDT", "2026-08-01", "2026-08-01",
                        false, 10000.0, 220L, null, null, null, null, false, false), BacktestProgressListener.NOOP);

                assertThat(windowedRes.trades()).isNotEmpty();
                assertThat(windowedRes.trades()).hasSameSizeAs(fullRes.trades());
                for (int i = 0; i < windowedRes.trades().size(); i++) {
                    assertThat(windowedRes.trades().get(i)).isEqualTo(fullRes.trades().get(i));
                }
                assertThat(windowedRes.metrics()).isEqualTo(fullRes.metrics());
            }
        }
    }

    @Test
    @DisplayName("Should warm up SpotTwap & FastSpotMicro over 15m pre-run window and preserve cross-candle strategy state without reset()")
    void shouldWarmUpSpotIndicatorsAndPreserveCrossCandleStrategyState(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager localDuckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp")) {
            Path collectorDir = tempDir.resolve("collector_warmup");
            CollectorParquetDatasetService localDataset = new CollectorParquetDatasetService(collectorDir.toString(), localDuckDb);

            long c1Start = BASE_START;          // 2026-08-01 00:00:00 UTC
            long c2Start = BASE_START + 900L;   // 2026-08-01 00:15:00 UTC
            String prevDate = "2026-07-31";
            String curDate = "2026-08-01";

            Path pmFile1 = localDataset.getPolymarket15mFile("BTCUSDT", c1Start);
            Path pmFile2 = localDataset.getPolymarket15mFile("BTCUSDT", c2Start);
            Path twapFile = localDataset.getPolymarketCryptoTwap60sFile("BTCUSDT", curDate);
            Path prevCbFile = localDataset.getCoinbaseSpotTradesFile("BTCUSDT", prevDate);
            Path curCbFile = localDataset.getCoinbaseSpotTradesFile("BTCUSDT", curDate);
            Path curKrFile = localDataset.getKrakenSpotTradesFile("BTCUSDT", curDate);
            Path curBnFile = localDataset.getBinanceSpotAggTradesFile("BTCUSDT", curDate);

            Files.createDirectories(pmFile1.getParent());
            Files.createDirectories(twapFile.getParent());
            Files.createDirectories(prevCbFile.getParent());
            Files.createDirectories(curKrFile.getParent());
            Files.createDirectories(curBnFile.getParent());

            try (Connection conn = localDuckDb.openSession();
                 Statement stmt = conn.createStatement()) {
                // Previous day (2026-07-31 23:55:00 UTC) warm-up trade within the 15-minute pre-run window
                String normPrevCb = prevCbFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L - 300_000L) + "::UBIGINT, 'BTC-USD'::VARCHAR, 1::BIGINT, 'sell'::VARCHAR, 59980.0::DOUBLE, 1.5::DOUBLE, '2026-07-31T23:55:00Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, price, qty, timestamp)) TO '" + normPrevCb + "' (FORMAT PARQUET)");

                String normCurCb = curCbFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L + 500L) + "::UBIGINT, 'BTC-USD'::VARCHAR, 2::BIGINT, 'sell'::VARCHAR, 60050.0::DOUBLE, 2.0::DOUBLE, '2026-08-01T00:00:00.500Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, price, qty, timestamp)) TO '" + normCurCb + "' (FORMAT PARQUET)");

                String normCurKr = curKrFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L + 500L) + "::UBIGINT, 'BTC/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'market'::VARCHAR, 60060.0::DOUBLE, 1.5::DOUBLE, '2026-08-01T00:00:00.500Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normCurKr + "' (FORMAT PARQUET)");

                String normCurBn = curBnFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 60040.0::DOUBLE, 2.5::DOUBLE, 1::BIGINT, 1::BIGINT, " + (c1Start * 1000L + 450L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                        ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normCurBn + "' (FORMAT PARQUET)");

                String normTwap = twapFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                        "(" + (c2Start * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60100.0::DOUBLE, '60100.0'::VARCHAR, 60::INT, " + (c2Start * 1000L) + "::UBIGINT), " +
                        "(" + ((c2Start + 900L) * 1000L) + "::UBIGINT, 3::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60200.0::DOUBLE, '60200.0'::VARCHAR, 60::INT, " + ((c2Start + 900L) * 1000L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

                TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, List.of(
                        ClobRow.discovered(c1Start * 1000L - 5000L, "101", "102"),
                        ClobRow.bookLevel(c1Start * 1000L + 2000L, c1Start * 1000L + 1990L, 4, true, true, 4900, 300.0),
                        ClobRow.bookLevel(c1Start * 1000L + 2000L, c1Start * 1000L + 1990L, 4, true, false, 5100, 300.0)
                ));
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile2, List.of(
                        ClobRow.discovered(c2Start * 1000L - 5000L, "201", "202"),
                        ClobRow.bookLevel(c2Start * 1000L + 2000L, c2Start * 1000L + 1990L, 4, true, true, 4900, 300.0),
                        ClobRow.bookLevel(c2Start * 1000L + 2000L, c2Start * 1000L + 1990L, 4, true, false, 5100, 300.0)
                ));
            }

            AtomicInteger resetCount = new AtomicInteger(0);
            List<Integer> observedCrossCandleMemory = new ArrayList<>();
            List<double[]> observedFastTicks = new ArrayList<>();

            class StatefulCrossCandleStrategy implements TradingStrategy, SpotTwapListener, FastSpotMicroListener {
                private int rollingMemoryCounter = 0;

                @Override
                public String getName() {
                    return "StatefulCrossCandleStrategy";
                }

                @Override
                public void init(StrategyContext context) {}

                @Override
                public void reset() {
                    resetCount.incrementAndGet();
                    rollingMemoryCounter = 0;
                }

                @Override
                public void onSpotTwap(long timestampMs, String symbol, double spotPrice, double twap60sPrice) {
                    rollingMemoryCounter++;
                }

                @Override
                public void onFastSpotMicro(long timestampMs, String symbol, double fastDirectUsd, double signedOfi1s, double syncMove500Usd) {
                    observedFastTicks.add(new double[]{timestampMs, fastDirectUsd, signedOfi1s});
                }

                @Override
                public void onOrderBookQuote(OrderBookQuote quote) {
                    observedCrossCandleMemory.add(rollingMemoryCounter);
                }

                @Override
                public void onExecutionReport(ExecutionReport report) {}
            }

            StatefulCrossCandleStrategy stateful = new StatefulCrossCandleStrategy();
            StrategyRegistry localRegistry = new StrategyRegistry(Map.of(stateful.getName(), stateful));
            PerformanceMetricsCalculator localCalc = new PerformanceMetricsCalculator();

            try (BacktestHistoryRepository localHistory = new BacktestHistoryRepository(
                    tempDir.resolve("history_warmup.duckdb"), new ObjectMapper(), localCalc)) {
                BacktestEngine localEngine = new BacktestEngine(
                        localDataset,
                        new StrategyParameterService(localRegistry),
                        localCalc,
                        localHistory,
                        stateful.getName(),
                        curDate,
                        10000.0,
                        220L,
                        100L,
                        50L,
                        50L
                );

                localEngine.runBacktest(null, new BacktestRunRequest(
                        null, stateful.getName(), "BTCUSDT", curDate, curDate,
                        false, 10000.0, 220L, null, null, null, null, false, false), BacktestProgressListener.NOOP);

                // reset() was called strictly ONCE at backtest start, NEVER between candle 1 and candle 2!
                assertThat(resetCount.get()).isEqualTo(1);
                // 1 warm-up tick from 2026-07-31 + 3 ticks in candle 1 = 4, preserved across candle 2!
                assertThat(observedCrossCandleMemory).containsExactly(4, 4);
                assertThat(observedFastTicks).hasSize(4);
                double[] latestFast = observedFastTicks.getLast();
                assertThat(latestFast[1]).isCloseTo(0.70 * 60050.0 + 0.30 * 60060.0, within(1e-4));
                assertThat(latestFast[2]).isBetween(0.95, 1.0);
            }
        }
    }

    @Test
    @DisplayName("Should reject FAK order during simulated ~196ms Polymarket speed bump when maker cancels best ask before matching")
    void shouldRejectFakOrderWhenMakerCancelsLiquidityDuringSpeedBump(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager localDuckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp")) {
            Path collectorDir = tempDir.resolve("collector_speedbump");
            CollectorParquetDatasetService localDataset = new CollectorParquetDatasetService(collectorDir.toString(), localDuckDb);

            long c1Start = BASE_START;
            long baseMs = c1Start * 1000L;
            String curDate = "2026-08-01";

            Path pmFile1 = localDataset.getPolymarket15mFile("BTCUSDT", c1Start);
            Path twapFile = localDataset.getPolymarketCryptoTwap60sFile("BTCUSDT", curDate);
            Files.createDirectories(pmFile1.getParent());
            Files.createDirectories(twapFile.getParent());

            try (Connection conn = localDuckDb.openSession();
                 Statement stmt = conn.createStatement()) {
                String normTwap = twapFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + baseMs + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + baseMs + "::UBIGINT), " +
                        "(" + (baseMs + 900_000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60100.0::DOUBLE, '60100.0'::VARCHAR, 60::INT, " + (baseMs + 900_000L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

                // At +1000ms ask is 0.50 (50 shares). Strategy submits FAK BUY @ 0.50.
                // At +1080ms (< +1196ms speed-bump match time!), maker cancels 0.50 ask and reprices to 0.56!
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, List.of(
                        ClobRow.discovered(baseMs - 5000L, "101", "102"),
                        ClobRow.bookLevel(baseMs + 1000L, baseMs + 990L, 4, true, true, 4800, 100.0),
                        ClobRow.bookLevel(baseMs + 1000L, baseMs + 990L, 4, true, false, 5000, 50.0),
                        ClobRow.priceChange(baseMs + 1080L, baseMs + 1070L, 8, true, false, 5000, 0.0),
                        ClobRow.priceChange(baseMs + 1080L, baseMs + 1070L, 8, true, false, 5600, 100.0)
                ));
            }

            List<ExecutionReport> reports = new ArrayList<>();
            TradingStrategy sniper = new TradingStrategy() {
                private StrategyContext ctx;
                private boolean sent = false;

                @Override
                public String getName() {
                    return "SpeedBumpTestStrategy";
                }

                @Override
                public void init(StrategyContext context) {
                    this.ctx = context;
                }

                @Override
                public void onOrderBookQuote(OrderBookQuote quote) {
                    if (!sent && quote.bestAskUp() <= 0.50) {
                        sent = true;
                        ctx.getExecutionRouter().submitOrder(new OrderCommand(
                                "fak-1", "mkt", "101", Timeframe.FIFTEEN_MINUTES,
                                "BUY", BigDecimal.valueOf(0.50), BigDecimal.valueOf(20.0)
                        ));
                    }
                }

                @Override
                public void onExecutionReport(ExecutionReport report) {
                    reports.add(report);
                }
            };

            PerformanceMetricsCalculator calc = new PerformanceMetricsCalculator();
            try (BacktestHistoryRepository localHistory = new BacktestHistoryRepository(
                    tempDir.resolve("history_sb.duckdb"), new ObjectMapper(), calc)) {
                BacktestEngine engine = new BacktestEngine(
                        localDataset,
                        new StrategyParameterService(new StrategyRegistry(Map.of(sniper.getName(), sniper))),
                        calc,
                        localHistory,
                        sniper.getName(),
                        curDate,
                        10000.0,
                        220L,
                        100L,
                        50L,
                        50L
                );

                BacktestResult res = engine.runBacktest(null, new BacktestRunRequest(
                        null, sniper.getName(), "BTCUSDT", curDate, curDate,
                        false, 10000.0, 220L, null, null, null, null, false, false), BacktestProgressListener.NOOP);

                assertThat(res.trades()).isEmpty();
                assertThat(reports).hasSize(1);
                assertThat(reports.getFirst().status()).isEqualTo("REJECTED");
                assertThat(reports.getFirst().message()).contains("no orders found to match with FAK order");
            }
        }
    }

    @Test
    @DisplayName("Exact PAPER == BACKTEST Parity: OraclePulseLockStrategy produces 100% bit-for-bit identical trades, fees, and equity in PAPER and BACKTEST")
    void shouldProduceExactBitForBitParityBetweenPaperEngineAndBacktestEngine(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager localDuckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp")) {
            Path collectorDir = tempDir.resolve("collector_parity");
            CollectorParquetDatasetService localDataset = new CollectorParquetDatasetService(collectorDir.toString(), localDuckDb);

            long c1Start = BASE_START;
            long c2Start = BASE_START + 900L;
            long c1Ms = c1Start * 1000L;
            long c2Ms = c2Start * 1000L;
            long c2EndMs = (c2Start + 900L) * 1000L;
            String curDate = "2026-08-01";

            Path pmFile1 = localDataset.getPolymarket15mFile("BTCUSDT", c1Start);
            Path pmFile2 = localDataset.getPolymarket15mFile("BTCUSDT", c2Start);
            Path cryptoFile = localDataset.getPolymarketCryptoPriceFile("BTCUSDT", curDate);
            Path twapFile = localDataset.getPolymarketCryptoTwap60sFile("BTCUSDT", curDate);
            Path cbFile = localDataset.getCoinbaseSpotTradesFile("BTCUSDT", curDate);

            Files.createDirectories(pmFile1.getParent());
            Files.createDirectories(cryptoFile.getParent());
            Files.createDirectories(twapFile.getParent());
            Files.createDirectories(cbFile.getParent());

            record TimedAction(long ts, int priority, long seq, Runnable task) implements Comparable<TimedAction> {
                @Override
                public int compareTo(TimedAction o) {
                    int c = Long.compare(ts, o.ts);
                    if (c != 0) return c;
                    int p = Integer.compare(priority, o.priority);
                    return p != 0 ? p : Long.compare(seq, o.seq);
                }
            }

            // Build an identical 2-candle realistic scenario (OraclePulseLockStrategy) for both Parquet BACKTEST and direct PAPER
            OraclePulseLockStrategyConfig cfg = OraclePulseLockStrategyConfig.defaults();
            OraclePulseLockStrategy paperStrategy = new OraclePulseLockStrategy(cfg);
            PolymarketOrderBook paperBook = new PolymarketOrderBook();
            CoreTradingEngine paperEngine = new CoreTradingEngine(
                    paperBook,
                    new SpotTwapIndicator(),
                    new FastSpotMicroIndicator(FastSpotMicroIndicatorConfig.defaults()),
                    paperStrategy,
                    "BTCUSDT",
                    10_000.0
            );
            SimulatedClobExecutionEngine paperSim = new SimulatedClobExecutionEngine(paperBook, 220L);
            paperEngine.setExecutionGateway(paperSim);

            PriorityQueue<TimedAction> paperQueue = new PriorityQueue<>();
            AtomicInteger actionSeq = new AtomicInteger();
            paperSim.bind(
                    (targetTs, action) -> paperQueue.add(new TimedAction(targetTs, 1, actionSeq.incrementAndGet(), action)),
                    paperEngine::onExecutionReport
            );

            List<String> twapValuesSql = new ArrayList<>();
            List<String> cryptoValuesSql = new ArrayList<>();
            List<String> cbValuesSql = new ArrayList<>();
            List<ClobRow> c1ClobRows = new ArrayList<>();
            List<ClobRow> c2ClobRows = new ArrayList<>();

            c1ClobRows.add(ClobRow.discovered(c1Ms - 5000L, "101", "102"));
            c2ClobRows.add(ClobRow.discovered(c2Ms - 5000L, "201", "202"));

            // Candle 1: opens at 60,000, quiet volatility around 60,002 for 870s, then strong +$180 spot + oracle impulse at +875s while ask is still 0.54!
            twapValuesSql.add("(" + c1Ms + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + c1Ms + "::UBIGINT)");
            twapValuesSql.add("(" + c2Ms + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60180.0::DOUBLE, '60180.0'::VARCHAR, 60::INT, " + c2Ms + "::UBIGINT)");
            twapValuesSql.add("(" + c2EndMs + "::UBIGINT, 3::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60040.0::DOUBLE, '60040.0'::VARCHAR, 60::INT, " + c2EndMs + "::UBIGINT)");

            paperQueue.add(new TimedAction(c1Ms, 0, actionSeq.incrementAndGet(),
                    () -> paperEngine.beginCandleCycle(c1Start, c2Start, 60000.0, "101", "102", true)));
            paperQueue.add(new TimedAction(c1Ms, 9, actionSeq.incrementAndGet(),
                    () -> paperEngine.onPolyBoltCryptoTwap60s(c1Ms, "btcusd", "chainlink", 60000.0, 60, c1Ms)));

            int seq = 0;
            for (int sec = 800; sec <= 885; sec++) {
                long ts = c1Ms + sec * 1000L;
                double p = sec < 872 ? (60002.0 + (sec % 2)) : 60180.0;
                seq++;
                cryptoValuesSql.add("(" + ts + "::UBIGINT, " + seq + "::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, "
                        + p + "::DOUBLE, '" + p + "'::VARCHAR, " + ts + "::UBIGINT)");
                paperQueue.add(new TimedAction(ts, 8, seq,
                        () -> paperEngine.onPolyBoltCryptoPrice(ts, "btcusd", "chainlink", p, ts)));

                long cbSrvTs = ts - 50L; // +50ms coinbase delay => arrives at ts
                int cbId = seq;
                cbValuesSql.add("(" + cbSrvTs + "::UBIGINT, 'BTC-USD'::VARCHAR, " + cbId + "::BIGINT, 'sell'::VARCHAR, "
                        + p + "::DOUBLE, 2.5::DOUBLE, '2026-08-01T00:14:00Z'::VARCHAR)");
                paperQueue.add(new TimedAction(ts, 7, cbId,
                        () -> paperEngine.onCoinbaseUsdTrade(ts, p, 2.5, true)));
            }

            long c1Book1Ts = c1Ms + 870_000L;
            long c1Book2Ts = c1Ms + 875_050L;
            c1ClobRows.add(ClobRow.bookLevel(c1Book1Ts, c1Book1Ts - 12L, 4, true, true, 5200, 400.0));
            c1ClobRows.add(ClobRow.bookLevel(c1Book1Ts, c1Book1Ts - 12L, 4, true, false, 5400, 400.0));
            c1ClobRows.add(ClobRow.priceChange(c1Book2Ts, c1Book2Ts - 12L, 8, true, false, 5500, 400.0));
            c1ClobRows.add(ClobRow.bboHint(c1Book2Ts, c1Book2Ts - 12L, 8, true, false, 5500));

            paperQueue.add(new TimedAction(c1Book1Ts, 5, actionSeq.incrementAndGet(), () -> {
                paperBook.updateObservedLatency(c1Book1Ts, c1Book1Ts - 12L);
                paperBook.tryAcceptServerTimestamp(true, c1Book1Ts - 12L);
                paperBook.clearSideForSnapshot(true);
                paperBook.setSnapshotLevel(true, true, 5200, 400.0);
                paperBook.setSnapshotLevel(true, false, 5400, 400.0);
                paperBook.finishBookSnapshot(true, c1Book1Ts);
                paperEngine.onOrderBookUpdated(c1Book1Ts);
            }));
            paperQueue.add(new TimedAction(c1Book2Ts, 5, actionSeq.incrementAndGet(), () -> {
                paperBook.updateObservedLatency(c1Book2Ts, c1Book2Ts - 12L);
                paperBook.tryAcceptServerTimestamp(true, c1Book2Ts - 12L);
                paperBook.applyTickDelta(true, false, 5500, 400.0, c1Book2Ts);
                paperBook.applyBboHint(true, -1, 5500, c1Book2Ts);
                paperEngine.onOrderBookUpdated(c1Book2Ts);
            }));
            paperQueue.add(new TimedAction(c2Ms, 99, actionSeq.incrementAndGet(),
                    () -> paperEngine.resolveCandle(TradeDirection.UP, 60180.0, c2Ms)));

            // Candle 2: opens at 60,180, drops sharply to 60,040 (-$140 DOWN impulse) at +875s while DOWN ask is 0.54!
            paperQueue.add(new TimedAction(c2Ms + 1L, 0, actionSeq.incrementAndGet(), () -> {})); // ordering anchor
            // Note: in BacktestEngine, c2 CANDLE_START runs right after c1 CANDLE_CLOSE at timestamp c2Ms:
            List<TimedAction> candle2PaperActions = new ArrayList<>();
            candle2PaperActions.add(new TimedAction(c2Ms, 0, actionSeq.incrementAndGet(),
                    () -> paperEngine.beginCandleCycle(c2Start, c2Start + 900L, 60180.0, "201", "202", true)));
            candle2PaperActions.add(new TimedAction(c2Ms, 9, actionSeq.incrementAndGet(),
                    () -> paperEngine.onPolyBoltCryptoTwap60s(c2Ms, "btcusd", "chainlink", 60180.0, 60, c2Ms)));

            for (int sec = 800; sec <= 885; sec++) {
                long ts = c2Ms + sec * 1000L;
                double p = sec < 872 ? (60178.0 - (sec % 2)) : 60040.0;
                seq++;
                cryptoValuesSql.add("(" + ts + "::UBIGINT, " + seq + "::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, "
                        + p + "::DOUBLE, '" + p + "'::VARCHAR, " + ts + "::UBIGINT)");
                int s = seq;
                candle2PaperActions.add(new TimedAction(ts, 8, s,
                        () -> paperEngine.onPolyBoltCryptoPrice(ts, "btcusd", "chainlink", p, ts)));

                long cbSrvTs = ts - 50L;
                cbValuesSql.add("(" + cbSrvTs + "::UBIGINT, 'BTC-USD'::VARCHAR, " + s + "::BIGINT, 'buy'::VARCHAR, "
                        + p + "::DOUBLE, 2.5::DOUBLE, '2026-08-01T00:29:00Z'::VARCHAR)");
                candle2PaperActions.add(new TimedAction(ts, 7, s,
                        () -> paperEngine.onCoinbaseUsdTrade(ts, p, 2.5, false)));
            }

            long c2BookTs = c2Ms + 875_050L;
            c2ClobRows.add(ClobRow.bookLevel(c2BookTs, c2BookTs - 12L, 4, false, true, 5200, 400.0));
            c2ClobRows.add(ClobRow.bookLevel(c2BookTs, c2BookTs - 12L, 4, false, false, 5400, 400.0));
            candle2PaperActions.add(new TimedAction(c2BookTs, 5, actionSeq.incrementAndGet(), () -> {
                paperBook.updateObservedLatency(c2BookTs, c2BookTs - 12L);
                paperBook.tryAcceptServerTimestamp(false, c2BookTs - 12L);
                paperBook.clearSideForSnapshot(false);
                paperBook.setSnapshotLevel(false, true, 5200, 400.0);
                paperBook.setSnapshotLevel(false, false, 5400, 400.0);
                paperBook.finishBookSnapshot(false, c2BookTs);
                paperEngine.onOrderBookUpdated(c2BookTs);
            }));
            candle2PaperActions.add(new TimedAction(c2EndMs, 99, actionSeq.incrementAndGet(),
                    () -> paperEngine.resolveCandle(TradeDirection.DOWN, 60040.0, c2EndMs)));

            // Execute Candle 1 on PAPER engine, then Candle 2 on PAPER engine
            while (!paperQueue.isEmpty()) {
                TimedAction act = paperQueue.poll();
                paperEngine.setCurrentClockMs(act.ts());
                act.task().run();
            }
            paperQueue.addAll(candle2PaperActions);
            while (!paperQueue.isEmpty()) {
                TimedAction act = paperQueue.poll();
                paperEngine.setCurrentClockMs(act.ts());
                act.task().run();
            }

            // Persist identical Parquet archives and run BacktestEngine
            try (Connection conn = localDuckDb.openSession();
                 Statement stmt = conn.createStatement()) {
                stmt.execute("COPY (SELECT * FROM (VALUES " + String.join(", ", twapValuesSql)
                        + ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '"
                        + twapFile.toAbsolutePath().toString().replace('\\', '/') + "' (FORMAT PARQUET)");
                stmt.execute("COPY (SELECT * FROM (VALUES " + String.join(", ", cryptoValuesSql)
                        + ") AS t(ts, seq, symbol, source, price, full_accuracy_price, price_timestamp)) TO '"
                        + cryptoFile.toAbsolutePath().toString().replace('\\', '/') + "' (FORMAT PARQUET)");
                stmt.execute("COPY (SELECT * FROM (VALUES " + String.join(", ", cbValuesSql)
                        + ") AS t(trade_timestamp, symbol, trade_id, side, price, qty, timestamp)) TO '"
                        + cbFile.toAbsolutePath().toString().replace('\\', '/') + "' (FORMAT PARQUET)");

                TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, c1ClobRows);
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile2, c2ClobRows);
            }

            OraclePulseLockStrategy btStrategy = new OraclePulseLockStrategy(cfg);
            PerformanceMetricsCalculator calc = new PerformanceMetricsCalculator();
            try (BacktestHistoryRepository localHistory = new BacktestHistoryRepository(
                    tempDir.resolve("history_parity.duckdb"), new ObjectMapper(), calc)) {
                BacktestEngine btEngine = new BacktestEngine(
                        localDataset,
                        new StrategyParameterService(new StrategyRegistry(Map.of(btStrategy.getName(), btStrategy))),
                        calc,
                        localHistory,
                        btStrategy.getName(),
                        curDate,
                        10_000.0,
                        220L,
                        100L,
                        50L,
                        50L
                );

                BacktestResult btResult = btEngine.runBacktest(null, new BacktestRunRequest(
                        null, btStrategy.getName(), "BTCUSDT", curDate, curDate,
                        false, 10_000.0, 220L, 100L, 50L, 50L, null, false, false), BacktestProgressListener.NOOP);

                List<TradeRecord> paperTrades = paperEngine.getCompletedTradesHistory();
                assertThat(btResult.trades()).isNotEmpty();
                assertThat(btResult.trades()).isEqualTo(paperTrades);
                assertThat(btResult.metrics().finalBalance()).isCloseTo(paperEngine.getRealizedEquityUsd(), within(1e-9));
            }
        }
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when unknown strategy requested")
    void shouldThrowWhenUnknownStrategy() {
        BacktestRunRequest req = new BacktestRunRequest(
                null, "UnknownStrategy", "BTCUSDT", null, null, false, 10000.0, null, null, null, null, null, false, false);
        assertThatThrownBy(() -> backtestEngine.runBacktest(null, req, BacktestProgressListener.NOOP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown strategy");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when no market rows available")
    void shouldThrowWhenNoMarketRowsAvailable() {
        BacktestRunRequest req = new BacktestRunRequest(
                null, "UtterParameciumStrategy", "UNKNOWN_SYMBOL", null, null, false, 10000.0, null, null, null, null, null, false, false);
        assertThatThrownBy(() -> backtestEngine.runBacktest(null, req, BacktestProgressListener.NOOP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No market rows available");
    }

    @Test
    @DisplayName("Should resolve default strategy, default startDate, current UTC endDate, default capital, and support fullRange & strategyParams")
    void shouldResolveDefaultParametersWhenOmitted() {
        BacktestRunRequest req = new BacktestRunRequest(
                null, null, "BTCUSDT", null, null, true, null, null, null, null, null,
                Map.of("minTauMs", 3000L), false, false);
        BacktestResult result = backtestEngine.runBacktest(null, req, BacktestProgressListener.NOOP);

        assertThat(result).isNotNull();
        assertThat(result.strategyName()).isEqualTo("OraclePulseLockStrategy");
        assertThat(result.totalMarkets()).isEqualTo(3);
        assertThat(result.metrics().initialCapital()).isEqualTo(10000.0);
        assertThat(result.config().orderGatewayDelayMs()).isEqualTo(220L);
        assertThat(result.config().strategyOverrides()).containsEntry("minTauMs", 3000L);
        assertThat(result.marketPoints()).hasSize(3);
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when startDate is after endDate")
    void shouldThrowWhenStartDateIsAfterEndDate() {
        BacktestRunRequest req = new BacktestRunRequest(
                null, "UtterParameciumStrategy", "BTCUSDT", "2026-08-10", "2026-08-01",
                false, 10000.0, null, null, null, null, null, false, false);
        assertThatThrownBy(() -> backtestEngine.runBacktest(null, req, BacktestProgressListener.NOOP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("startDate must not be after endDate");
    }

    @Test
    @DisplayName("Should support cooperative cancellation via BacktestProgressListener")
    void shouldCancelBacktestCooperatively() {
        BacktestRunRequest req = new BacktestRunRequest(
                null, "PolymarketOnlyStrategy", "BTCUSDT", "2026-08-01", "2026-08-01",
                false, 10000.0, null, null, null, null, null, false, false);
        BacktestProgressListener cancelledListener = new BacktestProgressListener() {
            @Override
            public boolean isCancelled() {
                return true;
            }
        };
        assertThatThrownBy(() -> backtestEngine.runBacktest(null, req, cancelledListener))
                .isInstanceOf(BacktestCancelledException.class)
                .hasMessageContaining("cancelled");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when any configured gateway or exchange delay is negative")
    void shouldThrowWhenDelayIsNegative(@TempDir Path tempDir) {
        CollectorParquetDatasetService localDataset = new CollectorParquetDatasetService(tempDir.toString(), duckDbSessionManager);
        StrategyParameterService paramService = new StrategyParameterService(new StrategyRegistry(Map.of()));
        PerformanceMetricsCalculator calc = new PerformanceMetricsCalculator();

        assertThatThrownBy(() -> new BacktestEngine(
                localDataset, paramService, calc, historyRepository,
                "UtterParameciumStrategy", "2026-08-01", 10000.0, -1L,
                100L, 50L, 50L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("defaultOrderGatewayDelayMs");

        assertThatThrownBy(() -> new BacktestEngine(
                localDataset, paramService, calc, historyRepository,
                "UtterParameciumStrategy", "2026-08-01", 10000.0, 220L,
                -1L, 50L, 50L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("binanceDelayMs");

        assertThatThrownBy(() -> new BacktestEngine(
                localDataset, paramService, calc, historyRepository,
                "UtterParameciumStrategy", "2026-08-01", 10000.0, 220L,
                100L, -1L, 50L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("krakenDelayMs");

        assertThatThrownBy(() -> new BacktestEngine(
                localDataset, paramService, calc, historyRepository,
                "UtterParameciumStrategy", "2026-08-01", 10000.0, 220L,
                100L, 50L, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("coinbaseDelayMs");
    }

    @Test
    @DisplayName("Should auto-merge new days, skip stored days by default, overwrite on flag, and create distinct run when forceNewRun=true")
    void shouldAutoMergeNewDaysSkipExistingOverwriteAndForceNewRun() throws Exception {
        BacktestResult run1 = backtestEngine.runBacktest(null, new BacktestRunRequest(
                "Incremental Test", "PolymarketOnlyStrategy", "BTCUSDT",
                "2026-08-01", "2026-08-01", false, 10000.0, 220L, null, null, null, null, false, false),
                BacktestProgressListener.NOOP);
        assertThat(run1.mergedIntoExisting()).isFalse();
        assertThat(run1.skippedDays()).isZero();
        assertThat(run1.totalMarkets()).isEqualTo(3);
        int initialTrades = run1.trades().size();

        BacktestResult allSkipped = backtestEngine.runBacktest(null, new BacktestRunRequest(
                null, "PolymarketOnlyStrategy", "BTCUSDT",
                "2026-08-01", "2026-08-01", false, 10000.0, 220L, null, null, null, null, false, false),
                BacktestProgressListener.NOOP);
        assertThat(allSkipped.runId()).isEqualTo(run1.runId());
        assertThat(allSkipped.label()).isEqualTo("Incremental Test");
        assertThat(allSkipped.mergedIntoExisting()).isTrue();
        assertThat(allSkipped.skippedDays()).isEqualTo(1);
        assertThat(allSkipped.totalMarkets()).isEqualTo(3);

        long day2Start = BASE_START + 86_400L;
        Path pmFileDay2 = datasetService.getPolymarket15mFile("BTCUSDT", day2Start);
        Path cryptoPriceDay2 = datasetService.getPolymarketCryptoPriceFile("BTCUSDT", "2026-08-02");
        Path twap60sDay2 = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-02");
        Path spotDay2 = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", "2026-08-02");
        Files.createDirectories(pmFileDay2.getParent());
        Files.createDirectories(cryptoPriceDay2.getParent());
        Files.createDirectories(twap60sDay2.getParent());
        Files.createDirectories(spotDay2.getParent());

        try (Connection conn = duckDbSessionManager.openSession();
             Statement stmt = conn.createStatement()) {
            String normTwap = twap60sDay2.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (day2Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (day2Start * 1000L) + "::UBIGINT), " +
                    "(" + ((day2Start + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60120.0::DOUBLE, '60120.0'::VARCHAR, 60::INT, " + ((day2Start + 900L) * 1000L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

            String normCrypto = cryptoPriceDay2.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + ((day2Start + 60L) * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60120.0::DOUBLE, '60120.0'::VARCHAR, " + ((day2Start + 60L) * 1000L - 5L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, price, full_accuracy_price, price_timestamp)) TO '" + normCrypto + "' (FORMAT PARQUET)");

            String normSpot = spotDay2.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 5.0::DOUBLE, 1::BIGINT, 1::BIGINT, " + (day2Start * 1000L + 100L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                    ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normSpot + "' (FORMAT PARQUET)");

            TypedClobParquetTestHelper.writeParquet(stmt, pmFileDay2, List.of(
                    ClobRow.discovered(day2Start * 1000L - 5000L, "401", "402"),
                    ClobRow.bookLevel((day2Start + 60L) * 1000L + 10L, (day2Start + 60L) * 1000L, 4, true, true, 4900, 300.0),
                    ClobRow.bookLevel((day2Start + 60L) * 1000L + 10L, (day2Start + 60L) * 1000L, 4, true, false, 5000, 300.0)
            ));
        }

        BacktestResult merged = backtestEngine.runBacktest(null, new BacktestRunRequest(
                null, "PolymarketOnlyStrategy", "BTCUSDT",
                "2026-08-01", "2026-08-02", false, 10000.0, 220L, null, null, null, null, false, false),
                BacktestProgressListener.NOOP);
        assertThat(merged.runId()).isEqualTo(run1.runId());
        assertThat(merged.label()).isEqualTo("Incremental Test");
        assertThat(merged.mergedIntoExisting()).isTrue();
        assertThat(merged.skippedDays()).isEqualTo(1);
        assertThat(merged.totalMarkets()).isEqualTo(4);
        assertThat(merged.config().startDate()).isEqualTo("2026-08-01");
        assertThat(merged.config().endDate()).isEqualTo("2026-08-02");
        assertThat(merged.trades().size()).isGreaterThan(initialTrades);

        BacktestResult overwritten = backtestEngine.runBacktest(null, new BacktestRunRequest(
                null, "PolymarketOnlyStrategy", "BTCUSDT",
                "2026-08-02", "2026-08-02", false, 10000.0, 220L, null, null, null, null, false, true),
                BacktestProgressListener.NOOP);
        assertThat(overwritten.runId()).isEqualTo(run1.runId());
        assertThat(overwritten.mergedIntoExisting()).isTrue();
        assertThat(overwritten.skippedDays()).isZero();
        assertThat(overwritten.totalMarkets()).isEqualTo(4);
        assertThat(overwritten.trades()).hasSameSizeAs(merged.trades());

        BacktestResult forced = backtestEngine.runBacktest(null, new BacktestRunRequest(
                "Forced Run", "PolymarketOnlyStrategy", "BTCUSDT",
                "2026-08-02", "2026-08-02", false, 10000.0, 220L, null, null, null, null, true, false),
                BacktestProgressListener.NOOP);
        assertThat(forced.runId()).isNotEqualTo(run1.runId());
        assertThat(forced.label()).isEqualTo("Forced Run");
        assertThat(forced.mergedIntoExisting()).isFalse();
        assertThat(forced.skippedDays()).isZero();
        assertThat(forced.totalMarkets()).isEqualTo(1);
    }
}
