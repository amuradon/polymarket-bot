package cz.polymarket.bot.backtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper.ClobRow;
import cz.polymarket.bot.backtest.data.BacktestJob;
import cz.polymarket.bot.backtest.data.BacktestJobStatus;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.export.BacktestJsonExporter;
import cz.polymarket.bot.backtest.service.BacktestJobManager;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.backtest.stream.PolymarketOrderBook;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.ExternalStreamType;
import cz.polymarket.bot.strategy.IndicatorType;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.StrategyContext;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradeDirection;
import cz.polymarket.bot.strategy.TradingStrategy;
import org.jboss.logmanager.ExtHandler;
import org.jboss.logmanager.ExtLogRecord;
import org.jboss.logmanager.Level;
import org.jboss.logmanager.Logger;
import org.jboss.logmanager.formatters.PatternFormatter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Comprehensive verification of native Quarkus {@link io.quarkus.logging.Log} usage,
 * log level hierarchy (FATAL, ERROR, WARN, INFO, DEBUG, TRACE), strict level-guarding
 * (zero execution of lower-level logging blocks when disabled), and log format configuration.
 */
class BacktestLoggingTest {

    private static final long BASE_START = 1785542400L; // 2026-08-01 00:00:00 UTC
    private static final PatternFormatter MESSAGE_FORMATTER = new PatternFormatter("%s");

    private static String formatRecord(ExtLogRecord record) {
        return MESSAGE_FORMATTER.format(record);
    }

    private Logger backtestLogger;
    private java.util.logging.Level originalLevel;
    private CapturingLogHandler logHandler;
    private DuckDbSessionManager activeDuckDb;
    private BacktestEngine activeEngine;

    @BeforeEach
    void setUpLogger() {
        backtestLogger = Logger.getLogger("cz.polymarket.bot.backtest");
        originalLevel = backtestLogger.getLevel();
        logHandler = new CapturingLogHandler();
        logHandler.setLevel(Level.ALL);
        backtestLogger.addHandler(logHandler);
    }

    @AfterEach
    void tearDownLogger() {
        if (activeEngine != null) {
            activeEngine.shutdown();
            activeEngine = null;
        }
        if (activeDuckDb != null) {
            activeDuckDb.close();
            activeDuckDb = null;
        }
        if (backtestLogger != null && logHandler != null) {
            backtestLogger.removeHandler(logHandler);
            backtestLogger.setLevel(originalLevel != null ? originalLevel : Level.INFO);
        }
    }

    @Test
    @DisplayName("All backtest source files using logging must use io.quarkus.logging.Log and never SLF4J or JBoss Logger")
    void shouldUseQuarkusLogInsteadOfSlf4jOrJbossLogger() throws IOException {
        Path mainJavaDir = Path.of("src/main/java");
        try (Stream<Path> paths = Files.walk(mainJavaDir)) {
            List<Path> javaFiles = paths.filter(p -> p.toString().endsWith(".java")).toList();
            assertThat(javaFiles).isNotEmpty();

            int quarkusLogImports = 0;
            for (Path file : javaFiles) {
                String content = Files.readString(file);
                assertThat(content)
                        .as("File %s must not import org.slf4j", file.getFileName())
                        .doesNotContain("org.slf4j");
                assertThat(content)
                        .as("File %s must not import org.jboss.logging.Logger", file.getFileName())
                        .doesNotContain("import org.jboss.logging.Logger;");
                if (content.contains("import io.quarkus.logging.Log;")) {
                    quarkusLogImports++;
                }
            }
            assertThat(quarkusLogImports).isGreaterThanOrEqualTo(6);
        }
    }

    @Test
    @DisplayName("application.properties should configure quarkus.log.console.format as <datetime> <logLevel> <loggerName> <message>")
    void shouldConfigureConsoleLogPatternInApplicationProperties() throws IOException {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(Path.of("src/main/resources/application.properties"))) {
            props.load(in);
        }
        String format = props.getProperty("quarkus.log.console.format");
        assertThat(format)
                .as("quarkus.log.console.format must be defined in application.properties")
                .isNotNull()
                .contains("%d")
                .contains("%p")
                .contains("%c")
                .contains("%s");
    }

    @Test
    @DisplayName("At INFO level, should log INFO (UTC day progress & completion) and WARN (skipped market), while DEBUG and TRACE do not execute")
    void shouldLogInfoAndWarnWhileSkippingDebugAndTraceWhenLevelIsInfo(@TempDir Path tempDir) throws Exception {
        backtestLogger.setLevel(Level.INFO);
        BacktestEngine engine = createSyntheticBacktestEngine(tempDir);

        engine.runBacktest("TestBatchStrategy", "BTCUSDT", "2026-08-01", "2026-08-01", 10000.0, tempDir.resolve("out").toString(), 1.0);

        List<ExtLogRecord> records = logHandler.records();
        assertThat(records).anySatisfy(r -> {
            assertThat(r.getLevel()).isEqualTo(Level.INFO);
            assertThat(formatRecord(r)).contains("Processing UTC day");
        });
        assertThat(records).anySatisfy(r -> {
            assertThat(r.getLevel()).isEqualTo(Level.WARN);
            assertThat(formatRecord(r)).contains("Skipping market");
        });
        assertThat(records)
                .filteredOn(r -> r.getLevel().intValue() < Level.INFO.intValue())
                .as("DEBUG and TRACE records must not be emitted when level is INFO")
                .isEmpty();
    }

    @Test
    @DisplayName("At DEBUG level, should log per-market processing and summary results at DEBUG while skipping TRACE trade details")
    void shouldLogMarketSummaryAtDebugWhileSkippingTrace(@TempDir Path tempDir) throws Exception {
        backtestLogger.setLevel(Level.DEBUG);
        BacktestEngine engine = createSyntheticBacktestEngine(tempDir);

        engine.runBacktest("TestBatchStrategy", "BTCUSDT", "2026-08-01", "2026-08-01", 10000.0, tempDir.resolve("out").toString(), 1.0);

        List<ExtLogRecord> records = logHandler.records();
        assertThat(records).anySatisfy(r -> {
            assertThat(r.getLevel()).isEqualTo(Level.DEBUG);
            assertThat(formatRecord(r)).contains("Processing market");
        });
        assertThat(records).anySatisfy(r -> {
            assertThat(r.getLevel()).isEqualTo(Level.DEBUG);
            assertThat(formatRecord(r)).contains("Market summary");
        });
        assertThat(records)
                .filteredOn(r -> r.getLevel().equals(Level.TRACE))
                .as("TRACE records must not be emitted when level is DEBUG")
                .isEmpty();
    }

    @Test
    @DisplayName("At TRACE level, should log every individual trade and order fill at TRACE")
    void shouldLogEveryIndividualTradeAtTrace(@TempDir Path tempDir) throws Exception {
        backtestLogger.setLevel(Level.TRACE);
        BacktestEngine engine = createSyntheticBacktestEngine(tempDir);

        engine.runBacktest("TestBatchStrategy", "BTCUSDT", "2026-08-01", "2026-08-01", 10000.0, tempDir.resolve("out").toString(), 1.0);

        List<ExtLogRecord> traceRecords = logHandler.records().stream()
                .filter(r -> r.getLevel().equals(Level.TRACE))
                .toList();
        assertThat(traceRecords).isNotEmpty();
        assertThat(traceRecords).anySatisfy(r ->
                assertThat(formatRecord(r)).contains("Trade executed")
        );
    }

    @Test
    @DisplayName("Should log ERROR when invalid mandatory argument or missing market data prevents backtest run")
    void shouldLogErrorOnInvalidArgumentOrMissingMarkets(@TempDir Path tempDir) throws Exception {
        backtestLogger.setLevel(Level.INFO);
        BacktestEngine engine = createSyntheticBacktestEngine(tempDir);

        assertThatThrownBy(() -> engine.runBacktest("", "BTCUSDT", "2026-08-01", "2026-08-01", 10000.0, null, 1.0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(logHandler.records()).anySatisfy(r -> {
            assertThat(r.getLevel()).isEqualTo(Level.ERROR);
            assertThat(formatRecord(r)).contains("Strategy name is mandatory");
        });

        logHandler.clear();

        assertThatThrownBy(() -> engine.runBacktest("TestBatchStrategy", "SOLUSDT", "2026-08-01", "2026-08-01", 10000.0, null, 1.0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(logHandler.records()).anySatisfy(r -> {
            assertThat(r.getLevel()).isEqualTo(Level.ERROR);
            assertThat(formatRecord(r)).contains("No market rows available");
        });
    }

    @Test
    @DisplayName("Should log ERROR on out-of-bounds tick level in PolymarketOrderBook")
    void shouldLogErrorOnInvalidOrderBookTickLevel() {
        backtestLogger.setLevel(Level.INFO);
        PolymarketOrderBook book = new PolymarketOrderBook();

        book.applyTickDelta(true, false, 15_000, 100.0, 1000L);

        assertThat(logHandler.records()).anySatisfy(r -> {
            assertThat(r.getLevel()).isEqualTo(Level.ERROR);
            assertThat(formatRecord(r)).contains("Invalid order book level");
        });
    }

    @Test
    @DisplayName("Should log FATAL when BacktestJobManager encounters an unrecoverable VirtualMachineError / Error")
    void shouldLogFatalWhenEngineCannotContinueInBacktestJobManager() {
        backtestLogger.setLevel(Level.INFO);
        BacktestEngine mockEngine = Mockito.mock(BacktestEngine.class);
        when(mockEngine.runBacktest(any(), any(), any(), any(), any(), any(), any()))
                .thenThrow(new OutOfMemoryError("Simulated fatal heap exhaustion"));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            BacktestJobManager manager = new BacktestJobManager(mockEngine, executor);
            BacktestJob job = manager.submitJob(new BacktestRunRequest(
                    "TestBatchStrategy", "BTCUSDT", "2026-08-01", "2026-08-01", 10000.0, null, 1.0
            ));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Optional<BacktestJob> current = manager.getJob(job.jobId());
                assertThat(current).isPresent();
                assertThat(current.get().status()).isEqualTo(BacktestJobStatus.FAILED);
            });

            assertThat(logHandler.records()).anySatisfy(r -> {
                assertThat(r.getLevel()).isEqualTo(Level.FATAL);
                assertThat(formatRecord(r)).contains("Fatal error");
            });
        } finally {
            executor.shutdownNow();
        }
    }

    private BacktestEngine createSyntheticBacktestEngine(Path tempDir) throws Exception {
        activeDuckDb = new DuckDbSessionManager("256MB");
        Path collectorDir = tempDir.resolve("collector_log_test");
        CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString(), activeDuckDb);

        long c1Start = BASE_START;
        long c2MissingOpenStart = BASE_START + 1800L;

        Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
        Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");
        Files.createDirectories(pmDir);
        Files.createDirectories(twap60sFile.getParent());

        Path pmFile1 = pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet");
        Path pmFile2 = pmDir.resolve("btc-updown-15m-" + c2MissingOpenStart + ".parquet");

        try (Connection conn = activeDuckDb.openSession();
             Statement stmt = conn.createStatement()) {

            String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                    "(" + ((c1Start + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60100.0::DOUBLE, '60100.0'::VARCHAR, 60::INT, " + ((c1Start + 900L) * 1000L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

            TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, List.of(
                    ClobRow.discovered(c1Start * 1000L - 5000L, "101", "102"),
                    ClobRow.bookLevel((c1Start + 60L) * 1000L + 10L, (c1Start + 60L) * 1000L, 4, true, true, 4900, 300.0),
                    ClobRow.bookLevel((c1Start + 60L) * 1000L + 10L, (c1Start + 60L) * 1000L, 4, true, false, 5000, 300.0)
            ));

            TypedClobParquetTestHelper.writeParquet(stmt, pmFile2, List.of(
                    ClobRow.discovered(c2MissingOpenStart * 1000L - 5000L, "201", "202")
            ));
        }

        TradingStrategy testStrategy = new TradingStrategy() {
            private StrategyContext ctx;
            private boolean ordered;

            @Override
            public String getName() {
                return "TestBatchStrategy";
            }

            @Override
            public Set<IndicatorType> getRequiredIndicators() {
                return Set.of();
            }

            @Override
            public Set<ExternalStreamType> getRequiredExternalStreams() {
                return Set.of();
            }

            @Override
            public TradingStrategy createInstance() {
                return this;
            }

            @Override
            public void init(StrategyContext context) {
                this.ctx = context;
            }

            @Override
            public void reset() {
                this.ordered = false;
            }

            @Override
            public void onOrderBookQuote(OrderBookQuote quote) {
                if (!ordered && !Double.isNaN(quote.bestAskUp())) {
                    ordered = true;
                    ctx.getExecutionRouter().submitOrder(new OrderCommand(
                            "test-ord-1", "mkt-1", "BTC-UP", Timeframe.FIFTEEN_MINUTES,
                            "BUY", BigDecimal.valueOf(quote.bestAskUp()), BigDecimal.valueOf(20.0)
                    ));
                }
            }

            @Override
            public void onCandleResolution(TradeDirection actualOutcome) {
                ordered = false;
            }

            @Override
            public void onExecutionReport(ExecutionReport report) {
            }
        };

        StrategyRegistry registry = new StrategyRegistry(Map.of(testStrategy.getName(), testStrategy));
        activeEngine = new BacktestEngine(
                datasetService,
                registry,
                new PerformanceMetricsCalculator(),
                new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("out").toString()),
                tempDir.resolve("out").toString(),
                10000.0,
                1.0,
                2
        );
        return activeEngine;
    }

    private static final class CapturingLogHandler extends ExtHandler {
        private final List<ExtLogRecord> captured = new CopyOnWriteArrayList<>();

        @Override
        protected void doPublish(ExtLogRecord record) {
            captured.add(record);
        }

        List<ExtLogRecord> records() {
            return captured;
        }

        void clear() {
            captured.clear();
        }
    }
}
