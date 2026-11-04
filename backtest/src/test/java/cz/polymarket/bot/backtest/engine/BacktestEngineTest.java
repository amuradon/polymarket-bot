package cz.polymarket.bot.backtest.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.export.BacktestJsonExporter;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
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
import cz.polymarket.bot.strategy.TradingStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategy;
import cz.polymarket.bot.strategy.UtterParameciumStrategyConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BacktestEngineTest {

    private BacktestEngine backtestEngine;
    private final AtomicInteger polymarketCryptoPriceCount = new AtomicInteger();
    private final AtomicInteger polymarketTwap60sCount = new AtomicInteger();
    private final AtomicInteger calculatedSpotTwapCount = new AtomicInteger();
    private final AtomicInteger externalSpotTradeCount = new AtomicInteger();

    private static final long BASE_START = 1785542400L; // 2026-08-01 00:00:00 UTC

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        Path collectorDir = tempDir.resolve("collector");
        CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString());

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

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
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

            // 3. Binance Spot aggTrades (using collector column `ts`)
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

            // 5. Polymarket 15m CLOB market files
            String normPm1 = pmFile1.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L - 5000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, '0x1'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"clobTokenIds\":\"[\\\"101\\\", \\\"102\\\"]\"}'::JSON), " +
                    "(" + ((c1Start + 60L) * 1000L + 10L) + "::UBIGINT, 'clob_ws'::VARCHAR, 'book'::VARCHAR, '0x1'::VARCHAR, '101'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"asset_id\":\"101\",\"timestamp\":\"" + ((c1Start + 60L) * 1000L) + "\",\"bids\":[{\"price\":\"0.49\",\"size\":\"300.0\"}],\"asks\":[{\"price\":\"0.50\",\"size\":\"300.0\"}]}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) TO '" + normPm1 + "' (FORMAT PARQUET)");

            String normPm2 = pmFile2.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c2Start * 1000L - 5000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, '0x2'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"clobTokenIds\":\"[\\\"201\\\", \\\"202\\\"]\"}'::JSON), " +
                    "(" + ((c2Start + 60L) * 1000L + 10L) + "::UBIGINT, 'clob_ws'::VARCHAR, 'book'::VARCHAR, '0x2'::VARCHAR, '201'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"asset_id\":\"201\",\"timestamp\":\"" + ((c2Start + 60L) * 1000L) + "\",\"bids\":[{\"price\":\"0.50\",\"size\":\"300.0\"}],\"asks\":[{\"price\":\"0.52\",\"size\":\"300.0\"}]}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) TO '" + normPm2 + "' (FORMAT PARQUET)");

            String normPm3 = pmFile3.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c3Start * 1000L - 5000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, '0x3'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"clobTokenIds\":\"[\\\"301\\\", \\\"302\\\"]\"}'::JSON), " +
                    "(" + ((c3Start + 60L) * 1000L + 10L) + "::UBIGINT, 'clob_ws'::VARCHAR, 'book'::VARCHAR, '0x3'::VARCHAR, '301'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"asset_id\":\"301\",\"timestamp\":\"" + ((c3Start + 60L) * 1000L) + "\",\"bids\":[{\"price\":\"0.50\",\"size\":\"300.0\"}],\"asks\":[{\"price\":\"0.52\",\"size\":\"300.0\"}]}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) TO '" + normPm3 + "' (FORMAT PARQUET)");
        }

        PerformanceMetricsCalculator metricsCalculator = new PerformanceMetricsCalculator();
        BacktestJsonExporter jsonExporter = new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export").toString());

        TWAPArbitrageStrategyConfig config = TWAPArbitrageStrategyConfig.defaultIteration20();
        KellyPositionSizer positionSizer = new KellyPositionSizer(10000.0, 0.25, 50.0, 300.0);
        NextCandleProbabilityModel probabilityModel = new NextCandleProbabilityModel(positionSizer);
        TWAPArbitrageStrategy twapStrategy = new TWAPArbitrageStrategy(probabilityModel, config);

        // Polymarket-only strategy requiring zero external streams and zero indicators,
        // buying in two consecutive batches per market and supporting parallel market instances
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
            public void onPolymarketCryptoPrice(long timestampMs, String symbol, String source, double price, long priceTimestampMs) {
                polymarketCryptoPriceCount.incrementAndGet();
                // Polymarket data must NOT be mixed into spot indicators (VWAP must remain NaN when no spot trades loaded)
                assertThat(ctx.getIndicatorValue(IndicatorType.VWAP)).isNaN();
            }

            @Override
            public void onPolymarketCryptoTwap60s(long timestampMs, String symbol, String source, double twapPrice, int windowSeconds, long twapTimestampMs) {
                polymarketTwap60sCount.incrementAndGet();
                assertThat(ctx.getIndicatorValue(IndicatorType.VWAP)).isNaN();
            }

            @Override
            public void onCalculatedSpotTwap(long timestampMs, String symbol, double spotPrice, double twap60sPrice) {
                calculatedSpotTwapCount.incrementAndGet();
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
                    // Submit 2 separate batch orders in the same market to verify multi-position support
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
            public void onCandleResolution(cz.polymarket.bot.strategy.TradeDirection actualOutcome) {
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
                tempDir.resolve("export").toString(),
                10000.0,
                1.0
        );
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
        calculatedSpotTwapCount.set(0);
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
        assertThat(calculatedSpotTwapCount.get()).isEqualTo(6);
        assertThat(externalSpotTradeCount.get()).isZero();
    }

    @Test
    @DisplayName("Should execute UtterParameciumStrategy final-minute TWAP lock-in sniping via normalized name")
    void shouldExecuteUtterParameciumStrategyInFinalMinuteWindow(@TempDir Path tempDir) throws Exception {
        Path collectorDir = tempDir.resolve("collector_utter");
        CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(collectorDir.toString());

        long c1Start = BASE_START;
        Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
        Path spotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", "2026-08-01");
        Path krakenFile = datasetService.getKrakenSpotUsdtUsdTradesFile("2026-08-01");
        Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", "2026-08-01");

        Files.createDirectories(pmDir);
        Files.createDirectories(spotFile.getParent());
        Files.createDirectories(krakenFile.getParent());
        Files.createDirectories(twap60sFile.getParent());

        Path pmFile1 = pmDir.resolve("btc-updown-15m-" + c1Start + ".parquet");
        long c2MissingTwapStart = BASE_START + 1800L;
        Path pmFile2 = pmDir.resolve("btc-updown-15m-" + c2MissingTwapStart + ".parquet");

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            // 1. Polymarket price_crypto_twap_60s (strike = 60,000.00, close TWAP = 60,050.00 -> UP wins; c2MissingTwapStart has NO TWAP data)
            String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60000.0::DOUBLE, '60000.0'::VARCHAR, 60::INT, " + (c1Start * 1000L) + "::UBIGINT), " +
                    "(" + ((c1Start + 900L) * 1000L) + "::UBIGINT, 2::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60050.0::DOUBLE, '60050.0'::VARCHAR, 60::INT, " + ((c1Start + 900L) * 1000L) + "::UBIGINT)" +
                    ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

            // 2. Kraken Spot USDTUSD trades (peg = 1.0000) and Binance Spot BTCUSDT aggTrades in the final minute [c1Start + 835s .. c1Start + 885s]
            String normKraken = krakenFile.toAbsolutePath().toString().replace('\\', '/');
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L) + "::UBIGINT, 1.0000::DOUBLE)" +
                    ") AS t(ts, price)) TO '" + normKraken + "' (FORMAT PARQUET)");

            String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
            StringBuilder spotValues = new StringBuilder();
            for (int sec = 835; sec <= 885; sec++) {
                if (spotValues.length() > 0) {
                    spotValues.append(", ");
                }
                long tsMs = (c1Start + sec) * 1000L;
                double price = 60050.0 + (sec % 2 == 0 ? 1.0 : -1.0);
                spotValues.append("(")
                        .append(tsMs).append("::UBIGINT, ")
                        .append(sec).append("::BIGINT, ")
                        .append(price).append("::DOUBLE, 1.0::DOUBLE, false::BOOLEAN)");
            }
            stmt.execute("COPY (SELECT * FROM (VALUES " + spotValues +
                    ") AS t(ts, aggregate_trade_id, price, quantity, is_buyer_maker)) TO '" + normSpot + "' (FORMAT PARQUET)");

            // 3. Polymarket 15m CLOB market file with stale ask at 0.55 during final minute
            String normPm1 = pmFile1.toAbsolutePath().toString().replace('\\', '/');
            long bookTs1 = (c1Start + 845L) * 1000L;
            long bookTs2 = (c1Start + 860L) * 1000L;
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c1Start * 1000L - 5000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, '0x1'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"clobTokenIds\":\"[\\\"101\\\", \\\"102\\\"]\"}'::JSON), " +
                    "(" + bookTs1 + "::UBIGINT, 'clob_ws'::VARCHAR, 'book'::VARCHAR, '0x1'::VARCHAR, '101'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"asset_id\":\"101\",\"timestamp\":\"" + bookTs1 + "\",\"bids\":[{\"price\":\"0.53\",\"size\":\"300.0\"}],\"asks\":[{\"price\":\"0.55\",\"size\":\"300.0\"}]}'::JSON), " +
                    "(" + bookTs2 + "::UBIGINT, 'clob_ws'::VARCHAR, 'book'::VARCHAR, '0x1'::VARCHAR, '101'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"asset_id\":\"101\",\"timestamp\":\"" + bookTs2 + "\",\"bids\":[{\"price\":\"0.54\",\"size\":\"300.0\"}],\"asks\":[{\"price\":\"0.56\",\"size\":\"300.0\"}]}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) TO '" + normPm1 + "' (FORMAT PARQUET)");

            // 4. Second market missing 60s TWAP target price -> must be skipped with WARN
            String normPm2 = pmFile2.toAbsolutePath().toString().replace('\\', '/');
            long m2BookTs = (c2MissingTwapStart + 850L) * 1000L;
            stmt.execute("COPY (SELECT * FROM (VALUES " +
                    "(" + (c2MissingTwapStart * 1000L - 5000L) + "::UBIGINT, 'market_lifecycle'::VARCHAR, 'market_discovered'::VARCHAR, '0x2'::VARCHAR, NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"clobTokenIds\":\"[\\\"201\\\", \\\"202\\\"]\"}'::JSON), " +
                    "(" + m2BookTs + "::UBIGINT, 'clob_ws'::VARCHAR, 'book'::VARCHAR, '0x2'::VARCHAR, '201'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, " +
                    "'{\"asset_id\":\"201\",\"timestamp\":\"" + m2BookTs + "\",\"bids\":[{\"price\":\"0.53\",\"size\":\"300.0\"}],\"asks\":[{\"price\":\"0.55\",\"size\":\"300.0\"}]}'::JSON)" +
                    ") AS t(ts, msg_type, event_type, market, asset_id, price, size, side, payload)) TO '" + normPm2 + "' (FORMAT PARQUET)");
        }

        UtterParameciumStrategy utterStrategy = new UtterParameciumStrategy(UtterParameciumStrategyConfig.defaultModelV1());
        StrategyRegistry localRegistry = new StrategyRegistry(Map.of(utterStrategy.getName(), utterStrategy));
        BacktestEngine utterEngine = new BacktestEngine(
                datasetService,
                localRegistry,
                new PerformanceMetricsCalculator(),
                new BacktestJsonExporter(new ObjectMapper(), tempDir.resolve("export_utter").toString()),
                tempDir.resolve("export_utter").toString(),
                10000.0,
                1.0
        );

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
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when unknown strategy requested")
    void shouldThrowWhenUnknownStrategy() {
        assertThatThrownBy(() -> backtestEngine.runBacktest("UnknownStrategy", "BTCUSDT", null, null, 10000.0, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown strategy");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when no market rows available")
    void shouldThrowWhenNoMarketRowsAvailable() {
        assertThatThrownBy(() -> backtestEngine.runBacktest("TWAPArbitrageStrategy", "UNKNOWN_SYMBOL", null, null, 10000.0, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No market rows available");
    }
}
