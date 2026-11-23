package cz.polymarket.bot.backtest.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper.ClobRow;
import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.indicator.DailyIndicatorTrack;
import cz.polymarket.bot.backtest.indicator.IndicatorCacheService;
import cz.polymarket.bot.backtest.indicator.SpotTwapBacktestIndicator;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.TradeDirection;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ChronologicalEventStreamerTest {

    @Test
    @DisplayName("Should pop events strictly in ascending receipt timestamp (ts) order")
    void shouldPopEventsInAscendingOrder() {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB");
             ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
            streamer.scheduleEvent(new BacktestEvent.PolymarketCryptoPriceEvent(3000L, 1L, "btcusd", "chainlink", 60200.0, 2995L));
            streamer.scheduleEvent(new BacktestEvent.PolymarketCryptoPriceEvent(1000L, 2L, "btcusd", "chainlink", 60000.0, 995L));
            streamer.scheduleEvent(new BacktestEvent.CalculatedSpotTwapEvent(2000L, 3L, "btcusd", 60100.0, 60050.0));

            List<Long> timestamps = new ArrayList<>();
            while (streamer.hasNext()) {
                timestamps.add(streamer.next().timestampMs());
            }

            assertThat(timestamps).containsExactly(1000L, 2000L, 3000L);
        }
    }

    @Test
    @DisplayName("Should stream typed Polymarket CLOB frames, price_crypto, price_crypto_twap_60s, and cached CalculatedSpotTwapEvents")
    void shouldStreamPolymarketAndCalculatedSpotTwapStreams(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB")) {
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(tempDir.toString(), duckDb);
            IndicatorCacheService indicatorCacheService = new IndicatorCacheService(
                    tempDir.resolve("cache"),
                    new ObjectMapper(),
                    List.of(new SpotTwapBacktestIndicator()),
                    100L, 50L, 50L
            );

            long tStart = 1785542400L; // 2026-08-01 00:00:00 UTC
            long tEnd = tStart + 900L;
            String dateStr = "2026-08-01";

            Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
            Path cryptoPriceFile = datasetService.getPolymarketCryptoPriceFile("BTCUSDT", dateStr);
            Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", dateStr);
            Path spotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", dateStr);
            Path krakenFile = datasetService.getKrakenSpotUsdtUsdTradesFile(dateStr);

            Files.createDirectories(pmDir);
            Files.createDirectories(cryptoPriceFile.getParent());
            Files.createDirectories(twap60sFile.getParent());
            Files.createDirectories(spotFile.getParent());
            Files.createDirectories(krakenFile.getParent());

            Path pmFile = pmDir.resolve("btc-updown-15m-" + tStart + ".parquet");

            try (Connection conn = duckDb.openSession();
                 Statement stmt = conn.createStatement()) {

                // Multi-row price_change with identical frame_id=8 must be grouped into a single PolymarketClobFrameEvent
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile, List.of(
                        ClobRow.discovered(tStart * 1000L - 1000L, "1001", "1002"),
                        ClobRow.priceChange(tStart * 1000L + 500L, tStart * 1000L + 490L, 8, true, false, 5200, 200.0),
                        ClobRow.priceChange(tStart * 1000L + 500L, tStart * 1000L + 490L, 8, false, true, 4800, 200.0)
                ));

                String normCrypto = cryptoPriceFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (tStart * 1000L + 700L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60050.0::DOUBLE, '60050.0'::VARCHAR, " + (tStart * 1000L + 690L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, price, full_accuracy_price, price_timestamp)) TO '" + normCrypto + "' (FORMAT PARQUET)");

                String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (tStart * 1000L + 800L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60025.0::DOUBLE, '60025.0'::VARCHAR, 60::INT, " + (tStart * 1000L + 790L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

                String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 0.5::DOUBLE, 1::BIGINT, 1::BIGINT, " + (tStart * 1000L + 1000L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                        ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normSpot + "' (FORMAT PARQUET)");

                String normKraken = krakenFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (tStart * 1000L + 900L) + "::UBIGINT, 'USDT/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'limit'::VARCHAR, 0.9990::DOUBLE, 1000.0::DOUBLE, '2026-08-01T00:00:00.900000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normKraken + "' (FORMAT PARQUET)");
            }

            CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                    tStart, tEnd, "2026-08-01T00:00:00Z", dateStr, pmFile,
                    "1001", "1002", 60000.0, 60120.0, TradeDirection.UP
            );

            BacktestEvent[] sharedDaily = ChronologicalEventStreamer.loadSharedDailyStreams(
                    datasetService, "BTCUSDT", dateStr);
            List<DailyIndicatorTrack> dailyIndicators = indicatorCacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", dateStr);

            try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
                streamer.loadCandleEvents(metadata, sharedDaily, dailyIndicators, Long.MAX_VALUE);

                int clobCount = 0;
                int clobLevelSum = 0;
                int calcSpotTwapCount = 0;
                int cryptoPriceCount = 0;
                int twap60sCount = 0;

                while (streamer.hasNext()) {
                    BacktestEvent event = streamer.next();
                    if (event instanceof BacktestEvent.PolymarketClobFrameEvent f) {
                        clobCount++;
                        clobLevelSum += f.levelCount();
                    }
                    if (event instanceof BacktestEvent.CalculatedSpotTwapEvent calcEv) {
                        calcSpotTwapCount++;
                        assertThat(calcEv.symbol()).isEqualTo("btcusd");
                        assertThat(calcEv.timestampMs()).isEqualTo(tStart * 1000L + 1100L);
                        assertThat(calcEv.spotPrice()).isCloseTo(60000.0 * 0.9990, within(1e-4));
                    }
                    if (event instanceof BacktestEvent.PolymarketCryptoPriceEvent) cryptoPriceCount++;
                    if (event instanceof BacktestEvent.PolymarketCryptoTwap60sEvent) twap60sCount++;
                }

                assertThat(clobCount).isEqualTo(1);
                assertThat(clobLevelSum).isEqualTo(2);
                assertThat(calcSpotTwapCount).isEqualTo(1);
                assertThat(cryptoPriceCount).isEqualTo(1);
                assertThat(twap60sCount).isEqualTo(1);
            }
        }
    }

    @Test
    @DisplayName("Should lazily merge CLOB frames, daily cursor, and dynamically scheduled pending orders")
    void shouldLazilyMergeStreamClobEventsWithDynamicEvents(@TempDir Path tempDir) throws Exception {
        long tStart = 1785542400L;
        long tEnd = tStart + 900L;
        long baseMs = tStart * 1000L;
        Path pmFile = tempDir.resolve("btc-updown-15m-" + tStart + ".parquet");

        TypedClobParquetTestHelper.writeParquet(pmFile, List.of(
                ClobRow.discovered(baseMs - 1000L, "1001", "1002"),
                ClobRow.priceChange(baseMs + 100L, baseMs + 90L, 4, true, true, 5100, 100.0),
                ClobRow.priceChange(baseMs + 500L, baseMs + 490L, 8, true, true, 5200, 150.0),
                ClobRow.lastTrade(baseMs + 900L, baseMs + 890L, 12)
        ));

        CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                tStart, tEnd, "2026-08-01T00:00:00Z", "2026-08-01", pmFile,
                "1001", "1002", 60000.0, 60100.0, TradeDirection.UP
        );

        BacktestEvent[] daily = new BacktestEvent[]{
                new BacktestEvent.PolymarketCryptoTwap60sEvent(baseMs, 1L, "btcusd", "chainlink", 60000.0, 60, baseMs),
                new BacktestEvent.PolymarketCryptoPriceEvent(baseMs + 250L, 2L, "btcusd", "chainlink", 60010.0, baseMs + 245L)
        };

        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB");
             ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
            streamer.loadCandleEvents(metadata, daily, List.of(), Long.MAX_VALUE);

            List<BacktestEvent> observed = new ArrayList<>();
            boolean scheduledDynamicOrder = false;

            while (streamer.hasNext()) {
                BacktestEvent ev = streamer.next();
                observed.add(ev);
                if (!scheduledDynamicOrder && ev instanceof BacktestEvent.PolymarketClobFrameEvent clob && clob.timestampMs() == baseMs + 100L) {
                    scheduledDynamicOrder = true;
                    streamer.scheduleEvent(new BacktestEvent.PendingOrderExecutionEvent(
                            baseMs + 300L,
                            new OrderCommand("ord-1", "0x1", "1001", Timeframe.FIFTEEN_MINUTES, "BUY", BigDecimal.valueOf(0.52), BigDecimal.TEN),
                            50L,
                            1L
                    ));
                }
            }

            List<Long> clobDailyAndOrderTimestamps = observed.stream()
                    .filter(e -> e instanceof BacktestEvent.PolymarketClobFrameEvent
                            || e instanceof BacktestEvent.PolymarketCryptoPriceEvent
                            || e instanceof BacktestEvent.PendingOrderExecutionEvent)
                    .map(BacktestEvent::timestampMs)
                    .toList();

            assertThat(clobDailyAndOrderTimestamps).containsExactly(
                    baseMs + 100L,
                    baseMs + 250L,
                    baseMs + 300L,
                    baseMs + 500L,
                    baseMs + 900L
            );

            // Verify clear() mid-stream closes active ResultSet cleanly and resets daily cursor
            streamer.loadCandleEvents(metadata, daily, List.of(), Long.MAX_VALUE);
            assertThat(streamer.hasNext()).isTrue();
            streamer.clear();
            assertThat(streamer.hasNext()).isFalse();
        }
    }

    @Test
    @DisplayName("Should merge-stream Binance, Coinbase, and Kraken spot trades with Kraken USDT/USD peg into cached CalculatedSpotTwapEvents")
    void shouldMergeStreamMultiExchangeSpotTradesIntoCalculatedSpotTwapEvents(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB")) {
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(tempDir.toString(), duckDb);
            IndicatorCacheService indicatorCacheService = new IndicatorCacheService(
                    tempDir.resolve("cache"),
                    new ObjectMapper(),
                    List.of(new SpotTwapBacktestIndicator()),
                    100L, 50L, 50L
            );

            long tStart = 1785542400L; // 2026-08-01 00:00:00 UTC
            long tEnd = tStart + 900L;
            long baseMs = tStart * 1000L;
            String dateStr = "2026-08-01";

            Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
            Path binanceSpotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", dateStr);
            Path coinbaseSpotFile = datasetService.getCoinbaseSpotTradesFile("BTCUSDT", dateStr);
            Path krakenSpotFile = datasetService.getKrakenSpotTradesFile("BTCUSDT", dateStr);
            Path krakenUsdtFile = datasetService.getKrakenSpotUsdtUsdTradesFile(dateStr);

            assertThat(coinbaseSpotFile).isEqualTo(
                    tempDir.resolve("coinbase").resolve("spot").resolve("BTCUSD").resolve("trades").resolve("BTCUSD-trades-2026-08-01.parquet"));
            assertThat(krakenSpotFile).isEqualTo(
                    tempDir.resolve("kraken").resolve("spot").resolve("BTCUSD").resolve("trades").resolve("BTCUSD-trades-2026-08-01.parquet"));

            Files.createDirectories(pmDir);
            Files.createDirectories(binanceSpotFile.getParent());
            Files.createDirectories(coinbaseSpotFile.getParent());
            Files.createDirectories(krakenSpotFile.getParent());
            Files.createDirectories(krakenUsdtFile.getParent());

            Path pmFile = pmDir.resolve("btc-updown-15m-" + tStart + ".parquet");

            try (Connection conn = duckDb.openSession();
                 Statement stmt = conn.createStatement()) {

                TypedClobParquetTestHelper.writeParquet(stmt, pmFile, List.of(
                        ClobRow.discovered(baseMs - 1000L, "1001", "1002"),
                        ClobRow.priceChange(baseMs + 500L, baseMs + 490L, 4, true, false, 5200, 200.0)
                ));

                // Kraken USDT/USD peg at 1.0000 (server +100ms -> +150ms with 50ms delay)
                String normUsdt = krakenUsdtFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 100L) + "::UBIGINT, 'USDT/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'limit'::VARCHAR, 1.0000::DOUBLE, 1000.0::DOUBLE, '2026-08-01T00:00:00.100000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normUsdt + "' (FORMAT PARQUET)");

                // Binance BTCUSDT trade at server +1000ms ($70,000) -> +1100ms with 100ms delay (NULL event_time -> trade_time)
                String normBinance = binanceSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 70000.0::DOUBLE, 1.0::DOUBLE, 1::BIGINT, 1::BIGINT, " + (baseMs + 1000L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                        ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normBinance + "' (FORMAT PARQUET)");

                // Coinbase BTCUSD trade at server +2000ms ($70,020) -> +2050ms with 50ms delay
                String normCoinbase = coinbaseSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 2000L) + "::UBIGINT, 'BTC-USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 70020.0::DOUBLE, 1.0::DOUBLE, '2026-08-01T00:00:02.000000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, price, qty, timestamp)) TO '" + normCoinbase + "' (FORMAT PARQUET)");

                // Kraken BTCUSD trade at server +3000ms ($70,040) -> +3050ms with 50ms delay
                String normKraken = krakenSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 3000L) + "::UBIGINT, 'BTC/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'market'::VARCHAR, 70040.0::DOUBLE, 1.0::DOUBLE, '2026-08-01T00:00:03.000000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normKraken + "' (FORMAT PARQUET)");
            }

            CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                    tStart, tEnd, "2026-08-01T00:00:00Z", dateStr, pmFile,
                    "1001", "1002", 70000.0, 70030.0, TradeDirection.UP
            );

            List<DailyIndicatorTrack> dailyIndicators = indicatorCacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", dateStr);

            try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
                streamer.loadCandleEvents(metadata, new BacktestEvent[0], dailyIndicators, Long.MAX_VALUE);

                List<BacktestEvent.CalculatedSpotTwapEvent> calcEvents = new ArrayList<>();
                while (streamer.hasNext()) {
                    BacktestEvent ev = streamer.next();
                    if (ev instanceof BacktestEvent.CalculatedSpotTwapEvent calcEv) {
                        calcEvents.add(calcEv);
                    }
                }

                // 3 spot trades shifted by configured exchange delays:
                // Binance @ +1100ms, Coinbase @ +2050ms, Kraken @ +3050ms
                assertThat(calcEvents).hasSize(3);
                assertThat(calcEvents.get(0).timestampMs()).isEqualTo(baseMs + 1100L);
                assertThat(calcEvents.get(0).spotPrice()).isCloseTo(70000.0, within(1e-4));

                assertThat(calcEvents.get(1).timestampMs()).isEqualTo(baseMs + 2050L);
                assertThat(calcEvents.get(1).spotPrice()).isBetween(70005.0, 70015.0);

                assertThat(calcEvents.get(2).timestampMs()).isEqualTo(baseMs + 3050L);
                assertThat(calcEvents.get(2).spotPrice()).isBetween(70010.0, 70030.0);
            }
        }
    }

    @Test
    @DisplayName("Should prune early CLOB frames when clobLookbackMs is bounded while seeding pre-window L2 book and preserving boundary 60s TWAP")
    void shouldPruneEarlyClobFramesWhileSeedingBookAndPreservingBoundaryTwap(@TempDir Path tempDir) throws Exception {
        long tStart = 1785542400L;
        long tEnd = tStart + 900L;
        long baseMs = tStart * 1000L;
        Path pmFile = tempDir.resolve("btc-updown-15m-" + tStart + ".parquet");

        // Early delta at +10s, mid-candle full snapshots at +818s (DOWN) and +820s (UP), post-snapshot delta at +822s, active window [824s..900s]
        TypedClobParquetTestHelper.writeParquet(pmFile, List.of(
                ClobRow.discovered(baseMs - 2000L, "1001", "1002"),
                ClobRow.bookLevel(baseMs + 5_000L, baseMs + 4_990L, 4, true, false, 6000, 100.0),
                ClobRow.priceChange(baseMs + 10_000L, baseMs + 9_990L, 8, true, false, 6100, 100.0),
                ClobRow.bookLevel(baseMs + 818_000L, baseMs + 817_985L, 12, false, false, 4700, 250.0),
                ClobRow.bookLevel(baseMs + 820_000L, baseMs + 819_985L, 16, true, false, 5300, 200.0),
                ClobRow.priceChange(baseMs + 822_000L, baseMs + 821_988L, 20, true, false, 5200, 180.0),
                ClobRow.priceChange(baseMs + 850_000L, baseMs + 849_990L, 24, true, true, 5000, 150.0)
        ));

        CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                tStart, tEnd, "2026-08-01T00:00:00Z", "2026-08-01", pmFile,
                "1001", "1002", 60000.0, 60080.0, TradeDirection.UP
        );

        // Nearest TWAP sample arrived slightly before tStartMs (at baseMs - 400ms), plus early price_crypto at +5s!
        BacktestEvent[] daily = new BacktestEvent[]{
                new BacktestEvent.PolymarketCryptoTwap60sEvent(baseMs - 400L, 1L, "btcusd", "chainlink", 60000.0, 60, baseMs - 400L),
                new BacktestEvent.PolymarketCryptoPriceEvent(baseMs + 5_000L, 2L, "btcusd", "chainlink", 60005.0, baseMs + 5_000L),
                new BacktestEvent.PolymarketCryptoTwap60sEvent(baseMs + 900_000L, 3L, "btcusd", "chainlink", 60080.0, 60, baseMs + 900_000L)
        };

        PolymarketOrderBook book = new PolymarketOrderBook();
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB");
             ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {

            streamer.loadCandleEvents(metadata, daily, List.of(), 76_000L);

            List<Long> clobTimestamps = new ArrayList<>();
            List<Double> twapPrices = new ArrayList<>();
            int cryptoPriceCount = 0;

            while (streamer.hasNext()) {
                BacktestEvent ev = streamer.next();
                if (ev instanceof BacktestEvent.PolymarketClobFrameEvent f) {
                    clobTimestamps.add(f.timestampMs());
                    book.applyClobFrame(f);
                } else if (ev instanceof BacktestEvent.PolymarketCryptoTwap60sEvent twap) {
                    twapPrices.add(twap.twapPrice());
                } else if (ev instanceof BacktestEvent.PolymarketCryptoPriceEvent) {
                    cryptoPriceCount++;
                }
            }

            // Early CLOB events (+5s, +10s) were pruned; seed started at +818s (min of UP/DOWN pre-window books)
            assertThat(clobTimestamps).containsExactly(
                    baseMs + 818_000L,
                    baseMs + 820_000L,
                    baseMs + 822_000L,
                    baseMs + 850_000L
            );
            // Book state matches full replay: UP bestAsk=0.52, UP bestBid=0.50, DOWN bestAsk=0.47
            assertThat(book.getBestAskUp()).isCloseTo(0.52, within(0.0001));
            assertThat(book.getBestBidUp()).isCloseTo(0.50, within(0.0001));
            assertThat(book.getBestAskDown()).isCloseTo(0.47, within(0.0001));

            // Boundary twapOpen (60000.0) at tStartMs AND early price_crypto (+5s) AND closing twap (60080.0) were ALL delivered!
            assertThat(twapPrices).containsExactly(60000.0, 60080.0);
            assertThat(cryptoPriceCount).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("Should reorder multi-exchange spot trades chronologically after adding per-exchange delays and respect candle boundaries")
    void shouldReorderSpotTradesByExchangeDelayAndRespectBoundaries(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB")) {
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(tempDir.toString(), duckDb);
            IndicatorCacheService indicatorCacheService = new IndicatorCacheService(
                    tempDir.resolve("cache"),
                    new ObjectMapper(),
                    List.of(new SpotTwapBacktestIndicator()),
                    100L, 50L, 50L
            );

            long tStart = 1785542400L; // 2026-08-01 00:00:00 UTC
            long tEnd = tStart + 900L;
            long baseMs = tStart * 1000L;
            long endMs = tEnd * 1000L;
            String dateStr = "2026-08-01";

            Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
            Path binanceSpotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", dateStr);
            Path coinbaseSpotFile = datasetService.getCoinbaseSpotTradesFile("BTCUSDT", dateStr);
            Path krakenSpotFile = datasetService.getKrakenSpotTradesFile("BTCUSDT", dateStr);
            Path krakenUsdtFile = datasetService.getKrakenSpotUsdtUsdTradesFile(dateStr);

            Files.createDirectories(pmDir);
            Files.createDirectories(binanceSpotFile.getParent());
            Files.createDirectories(coinbaseSpotFile.getParent());
            Files.createDirectories(krakenSpotFile.getParent());
            Files.createDirectories(krakenUsdtFile.getParent());

            Path pmFile = pmDir.resolve("btc-updown-15m-" + tStart + ".parquet");

            try (Connection conn = duckDb.openSession();
                 Statement stmt = conn.createStatement()) {

                TypedClobParquetTestHelper.writeParquet(stmt, pmFile, List.of(
                        ClobRow.discovered(baseMs - 1000L, "1001", "1002")
                ));

                // Kraken USDT/USD peg before candle open (server -1000ms + 50ms = -950ms)
                String normUsdt = krakenUsdtFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs - 1000L) + "::UBIGINT, 'USDT/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'limit'::VARCHAR, 1.0000::DOUBLE, 1000.0::DOUBLE, '2026-07-31T23:59:59.000000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normUsdt + "' (FORMAT PARQUET)");

                // Binance Spot:
                // 1) event_time populated (-60ms + 100ms = baseMs + 40ms -> shifted INTO candle!)
                // 2) event_time NULL, trade_time (+1000ms + 100ms = baseMs + 1100ms -> arrives AFTER Coinbase & Kraken!)
                // 3) trade_time near candle close (endMs - 50ms + 100ms = endMs + 50ms -> excluded from candle 1!)
                String normBinance = binanceSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs - 60L) + "::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 69950.0::DOUBLE, 1.0::DOUBLE, 1::BIGINT, 1::BIGINT, " + (baseMs - 70L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                        "(NULL::UBIGINT, 2::BIGINT, 'BTCUSDT'::VARCHAR, 70000.0::DOUBLE, 1.0::DOUBLE, 2::BIGINT, 2::BIGINT, " + (baseMs + 1000L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                        "(NULL::UBIGINT, 3::BIGINT, 'BTCUSDT'::VARCHAR, 70500.0::DOUBLE, 1.0::DOUBLE, 3::BIGINT, 3::BIGINT, " + (endMs - 50L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                        ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normBinance + "' (FORMAT PARQUET)");

                // Coinbase Spot at server +1020ms (+50ms = baseMs + 1070ms -> arrives BEFORE Binance +1000ms!)
                String normCoinbase = coinbaseSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 1020L) + "::UBIGINT, 'BTC-USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 70100.0::DOUBLE, 1.0::DOUBLE, '2026-08-01T00:00:01.020000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, price, qty, timestamp)) TO '" + normCoinbase + "' (FORMAT PARQUET)");

                // Kraken Spot at server +1040ms (+50ms = baseMs + 1090ms -> arrives BEFORE Binance +1000ms!)
                String normKraken = krakenSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 1040L) + "::UBIGINT, 'BTC/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'limit'::VARCHAR, 70200.0::DOUBLE, 1.0::DOUBLE, '2026-08-01T00:00:01.040000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normKraken + "' (FORMAT PARQUET)");
            }

            CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                    tStart, tEnd, "2026-08-01T00:00:00Z", dateStr, pmFile,
                    "1001", "1002", 70000.0, 70100.0, TradeDirection.UP
            );

            List<DailyIndicatorTrack> dailyIndicators = indicatorCacheService.loadOrComputeDailyIndicators(
                    datasetService, "BTCUSDT", dateStr);

            try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
                streamer.loadCandleEvents(metadata, new BacktestEvent[0], dailyIndicators, Long.MAX_VALUE);

                List<BacktestEvent.CalculatedSpotTwapEvent> calcEvents = new ArrayList<>();
                while (streamer.hasNext()) {
                    BacktestEvent ev = streamer.next();
                    if (ev instanceof BacktestEvent.CalculatedSpotTwapEvent calcEv) {
                        calcEvents.add(calcEv);
                    }
                }

                assertThat(calcEvents)
                        .extracting(BacktestEvent.CalculatedSpotTwapEvent::timestampMs)
                        .containsExactly(
                                baseMs + 40L,    // Binance pre-open event_time (-60 + 100)
                                baseMs + 1070L,  // Coinbase (+1020 + 50)
                                baseMs + 1090L,  // Kraken (+1040 + 50)
                                baseMs + 1100L   // Binance (+1000 + 100)
                        );
            }
        }
    }
}
