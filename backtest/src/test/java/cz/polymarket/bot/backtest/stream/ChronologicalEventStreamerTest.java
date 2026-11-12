package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.TypedClobParquetTestHelper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper.ClobRow;
import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.strategy.ExternalStreamType;
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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ChronologicalEventStreamerTest {

    @Test
    @DisplayName("Should pop events strictly in ascending receipt timestamp (ts) order")
    void shouldPopEventsInAscendingOrder() {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB");
             ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
            streamer.scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(3000L, 1L, 60200.0, 1.0, false));
            streamer.scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(1000L, 2L, 60000.0, 1.0, false));
            streamer.scheduleEvent(new BacktestEvent.BinanceFuturesTradeEvent(2000L, 3L, 60100.0, 1.0, false));

            List<Long> timestamps = new ArrayList<>();
            while (streamer.hasNext()) {
                timestamps.add(streamer.next().timestampMs());
            }

            assertThat(timestamps).containsExactly(1000L, 2000L, 3000L);
        }
    }

    @Test
    @DisplayName("Should stream typed Polymarket CLOB frames, price_crypto, and price_crypto_twap_60s while skipping unrequested external streams")
    void shouldStreamPolymarketAndSkipUnrequestedExternalStreams(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB")) {
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(tempDir.toString(), duckDb);

            long tStart = 1785542400L; // 2026-08-01 00:00:00 UTC
            long tEnd = tStart + 900L;
            String dateStr = "2026-08-01";

            Path pmDir = datasetService.getPolymarket15mDir("BTCUSDT");
            Path cryptoPriceFile = datasetService.getPolymarketCryptoPriceFile("BTCUSDT", dateStr);
            Path twap60sFile = datasetService.getPolymarketCryptoTwap60sFile("BTCUSDT", dateStr);
            Path spotFile = datasetService.getBinanceSpotAggTradesFile("BTCUSDT", dateStr);
            Path krakenFile = datasetService.getKrakenSpotUsdtUsdTradesFile(dateStr);
            Path futFile = datasetService.getBinanceFuturesAggTradesFile("BTCUSDT", dateStr);

            Files.createDirectories(pmDir);
            Files.createDirectories(cryptoPriceFile.getParent());
            Files.createDirectories(twap60sFile.getParent());
            Files.createDirectories(spotFile.getParent());
            Files.createDirectories(krakenFile.getParent());
            Files.createDirectories(futFile.getParent());

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
                        "(" + (tStart * 1000L + 1000L) + "::UBIGINT, 1::BIGINT, 60000.0::DOUBLE, 0.5::DOUBLE, false::BOOLEAN)" +
                        ") AS t(ts, aggregate_trade_id, price, quantity, is_buyer_maker)) TO '" + normSpot + "' (FORMAT PARQUET)");

                String normKraken = krakenFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (tStart * 1000L + 900L) + "::UBIGINT, 0.9990::DOUBLE)" +
                        ") AS t(ts, price)) TO '" + normKraken + "' (FORMAT PARQUET)");

                String normFut = futFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (tStart * 1000L + 1500L) + "::UBIGINT, 1::BIGINT, 60010.0::DOUBLE, 1.0::DOUBLE, false::BOOLEAN)" +
                        ") AS t(ts, aggregate_trade_id, price, quantity, is_buyer_maker)) TO '" + normFut + "' (FORMAT PARQUET)");
            }

            CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                    tStart, tEnd, "2026-08-01T00:00:00Z", dateStr, pmFile,
                    "1001", "1002", 60000.0, 60120.0, TradeDirection.UP
            );

            // Case 1: Strategy requires NO external streams -> CalculatedSpotTwapEvent IS streamed when datasetService is passed,
            // while raw Binance Spot/Futures events must NOT be loaded
            BacktestEvent[] noExtDaily = ChronologicalEventStreamer.loadSharedDailyStreams(
                    datasetService, "BTCUSDT", dateStr, Set.of());
            try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
                streamer.loadCandleEvents(metadata, noExtDaily, datasetService, "BTCUSDT", Long.MAX_VALUE);

                int clobCount = 0;
                int clobLevelSum = 0;
                int calcSpotTwapCount = 0;
                int cryptoPriceCount = 0;
                int twap60sCount = 0;
                int spotCount = 0;
                int futCount = 0;

                while (streamer.hasNext()) {
                    BacktestEvent event = streamer.next();
                    if (event instanceof BacktestEvent.PolymarketClobFrameEvent f) {
                        clobCount++;
                        clobLevelSum += f.levelCount();
                    }
                    if (event instanceof BacktestEvent.CalculatedSpotTwapEvent calcEv) {
                        calcSpotTwapCount++;
                        assertThat(calcEv.symbol()).isEqualTo("btcusd");
                        assertThat(calcEv.spotPrice()).isCloseTo(60000.0 * 0.9990, within(1e-4));
                    }
                    if (event instanceof BacktestEvent.PolymarketCryptoPriceEvent) cryptoPriceCount++;
                    if (event instanceof BacktestEvent.PolymarketCryptoTwap60sEvent) twap60sCount++;
                    if (event instanceof BacktestEvent.BinanceSpotTradeEvent) spotCount++;
                    if (event instanceof BacktestEvent.BinanceFuturesTradeEvent) futCount++;
                }

                assertThat(clobCount).isEqualTo(1);
                assertThat(clobLevelSum).isEqualTo(2);
                assertThat(calcSpotTwapCount).isEqualTo(1);
                assertThat(cryptoPriceCount).isEqualTo(1);
                assertThat(twap60sCount).isEqualTo(1);
                assertThat(spotCount).isZero();
                assertThat(futCount).isZero();
            }

            // Case 2: Strategy requests Binance Spot and Futures aggTrades -> all streams loaded and ordered by ts
            BacktestEvent[] withExtDaily = ChronologicalEventStreamer.loadSharedDailyStreams(
                    datasetService, "BTCUSDT", dateStr, Set.of(
                            ExternalStreamType.BINANCE_SPOT_AGG_TRADES,
                            ExternalStreamType.BINANCE_FUTURES_AGG_TRADES
                    ));
            try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
                streamer.loadCandleEvents(metadata, withExtDaily, Long.MAX_VALUE);

                long prevTs = 0L;
                int spotCount = 0;
                int futCount = 0;
                while (streamer.hasNext()) {
                    BacktestEvent event = streamer.next();
                    assertThat(event.timestampMs()).isGreaterThanOrEqualTo(prevTs);
                    prevTs = event.timestampMs();
                    if (event instanceof BacktestEvent.BinanceSpotTradeEvent) spotCount++;
                    if (event instanceof BacktestEvent.BinanceFuturesTradeEvent) futCount++;
                }

                assertThat(spotCount).isEqualTo(1);
                assertThat(futCount).isEqualTo(1);
            }
        }
    }

    @Test
    @DisplayName("Should lazily 3-way merge CLOB frames, daily cursor, and dynamically scheduled pending orders")
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
            streamer.loadCandleEvents(metadata, daily, Long.MAX_VALUE);

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
            streamer.loadCandleEvents(metadata, daily, Long.MAX_VALUE);
            assertThat(streamer.hasNext()).isTrue();
            streamer.clear();
            assertThat(streamer.hasNext()).isFalse();
        }
    }

    @Test
    @DisplayName("Should merge-stream Binance, Coinbase, and Kraken spot trades with Kraken USDT/USD peg into CalculatedSpotTwapEvents")
    void shouldMergeStreamMultiExchangeSpotTradesIntoCalculatedSpotTwapEvents(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB")) {
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(tempDir.toString(), duckDb);

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

                // Kraken USDT/USD peg at 1.0000
                String normUsdt = krakenUsdtFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 100L) + "::UBIGINT, 1.0000::DOUBLE, 1000.0::DOUBLE)" +
                        ") AS t(ts, price, qty)) TO '" + normUsdt + "' (FORMAT PARQUET)");

                // Binance BTCUSDT trade at +1000ms ($70,000)
                String normBinance = binanceSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 1000L) + "::UBIGINT, 1::BIGINT, 70000.0::DOUBLE, 1.0::DOUBLE, false::BOOLEAN)" +
                        ") AS t(ts, aggregate_trade_id, price, quantity, is_buyer_maker)) TO '" + normBinance + "' (FORMAT PARQUET)");

                // Coinbase BTCUSD trade at +2000ms ($70,020)
                String normCoinbase = coinbaseSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 2000L) + "::UBIGINT, " + (baseMs + 2000L) + "::UBIGINT, 'BTCUSD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 70020.0::DOUBLE, 1.0::DOUBLE)" +
                        ") AS t(ts, trade_timestamp, symbol, trade_id, side, price, qty)) TO '" + normCoinbase + "' (FORMAT PARQUET)");

                // Kraken BTCUSD trade at +3000ms ($70,040)
                String normKraken = krakenSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 3000L) + "::UBIGINT, " + (baseMs + 3000L) + "::UBIGINT, 'BTCUSD'::VARCHAR, 1::BIGINT, 'b'::VARCHAR, 'm'::VARCHAR, 70040.0::DOUBLE, 1.0::DOUBLE)" +
                        ") AS t(ts, trade_timestamp, symbol, trade_id, side, ord_type, price, qty)) TO '" + normKraken + "' (FORMAT PARQUET)");
            }

            CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                    tStart, tEnd, "2026-08-01T00:00:00Z", dateStr, pmFile,
                    "1001", "1002", 70000.0, 70030.0, TradeDirection.UP
            );

            try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
                streamer.loadCandleEvents(metadata, new BacktestEvent[0], datasetService, "BTCUSDT", Long.MAX_VALUE);

                List<BacktestEvent.CalculatedSpotTwapEvent> calcEvents = new ArrayList<>();
                while (streamer.hasNext()) {
                    BacktestEvent ev = streamer.next();
                    if (ev instanceof BacktestEvent.CalculatedSpotTwapEvent calcEv) {
                        calcEvents.add(calcEv);
                    }
                }

                // 3 spot trades (Binance @ +1000ms, Coinbase @ +2000ms, Kraken @ +3000ms) -> 3 CalculatedSpotTwapEvents
                assertThat(calcEvents).hasSize(3);
                assertThat(calcEvents.get(0).timestampMs()).isEqualTo(baseMs + 1000L);
                assertThat(calcEvents.get(0).spotPrice()).isCloseTo(70000.0, within(1e-4));

                assertThat(calcEvents.get(1).timestampMs()).isEqualTo(baseMs + 2000L);
                assertThat(calcEvents.get(1).spotPrice()).isBetween(70005.0, 70015.0);

                assertThat(calcEvents.get(2).timestampMs()).isEqualTo(baseMs + 3000L);
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

            streamer.loadCandleEvents(metadata, daily, 76_000L);

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
}
