package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.TypedClobParquetTestHelper;
import cz.polymarket.bot.backtest.TypedClobParquetTestHelper.ClobRow;
import cz.polymarket.bot.backtest.data.CollectorMarketMetadata;
import cz.polymarket.bot.backtest.data.ExchangeDelays;
import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.DuckDbSessionManager;
import cz.polymarket.bot.orderbook.PolymarketOrderBook;
import cz.polymarket.bot.strategy.TradeDirection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
    @DisplayName("Should pop events strictly in ascending receipt timestamp (ts) and priority order")
    void shouldPopEventsInAscendingOrder() {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp");
             ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
            streamer.scheduleEvent(new BacktestEvent.PolymarketCryptoPriceEvent(3000L, 1L, "btcusd", "chainlink", 60200.0, 2995L));
            streamer.scheduleEvent(new BacktestEvent.PolymarketCryptoPriceEvent(1000L, 2L, "btcusd", "chainlink", 60000.0, 995L));
            streamer.scheduleActionAt(2000L, () -> {});

            List<Long> timestamps = new ArrayList<>();
            while (streamer.hasNext()) {
                timestamps.add(streamer.next().timestampMs());
            }

            assertThat(timestamps).containsExactly(1000L, 2000L, 3000L);
        }
    }

    @Test
    @DisplayName("Should stream typed Polymarket CLOB frames + BBO hints, full-accuracy PolyBolt feeds, and raw CEX spot trades")
    void shouldStreamPolymarketFullAccuracyAndRawSpotStreams(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp")) {
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(tempDir.toString(), duckDb);

            long tStart = 1785542400L; // 2026-08-01 00:00:00 UTC
            long tEnd = tStart + 900L;
            long baseMs = tStart * 1000L;
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

                // Book snapshot with stale ask at 0.50, followed by price_change + bbo_hint pruning stale 0.50 ask up to 0.52
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile, List.of(
                        ClobRow.discovered(baseMs - 1000L, "1001", "1002"),
                        ClobRow.bookLevel(baseMs + 200L, baseMs + 190L, 4, true, true, 4800, 150.0),
                        ClobRow.bookLevel(baseMs + 200L, baseMs + 190L, 4, true, false, 5000, 100.0),
                        ClobRow.bookLevel(baseMs + 200L, baseMs + 190L, 4, true, false, 5200, 200.0),
                        ClobRow.priceChange(baseMs + 500L, baseMs + 490L, 8, true, false, 5200, 250.0),
                        ClobRow.priceChange(baseMs + 500L, baseMs + 490L, 8, false, true, 4800, 250.0),
                        ClobRow.bboHint(baseMs + 500L, baseMs + 490L, 8, true, true, 4800),
                        ClobRow.bboHint(baseMs + 500L, baseMs + 490L, 8, true, false, 5200)
                ));

                // Full accuracy strings carry exact sub-cent decimals while rounded double column is coarse
                String normCrypto = cryptoPriceFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 700L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60050.0::DOUBLE, '60050.123456'::VARCHAR, " + (baseMs + 690L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, price, full_accuracy_price, price_timestamp)) TO '" + normCrypto + "' (FORMAT PARQUET)");

                String normTwap = twap60sFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 800L) + "::UBIGINT, 1::UBIGINT, 'btcusd'::VARCHAR, 'chainlink'::VARCHAR, 60025.0::DOUBLE, '60025.987654'::VARCHAR, 60::INT, " + (baseMs + 790L) + "::UBIGINT)" +
                        ") AS t(ts, seq, symbol, source, twap_price, full_accuracy_twap_price, window_seconds, twap_timestamp)) TO '" + normTwap + "' (FORMAT PARQUET)");

                String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(NULL::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 60000.0::DOUBLE, 0.5::DOUBLE, 1::BIGINT, 1::BIGINT, " + (baseMs + 1000L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                        ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normSpot + "' (FORMAT PARQUET)");

                String normKraken = krakenFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 900L) + "::UBIGINT, 'USDT/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'limit'::VARCHAR, 0.9990::DOUBLE, 1000.0::DOUBLE, '2026-08-01T00:00:00.900000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normKraken + "' (FORMAT PARQUET)");
            }

            CollectorMarketMetadata metadata = new CollectorMarketMetadata(
                    tStart, tEnd, "2026-08-01T00:00:00Z", dateStr, pmFile,
                    "1001", "1002", 60025.987654, 60120.0, TradeDirection.UP
            );

            BacktestEvent[] sharedDaily = ChronologicalEventStreamer.loadSharedDailyStreams(
                    datasetService, "BTCUSDT", dateStr);
            PolymarketOrderBook book = new PolymarketOrderBook();

            try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
                streamer.openDailySpotStream(
                        datasetService, "BTCUSDT", dateStr, new ExchangeDelays(100L, 50L, 50L), baseMs, tEnd * 1000L, true);
                streamer.loadCandleEvents(metadata, sharedDaily, Long.MAX_VALUE);

                int clobBatchCount = 0;
                List<BacktestEvent.ExchangeSpotTradeEvent> spotEvents = new ArrayList<>();
                double observedCryptoPrice = Double.NaN;
                double observedTwapPrice = Double.NaN;

                while (streamer.hasNext()) {
                    BacktestEvent event = streamer.next();
                    if (event instanceof BacktestEvent.PolymarketClobFrameEvent f) {
                        clobBatchCount++;
                        BacktestEngine.applyClobFrameBatch(streamer, book, f);
                    } else if (event instanceof BacktestEvent.ExchangeSpotTradeEvent spotEv) {
                        spotEvents.add(spotEv);
                    } else if (event instanceof BacktestEvent.PolymarketCryptoPriceEvent pEv) {
                        observedCryptoPrice = pEv.price();
                    } else if (event instanceof BacktestEvent.PolymarketCryptoTwap60sEvent tEv) {
                        observedTwapPrice = tEv.twapPrice();
                    }
                }

                // Book snapshot (1) + co-located price_change+bbo_hint batch (2) -> stale 0.50 ask pruned to 0.52!
                assertThat(clobBatchCount).isEqualTo(2);
                assertThat(book.getBestAskUp()).isCloseTo(0.52, within(1e-6));
                assertThat(book.getBestBidUp()).isCloseTo(0.48, within(1e-6));

                // Full accuracy values preserved without double-truncation loss
                assertThat(observedCryptoPrice).isCloseTo(60050.123456, within(1e-6));
                assertThat(observedTwapPrice).isCloseTo(60025.987654, within(1e-6));

                // Kraken USDT/USD (+900 + 50 = +950ms) and Binance (+1000 + 100 = +1100ms)
                assertThat(spotEvents).hasSize(2);
                assertThat(spotEvents.get(0).venueCode()).isEqualTo(BacktestEvent.ExchangeSpotTradeEvent.VENUE_KRAKEN_USDT);
                assertThat(spotEvents.get(0).timestampMs()).isEqualTo(baseMs + 950L);
                assertThat(spotEvents.get(1).venueCode()).isEqualTo(BacktestEvent.ExchangeSpotTradeEvent.VENUE_BINANCE);
                assertThat(spotEvents.get(1).timestampMs()).isEqualTo(baseMs + 1100L);
                assertThat(spotEvents.get(1).isTakerBuy()).isTrue();
            }
        }
    }

    @Test
    @DisplayName("Should lazily merge CLOB frames, daily cursor, and dynamically scheduled engine actions")
    void shouldLazilyMergeStreamClobEventsWithDynamicScheduledActions(@TempDir Path tempDir) throws Exception {
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

        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp");
             ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
            streamer.loadCandleEvents(metadata, daily, Long.MAX_VALUE);

            List<BacktestEvent> observed = new ArrayList<>();
            boolean scheduledDynamicOrder = false;

            while (streamer.hasNext()) {
                BacktestEvent ev = streamer.next();
                observed.add(ev);
                if (!scheduledDynamicOrder && ev instanceof BacktestEvent.PolymarketClobFrameEvent clob
                        && clob.timestampMs() == baseMs + 100L) {
                    scheduledDynamicOrder = true;
                    streamer.scheduleActionAt(baseMs + 300L, () -> {});
                }
            }

            List<Long> clobDailyAndOrderTimestamps = observed.stream()
                    .filter(e -> e instanceof BacktestEvent.PolymarketClobFrameEvent
                            || e instanceof BacktestEvent.PolymarketCryptoPriceEvent
                            || e instanceof BacktestEvent.ScheduledEngineActionEvent)
                    .map(BacktestEvent::timestampMs)
                    .toList();

            assertThat(clobDailyAndOrderTimestamps).containsExactly(
                    baseMs + 100L,
                    baseMs + 250L,
                    baseMs + 300L,
                    baseMs + 500L,
                    baseMs + 900L
            );

            streamer.loadCandleEvents(metadata, daily, Long.MAX_VALUE);
            assertThat(streamer.hasNext()).isTrue();
            streamer.clear();
            assertThat(streamer.hasNext()).isFalse();
        }
    }

    @Test
    @DisplayName("Should prune early CLOB frames when clobLookbackMs is bounded and enforce half-open [tStartMs, tEndMs) PolyBolt ownership")
    void shouldPruneEarlyClobFramesWhileSeedingBookAndRespectingHalfOpenBoundary(@TempDir Path tempDir) throws Exception {
        long tStart = 1785542400L;
        long tEnd = tStart + 900L;
        long baseMs = tStart * 1000L;
        long endMs = tEnd * 1000L;
        Path pmFile = tempDir.resolve("btc-updown-15m-" + tStart + ".parquet");
        Path pmFileNext = tempDir.resolve("btc-updown-15m-" + tEnd + ".parquet");

        TypedClobParquetTestHelper.writeParquet(pmFile, List.of(
                ClobRow.discovered(baseMs - 2000L, "1001", "1002"),
                ClobRow.bookLevel(baseMs + 5_000L, baseMs + 4_990L, 4, true, false, 6000, 100.0),
                ClobRow.priceChange(baseMs + 10_000L, baseMs + 9_990L, 8, true, false, 6100, 100.0),
                ClobRow.bookLevel(baseMs + 818_000L, baseMs + 817_985L, 12, false, false, 4700, 250.0),
                ClobRow.bookLevel(baseMs + 820_000L, baseMs + 819_985L, 16, true, false, 5300, 200.0),
                ClobRow.priceChange(baseMs + 822_000L, baseMs + 821_988L, 20, true, false, 5200, 180.0),
                ClobRow.priceChange(baseMs + 850_000L, baseMs + 849_990L, 24, true, true, 5000, 150.0)
        ));
        TypedClobParquetTestHelper.writeParquet(pmFileNext, List.of(
                ClobRow.discovered(endMs - 2000L, "2001", "2002")
        ));

        CollectorMarketMetadata metadata1 = new CollectorMarketMetadata(
                tStart, tEnd, "2026-08-01T00:00:00Z", "2026-08-01", pmFile,
                "1001", "1002", 60000.0, 60080.0, TradeDirection.UP
        );
        CollectorMarketMetadata metadata2 = new CollectorMarketMetadata(
                tEnd, tEnd + 900L, "2026-08-01T00:15:00Z", "2026-08-01", pmFileNext,
                "2001", "2002", 60080.0, 60120.0, TradeDirection.UP
        );

        BacktestEvent[] daily = new BacktestEvent[]{
                new BacktestEvent.PolymarketCryptoTwap60sEvent(baseMs - 400L, 1L, "btcusd", "chainlink", 60000.0, 60, baseMs - 400L),
                new BacktestEvent.PolymarketCryptoPriceEvent(baseMs + 5_000L, 2L, "btcusd", "chainlink", 60005.0, baseMs + 5_000L),
                new BacktestEvent.PolymarketCryptoTwap60sEvent(endMs, 3L, "btcusd", "chainlink", 60080.0, 60, endMs)
        };

        PolymarketOrderBook book = new PolymarketOrderBook();
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp");
             ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {

            streamer.loadCandleEvents(metadata1, daily, 76_000L);

            List<Long> clobTimestamps = new ArrayList<>();
            List<Double> candle1TwapPrices = new ArrayList<>();
            int cryptoPriceCount = 0;

            while (streamer.hasNext()) {
                BacktestEvent ev = streamer.next();
                if (ev instanceof BacktestEvent.PolymarketClobFrameEvent f) {
                    clobTimestamps.add(f.timestampMs());
                    BacktestEngine.applyClobFrameBatch(streamer, book, f);
                } else if (ev instanceof BacktestEvent.PolymarketCryptoTwap60sEvent twap) {
                    candle1TwapPrices.add(twap.twapPrice());
                } else if (ev instanceof BacktestEvent.PolymarketCryptoPriceEvent) {
                    cryptoPriceCount++;
                }
            }

            assertThat(clobTimestamps).containsExactly(
                    baseMs + 818_000L,
                    baseMs + 820_000L,
                    baseMs + 822_000L,
                    baseMs + 850_000L
            );
            assertThat(book.getBestAskUp()).isCloseTo(0.52, within(0.0001));
            assertThat(book.getBestBidUp()).isCloseTo(0.50, within(0.0001));
            assertThat(book.getBestAskDown()).isCloseTo(0.47, within(0.0001));

            // Boundary 60080.0 at exact endMs belongs strictly to candle 2 [endMs, endMs + 900s), never duplicated in candle 1!
            assertThat(candle1TwapPrices).containsExactly(60000.0);
            assertThat(cryptoPriceCount).isEqualTo(1);

            streamer.loadCandleEvents(metadata2, daily, 76_000L);
            List<Double> candle2TwapPrices = new ArrayList<>();
            while (streamer.hasNext()) {
                BacktestEvent ev = streamer.next();
                if (ev instanceof BacktestEvent.PolymarketCryptoTwap60sEvent twap) {
                    candle2TwapPrices.add(twap.twapPrice());
                }
            }
            assertThat(candle2TwapPrices).containsExactly(60080.0);
        }
    }

    @Test
    @DisplayName("Should reorder multi-exchange spot trades chronologically after adding per-exchange delays and slice continuously across candle boundaries")
    void shouldReorderSpotTradesByExchangeDelayAndSliceAcrossBoundaries(@TempDir Path tempDir) throws Exception {
        try (DuckDbSessionManager duckDb = new DuckDbSessionManager("256MB", 1, "target/test-data/duckdb-tmp")) {
            CollectorParquetDatasetService datasetService = new CollectorParquetDatasetService(tempDir.toString(), duckDb);

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

            Path pmFile1 = pmDir.resolve("btc-updown-15m-" + tStart + ".parquet");
            Path pmFile2 = pmDir.resolve("btc-updown-15m-" + tEnd + ".parquet");

            try (Connection conn = duckDb.openSession();
                 Statement stmt = conn.createStatement()) {

                TypedClobParquetTestHelper.writeParquet(stmt, pmFile1, List.of(
                        ClobRow.discovered(baseMs - 1000L, "1001", "1002")
                ));
                TypedClobParquetTestHelper.writeParquet(stmt, pmFile2, List.of(
                        ClobRow.discovered(endMs - 1000L, "2001", "2002")
                ));

                // Kraken USDT/USD peg before candle open (server -1000ms + 50ms = -950ms -> seeded at windowStartMs)
                String normUsdt = krakenUsdtFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs - 1000L) + "::UBIGINT, 'USDT/USD'::VARCHAR, 1::BIGINT, 'buy'::VARCHAR, 'limit'::VARCHAR, 1.0000::DOUBLE, 1000.0::DOUBLE, '2026-07-31T23:59:59.000000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normUsdt + "' (FORMAT PARQUET)");

                // Binance Spot:
                // 1) event_time (-60ms + 100ms = baseMs + 40ms -> shifted INTO candle 1!)
                // 2) event_time NULL, trade_time (+1000ms + 100ms = baseMs + 1100ms -> arrives AFTER Coinbase & Kraken!)
                // 3) trade_time near candle close (endMs - 50ms + 100ms = endMs + 50ms -> shifted INTO candle 2!)
                String normBinance = binanceSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs - 60L) + "::UBIGINT, 1::BIGINT, 'BTCUSDT'::VARCHAR, 69950.0::DOUBLE, 1.0::DOUBLE, 1::BIGINT, 1::BIGINT, " + (baseMs - 70L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                        "(NULL::UBIGINT, 2::BIGINT, 'BTCUSDT'::VARCHAR, 70000.0::DOUBLE, 1.0::DOUBLE, 2::BIGINT, 2::BIGINT, " + (baseMs + 1000L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN), " +
                        "(NULL::UBIGINT, 3::BIGINT, 'BTCUSDT'::VARCHAR, 70500.0::DOUBLE, 1.0::DOUBLE, 3::BIGINT, 3::BIGINT, " + (endMs - 50L) + "::UBIGINT, false::BOOLEAN, true::BOOLEAN)" +
                        ") AS t(event_time, aggregate_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, trade_time, is_buyer_maker, is_best_match)) TO '" + normBinance + "' (FORMAT PARQUET)");

                // Coinbase Spot at server +1020ms ('sell' maker -> taker buy = true, +50ms = baseMs + 1070ms)
                String normCoinbase = coinbaseSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 1020L) + "::UBIGINT, 'BTC-USD'::VARCHAR, 1::BIGINT, 'sell'::VARCHAR, 70100.0::DOUBLE, 1.0::DOUBLE, '2026-08-01T00:00:01.020000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, price, qty, timestamp)) TO '" + normCoinbase + "' (FORMAT PARQUET)");

                // Kraken Spot at server +1040ms ('sell' -> taker buy = false, +50ms = baseMs + 1090ms)
                String normKraken = krakenSpotFile.toAbsolutePath().toString().replace('\\', '/');
                stmt.execute("COPY (SELECT * FROM (VALUES " +
                        "(" + (baseMs + 1040L) + "::UBIGINT, 'BTC/USD'::VARCHAR, 1::BIGINT, 'sell'::VARCHAR, 'limit'::VARCHAR, 70200.0::DOUBLE, 1.0::DOUBLE, '2026-08-01T00:00:01.040000Z'::VARCHAR)" +
                        ") AS t(trade_timestamp, symbol, trade_id, side, ord_type, price, qty, timestamp)) TO '" + normKraken + "' (FORMAT PARQUET)");
            }

            CollectorMarketMetadata metadata1 = new CollectorMarketMetadata(
                    tStart, tEnd, "2026-08-01T00:00:00Z", dateStr, pmFile1,
                    "1001", "1002", 70000.0, 70100.0, TradeDirection.UP
            );
            CollectorMarketMetadata metadata2 = new CollectorMarketMetadata(
                    tEnd, tEnd + 900L, "2026-08-01T00:15:00Z", dateStr, pmFile2,
                    "2001", "2002", 70100.0, 70200.0, TradeDirection.UP
            );

            try (ChronologicalEventStreamer streamer = new ChronologicalEventStreamer(duckDb)) {
                streamer.openDailySpotStream(
                        datasetService,
                        "BTCUSDT",
                        dateStr,
                        new ExchangeDelays(100L, 50L, 50L),
                        baseMs,
                        (tEnd + 900L) * 1000L,
                        true
                );

                streamer.loadCandleEvents(metadata1, new BacktestEvent[0], Long.MAX_VALUE);
                List<BacktestEvent.ExchangeSpotTradeEvent> candle1Spots = new ArrayList<>();
                while (streamer.hasNext()) {
                    if (streamer.next() instanceof BacktestEvent.ExchangeSpotTradeEvent spotEv) {
                        candle1Spots.add(spotEv);
                    }
                }

                assertThat(candle1Spots)
                        .extracting(BacktestEvent.ExchangeSpotTradeEvent::timestampMs)
                        .containsExactly(
                                baseMs,          // Seeded prior Kraken USDT/USD peg
                                baseMs + 40L,    // Binance pre-open event_time (-60 + 100)
                                baseMs + 1070L,  // Coinbase (+1020 + 50)
                                baseMs + 1090L,  // Kraken (+1040 + 50)
                                baseMs + 1100L   // Binance (+1000 + 100)
                        );
                assertThat(candle1Spots.get(2).isTakerBuy()).isTrue();
                assertThat(candle1Spots.get(3).isTakerBuy()).isFalse();

                // Candle 2 seamlessly picks up the delayed Binance trade at endMs + 50ms without reopening the daily cursor!
                streamer.loadCandleEvents(metadata2, new BacktestEvent[0], Long.MAX_VALUE);
                List<BacktestEvent.ExchangeSpotTradeEvent> candle2Spots = new ArrayList<>();
                while (streamer.hasNext()) {
                    if (streamer.next() instanceof BacktestEvent.ExchangeSpotTradeEvent spotEv) {
                        candle2Spots.add(spotEv);
                    }
                }
                assertThat(candle2Spots)
                        .extracting(BacktestEvent.ExchangeSpotTradeEvent::timestampMs)
                        .containsExactly(endMs + 50L);
            }
        }
    }
}
