package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.cache.RawMarketDataProcessor;
import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.strategy.TradeDirection;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.PriorityQueue;

/**
 * Multiplexes and streams time-ordered market events across Polymarket, Binance Spot,
 * Binance Futures trades, and OrderBook depth.
 */
public class ChronologicalEventStreamer {

    private static final Logger LOG = Logger.getLogger(ChronologicalEventStreamer.class);

    private final PriorityQueue<BacktestEvent> queue = new PriorityQueue<>();

    public boolean hasNext() {
        return !queue.isEmpty();
    }

    public BacktestEvent next() {
        return queue.poll();
    }

    public BacktestEvent peek() {
        return queue.peek();
    }

    public void scheduleEvent(BacktestEvent event) {
        if (event != null) {
            queue.add(event);
        }
    }

    public void clear() {
        queue.clear();
    }

    public int size() {
        return queue.size();
    }

    /**
     * Loads events for a specific 15-minute candle interval into the priority queue.
     * Uses raw data files if available, otherwise synthesizes events from candle metrics.
     */
    public void loadCandleEvents(
            BacktestMarketRow candle,
            boolean preferRawFiles,
            RawMarketDataProcessor rawProcessor,
            String symbol) {

        if (candle == null) {
            return;
        }

        long tStart = candle.tStart();
        long tEnd = candle.tEnd();
        long tStartMs = tStart * 1000L;
        long tEndMs = tEnd * 1000L;
        TradeDirection actualOutcome = "UP".equalsIgnoreCase(candle.actualOutcome()) ? TradeDirection.UP : TradeDirection.DOWN;

        // 1. Mandatory Candle Lifecycle Milestones
        scheduleEvent(new BacktestEvent.CandleLifecycleEvent(
                tStartMs,
                BacktestEvent.CandleLifecycleType.CANDLE_START,
                tStart, tEnd, actualOutcome, null
        ));

        scheduleEvent(new BacktestEvent.CandleLifecycleEvent(
                (tStart + 60L) * 1000L,
                BacktestEvent.CandleLifecycleType.TWAP_60S_SAMPLE,
                tStart, tEnd, actualOutcome, null
        ));

        scheduleEvent(new BacktestEvent.CandleLifecycleEvent(
                tEndMs,
                BacktestEvent.CandleLifecycleType.CANDLE_CLOSE,
                tStart, tEnd, actualOutcome, candle.toMarketCandle()
        ));

        boolean rawLoaded = false;
        if (preferRawFiles && rawProcessor != null) {
            rawLoaded = tryLoadFromRawFiles(candle, rawProcessor, symbol);
        }

        if (!rawLoaded) {
            loadSynthesizedCandleEvents(candle);
        }
    }

    private boolean tryLoadFromRawFiles(BacktestMarketRow candle, RawMarketDataProcessor rawProcessor, String symbol) {
        long tStart = candle.tStart();
        long tEnd = candle.tEnd();
        long tStartMs = tStart * 1000L;
        long tEndMs = tEnd * 1000L;

        Path pmFile = rawProcessor.getPolymarketDir(symbol).resolve(tStart + ".parquet");
        if (!Files.exists(pmFile)) {
            return false;
        }

        int loadedCount = 0;

        // 1. Load Polymarket Parquet L2 events
        String normPath = pmFile.toAbsolutePath().toString().replace('\\', '/');
        String query = "SELECT event_type, t, price, size, side, bids, asks FROM read_parquet('" + normPath + "') " +
                "WHERE t >= " + tStartMs + " AND t <= " + tEndMs + " ORDER BY t ASC";

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String et = rs.getString(1);
                long tMs = rs.getLong(2);
                double price = rs.getDouble(3);
                double size = rs.getDouble(4);
                String side = rs.getString(5);
                String bidsJson = rs.getString(6);
                String asksJson = rs.getString(7);

                scheduleEvent(new BacktestEvent.PolymarketL2UpdateEvent(
                        tMs, et, price, size, side, bidsJson, asksJson
                ));
                loadedCount++;
            }
        } catch (SQLException e) {
            LOG.warnf("Could not stream raw Polymarket events for %d: %s", tStart, e.getMessage());
            return false;
        }

        // 2. Synthesize or stream Spot / Futures trade updates
        scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(tStartMs, candle.sOpen(), 1.0, false));
        scheduleEvent(new BacktestEvent.BinanceFuturesTradeEvent(tStartMs, candle.fOpen(), 1.0, false));

        scheduleEvent(new BacktestEvent.TwapUpdateEvent(
                (tStart + 60L) * 1000L,
                new TwapUpdate(
                        Timeframe.FIFTEEN_MINUTES,
                        tStart,
                        tEnd,
                        BigDecimal.valueOf(candle.twapOpen()),
                        new TwapPoint(tStart + 60L, BigDecimal.valueOf(candle.sOpen()), BigDecimal.valueOf(candle.sOpen())),
                        false
                )
        ));

        scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent((tStart + 60L) * 1000L, candle.sOpen(), 1.0, false));
        scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(tEndMs, candle.sClose(), 1.0, false));
        scheduleEvent(new BacktestEvent.BinanceFuturesTradeEvent(tEndMs, candle.fClose(), 1.0, false));

        return loadedCount > 0;
    }

    private void loadSynthesizedCandleEvents(BacktestMarketRow candle) {
        long tStart = candle.tStart();
        long tEnd = candle.tEnd();
        long tStartMs = tStart * 1000L;
        long tEndMs = tEnd * 1000L;

        // Spot & Futures trades at Open
        scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(tStartMs, candle.sOpen(), 1.0, false));
        scheduleEvent(new BacktestEvent.BinanceFuturesTradeEvent(tStartMs, candle.fOpen(), 1.0, false));

        // Initial TWAP at Open
        scheduleEvent(new BacktestEvent.TwapUpdateEvent(
                tStartMs + 5L,
                new TwapUpdate(
                        Timeframe.FIFTEEN_MINUTES,
                        tStart,
                        tEnd,
                        BigDecimal.valueOf(candle.twapOpen()),
                        new TwapPoint(tStart, BigDecimal.valueOf(candle.sOpen()), BigDecimal.valueOf(candle.sOpen())),
                        false
                )
        ));

        // Initial Quote at t = 0
        OrderBookQuote quote0 = candle.toOrderBookQuote(0);
        scheduleEvent(new BacktestEvent.PolymarketQuoteEvent(tStartMs + 10L, quote0));

        // Intermediate spot & futures price and delta at t = 60s
        double spotPrice60 = (Math.abs(candle.sClose() - candle.twapOpen()) > 50.0) ? candle.sClose() : candle.sOpen();
        double spotDelta = candle.sDeltaBtc();
        boolean isSpotBuyerMaker = spotDelta < 0.0;
        double spotSize = Math.max(1.0, Math.abs(spotDelta));

        // TWAP update at t = 60s
        scheduleEvent(new BacktestEvent.TwapUpdateEvent(
                (tStart + 60L) * 1000L,
                new TwapUpdate(
                        Timeframe.FIFTEEN_MINUTES,
                        tStart,
                        tEnd,
                        BigDecimal.valueOf(candle.twapOpen()),
                        new TwapPoint(tStart + 60L, BigDecimal.valueOf(spotPrice60), BigDecimal.valueOf(spotPrice60)),
                        true
                )
        ));

        // Spot & Futures Trades at t = 60s
        scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent((tStart + 60L) * 1000L, spotPrice60, spotSize, isSpotBuyerMaker));
        double futPrice60 = (Math.abs(candle.fClose() - candle.fOpen()) > 50.0) ? candle.fClose() : candle.fOpen();
        double futDelta = candle.fDeltaBtc();
        boolean isFutBuyerMaker = futDelta < 0.0;
        double futSize = Math.max(1.0, Math.abs(futDelta));
        scheduleEvent(new BacktestEvent.BinanceFuturesTradeEvent((tStart + 60L) * 1000L, futPrice60, futSize, isFutBuyerMaker));

        // Order Book Quote at t = 60s
        OrderBookQuote quote60 = candle.toOrderBookQuote(60);
        scheduleEvent(new BacktestEvent.PolymarketQuoteEvent((tStart + 60L) * 1000L + 5L, quote60));

        // Intermediate Quote at t = 300s (5 min) for intra-candle exit checks
        OrderBookQuote quote300 = candle.toOrderBookQuote(300);
        scheduleEvent(new BacktestEvent.PolymarketQuoteEvent((tStart + 300L) * 1000L, quote300));

        // High / Low contract extremes if available
        if (!Double.isNaN(candle.pmMaxPrice()) && candle.pmMaxPrice() > 0.0) {
            OrderBookQuote maxQuote = new OrderBookQuote(
                    candle.pmMaxPrice(), candle.pmMaxPrice(),
                    Math.max(0.01, 1.0 - candle.pmMinPrice()), Math.max(0.01, 1.0 - candle.pmMinPrice()),
                    candle.pmDepth1cUp(), candle.pmDepth1cDown(),
                    candle.pmMaxPrice(), Math.max(0.01, 1.0 - candle.pmMinPrice()),
                    (tStart + 450L) * 1000L
            );
            scheduleEvent(new BacktestEvent.PolymarketQuoteEvent((tStart + 450L) * 1000L, maxQuote));
        }

        // Spot & Futures trades at Close
        scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(tEndMs - 10L, candle.sClose(), 1.0, false));
        scheduleEvent(new BacktestEvent.BinanceFuturesTradeEvent(tEndMs - 10L, candle.fClose(), 1.0, false));
    }
}
