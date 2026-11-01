package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.backtest.cache.RawMarketDataProcessor;
import cz.polymarket.bot.backtest.data.BacktestMarketRow;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
import cz.polymarket.bot.strategy.TradeDirection;
import org.jboss.logging.Logger;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.PriorityQueue;

/**
 * Multiplexes and streams time-ordered market events across Polymarket, Binance Spot,
 * Binance Futures trades, and OrderBook depth directly from captured raw files.
 */
public class ChronologicalEventStreamer {

    private static final Logger LOG = Logger.getLogger(ChronologicalEventStreamer.class);

    private final PriorityQueue<BacktestEvent> queue = new PriorityQueue<>();

    /**
     * Checks if there are pending events in the priority queue.
     *
     * @return true if more events exist
     */
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    /**
     * Retrieves and removes the next chronological event from the priority queue.
     *
     * @return next event, or null if queue is empty
     */
    public BacktestEvent next() {
        return queue.poll();
    }

    /**
     * Schedules a future or immediate event into the chronological queue.
     *
     * @param event BacktestEvent to schedule
     */
    public void scheduleEvent(BacktestEvent event) {
        if (event != null) {
            queue.add(event);
        }
    }

    /**
     * Clears all pending events from the queue.
     */
    public void clear() {
        queue.clear();
    }

    /**
     * Loads events for a specific 15-minute candle interval into the priority queue
     * strictly from raw data files without synthesizing missing events.
     *
     * @param candle candle interval market row
     * @param rawProcessor processor providing raw directory locations and reading
     * @param symbol trading asset pair
     */
    public void loadCandleEvents(
            BacktestMarketRow candle,
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

        if (rawProcessor != null) {
            loadFromRawFiles(candle, rawProcessor, symbol);
        }
    }

    /**
     * Reads raw Level 2 Polymarket book snapshots, deltas, trades, and Binance Spot/Futures trades from Parquet files.
     * Missing files are not synthesized.
     *
     * @param candle market row
     * @param rawProcessor processor resolving directory paths
     * @param symbol asset symbol
     */
    private void loadFromRawFiles(BacktestMarketRow candle, RawMarketDataProcessor rawProcessor, String symbol) {
        long tStart = candle.tStart();
        long tEnd = candle.tEnd();
        long tStartMs = tStart * 1000L;
        long tEndMs = tEnd * 1000L;
        String sym = (symbol != null && !symbol.isBlank()) ? symbol.toUpperCase() : "BTCUSDT";

        // 1. Load Polymarket Parquet L2 events if file exists
        Path pmFile = rawProcessor.getPolymarketDir(sym).resolve(tStart + ".parquet");
        if (Files.exists(pmFile)) {
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
                }
            } catch (SQLException e) {
                LOG.warnf("Could not stream raw Polymarket events for %d: %s", tStart, e.getMessage());
            }
        }

        // 2. Load raw Binance Spot trades if daily file exists
        String dateStr = candle.dateStr();
        if (dateStr != null && !dateStr.isBlank()) {
            Path spotFile = rawProcessor.getBinanceSpotAggTradesDir(sym)
                    .resolve(sym + "-aggTrades-" + dateStr + ".parquet");
            if (Files.exists(spotFile)) {
                double lastSpotPrice = Double.NaN;
                String normSpot = spotFile.toAbsolutePath().toString().replace('\\', '/');
                String spotQuery = "SELECT transact_time, price, quantity, is_buyer_maker FROM read_parquet('" + normSpot + "') " +
                        "WHERE (CASE WHEN transact_time > 1000000000000000 THEN transact_time / 1000 ELSE transact_time END) >= " + tStartMs +
                        " AND (CASE WHEN transact_time > 1000000000000000 THEN transact_time / 1000 ELSE transact_time END) <= " + tEndMs +
                        " ORDER BY transact_time ASC";

                try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(spotQuery)) {

                    while (rs.next()) {
                        long tRaw = rs.getLong(1);
                        long tMs = (tRaw > 1_000_000_000_000_000L) ? (tRaw / 1000L) : tRaw;
                        double price = rs.getDouble(2);
                        double qty = rs.getDouble(3);
                        boolean isBuyerMaker = rs.getBoolean(4);
                        lastSpotPrice = price;

                        scheduleEvent(new BacktestEvent.BinanceSpotTradeEvent(tMs, price, qty, isBuyerMaker));
                    }
                } catch (SQLException e) {
                    LOG.warnf("Could not stream raw Binance Spot events for %d: %s", tStart, e.getMessage());
                }

                if (!Double.isNaN(candle.twapOpen()) && candle.twapOpen() > 0.0 && !Double.isNaN(lastSpotPrice)) {
                    scheduleEvent(new BacktestEvent.TwapUpdateEvent(
                            (tStart + 60L) * 1000L,
                            new TwapUpdate(
                                    Timeframe.FIFTEEN_MINUTES,
                                    tStart,
                                    tEnd,
                                    BigDecimal.valueOf(candle.twapOpen()),
                                    new TwapPoint(tStart + 60L, BigDecimal.valueOf(lastSpotPrice), BigDecimal.valueOf(lastSpotPrice)),
                                    false
                            )
                    ));
                }
            }

            // 3. Load raw Binance Futures trades if daily file exists
            Path futFile = rawProcessor.getBinanceFuturesAggTradesDir(sym)
                    .resolve(sym + "-aggTrades-" + dateStr + ".parquet");
            if (Files.exists(futFile)) {
                String normFut = futFile.toAbsolutePath().toString().replace('\\', '/');
                String futQuery = "SELECT transact_time, price, quantity, is_buyer_maker FROM read_parquet('" + normFut + "') " +
                        "WHERE (CASE WHEN transact_time > 1000000000000000 THEN transact_time / 1000 ELSE transact_time END) >= " + tStartMs +
                        " AND (CASE WHEN transact_time > 1000000000000000 THEN transact_time / 1000 ELSE transact_time END) <= " + tEndMs +
                        " ORDER BY transact_time ASC";

                try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(futQuery)) {

                    while (rs.next()) {
                        long tRaw = rs.getLong(1);
                        long tMs = (tRaw > 1_000_000_000_000_000L) ? (tRaw / 1000L) : tRaw;
                        double price = rs.getDouble(2);
                        double qty = rs.getDouble(3);
                        boolean isBuyerMaker = rs.getBoolean(4);

                        scheduleEvent(new BacktestEvent.BinanceFuturesTradeEvent(tMs, price, qty, isBuyerMaker));
                    }
                } catch (SQLException e) {
                    LOG.warnf("Could not stream raw Binance Futures events for %d: %s", tStart, e.getMessage());
                }
            }
        }
    }
}
