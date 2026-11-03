package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.TradeDirection;
import io.quarkus.logging.Log;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Matching engine simulating realistic order placement, round-trip network/exchange latency
 * derived from captured Polymarket timestamps ({@code delayMs = round(2.0 * observedOneWayLatencyMs * latencyFactor)}),
 * delayed order book liquidity depletion at execution time, multiple simultaneous positions per market,
 * crypto taker fees, and contract settlements.
 */
public class SimulatedMatchingEngine {

    private static final double EPS = 1e-9;

    private final PolymarketOrderBook orderBook;
    private final double latencyFactor;
    private Consumer<BacktestEvent> eventScheduler;
    private long orderSequence = 0L;

    private final List<OpenPosition> openPositions = new ArrayList<>();
    private final List<CompletedPosition> completedPositions = new ArrayList<>();

    /**
     * Constructs the simulated matching engine with a configurable latency scaling factor.
     *
     * @param orderBook reconstructed Polymarket L2 order book
     * @param latencyFactor multiplier applied to the real-time observed Polymarket latency (default {@code 1.0})
     */
    public SimulatedMatchingEngine(PolymarketOrderBook orderBook, double latencyFactor) {
        if (orderBook == null) {
            Log.error("Invalid argument in SimulatedMatchingEngine: orderBook cannot be null");
            throw new IllegalArgumentException("orderBook cannot be null");
        }
        if (Double.isNaN(latencyFactor) || latencyFactor < 0.0) {
            Log.error("Invalid argument in SimulatedMatchingEngine: latencyFactor must be non-negative");
            throw new IllegalArgumentException("latencyFactor must be non-negative");
        }
        this.orderBook = orderBook;
        this.latencyFactor = latencyFactor;
    }

    /**
     * Registers the event scheduler consumer to dispatch delayed order executions and execution reports.
     *
     * @param eventScheduler consumer scheduling future BacktestEvents
     */
    public void setEventScheduler(Consumer<BacktestEvent> eventScheduler) {
        this.eventScheduler = eventScheduler;
    }

    /**
     * Resets trade tracking state between candle intervals.
     */
    public void resetForCandle() {
        this.openPositions.clear();
        this.completedPositions.clear();
    }

    /**
     * Submits an order into the matching engine at {@code submitTimestampMs}.
     * <p>
     * Because collector order book states are indexed by local EC2 receipt timestamp
     * {@code ts = T_server + delta_1way}, an order submitted by the strategy at {@code T_submit}
     * reaches the Polymarket exchange server at {@code T_server + 2 * delta_1way}, which corresponds
     * to the collector order book state at:
     * {@code T_exec = T_submit + round(2.0 * observedOneWayLatencyMs * latencyFactor)}.
     *
     * @param command order command specifying token, direction, price, and size
     * @param submitTimestampMs local EC2 timestamp when the strategy submitted the order
     */
    public void submitOrder(OrderCommand command, long submitTimestampMs) {
        if (command == null) {
            return;
        }

        long observedOneWayMs = orderBook.getLastObservedOneWayLatencyMs();
        long delayMs = Math.max(0L, Math.round(2.0 * observedOneWayMs * latencyFactor));
        long executionTimestampMs = submitTimestampMs + delayMs;

        if (Log.isTraceEnabled()) {
            Log.tracef("Order submitted: clientOrderId=%s, side=%s, token=%s, limitPrice=%s, size=%s, submitTsMs=%d, delayMs=%d, execTsMs=%d",
                    command.clientOrderId(), command.side(), command.token(),
                    command.price(), command.size(), submitTimestampMs, delayMs, executionTimestampMs);
        }

        if (eventScheduler != null) {
            eventScheduler.accept(new BacktestEvent.PendingOrderExecutionEvent(
                    executionTimestampMs,
                    command,
                    delayMs,
                    ++orderSequence
            ));
        } else {
            executeOrderAtExchange(command, executionTimestampMs, delayMs);
        }
    }

    /**
     * Executes a scheduled pending order when it arrives at the exchange matching engine at {@code event.timestampMs()},
     * sweeping and depleting the current order book depth at that exact delayed timestamp.
     *
     * @param event pending order execution event popped from the chronological event queue
     * @return generated {@link ExecutionReport} to deliver to the strategy
     */
    public ExecutionReport executePendingOrder(BacktestEvent.PendingOrderExecutionEvent event) {
        if (event == null || event.command() == null) {
            return null;
        }
        return executeOrderAtExchange(event.command(), event.timestampMs(), event.delayMs());
    }

    private ExecutionReport executeOrderAtExchange(OrderCommand command, long fillTimestampMs, long delayMs) {
        boolean isBuy = "BUY".equalsIgnoreCase(command.side());
        boolean isUpToken = orderBook.isUpToken(command.token());
        double reqShares = command.size().doubleValue();
        double limitPrice = command.price().doubleValue();

        PolymarketOrderBook.FillResult fill = orderBook.executeFill(isUpToken, isBuy, reqShares, limitPrice);
        if (fill.executedShares() <= 0.0) {
            if (Log.isTraceEnabled()) {
                Log.tracef("Order rejected (no liquidity): clientOrderId=%s, side=%s, token=%s, limitPrice=%.4f, reqShares=%.2f, fillTsMs=%d, delayMs=%d",
                        command.clientOrderId(), command.side(), command.token(),
                        limitPrice, reqShares, fillTimestampMs, delayMs);
            }
            return new ExecutionReport(
                    command.clientOrderId(),
                    "sim-reject-" + command.clientOrderId(),
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    fillTimestampMs,
                    "No liquidity available in order book at execution timestamp (latency " + delayMs + "ms)"
            );
        }

        TradeDirection tokenDirection = isUpToken ? TradeDirection.UP : TradeDirection.DOWN;

        if (isBuy) {
            double entryPrice = fill.avgPrice();
            double shares = fill.executedShares();
            double entryFee = fill.fee();
            double modelProb = Math.min(0.95, entryPrice + 0.10);
            double edge = Math.max(0.0, modelProb - entryPrice);

            openPositions.add(new OpenPosition(
                    tokenDirection,
                    entryPrice,
                    shares,
                    entryFee,
                    modelProb,
                    entryPrice,
                    edge
            ));

            if (Log.isTraceEnabled()) {
                Log.tracef("Order filled (BUY): clientOrderId=%s, direction=%s, shares=%.2f, avgPrice=%.4f, feeUsd=%.4f, fillTsMs=%d, delayMs=%d",
                        command.clientOrderId(), tokenDirection, shares, entryPrice, entryFee, fillTimestampMs, delayMs);
            }

            return new ExecutionReport(
                    command.clientOrderId(),
                    "sim-fill-" + command.clientOrderId(),
                    "FILLED",
                    BigDecimal.valueOf(fill.avgPrice()),
                    BigDecimal.valueOf(fill.executedShares()),
                    fillTimestampMs,
                    "Simulated fill with " + delayMs + "ms round-trip latency"
            );
        } else {
            double sharesToClose = fill.executedShares();
            double totalSoldShares = fill.executedShares();
            double exitPrice = fill.avgPrice();
            double totalExitFee = fill.fee();
            String exitReason = exitPrice >= 0.70 ? "Take Profit (0.70)" : "Trailing Stop (+0.05)";

            Iterator<OpenPosition> it = openPositions.iterator();
            while (it.hasNext() && sharesToClose > EPS) {
                OpenPosition pos = it.next();
                if (pos.side != tokenDirection || pos.remainingShares <= EPS) {
                    continue;
                }
                double closedShares = Math.min(sharesToClose, pos.remainingShares);
                double posFraction = closedShares / pos.remainingShares;
                double allocatedEntryFee = pos.remainingFee * posFraction;
                double allocatedExitFee = totalExitFee * (closedShares / totalSoldShares);

                completedPositions.add(new CompletedPosition(
                        pos.side,
                        pos.entryPrice,
                        exitPrice,
                        closedShares * pos.entryPrice,
                        closedShares,
                        allocatedEntryFee + allocatedExitFee,
                        exitReason,
                        pos.modelProbability,
                        pos.marketPrice,
                        pos.edge
                ));

                pos.remainingShares -= closedShares;
                pos.remainingFee -= allocatedEntryFee;
                sharesToClose -= closedShares;

                if (pos.remainingShares <= EPS) {
                    it.remove();
                }
            }

            if (Log.isTraceEnabled()) {
                Log.tracef("Order filled (SELL): clientOrderId=%s, direction=%s, soldShares=%.2f, exitPrice=%.4f, exitFeeUsd=%.4f, reason=%s, fillTsMs=%d, delayMs=%d",
                        command.clientOrderId(), tokenDirection, totalSoldShares, exitPrice, totalExitFee, exitReason, fillTimestampMs, delayMs);
            }

            return new ExecutionReport(
                    command.clientOrderId(),
                    "sim-exit-" + command.clientOrderId(),
                    "FILLED",
                    BigDecimal.valueOf(fill.avgPrice()),
                    BigDecimal.valueOf(fill.executedShares()),
                    fillTimestampMs,
                    "Simulated exit fill with " + delayMs + "ms round-trip latency"
            );
        }
    }

    /**
     * Resolves and finalizes all active and previously exited positions for the candle interval
     * against the actual candle outcome.
     *
     * @param candleStartSec candle interval start in epoch seconds
     * @param datetimeUtc ISO-8601 formatted datetime UTC
     * @param actualOutcome settled binary outcome direction
     * @param currentBalance account balance before trade payouts
     * @return list of finalized {@link TradeRecord} entries in execution order
     */
    public List<TradeRecord> finalizeTrades(
            long candleStartSec,
            String datetimeUtc,
            TradeDirection actualOutcome,
            double currentBalance) {

        if (completedPositions.isEmpty() && openPositions.isEmpty()) {
            return List.of();
        }

        List<TradeRecord> records = new ArrayList<>(completedPositions.size() + openPositions.size());
        double runningBalance = currentBalance;

        for (CompletedPosition cp : completedPositions) {
            double payout = cp.shares * cp.exitPrice;
            double netPnl = payout - cp.sizeUsd - cp.totalFee;
            runningBalance += netPnl;
            boolean isWin = netPnl > 0.0;

            records.add(new TradeRecord(
                    datetimeUtc,
                    candleStartSec,
                    cp.side,
                    cp.entryPrice,
                    cp.exitPrice,
                    cp.sizeUsd,
                    cp.shares,
                    cp.totalFee,
                    netPnl,
                    cp.exitReason,
                    cp.modelProbability,
                    cp.marketPrice,
                    cp.edge,
                    isWin,
                    runningBalance
            ));
        }

        for (OpenPosition op : openPositions) {
            if (op.remainingShares <= EPS) {
                continue;
            }
            double finalExitPrice = (op.side == actualOutcome) ? 1.00 : 0.00;
            String finalExitReason = "Resolution (TWAP Oracle)";
            double sizeUsd = op.remainingShares * op.entryPrice;
            double payout = op.remainingShares * finalExitPrice;
            double totalFee = op.remainingFee;
            double netPnl = payout - sizeUsd - totalFee;
            runningBalance += netPnl;
            boolean isWin = netPnl > 0.0;

            records.add(new TradeRecord(
                    datetimeUtc,
                    candleStartSec,
                    op.side,
                    op.entryPrice,
                    finalExitPrice,
                    sizeUsd,
                    op.remainingShares,
                    totalFee,
                    netPnl,
                    finalExitReason,
                    op.modelProbability,
                    op.marketPrice,
                    op.edge,
                    isWin,
                    runningBalance
            ));
        }

        completedPositions.clear();
        openPositions.clear();
        return records;
    }

    /**
     * Checks if the matching engine holds any open or completed trades in the current candle.
     *
     * @return true if at least one trade was executed in the current candle
     */
    public boolean hasActiveTrade() {
        return !openPositions.isEmpty() || !completedPositions.isEmpty();
    }

    private static final class OpenPosition {
        private final TradeDirection side;
        private final double entryPrice;
        private double remainingShares;
        private double remainingFee;
        private final double modelProbability;
        private final double marketPrice;
        private final double edge;

        private OpenPosition(
                TradeDirection side,
                double entryPrice,
                double remainingShares,
                double remainingFee,
                double modelProbability,
                double marketPrice,
                double edge) {
            this.side = side;
            this.entryPrice = entryPrice;
            this.remainingShares = remainingShares;
            this.remainingFee = remainingFee;
            this.modelProbability = modelProbability;
            this.marketPrice = marketPrice;
            this.edge = edge;
        }
    }

    private record CompletedPosition(
            TradeDirection side,
            double entryPrice,
            double exitPrice,
            double sizeUsd,
            double shares,
            double totalFee,
            String exitReason,
            double modelProbability,
            double marketPrice,
            double edge
    ) {}
}
