package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.TradeDirection;

import java.math.BigDecimal;
import java.util.function.Consumer;

/**
 * Matching engine simulating realistic order placement, time-varying network/exchange latency
 * derived from captured Polymarket timestamps ({@code delayMs = round(observedOneWayLatencyMs * latencyFactor)}),
 * delayed order book depth consumption at execution time, crypto taker fees, and contract settlements.
 */
public class SimulatedMatchingEngine {

    private final PolymarketOrderBook orderBook;
    private final double latencyFactor;
    private Consumer<BacktestEvent> eventScheduler;
    private long orderSequence = 0L;

    private boolean hasActiveTrade = false;
    private TradeDirection side = TradeDirection.NO_TRADE;
    private double entryPrice = 0.0;
    private double sizeUsd = 0.0;
    private double shares = 0.0;
    private double entryFee = 0.0;
    private double modelProbability = 0.5;
    private double marketPrice = 0.5;
    private double edge = 0.0;

    private boolean exitExecuted = false;
    private double exitPrice = 0.0;
    private String exitReason = null;
    private double exitFee = 0.0;

    /**
     * Constructs the simulated matching engine with a configurable latency scaling factor.
     *
     * @param orderBook reconstructed Polymarket L2 order book
     * @param latencyFactor multiplier applied to the real-time observed Polymarket latency (default {@code 1.0})
     */
    public SimulatedMatchingEngine(PolymarketOrderBook orderBook, double latencyFactor) {
        if (orderBook == null) {
            throw new IllegalArgumentException("orderBook cannot be null");
        }
        if (Double.isNaN(latencyFactor) || latencyFactor < 0.0) {
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
        this.hasActiveTrade = false;
        this.side = TradeDirection.NO_TRADE;
        this.entryPrice = 0.0;
        this.sizeUsd = 0.0;
        this.shares = 0.0;
        this.entryFee = 0.0;
        this.modelProbability = 0.5;
        this.marketPrice = 0.5;
        this.edge = 0.0;
        this.exitExecuted = false;
        this.exitPrice = 0.0;
        this.exitReason = null;
        this.exitFee = 0.0;
    }

    /**
     * Submits an order into the matching engine at {@code submitTimestampMs}.
     * <p>
     * Computes the time-varying execution delay {@code delayMs = round(observedOneWayLatencyMs * latencyFactor)}
     * and schedules a {@link BacktestEvent.PendingOrderExecutionEvent} at {@code submitTimestampMs + delayMs}
     * so that any order book updates occurring during transit are applied before the order matches.
     *
     * @param command order command specifying token, direction, price, and size
     * @param submitTimestampMs local EC2 timestamp when the strategy submitted the order
     */
    public void submitOrder(OrderCommand command, long submitTimestampMs) {
        if (command == null) {
            return;
        }

        long observedOneWayMs = orderBook.getLastObservedOneWayLatencyMs();
        long delayMs = Math.max(0L, Math.round(observedOneWayMs * latencyFactor));
        long executionTimestampMs = submitTimestampMs + delayMs;

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
     * sweeping the current order book depth at that exact delayed timestamp.
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

        PolymarketOrderBook.FillResult fill = orderBook.simulateFill(isUpToken, isBuy, reqShares, limitPrice);
        if (fill.executedShares() <= 0.0) {
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

        if (isBuy) {
            this.hasActiveTrade = true;
            this.entryPrice = fill.avgPrice();
            this.shares = fill.executedShares();
            this.sizeUsd = this.entryPrice * this.shares;
            this.side = isUpToken ? TradeDirection.UP : TradeDirection.DOWN;
            this.entryFee = fill.fee();
            this.marketPrice = this.entryPrice;
            this.modelProbability = Math.min(0.95, this.entryPrice + 0.10);
            this.edge = Math.max(0.0, this.modelProbability - this.marketPrice);

            return new ExecutionReport(
                    command.clientOrderId(),
                    "sim-fill-" + command.clientOrderId(),
                    "FILLED",
                    BigDecimal.valueOf(fill.avgPrice()),
                    BigDecimal.valueOf(fill.executedShares()),
                    fillTimestampMs,
                    "Simulated fill with " + delayMs + "ms time-varying latency"
            );
        } else {
            this.exitExecuted = true;
            this.exitPrice = fill.avgPrice();
            this.exitFee = fill.fee();
            this.exitReason = fill.avgPrice() >= 0.70 ? "Take Profit (0.70)" : "Trailing Stop (+0.05)";

            return new ExecutionReport(
                    command.clientOrderId(),
                    "sim-exit-" + command.clientOrderId(),
                    "FILLED",
                    BigDecimal.valueOf(fill.avgPrice()),
                    BigDecimal.valueOf(fill.executedShares()),
                    fillTimestampMs,
                    "Simulated exit fill with " + delayMs + "ms time-varying latency"
            );
        }
    }

    /**
     * Resolves and finalizes an active trade against actual candle outcome or prior market exit.
     * Computes net PnL after entry and exit fees, resulting account balance, and win/loss classification.
     *
     * @param candleStartSec candle interval start in epoch seconds
     * @param datetimeUtc ISO-8601 formatted datetime UTC
     * @param actualOutcome settled binary outcome direction
     * @param currentBalance account balance before trade payout
     * @return constructed TradeRecord, or null if no trade was active
     */
    public TradeRecord finalizeTrade(long candleStartSec, String datetimeUtc, TradeDirection actualOutcome, double currentBalance) {
        if (!hasActiveTrade) {
            return null;
        }

        double finalExitPrice;
        String finalExitReason;
        double finalExitFee;

        if (exitExecuted) {
            finalExitPrice = this.exitPrice;
            finalExitReason = this.exitReason;
            finalExitFee = this.exitFee;
        } else {
            finalExitPrice = (this.side == actualOutcome) ? 1.00 : 0.00;
            finalExitReason = "Resolution (TWAP Oracle)";
            finalExitFee = 0.0;
        }

        double payout = this.shares * finalExitPrice;
        double totalFee = this.entryFee + finalExitFee;
        double netPnl = payout - this.sizeUsd - totalFee;
        double newBalance = currentBalance + netPnl;
        boolean isWin = netPnl > 0.0;

        return new TradeRecord(
                datetimeUtc,
                candleStartSec,
                this.side,
                this.entryPrice,
                finalExitPrice,
                this.sizeUsd,
                this.shares,
                totalFee,
                netPnl,
                finalExitReason,
                this.modelProbability,
                this.marketPrice,
                this.edge,
                isWin,
                newBalance
        );
    }

    /**
     * Checks if the matching engine currently holds an active trade.
     *
     * @return true if holding an active position
     */
    public boolean hasActiveTrade() {
        return hasActiveTrade;
    }
}
