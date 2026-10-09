package cz.polymarket.bot.backtest.stream;

import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.TradeDirection;

import java.math.BigDecimal;
import java.util.function.Consumer;

/**
 * Matching engine simulating realistic order placement, network/processing latency (e.g. 50ms),
 * order book depth consumption, crypto taker fees, and contract settlements.
 */
public class SimulatedMatchingEngine {

    private final PolymarketOrderBook orderBook;
    private final long latencyMs;
    private Consumer<BacktestEvent> eventScheduler;

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
     * Constructs the simulated matching engine.
     *
     * @param orderBook reconstructed Polymarket L2 order book
     * @param latencyMs simulated network and execution latency in milliseconds (e.g. 50ms)
     */
    public SimulatedMatchingEngine(PolymarketOrderBook orderBook, long latencyMs) {
        if (orderBook == null) {
            throw new IllegalArgumentException("orderBook cannot be null");
        }
        this.orderBook = orderBook;
        this.latencyMs = Math.max(0L, latencyMs);
    }

    /**
     * Registers the event scheduler consumer to dispatch delayed execution reports.
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
     * Submits an order into the matching engine.
     * With simulated latency, an ExecutionReportEvent is scheduled at (submitTimestampMs + latencyMs).
     */
    public void submitOrder(OrderCommand command, long submitTimestampMs) {
        if (command == null) {
            return;
        }

        long fillTimestampMs = submitTimestampMs + latencyMs;
        boolean isBuy = "BUY".equalsIgnoreCase(command.side());
        boolean isUpToken = !command.token().toLowerCase().contains("down");
        double reqShares = command.size().doubleValue();
        double limitPrice = command.price().doubleValue();

        // Simulate fill against order book at time of fill
        PolymarketOrderBook.FillResult fill = orderBook.simulateFill(isUpToken, isBuy, reqShares, limitPrice);

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

            ExecutionReport report = new ExecutionReport(
                    command.clientOrderId(),
                    "sim-fill-" + command.clientOrderId(),
                    "FILLED",
                    BigDecimal.valueOf(fill.avgPrice()),
                    BigDecimal.valueOf(fill.executedShares()),
                    fillTimestampMs,
                    "Simulated fill with " + latencyMs + "ms latency"
            );

            if (eventScheduler != null) {
                eventScheduler.accept(new BacktestEvent.ExecutionReportEvent(fillTimestampMs, report));
            }
        } else {
            this.exitExecuted = true;
            this.exitPrice = fill.avgPrice();
            this.exitFee = fill.fee();
            this.exitReason = fill.avgPrice() >= 0.70 ? "Take Profit (0.70)" : "Trailing Stop (+0.05)";

            ExecutionReport report = new ExecutionReport(
                    command.clientOrderId(),
                    "sim-exit-" + command.clientOrderId(),
                    "FILLED",
                    BigDecimal.valueOf(fill.avgPrice()),
                    BigDecimal.valueOf(fill.executedShares()),
                    fillTimestampMs,
                    "Simulated exit fill with " + latencyMs + "ms latency"
            );

            if (eventScheduler != null) {
                eventScheduler.accept(new BacktestEvent.ExecutionReportEvent(fillTimestampMs, report));
            }
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
     */
    public boolean hasActiveTrade() {
        return hasActiveTrade;
    }

    /**
     * Returns the execution entry price of the active position.
     */
    public double getEntryPrice() {
        return entryPrice;
    }

    /**
     * Returns the quantity of shares in the active position.
     */
    public double getShares() {
        return shares;
    }

    /**
     * Returns the directional side of the active position.
     */
    public TradeDirection getSide() {
        return side;
    }
}
