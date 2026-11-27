package cz.polymarket.bot.engine;

import cz.polymarket.bot.orderbook.PolymarketOrderBook;
import cz.polymarket.bot.strategy.ExecutionReport;
import cz.polymarket.bot.strategy.OrderCommand;
import cz.polymarket.bot.strategy.TradeDirection;

import java.math.BigDecimal;

/**
 * Shared Polymarket CLOB L2 order matching simulator used identically by {@code PAPER} trading
 * and historical {@code BACKTEST} simulation.
 * <p>
 * Models the empirically measured two-phase execution pipeline between {@code eu-west-1} (Dublin)
 * and the Polymarket CLOB engine (London via Cloudflare Dublin):
 * <ol>
 *   <li><b>Outbound + Taker Speed Bump ({@code ~89%} of {@code orderGatewayDelayMs}, default {@code ~196 ms})</b>:
 *       Local EIP-712 signing, warm HTTP/2 transit, server-side EIP-1271 DepositWallet signature &amp; collateral
 *       validation, and Polymarket's marketable taker speed bump prior to entering the matching engine.
 *       At {@code tMatch = tSubmit + outboundMatchDelayMs}, the order sweeps {@link PolymarketOrderBook}
 *       atomically up to {@code limitPriceMc}, depleting matched levels or failing with
 *       {@code no orders found to match with FAK order} if makers cancelled liquidity in the interim.</li>
 *   <li><b>Inbound HTTP/2 Response ({@code ~11%} of {@code orderGatewayDelayMs}, default {@code ~24 ms})</b>:
 *       Response serialization and return transit delivering the {@link ExecutionReport} back to
 *       {@link CoreTradingEngine} at {@code tReport = tMatch + returnReportDelayMs}.</li>
 * </ol>
 */
public class SimulatedClobExecutionEngine implements OrderExecutionGateway {

    /** Default total CLOB {@code POST /order} round-trip delay in milliseconds measured from {@code eu-west-1}. */
    public static final long DEFAULT_ORDER_GATEWAY_DELAY_MS = 220L;

    /** Fraction of total gateway delay elapsed before atomic CLOB matching (~196 ms of 220 ms). */
    public static final double OUTBOUND_MATCH_DELAY_SHARE = 0.89;

    /** Baseline healthy WebSocket one-way latency in milliseconds from Polymarket CLOB to {@code eu-west-1}. */
    private static final long BASELINE_WS_ONE_WAY_MS = 15L;

    private static final String FAK_NO_MATCH_ERROR =
            "HTTP 400: {\"error\":\"no orders found to match with FAK order\"}";

    /**
     * Scheduler callback used to defer matching and report delivery onto either the backtest chronological
     * priority queue or a non-blocking real-time paper-trading timer.
     */
    @FunctionalInterface
    public interface DelayedActionScheduler {
        /**
         * Schedules {@code action} to run at simulated or wall-clock {@code targetTimestampMs}.
         *
         * @param targetTimestampMs target execution timestamp in epoch milliseconds UTC
         * @param action            callback to invoke
         */
        void scheduleAt(long targetTimestampMs, Runnable action);
    }

    /**
     * Sink receiving terminal simulated {@link ExecutionReport} notifications.
     */
    @FunctionalInterface
    public interface SimulatedReportSink {
        /**
         * Delivers a completed simulated execution report.
         *
         * @param report            generated execution report
         * @param reportTimestampMs timestamp when the report arrives back at the engine
         * @param totalRttMs        simulated round-trip order latency in milliseconds
         */
        void onSimulatedExecutionReport(ExecutionReport report, long reportTimestampMs, long totalRttMs);
    }

    private final PolymarketOrderBook orderBook;
    private final long orderGatewayDelayMs;
    private DelayedActionScheduler scheduler;
    private SimulatedReportSink reportSink;

    /**
     * Constructs a {@link SimulatedClobExecutionEngine} with explicit gateway delay.
     *
     * @param orderBook           shared L2 order book
     * @param orderGatewayDelayMs base {@code POST /order} round-trip delay in milliseconds ({@code >= 0})
     */
    public SimulatedClobExecutionEngine(
            PolymarketOrderBook orderBook,
            long orderGatewayDelayMs) {
        if (orderBook == null) {
            throw new IllegalArgumentException("orderBook cannot be null");
        }
        if (orderGatewayDelayMs < 0L) {
            throw new IllegalArgumentException("orderGatewayDelayMs must be non-negative");
        }
        this.orderBook = orderBook;
        this.orderGatewayDelayMs = orderGatewayDelayMs;
    }

    /**
     * Binds the delayed action scheduler and execution report sink.
     *
     * @param scheduler  scheduler deferring match and report callbacks (when {@code null}, executes immediately)
     * @param reportSink consumer receiving terminal execution reports
     */
    public void bind(DelayedActionScheduler scheduler, SimulatedReportSink reportSink) {
        this.scheduler = scheduler;
        this.reportSink = reportSink;
    }

    /**
     * Returns the configured base order gateway round-trip delay in milliseconds.
     *
     * @return base order gateway delay in ms
     */
    public long getOrderGatewayDelayMs() {
        return orderGatewayDelayMs;
    }

    /**
     * Computes the outbound delay in milliseconds from order submission to atomic CLOB matching.
     *
     * @return outbound match delay in milliseconds
     */
    public long computeOutboundMatchDelayMs() {
        if (orderGatewayDelayMs <= 0L) {
            return 0L;
        }
        long observedWsMs = orderBook.getLastObservedOneWayLatencyMs();
        long extraCongestionMs = Math.max(0L, observedWsMs - BASELINE_WS_ONE_WAY_MS);
        return Math.max(0L, Math.round(OUTBOUND_MATCH_DELAY_SHARE * orderGatewayDelayMs) + extraCongestionMs);
    }

    /**
     * Computes the inbound delay in milliseconds from CLOB matching to HTTP/2 response receipt.
     *
     * @return return report delay in milliseconds
     */
    public long computeReturnReportDelayMs() {
        if (orderGatewayDelayMs <= 0L) {
            return 0L;
        }
        double returnShare = 1.0 - OUTBOUND_MATCH_DELAY_SHARE;
        return Math.max(0L, Math.round(returnShare * orderGatewayDelayMs));
    }

    @Override
    public void executeOrder(
            OrderCommand command,
            TradeDirection side,
            boolean isBuy,
            int limitPriceMc,
            double normalizedShares,
            long submitTimestampMs) {
        if (command == null || reportSink == null) {
            return;
        }
        long outboundDelayMs = computeOutboundMatchDelayMs();
        long returnDelayMs = computeReturnReportDelayMs();
        long matchTimestampMs = submitTimestampMs + outboundDelayMs;
        long reportTimestampMs = matchTimestampMs + returnDelayMs;
        long totalRttMs = outboundDelayMs + returnDelayMs;

        DelayedActionScheduler activeScheduler = this.scheduler;
        if (activeScheduler == null || outboundDelayMs <= 0L) {
            ExecutionReport report = matchAtClob(
                    command, side == TradeDirection.UP, isBuy, limitPriceMc, normalizedShares, reportTimestampMs, totalRttMs);
            if (activeScheduler == null || returnDelayMs <= 0L) {
                reportSink.onSimulatedExecutionReport(report, reportTimestampMs, totalRttMs);
            } else {
                activeScheduler.scheduleAt(reportTimestampMs,
                        () -> reportSink.onSimulatedExecutionReport(report, reportTimestampMs, totalRttMs));
            }
            return;
        }

        activeScheduler.scheduleAt(matchTimestampMs, () -> {
            ExecutionReport report = matchAtClob(
                    command, side == TradeDirection.UP, isBuy, limitPriceMc, normalizedShares, reportTimestampMs, totalRttMs);
            if (returnDelayMs <= 0L) {
                reportSink.onSimulatedExecutionReport(report, reportTimestampMs, totalRttMs);
            } else {
                activeScheduler.scheduleAt(reportTimestampMs,
                        () -> reportSink.onSimulatedExecutionReport(report, reportTimestampMs, totalRttMs));
            }
        });
    }

    private ExecutionReport matchAtClob(
            OrderCommand command,
            boolean isUpToken,
            boolean isBuy,
            int limitPriceMc,
            double requestedShares,
            long reportTimestampMs,
            long totalRttMs) {
        double limitPrice = limitPriceMc * 1e-3;
        PolymarketOrderBook.FillResult fill = orderBook.executeFill(isUpToken, isBuy, requestedShares, limitPrice);
        if (fill.executedShares() <= 1e-9) {
            return new ExecutionReport(
                    command.clientOrderId(),
                    "sim-reject-" + command.clientOrderId(),
                    "REJECTED",
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    reportTimestampMs,
                    FAK_NO_MATCH_ERROR
            );
        }
        return new ExecutionReport(
                command.clientOrderId(),
                (isBuy ? "sim-fill-" : "sim-exit-") + command.clientOrderId(),
                "FILLED",
                BigDecimal.valueOf(fill.avgPrice()),
                BigDecimal.valueOf(fill.executedShares()),
                reportTimestampMs,
                "Simulated CLOB FAK match (rtt=" + totalRttMs + "ms)"
        );
    }
}
