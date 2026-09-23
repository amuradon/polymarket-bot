package cz.polymarket.bot.strategy;

/**
 * Interface for routing orders to an execution engine.
 * Implemented by:
 * - Backtest simulated matching engine
 * - Paper trading virtual execution router
 * - Live Polymarket CLOB order gateway
 */
public interface ExecutionRouter {

    /**
     * Submits an order command to the underlying execution engine.
     *
     * @param command the order command
     */
    void submitOrder(OrderCommand command);

    /**
     * Requests cancellation of an open order by clientOrderId.
     *
     * @param clientOrderId the client-assigned order ID
     */
    void cancelOrder(String clientOrderId);
}
