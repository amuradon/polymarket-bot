package cz.polymarket.bot.exchange;

import cz.polymarket.bot.domain.Exchange;

import java.math.BigDecimal;

/**
 * Listener contract for receiving real-time spot price updates from exchange WebSocket connectors.
 */
@FunctionalInterface
public interface ExchangePriceListener {

    /**
     * Invoked when a new price update is received from the specified exchange.
     *
     * @param exchange source cryptocurrency exchange
     * @param price    latest traded price
     */
    void updatePrice(Exchange exchange, BigDecimal price);
}
