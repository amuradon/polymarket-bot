package cz.polymarket.bot.strategy;

/**
 * Trade direction for Polymarket binary markets.
 */
public enum TradeDirection {
    UP,
    DOWN,
    NO_TRADE;

    public boolean isTrade() {
        return this != NO_TRADE;
    }
}
