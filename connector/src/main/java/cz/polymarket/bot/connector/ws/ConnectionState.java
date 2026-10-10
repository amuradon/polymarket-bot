package cz.polymarket.bot.connector.ws;

/**
 * Represents the lifecycle state of a managed WebSocket connection.
 */
public enum ConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    RECONNECTING
}
