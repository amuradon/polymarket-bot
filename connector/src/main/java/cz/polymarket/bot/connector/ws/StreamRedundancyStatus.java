package cz.polymarket.bot.connector.ws;

/**
 * Describes the real-time failover state of the peer WebSocket connection when one leg of a dual
 * Active-Active stream (such as Polymarket CLOB Market or Binance Spot {@code aggTrades}) disconnects.
 *
 * @param dualStream        {@code true} if the stream belongs to a dual Active-Active subscription
 * @param peerStreamName    identifier of the peer stream (or {@code ""} if single-leg stream)
 * @param peerState         current connection state of the peer stream
 * @param peerRemoteAddress resolved remote endpoint address ({@code IP:port}) of the peer stream
 * @param failoverActive    {@code true} if the peer stream is currently {@link ConnectionState#CONNECTED}
 */
public record StreamRedundancyStatus(
        boolean dualStream,
        String peerStreamName,
        ConnectionState peerState,
        String peerRemoteAddress,
        boolean failoverActive
) {
    /** Sentinel status for single-leg streams without a redundant peer connection. */
    public static final StreamRedundancyStatus NONE = new StreamRedundancyStatus(
            false, "", ConnectionState.DISCONNECTED, "", false
    );
}
