package cz.polymarket.bot.connector.binance;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Manages resilient WebSocket connections for all 5 Binance Spot and USD-M Futures data streams:
 * Spot aggTrades, Futures aggTrades, Futures Diff. Book Depth (100ms),
 * Futures Partial Book Depth (100ms), and Futures Liquidation Orders (forceOrder).
 */
@ApplicationScoped
public class BinanceStreamConnector {

    private final Vertx vertx;
    private final String spotWsBaseUrl;
    private final String futuresWsBaseUrl;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;
    private final Map<BinanceStreamType, ReconnectingWebSocketStream> streams = new EnumMap<>(BinanceStreamType.class);

    /**
     * Constructs the Binance multi-stream WebSocket connector.
     *
     * @param vertx            Vert.x instance
     * @param spotWsBaseUrl    Binance Spot WebSocket base URL
     * @param futuresWsBaseUrl Binance USD-M Futures WebSocket base URL
     * @param reconnectDelayMs delay in ms before reconnecting a dropped stream
     * @param pingIntervalMs   interval in ms for sending WebSocket ping frames
     */
    @Inject
    public BinanceStreamConnector(
            Vertx vertx,
            @ConfigProperty(name = "polymarket.connector.binance.spot-ws-base-url", defaultValue = "wss://stream.binance.com:9443/ws")
            String spotWsBaseUrl,
            @ConfigProperty(name = "polymarket.connector.binance.futures-ws-base-url", defaultValue = "wss://fstream.binance.com/ws")
            String futuresWsBaseUrl,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "15000")
            long pingIntervalMs) {
        this.vertx = vertx;
        this.spotWsBaseUrl = spotWsBaseUrl;
        this.futuresWsBaseUrl = futuresWsBaseUrl;
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    /**
     * Starts all 5 Binance WebSocket streams for the given symbol and routes messages to the listener.
     *
     * @param symbol   trading pair symbol (e.g. BTCUSDT)
     * @param listener callback receiving stream type, raw JSON payload, and local receipt timestamp
     */
    public synchronized void start(String symbol, BinanceStreamListener listener) {
        stop();
        String normalizedSymbol = normalizeSymbol(symbol);

        for (BinanceStreamType type : BinanceStreamType.values()) {
            String wsUrl = type.buildWsUrl(spotWsBaseUrl, futuresWsBaseUrl, normalizedSymbol);
            String streamName = "BINANCE-" + type.name() + "-" + normalizedSymbol;
            ReconnectingWebSocketStream stream = new ReconnectingWebSocketStream(
                    vertx,
                    streamName,
                    wsUrl,
                    List::of,
                    (rawJson, recvTs) -> {
                        if (listener != null) {
                            listener.onStreamMessage(type, rawJson, recvTs);
                        }
                    },
                    reconnectDelayMs,
                    pingIntervalMs
            );
            streams.put(type, stream);
            stream.start();
        }
    }

    /**
     * Stops and closes all active Binance WebSocket stream connections.
     */
    public synchronized void stop() {
        for (ReconnectingWebSocketStream stream : streams.values()) {
            stream.stop();
        }
        streams.clear();
    }

    /**
     * Returns an immutable snapshot of the connection state for each Binance stream type.
     *
     * @return map of stream type to its current {@link ConnectionState}
     */
    public synchronized Map<BinanceStreamType, ConnectionState> getConnectionStates() {
        Map<BinanceStreamType, ConnectionState> result = new EnumMap<>(BinanceStreamType.class);
        for (BinanceStreamType type : BinanceStreamType.values()) {
            ReconnectingWebSocketStream stream = streams.get(type);
            result.put(type, stream != null ? stream.getState() : ConnectionState.DISCONNECTED);
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Checks whether all 5 Binance streams are currently in the {@link ConnectionState#CONNECTED} state.
     *
     * @return true if all 5 streams are connected
     */
    public synchronized boolean isAllConnected() {
        if (streams.size() != BinanceStreamType.values().length) {
            return false;
        }
        for (ReconnectingWebSocketStream stream : streams.values()) {
            if (stream.getState() != ConnectionState.CONNECTED) {
                return false;
            }
        }
        return true;
    }

    /**
     * Normalizes a symbol such as "BTC" or "BTCUSDT" into a standard Binance pair symbol (defaulting to USDT quote).
     *
     * @param symbol input symbol
     * @return uppercase trading pair symbol (e.g. BTCUSDT)
     */
    public static String normalizeSymbol(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return "BTCUSDT";
        }
        String clean = symbol.trim().toUpperCase().replace("/", "").replace("-", "");
        if (!clean.endsWith("USDT") && !clean.endsWith("USDC") && !clean.endsWith("USD")) {
            return clean + "USDT";
        }
        return clean;
    }
}
