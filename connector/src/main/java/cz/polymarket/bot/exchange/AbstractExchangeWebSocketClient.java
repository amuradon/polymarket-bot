package cz.polymarket.bot.exchange;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import cz.polymarket.bot.domain.Exchange;
import io.vertx.core.Vertx;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * Base non-blocking Vert.x WebSocket client for exchange ticker feeds with automatic reconnection,
 * native BoringSSL ({@code netty-tcnative}), {@code TCP_NODELAY}, and lock-free JCTools queue handoff
 * backed by {@link ReconnectingWebSocketStream}.
 */
public abstract class AbstractExchangeWebSocketClient {

    private static final long RECONNECT_DELAY_MS = 3000L;
    private static final long PING_INTERVAL_MS = 15000L;

    private final ExchangePriceListener tracker;
    private final Exchange exchange;
    private final ReconnectingWebSocketStream stream;

    /**
     * Default no-arg constructor required for CDI proxies.
     */
    protected AbstractExchangeWebSocketClient() {
        this.tracker = null;
        this.exchange = null;
        this.stream = null;
    }

    /**
     * Constructs an exchange WebSocket client backed by {@link ReconnectingWebSocketStream}.
     *
     * @param vertx            Vert.x instance for non-blocking network I/O
     * @param tracker          price listener notified on incoming ticker prices
     * @param exchange         exchange identifier
     * @param wsUrl            target WebSocket endpoint URL
     * @param subscribePayload optional JSON subscription message sent immediately upon connection
     */
    public AbstractExchangeWebSocketClient(
            Vertx vertx,
            ExchangePriceListener tracker,
            Exchange exchange,
            String wsUrl,
            String subscribePayload) {
        this.tracker = tracker;
        this.exchange = exchange;

        if (wsUrl != null && vertx != null) {
            List<String> initialSubs = (subscribePayload != null && !subscribePayload.isBlank())
                    ? List.of(subscribePayload)
                    : List.of();
            String streamName = "EXCHANGE-" + (exchange != null ? exchange.name() : "UNKNOWN");
            this.stream = new ReconnectingWebSocketStream(
                    vertx,
                    streamName,
                    wsUrl,
                    () -> initialSubs,
                    (rawJson, recvTs) -> handleMessage(rawJson),
                    RECONNECT_DELAY_MS,
                    PING_INTERVAL_MS
            );
        } else {
            this.stream = null;
        }
    }

    /**
     * Starts the WebSocket connection loop if not already running.
     */
    public void start() {
        if (stream != null) {
            stream.start();
        }
    }

    /**
     * Stops the WebSocket client and closes any active connection.
     */
    public void stop() {
        if (stream != null) {
            stream.stop();
        }
    }

    /**
     * Returns the current connection state of the underlying WebSocket stream.
     *
     * @return current {@link ConnectionState}
     */
    public ConnectionState getState() {
        return stream != null ? stream.getState() : ConnectionState.DISCONNECTED;
    }

    /**
     * Handles an incoming text frame from the WebSocket and updates the price listener if a price is parsed.
     *
     * @param message raw JSON text frame
     */
    public void handleMessage(String message) {
        parsePrice(message).ifPresent(price -> tracker.updatePrice(exchange, price));
    }

    /**
     * Forwards a parsed price update to the configured price listener.
     *
     * @param price latest traded price
     */
    protected void updatePrice(BigDecimal price) {
        tracker.updatePrice(exchange, price);
    }

    /**
     * Extracts the traded price from a raw exchange WebSocket JSON payload.
     *
     * @param message raw JSON payload
     * @return parsed price if present
     */
    protected abstract Optional<BigDecimal> parsePrice(String message);
}
