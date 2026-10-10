package cz.polymarket.bot.exchange;

import cz.polymarket.bot.domain.Exchange;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import org.jboss.logging.Logger;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base non-blocking Vert.x WebSocket client for exchange ticker feeds with automatic reconnection.
 */
public abstract class AbstractExchangeWebSocketClient {

    private static final Logger LOG = Logger.getLogger(AbstractExchangeWebSocketClient.class);
    private static final long RECONNECT_DELAY_MS = 3000L;

    private final Vertx vertx;
    private final ExchangePriceListener tracker;
    private final Exchange exchange;
    private final String wsUrl;
    private final String subscribePayload;
    private final HttpClient httpClient;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile WebSocket currentWebSocket;

    /**
     * Default no-arg constructor required for CDI proxies.
     */
    protected AbstractExchangeWebSocketClient() {
        this.vertx = null;
        this.tracker = null;
        this.exchange = null;
        this.wsUrl = null;
        this.subscribePayload = null;
        this.httpClient = null;
    }

    /**
     * Constructs an exchange WebSocket client.
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
        this.vertx = vertx;
        this.tracker = tracker;
        this.exchange = exchange;
        this.wsUrl = wsUrl;
        this.subscribePayload = subscribePayload;

        if (wsUrl != null && vertx != null) {
            HttpClientOptions options = new HttpClientOptions().setSsl(wsUrl.toLowerCase().startsWith("wss"));
            this.httpClient = vertx.createHttpClient(options);
        } else {
            this.httpClient = null;
        }
    }

    /**
     * Starts the WebSocket connection loop if not already running.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            connect();
        }
    }

    /**
     * Stops the WebSocket client and closes any active connection.
     */
    public void stop() {
        running.set(false);
        if (currentWebSocket != null) {
            try {
                currentWebSocket.close();
            } catch (Exception ignored) {
            }
        }
        if (httpClient != null) {
            httpClient.close();
        }
    }

    private void connect() {
        if (!running.get()) {
            return;
        }

        try {
            URI uri = URI.create(wsUrl);
            boolean isSsl = "wss".equalsIgnoreCase(uri.getScheme());
            int port = uri.getPort() == -1 ? (isSsl ? 443 : 80) : uri.getPort();
            String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
            if (uri.getRawQuery() != null) {
                path += "?" + uri.getRawQuery();
            }

            WebSocketConnectOptions options = new WebSocketConnectOptions()
                    .setHost(uri.getHost())
                    .setPort(port)
                    .setSsl(isSsl)
                    .setURI(path);

            httpClient.webSocket(options)
                    .onSuccess(ws -> {
                        LOG.infof("[%s] WebSocket connected to %s", exchange, wsUrl);
                        this.currentWebSocket = ws;

                        if (subscribePayload != null && !subscribePayload.isBlank()) {
                            ws.writeTextMessage(subscribePayload);
                        }

                        ws.textMessageHandler(this::handleMessage);
                        ws.closeHandler(v -> onConnectionClosed());
                        ws.exceptionHandler(err -> {
                            LOG.warnf("[%s] WebSocket error: %s", exchange, err.getMessage());
                            onConnectionClosed();
                        });
                    })
                    .onFailure(err -> {
                        LOG.warnf("[%s] WebSocket connection failed: %s", exchange, err.getMessage());
                        scheduleReconnect();
                    });
        } catch (Exception e) {
            LOG.warnf("[%s] Error initiating WebSocket connection: %s", exchange, e.getMessage());
            scheduleReconnect();
        }
    }

    private void onConnectionClosed() {
        this.currentWebSocket = null;
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (running.get() && vertx != null) {
            vertx.setTimer(RECONNECT_DELAY_MS, timerId -> connect());
        }
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
