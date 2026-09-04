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

public abstract class AbstractExchangeWebSocketClient {

    private static final Logger LOG = Logger.getLogger(AbstractExchangeWebSocketClient.class);
    private static final long RECONNECT_DELAY_MS = 3000L;

    private final Vertx vertx;
    private final ExchangePriceTracker tracker;
    private final Exchange exchange;
    private final String wsUrl;
    private final String subscribePayload;
    private final HttpClient httpClient;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile WebSocket currentWebSocket;

    protected AbstractExchangeWebSocketClient() {
        this.vertx = null;
        this.tracker = null;
        this.exchange = null;
        this.wsUrl = null;
        this.subscribePayload = null;
        this.httpClient = null;
    }

    public AbstractExchangeWebSocketClient(
            Vertx vertx,
            ExchangePriceTracker tracker,
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

    public void start() {
        if (running.compareAndSet(false, true)) {
            connect();
        }
    }

    public void stop() {
        running.set(false);
        if (currentWebSocket != null) {
            try {
                currentWebSocket.close();
            } catch (Exception ignored) {
            }
        }
        httpClient.close();
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
        if (running.get()) {
            vertx.setTimer(RECONNECT_DELAY_MS, timerId -> connect());
        }
    }

    public void handleMessage(String message) {
        parsePrice(message).ifPresent(price -> tracker.updatePrice(exchange, price));
    }

    protected void updatePrice(BigDecimal price) {
        tracker.updatePrice(exchange, price);
    }

    protected abstract Optional<BigDecimal> parsePrice(String message);
}
