package cz.polymarket.bot.exchange;

import cz.polymarket.bot.domain.BinanceTicker;
import cz.polymarket.bot.domain.Exchange;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Consumer;

@ApplicationScoped
public class BinanceWebSocketClient extends AbstractExchangeWebSocketClient {

    private final ExchangePayloadParser parser;
    private volatile Consumer<Long> tickListener;

    protected BinanceWebSocketClient() {
        super();
        this.parser = null;
    }

    @Inject
    public BinanceWebSocketClient(
            Vertx vertx,
            ExchangePriceTracker tracker,
            ExchangePayloadParser parser,
            @ConfigProperty(name = "polymarket.exchange.binance.ws-url", defaultValue = "wss://stream.binance.com:9443/ws/btcusdt@ticker") String wsUrl) {
        super(vertx, tracker, Exchange.BINANCE, wsUrl, null);
        this.parser = parser;
    }

    public void setTickListener(Consumer<Long> tickListener) {
        this.tickListener = tickListener;
    }

    @Override
    public void handleMessage(String message) {
        parser.parseBinanceTicker(message).ifPresent(ticker -> {
            updatePrice(ticker.price());
            Consumer<Long> listener = this.tickListener;
            if (listener != null) {
                listener.accept(ticker.eventTimeMs());
            }
        });
    }

    @Override
    protected Optional<BigDecimal> parsePrice(String message) {
        return parser.parseBinanceTicker(message).map(BinanceTicker::price);
    }
}
