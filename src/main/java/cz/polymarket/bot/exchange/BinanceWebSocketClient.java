package cz.polymarket.bot.exchange;

import cz.polymarket.bot.domain.Exchange;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.util.Optional;

@ApplicationScoped
public class BinanceWebSocketClient extends AbstractExchangeWebSocketClient {

    private final ExchangePayloadParser parser;

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

    @Override
    protected Optional<BigDecimal> parsePrice(String message) {
        return parser.parseBinanceTicker(message);
    }
}
