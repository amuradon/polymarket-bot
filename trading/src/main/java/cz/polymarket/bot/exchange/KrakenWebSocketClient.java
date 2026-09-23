package cz.polymarket.bot.exchange;

import cz.polymarket.bot.domain.Exchange;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.util.Optional;

@ApplicationScoped
public class KrakenWebSocketClient extends AbstractExchangeWebSocketClient {

    private static final String SUBSCRIBE_PAYLOAD = "{\"method\":\"subscribe\",\"params\":{\"channel\":\"ticker\",\"symbol\":[\"BTC/USD\"]}}";

    private final ExchangePayloadParser parser;

    protected KrakenWebSocketClient() {
        super();
        this.parser = null;
    }

    @Inject
    public KrakenWebSocketClient(
            Vertx vertx,
            ExchangePriceTracker tracker,
            ExchangePayloadParser parser,
            @ConfigProperty(name = "polymarket.exchange.kraken.ws-url", defaultValue = "wss://ws.kraken.com/v2") String wsUrl) {
        super(vertx, tracker, Exchange.KRAKEN, wsUrl, SUBSCRIBE_PAYLOAD);
        this.parser = parser;
    }

    @Override
    protected Optional<BigDecimal> parsePrice(String message) {
        return parser.parseKrakenTicker(message);
    }
}
