package cz.polymarket.bot.exchange;

import cz.polymarket.bot.domain.Exchange;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.math.BigDecimal;
import java.util.Optional;

@ApplicationScoped
public class CoinbaseWebSocketClient extends AbstractExchangeWebSocketClient {

    private static final String SUBSCRIBE_PAYLOAD = "{\"type\":\"subscribe\",\"product_ids\":[\"BTC-USD\"],\"channels\":[\"ticker\"]}";

    private final ExchangePayloadParser parser;

    protected CoinbaseWebSocketClient() {
        super();
        this.parser = null;
    }

    @Inject
    public CoinbaseWebSocketClient(
            Vertx vertx,
            ExchangePriceTracker tracker,
            ExchangePayloadParser parser,
            @ConfigProperty(name = "polymarket.exchange.coinbase.ws-url", defaultValue = "wss://ws-feed.exchange.coinbase.com") String wsUrl) {
        super(vertx, tracker, Exchange.COINBASE, wsUrl, SUBSCRIBE_PAYLOAD);
        this.parser = parser;
    }

    @Override
    protected Optional<BigDecimal> parsePrice(String message) {
        return parser.parseCoinbaseTicker(message);
    }
}
