package cz.polymarket.bot.exchange;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.domain.Exchange;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.mockito.Mockito.*;

class ExchangeWebSocketClientTest {

    private Vertx vertx;
    private ExchangePriceTracker tracker;
    private ExchangePayloadParser parser;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        tracker = mock(ExchangePriceTracker.class);
        parser = new ExchangePayloadParser(new ObjectMapper());
    }

    @AfterEach
    void tearDown() {
        vertx.close();
    }

    @Test
    void shouldHandleBinanceMessage() {
        BinanceWebSocketClient client = new BinanceWebSocketClient(vertx, tracker, parser, "wss://stream.binance.com:9443/ws/btcusdt@ticker");
        String message = "{\"e\":\"24hrTicker\",\"s\":\"BTCUSDT\",\"c\":\"78050.00\"}";

        client.handleMessage(message);

        verify(tracker, times(1)).updatePrice(Exchange.BINANCE, new BigDecimal("78050.00"));
    }

    @Test
    void shouldHandleCoinbaseMessage() {
        CoinbaseWebSocketClient client = new CoinbaseWebSocketClient(vertx, tracker, parser, "wss://ws-feed.exchange.coinbase.com");
        String message = "{\"type\":\"ticker\",\"product_id\":\"BTC-USD\",\"price\":\"78055.20\"}";

        client.handleMessage(message);

        verify(tracker, times(1)).updatePrice(Exchange.COINBASE, new BigDecimal("78055.20"));
    }

    @Test
    void shouldHandleKrakenMessage() {
        KrakenWebSocketClient client = new KrakenWebSocketClient(vertx, tracker, parser, "wss://ws.kraken.com/v2");
        String message = "{\"channel\":\"ticker\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"last\":78048.80}]}";

        client.handleMessage(message);

        verify(tracker, times(1)).updatePrice(eq(Exchange.KRAKEN), argThat(p -> p.compareTo(new BigDecimal("78048.80")) == 0));
    }
}
