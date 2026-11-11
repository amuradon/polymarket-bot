package cz.polymarket.bot.web;

import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.domain.TwapPoint;
import cz.polymarket.bot.domain.TwapUpdate;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class TwapWebSocketTest {

    @TestHTTPResource("/ws/twap")
    URI websocketUri;

    @Inject
    TwapWebSocketEndpoint endpoint;

    @Test
    void shouldConnectToTwapWebSocketAndReceiveOrSendMessages() throws Exception {
        CompletableFuture<String> receivedFuture = new CompletableFuture<>();
        HttpClient client = HttpClient.newHttpClient();

        URI wsUri = URI.create(websocketUri.toString().replaceFirst("^http", "ws"));
        WebSocket webSocket = client.newWebSocketBuilder()
                .buildAsync(wsUri, new WebSocket.Listener() {
                    @Override
                    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                        receivedFuture.complete(data.toString());
                        return WebSocket.Listener.super.onText(ws, data, last);
                    }
                })
                .get(10, TimeUnit.SECONDS);

        assertThat(webSocket).isNotNull();

        // Send timeframe change message
        webSocket.sendText("{\"action\":\"setTimeframe\",\"timeframe\":\"15m\"}", true)
                .get(5, TimeUnit.SECONDS);

        // Give the server a moment to process the setTimeframe frame, then broadcast a synthetic 15m update
        Thread.sleep(50);
        endpoint.broadcastUpdate(new TwapUpdate(
                Timeframe.FIFTEEN_MINUTES,
                1788444000L,
                1788444900L,
                new BigDecimal("78800.00"),
                new TwapPoint(1788444001L, new BigDecimal("78805.00"), new BigDecimal("78810.00")),
                false));

        String received = receivedFuture.get(5, TimeUnit.SECONDS);
        assertThat(received).isNotNull();
        assertThat(received).contains("timeframe");
        assertThat(received).contains("openPrice");

        webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "Done").get(5, TimeUnit.SECONDS);
    }
}
