package cz.polymarket.bot.web;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

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

        // Wait up to 5 seconds for a 1s scheduled TWAP update frame
        String received = receivedFuture.get(5, TimeUnit.SECONDS);
        assertThat(received).isNotNull();
        assertThat(received).contains("timeframe");
        assertThat(received).contains("openPrice");

        webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "Done").get(5, TimeUnit.SECONDS);
    }
}
