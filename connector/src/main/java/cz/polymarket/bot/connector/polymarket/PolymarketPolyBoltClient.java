package cz.polymarket.bot.connector.polymarket;

import cz.polymarket.bot.connector.ws.ConnectionState;
import cz.polymarket.bot.connector.ws.RawWebSocketListener;
import cz.polymarket.bot.connector.ws.ReconnectingWebSocketStream;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Resilient WebSocket client for Polymarket PolyBolt v2 ({@code wss://ws-live-v2.polymarket.com/ws}).
 * Manages dedicated authenticated streams for {@code price.crypto.twap} (60-second TWAP) and
 * {@code price.crypto} (spot reference price), performing the mandatory {@code op: "auth"} ->
 * {@code op: "authed"} -> {@code op: "subscribe"} handshake when credentials are provided and
 * forwarding raw WebSocket JSON messages without parsing overhead.
 */
@ApplicationScoped
public class PolymarketPolyBoltClient {

    private static final Logger LOG = Logger.getLogger(PolymarketPolyBoltClient.class);

    private final Vertx vertx;
    private final String wsUrl;
    private final String apiKey;
    private final String secret;
    private final String passphrase;
    private final long reconnectDelayMs;
    private final long pingIntervalMs;

    private volatile ReconnectingWebSocketStream twapStream;
    private volatile ReconnectingWebSocketStream cryptoPriceStream;

    /**
     * Constructs the Polymarket PolyBolt WebSocket client.
     *
     * @param vertx            Vert.x runtime instance
     * @param wsUrl            Polymarket PolyBolt v2 WebSocket endpoint URL
     * @param apiKey           Polymarket CLOB API key required for gated PolyBolt channels
     * @param secret           Polymarket CLOB API secret required for gated PolyBolt channels
     * @param passphrase       Polymarket CLOB API passphrase required for gated PolyBolt channels
     * @param reconnectDelayMs delay in milliseconds before attempting reconnection
     * @param pingIntervalMs   interval in milliseconds for sending WebSocket ping frames
     */
    @Inject
    public PolymarketPolyBoltClient(
            Vertx vertx,
            @ConfigProperty(name = "polymarket.connector.polybolt.ws-url", defaultValue = "wss://ws-live-v2.polymarket.com/ws")
            String wsUrl,
            @ConfigProperty(name = "polymarket.connector.api-key")
            Optional<String> apiKey,
            @ConfigProperty(name = "polymarket.connector.secret")
            Optional<String> secret,
            @ConfigProperty(name = "polymarket.connector.passphrase")
            Optional<String> passphrase,
            @ConfigProperty(name = "polymarket.connector.ws.reconnect-delay-ms", defaultValue = "3000")
            long reconnectDelayMs,
            @ConfigProperty(name = "polymarket.connector.ws.ping-interval-ms", defaultValue = "10000")
            long pingIntervalMs) {
        this.vertx = vertx;
        this.wsUrl = wsUrl;
        this.apiKey = apiKey.map(String::trim).orElse("");
        this.secret = secret.map(String::trim).orElse("");
        this.passphrase = passphrase.map(String::trim).orElse("");
        this.reconnectDelayMs = reconnectDelayMs;
        this.pingIntervalMs = pingIntervalMs;
    }

    /**
     * Starts the PolyBolt {@code price.crypto.twap} (60s window) WebSocket stream for the given asset symbol,
     * performing the {@code auth} -> {@code authed} -> {@code subscribe} handshake and forwarding raw messages
     * directly to the listener without parsing.
     *
     * @param symbol   trading symbol (e.g. BTCUSDT or BTC), normalized to PolyBolt symbol (e.g. btcusd)
     * @param listener raw WebSocket listener receiving unparsed JSON frames and local receipt timestamps
     */
    public synchronized void startTwapStream(String symbol, RawWebSocketListener listener) {
        stopTwapStream();
        String polyBoltSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(symbol);
        String subscribeMsg = buildTwapSubscribeMessage(polyBoltSymbol);
        ReconnectingWebSocketStream newStream = createAuthenticatedStream(
                "POLYMARKET-POLYBOLT-TWAP",
                subscribeMsg,
                () -> this.twapStream,
                listener
        );
        this.twapStream = newStream;
        newStream.start();
    }

    /**
     * Starts the PolyBolt {@code price.crypto} WebSocket stream for the given asset symbol,
     * performing the {@code auth} -> {@code authed} -> {@code subscribe} handshake and forwarding raw messages
     * directly to the listener without parsing.
     *
     * @param symbol   trading symbol (e.g. BTCUSDT or BTC), normalized to PolyBolt symbol (e.g. btcusd)
     * @param listener raw WebSocket listener receiving unparsed JSON frames and local receipt timestamps
     */
    public synchronized void startCryptoPriceStream(String symbol, RawWebSocketListener listener) {
        stopCryptoPriceStream();
        String polyBoltSymbol = PolymarketMarketInfo.normalizePolyBoltSymbol(symbol);
        String subscribeMsg = buildCryptoPriceSubscribeMessage(polyBoltSymbol);
        ReconnectingWebSocketStream newStream = createAuthenticatedStream(
                "POLYMARKET-POLYBOLT-CRYPTO-PRICE",
                subscribeMsg,
                () -> this.cryptoPriceStream,
                listener
        );
        this.cryptoPriceStream = newStream;
        newStream.start();
    }

    /**
     * Stops the PolyBolt {@code price.crypto.twap} WebSocket connection.
     */
    public synchronized void stopTwapStream() {
        ReconnectingWebSocketStream current = this.twapStream;
        this.twapStream = null;
        if (current != null) {
            current.stop();
        }
    }

    /**
     * Stops the PolyBolt {@code price.crypto} WebSocket connection.
     */
    public synchronized void stopCryptoPriceStream() {
        ReconnectingWebSocketStream current = this.cryptoPriceStream;
        this.cryptoPriceStream = null;
        if (current != null) {
            current.stop();
        }
    }

    /**
     * Stops all active PolyBolt WebSocket connections.
     */
    public synchronized void stop() {
        stopTwapStream();
        stopCryptoPriceStream();
    }

    /**
     * Returns the current connection state of the PolyBolt {@code price.crypto.twap} WebSocket stream.
     *
     * @return current {@link ConnectionState}
     */
    public ConnectionState getTwapState() {
        ReconnectingWebSocketStream current = this.twapStream;
        return current != null ? current.getState() : ConnectionState.DISCONNECTED;
    }

    /**
     * Returns the current connection state of the PolyBolt {@code price.crypto} WebSocket stream.
     *
     * @return current {@link ConnectionState}
     */
    public ConnectionState getCryptoPriceState() {
        ReconnectingWebSocketStream current = this.cryptoPriceStream;
        return current != null ? current.getState() : ConnectionState.DISCONNECTED;
    }

    /**
     * Checks whether all required Polymarket CLOB API credentials ({@code apiKey}, {@code secret}, {@code passphrase})
     * are configured and non-blank.
     *
     * @return true if all three credentials are present
     */
    public boolean hasCredentials() {
        return !apiKey.isEmpty() && !secret.isEmpty() && !passphrase.isEmpty();
    }

    private ReconnectingWebSocketStream createAuthenticatedStream(
            String streamName,
            String subscribeMsg,
            Supplier<ReconnectingWebSocketStream> streamRef,
            RawWebSocketListener listener) {
        if (!hasCredentials()) {
            String errorMsg = String.format(
                    "[%s] Missing required Polymarket CLOB API credentials (POLYMARKET_API_KEY, POLYMARKET_SECRET, POLYMARKET_PASSPHRASE). All streams must be recorded; aborting startup.",
                    streamName
            );
            LOG.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }
        String initialMessage = buildAuthMessage();

        return new ReconnectingWebSocketStream(
                vertx,
                streamName,
                wsUrl,
                () -> List.of(initialMessage),
                (rawJson, recvTs) -> {
                    if (rawJson == null || rawJson.isBlank()) {
                        return;
                    }
                    if (isAuthedConfirmation(rawJson)) {
                        ReconnectingWebSocketStream current = streamRef.get();
                        if (current != null) {
                            current.sendText(subscribeMsg);
                        }
                        return;
                    }
                    if (isSubscribedConfirmation(rawJson)) {
                        return;
                    }
                    if (isErrorFrame(rawJson)) {
                        LOG.warnf("[%s] PolyBolt server returned error: %s", streamName, rawJson);
                        if (isAuthRequiredError(rawJson)) {
                            ReconnectingWebSocketStream current = streamRef.get();
                            if (current != null) {
                                current.sendText(buildAuthMessage());
                            }
                        }
                        return;
                    }
                    if (listener != null) {
                        listener.onMessage(rawJson, recvTs);
                    }
                },
                reconnectDelayMs,
                pingIntervalMs
        );
    }

    private String buildAuthMessage() {
        return "{\"op\":\"auth\",\"rid\":\"a1\",\"auth\":{\"apiKey\":\""
                + apiKey + "\",\"secret\":\"" + secret + "\",\"passphrase\":\"" + passphrase + "\"}}";
    }

    private static String buildTwapSubscribeMessage(String polyBoltSymbol) {
        return "{\"op\":\"subscribe\",\"rid\":\"s1\",\"subscriptions\":["
                + "{\"channel\":\"price.crypto.twap\",\"filter\":{\"symbol\":\"" + polyBoltSymbol + "\",\"window_seconds\":60}}"
                + "]}";
    }

    private static String buildCryptoPriceSubscribeMessage(String polyBoltSymbol) {
        return "{\"op\":\"subscribe\",\"rid\":\"s1\",\"subscriptions\":["
                + "{\"channel\":\"price.crypto\",\"filter\":{\"symbol\":\"" + polyBoltSymbol + "\"}}"
                + "]}";
    }

    private static boolean isAuthedConfirmation(String rawJson) {
        return rawJson.contains("\"op\":\"authed\"") || rawJson.contains("\"op\": \"authed\"");
    }

    private static boolean isSubscribedConfirmation(String rawJson) {
        return rawJson.contains("\"op\":\"subscribed\"") || rawJson.contains("\"op\": \"subscribed\"");
    }

    private static boolean isErrorFrame(String rawJson) {
        return rawJson.contains("\"op\":\"error\"") || rawJson.contains("\"op\": \"error\"");
    }

    private static boolean isAuthRequiredError(String rawJson) {
        return rawJson.contains("\"code\":\"auth_required\"") || rawJson.contains("\"code\": \"auth_required\"");
    }
}
