package cz.polymarket.bot.connector.polymarket.clob;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.connector.polymarket.auth.PolymarketCredentials;
import cz.polymarket.bot.connector.polymarket.auth.PolymarketSecretsResolver;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract tests for {@link PolymarketClobRestClient} verifying exact parity with
 * {@code clob_order_buy_fak.json}, {@code clob_order_sell_fak.json}, {@code clob_order_cancel.json},
 * {@code clob_balance_allowance.json}, and {@code clob_time.json} from {@code polymarket-stale-order-sniper}.
 */
class PolymarketClobRestClientContractTest {

    private static final String TEST_PRIVATE_KEY_HEX =
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    private static final String TEST_SIGNER_ADDR =
            "0x2c7536E3605D9C16a7a3D7b1898e529396a65c23";
    private static final String TEST_MAKER_ADDR =
            "0x90F8bf6A479f320ead074411a4B0e7944Ea8c9C1";
    private static final String TEST_API_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final String TEST_SECRET =
            "c2VjcmV0X2tleV9mb3JfdGVzdGluZ19wb2x5bWFya2V0X2htYWM=";
    private static final String TEST_PASSPHRASE =
            "test_passphrase_123";
    private static final String TEST_TOKEN_ID_NO =
            "100885549506856203411133717430816201540241829528439305131467520579115934905068";

    private Vertx vertx;
    private HttpServer server;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldSubmitBuyAndSellFakOrdersWithValidL2HmacHeaders() throws Exception {
        AtomicReference<String> lastMethod = new AtomicReference<>();
        AtomicReference<String> lastPath = new AtomicReference<>();

        server = vertx.createHttpServer()
                .requestHandler(req -> req.bodyHandler(bodyBuf -> {
                    String method = req.method().name();
                    String path = req.path();
                    byte[] bodyBytes = bodyBuf.getBytes();
                    lastMethod.set(method);
                    lastPath.set(path);

                    assertThat(req.getHeader("POLY_ADDRESS")).isEqualTo(TEST_SIGNER_ADDR);
                    assertThat(req.getHeader("POLY_API_KEY")).isEqualTo(TEST_API_KEY);
                    assertThat(req.getHeader("POLY_PASSPHRASE")).isEqualTo(TEST_PASSPHRASE);
                    String ts = req.getHeader("POLY_TIMESTAMP");
                    String sig = req.getHeader("POLY_SIGNATURE");
                    assertThat(verifyL2Hmac(method, path, ts, sig, bodyBytes)).isTrue();

                    String bodyStr = new String(bodyBytes, StandardCharsets.UTF_8);
                    if (bodyStr.contains("\"side\":\"BUY\"")) {
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("""
                                        {
                                          "errorMsg": "",
                                          "orderID": "0x53c759df593d3fe70f949985f373e3a561952baafe87d14096f165fa390de9ce",
                                          "takingAmount": "5",
                                          "makingAmount": "3.45",
                                          "status": "matched",
                                          "transactionsHashes": [
                                            "0x154ed153f5ce5c1a9f0eb72c2a1b2fb566ee26b0c82e812fa8fa284d2a1b9c89"
                                          ],
                                          "success": true
                                        }
                                        """);
                    } else {
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("""
                                        {
                                          "errorMsg": "",
                                          "orderID": "0x8a91b22df4c3a7e102f949985f373e3a561952baafe87d14096f165fa3901122",
                                          "takingAmount": "3.25",
                                          "makingAmount": "5",
                                          "status": "matched",
                                          "transactionsHashes": [
                                            "0x99887766554433221100aabbccddeeff00112233445566778899aabbccddeeff"
                                          ],
                                          "success": true
                                        }
                                        """);
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketClobRestClient client = createClient(port);
        PolymarketEip712Signer signer = new PolymarketEip712Signer(TEST_PRIVATE_KEY_HEX);

        ClobOrderEnvelope buyEnvelope = signer.buildSignedBuyOrder(
                TEST_MAKER_ADDR, TEST_API_KEY, TEST_TOKEN_ID_NO, 690, 5L, "FAK", 5423363231144297L, 1792741778193L
        );
        ClobOrderResponse buyResp = client.submitOrder(buyEnvelope);
        assertThat(lastMethod.get()).isEqualTo("POST");
        assertThat(lastPath.get()).isEqualTo("/order");
        assertThat(buyResp.success()).isTrue();
        assertThat(buyResp.orderID()).isEqualTo("0x53c759df593d3fe70f949985f373e3a561952baafe87d14096f165fa390de9ce");
        assertThat(buyResp.status()).isEqualTo("matched");
        assertThat(buyResp.filledShares("BUY")).isEqualTo(5.0);
        assertThat(buyResp.avgPriceMc("BUY")).isEqualTo(690);

        ClobOrderEnvelope sellEnvelope = signer.buildSignedSellOrder(
                TEST_MAKER_ADDR, TEST_API_KEY, TEST_TOKEN_ID_NO, 650, 5_000_000L, "FAK", 5423363231144297L, 1792741778193L
        );
        ClobOrderResponse sellResp = client.submitOrder(sellEnvelope);
        assertThat(sellResp.success()).isTrue();
        assertThat(sellResp.orderID()).isEqualTo("0x8a91b22df4c3a7e102f949985f373e3a561952baafe87d14096f165fa3901122");
        assertThat(sellResp.filledShares("SELL")).isEqualTo(5.0);
        assertThat(sellResp.avgPriceMc("SELL")).isEqualTo(650);
    }

    @Test
    void shouldCancelOrderAndQueryBalanceAllowanceAndTimeMatchingContracts() throws Exception {
        String targetOrderId = "0x53c759df593d3fe70f949985f373e3a561952baafe87d14096f165fa390de9ce";

        server = vertx.createHttpServer()
                .requestHandler(req -> req.bodyHandler(bodyBuf -> {
                    String method = req.method().name();
                    String path = req.path();
                    String query = req.query();
                    byte[] bodyBytes = bodyBuf.getBytes();

                    if ("DELETE".equals(method) && "/order".equals(path)) {
                        String ts = req.getHeader("POLY_TIMESTAMP");
                        String sig = req.getHeader("POLY_SIGNATURE");
                        assertThat(verifyL2Hmac("DELETE", "/order", ts, sig, bodyBytes)).isTrue();
                        assertThat(new String(bodyBytes, StandardCharsets.UTF_8))
                                .isEqualTo("{\"orderID\":\"" + targetOrderId + "\"}");
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("""
                                        {
                                          "canceled": ["0x53c759df593d3fe70f949985f373e3a561952baafe87d14096f165fa390de9ce"],
                                          "not_canceled": {}
                                        }
                                        """);
                    } else if ("GET".equals(method) && "/balance-allowance".equals(path)) {
                        assertThat(query).isEqualTo("asset_type=CONDITIONAL&signature_type=3&token_id=" + TEST_TOKEN_ID_NO);
                        String ts = req.getHeader("POLY_TIMESTAMP");
                        String sig = req.getHeader("POLY_SIGNATURE");
                        // Critical contract rule: L2 HMAC uses clean path "/balance-allowance" without query string
                        assertThat(verifyL2Hmac("GET", "/balance-allowance", ts, sig, new byte[0])).isTrue();
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("""
                                        {
                                          "balance": "5000000",
                                          "allowances": {
                                            "0xE111180000d2663C0091e4f400237545B87B996B": "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                                          }
                                        }
                                        """);
                    } else if ("GET".equals(method) && "/time".equals(path)) {
                        req.response()
                                .putHeader("Content-Type", "application/json")
                                .end("{\"serverTime\":1792741777000}");
                    } else {
                        req.response().setStatusCode(404).end();
                    }
                }));

        int port = server.listen(0).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS).actualPort();
        PolymarketClobRestClient client = createClient(port);

        ClobCancelResponse cancelResp = client.cancelOrder(targetOrderId);
        assertThat(cancelResp.isCanceled(targetOrderId)).isTrue();
        assertThat(cancelResp.canceled()).containsExactly(targetOrderId);

        long microShares = client.getConditionalTokenBalanceMicroShares(TEST_TOKEN_ID_NO);
        assertThat(microShares).isEqualTo(5_000_000L);

        assertThat(client.pingKeepalive()).isTrue();
    }

    private PolymarketClobRestClient createClient(int port) {
        PolymarketSecretsResolver resolver = new PolymarketSecretsResolver(
                Optional.of(TEST_PRIVATE_KEY_HEX),
                Optional.of(TEST_API_KEY),
                Optional.of(TEST_PASSPHRASE),
                Optional.of(TEST_SECRET),
                Optional.of(TEST_MAKER_ADDR)
        );
        return new PolymarketClobRestClient(
                "http://localhost:" + port,
                HttpClient.newHttpClient(),
                new ObjectMapper(),
                resolver
        );
    }

    private static boolean verifyL2Hmac(String method, String cleanPath, String timestamp, String signature, byte[] body) {
        try {
            byte[] key = Base64.getUrlDecoder().decode(TEST_SECRET);
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            mac.update(timestamp.getBytes(StandardCharsets.UTF_8));
            mac.update(method.getBytes(StandardCharsets.UTF_8));
            mac.update(cleanPath.getBytes(StandardCharsets.UTF_8));
            if (body != null && body.length > 0) {
                mac.update(body);
            }
            String expected = Base64.getUrlEncoder().encodeToString(mac.doFinal());
            return expected.equals(signature);
        } catch (Exception e) {
            return false;
        }
    }
}
