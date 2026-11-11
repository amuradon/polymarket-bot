package cz.polymarket.bot.connector.polymarket.clob;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PolymarketEip712Signer}, {@link ClobOrderPayload}, and {@link ClobOrderEnvelope}
 * verifying byte-for-byte cryptographic and wire parity with the Rust reference implementation
 * ({@code clob_order_buy_fak.json} and {@code clob_order_sell_fak.json}).
 */
class PolymarketEip712SignerTest {

    private static final String TEST_PRIVATE_KEY_HEX =
            "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    private static final String EXPECTED_DERIVED_SIGNER_EOA =
            "0x2c7536E3605D9C16a7a3D7b1898e529396a65c23";
    private static final String TEST_MAKER =
            "0x90F8bf6A479f320ead074411a4B0e7944Ea8c9C1";
    private static final String TEST_API_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final String TEST_TOKEN_ID_NO =
            "100885549506856203411133717430816201540241829528439305131467520579115934905068";
    private static final long FIXED_SALT = 5423363231144297L;
    private static final long FIXED_TIMESTAMP_MS = 1792741778193L;

    private static final String EXPECTED_BUY_FAK_SIGNATURE =
            "0x7c257b2ac6e3326757099023d1e025551d3835bd021f38814fe8ce724dd1ceab"
                    + "27677e4b61c39ea25dd34ad9af9b55f8999e86891f727ce95460a34c9120d5dc1b"
                    + "3264e159346253e26a64e00b69032db0e7d32f94628de3e6eecb50304d7af3d2"
                    + "7f17cf2de4344e3f55164baebb6b24dccb03e83ceb29f1a7fc2e141d8fcf62c1"
                    + "4f726465722875696e743235362073616c742c61646472657373206d616b65722c"
                    + "61646472657373207369676e65722c75696e7432353620746f6b656e49642c7569"
                    + "6e74323536206d616b6572416d6f756e742c75696e743235362074616b6572416d"
                    + "6f756e742c75696e743820736964652c75696e7438207369676e61747572655479"
                    + "70652c75696e743235362074696d657374616d702c62797465733332206d657461"
                    + "646174612c62797465733332206275696c6465722900ba";

    private static final String EXPECTED_SELL_FAK_SIGNATURE =
            "0xe73a9523942f973d6fd7f9e5ad5ee9345186ce5f976086233ed2db7ea784fd16"
                    + "5e6ecff88f6c26e9c7d897efc03a36ac7c0fecec8b386d03c6d9456399adf08a1b"
                    + "3264e159346253e26a64e00b69032db0e7d32f94628de3e6eecb50304d7af3d2"
                    + "b791473e390bae1ab39d57cb908da89ac0a4337b77486f364d8fa549e385a088"
                    + "4f726465722875696e743235362073616c742c61646472657373206d616b65722c"
                    + "61646472657373207369676e65722c75696e7432353620746f6b656e49642c7569"
                    + "6e74323536206d616b6572416d6f756e742c75696e743235362074616b6572416d"
                    + "6f756e742c75696e743820736964652c75696e7438207369676e61747572655479"
                    + "70652c75696e743235362074696d657374616d702c62797465733332206d657461"
                    + "646174612c62797465733332206275696c6465722900ba";

    @Test
    void shouldDeriveEip55ChecksumAddressFromPrivateKey() {
        String derived = PolymarketEip712Signer.deriveEip55AddressFromPrivateKey(TEST_PRIVATE_KEY_HEX);
        assertThat(derived).isEqualTo(EXPECTED_DERIVED_SIGNER_EOA);
        assertThat(PolymarketEip712Signer.toEip55ChecksumAddress(TEST_MAKER.toLowerCase()))
                .isEqualTo(TEST_MAKER);
        assertThat(PolymarketEip712Signer.toEip55ChecksumAddress("0x42e8102eada3250f44ac67fcf3c40822a4b0038e"))
                .isEqualTo("0x42E8102eAda3250F44aC67fCF3C40822a4b0038e");
    }

    @Test
    void shouldSignBuyFakOrderMatchingRustContractByteForByte() throws Exception {
        PolymarketEip712Signer signer = new PolymarketEip712Signer(TEST_PRIVATE_KEY_HEX);
        ClobOrderEnvelope envelope = signer.buildSignedBuyOrder(
                TEST_MAKER,
                TEST_API_KEY,
                TEST_TOKEN_ID_NO,
                690, // 0.690 price in millicents
                5L,  // 5 shares
                "FAK",
                FIXED_SALT,
                FIXED_TIMESTAMP_MS
        );

        assertThat(envelope.owner()).isEqualTo(TEST_API_KEY);
        assertThat(envelope.orderType()).isEqualTo("FAK");
        assertThat(envelope.deferExec()).isFalse();

        ClobOrderPayload order = envelope.order();
        assertThat(order.salt()).isEqualTo(FIXED_SALT);
        assertThat(order.maker()).isEqualTo(TEST_MAKER);
        assertThat(order.signer()).isEqualTo(TEST_MAKER);
        assertThat(order.tokenId()).isEqualTo(TEST_TOKEN_ID_NO);
        assertThat(order.makerAmount()).isEqualTo("3450000");
        assertThat(order.takerAmount()).isEqualTo("5000000");
        assertThat(order.side()).isEqualTo("BUY");
        assertThat(order.expiration()).isEqualTo("0");
        assertThat(order.timestamp()).isEqualTo(String.valueOf(FIXED_TIMESTAMP_MS));
        assertThat(order.signatureType()).isEqualTo(3);
        assertThat(order.signature()).hasSize(636);
        assertThat(order.signature()).isEqualTo(EXPECTED_BUY_FAK_SIGNATURE);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(mapper.writeValueAsString(envelope));
        assertThat(root.path("order").path("salt").isIntegralNumber()).isTrue();
        assertThat(root.path("order").path("salt").asLong()).isEqualTo(FIXED_SALT);
        assertThat(root.path("order").path("signatureType").asInt()).isEqualTo(3);
        assertThat(root.path("deferExec").asBoolean()).isFalse();
    }

    @Test
    void shouldSignSellFakOrderMatchingRustContractByteForByte() {
        PolymarketEip712Signer signer = new PolymarketEip712Signer(TEST_PRIVATE_KEY_HEX);
        ClobOrderEnvelope envelope = signer.buildSignedSellOrder(
                TEST_MAKER,
                TEST_API_KEY,
                TEST_TOKEN_ID_NO,
                650,       // 0.650 limit price in millicents
                5_000_000L, // 5.00 shares in micro-shares
                "FAK",
                FIXED_SALT,
                FIXED_TIMESTAMP_MS
        );

        ClobOrderPayload order = envelope.order();
        assertThat(order.makerAmount()).isEqualTo("5000000");
        assertThat(order.takerAmount()).isEqualTo("3250000");
        assertThat(order.side()).isEqualTo("SELL");
        assertThat(order.signature()).hasSize(636);
        assertThat(order.signature()).isEqualTo(EXPECTED_SELL_FAK_SIGNATURE);
    }

    @Test
    void shouldEnforcePolymarketMinSharesAndOneDollarBuyFloor() {
        PolymarketEip712Signer signer = new PolymarketEip712Signer(TEST_PRIVATE_KEY_HEX);
        // At 0.10 (100 mc), 5 shares would only be $0.50 -> must scale up to 10 shares ($1.00)
        ClobOrderEnvelope envelope = signer.buildSignedBuyOrder(
                TEST_MAKER,
                TEST_API_KEY,
                TEST_TOKEN_ID_NO,
                100,
                5L,
                "FAK",
                FIXED_SALT,
                FIXED_TIMESTAMP_MS
        );

        assertThat(envelope.order().takerAmount()).isEqualTo("10000000"); // 10 shares
        assertThat(envelope.order().makerAmount()).isEqualTo("1000000");  // $1.00
    }
}
