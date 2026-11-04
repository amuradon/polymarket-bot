package cz.polymarket.bot.connector.polymarket.clob;

/**
 * Polymarket CLOB order wire structure matching the {@code POST /order} JSON specification.
 *
 * @param salt          random order salt (serialized as JSON integer on the wire)
 * @param maker         EIP-55 checksummed Deposit/Maker wallet address
 * @param signer        EIP-55 checksummed signer address (equals {@code maker} for {@code signatureType = 3})
 * @param tokenId       256-bit decimal token ID string
 * @param makerAmount   maker amount in micro-units (micro-USDC for BUY, micro-shares for SELL)
 * @param takerAmount   taker amount in micro-units (micro-shares for BUY, micro-USDC for SELL)
 * @param side          {@code "BUY"} or {@code "SELL"}
 * @param expiration    order expiration Unix epoch seconds string ({@code "0"} for FAK/FOK/GTC)
 * @param timestamp     order creation timestamp in epoch milliseconds string
 * @param metadata      32-byte zero hex string ({@code 0x00...00})
 * @param builder       32-byte zero hex string ({@code 0x00...00})
 * @param signatureType Polymarket signature type ({@code 3} for Deposit Wallet EIP-1271)
 * @param signature     636-character EIP-712 + EIP-1271 hex signature ({@code "0x" + 634 hex chars})
 */
public record ClobOrderPayload(
        long salt,
        String maker,
        String signer,
        String tokenId,
        String makerAmount,
        String takerAmount,
        String side,
        String expiration,
        String timestamp,
        String metadata,
        String builder,
        int signatureType,
        String signature
) {
}
