package cz.polymarket.bot.connector.polymarket.auth;

/**
 * Resolved Polymarket CLOB Level 1 (EIP-712 / EIP-1271 Deposit Wallet signing) and
 * Level 2 (HMAC-SHA256 REST & PolyBolt WebSocket authentication) credentials.
 *
 * @param signerKeyHex  64-character secp256k1 private key hex string
 * @param apiKey        Polymarket CLOB API key UUID (also used as {@code owner} in order envelopes)
 * @param passphrase    Polymarket CLOB L2 passphrase
 * @param secret        Base64-encoded Polymarket CLOB L2 HMAC secret
 * @param address       EIP-55 checksummed Polymarket Deposit/Proxy wallet address
 * @param makerAddress  EIP-55 checksummed maker address (defaults to {@code address})
 * @param signerAddress EIP-55 checksummed EOA address derived from {@code signerKeyHex} (sent in {@code POLY_ADDRESS})
 */
public record PolymarketCredentials(
        String signerKeyHex,
        String apiKey,
        String passphrase,
        String secret,
        String address,
        String makerAddress,
        String signerAddress
) {

    /**
     * Checks whether all Level 2 API credentials ({@code apiKey}, {@code secret}, {@code passphrase}) are present.
     *
     * @return true if non-blank L2 credentials are available
     */
    public boolean hasL2Credentials() {
        return apiKey != null && !apiKey.isBlank()
                && secret != null && !secret.isBlank()
                && passphrase != null && !passphrase.isBlank();
    }

    /**
     * Checks whether both Level 1 signing credentials and Level 2 API credentials are complete.
     *
     * @return true if ready for live order signing and CLOB submission
     */
    public boolean isCompleteForTrading() {
        return hasL2Credentials()
                && signerKeyHex != null && !signerKeyHex.isBlank()
                && makerAddress != null && !makerAddress.isBlank()
                && signerAddress != null && !signerAddress.isBlank();
    }
}
