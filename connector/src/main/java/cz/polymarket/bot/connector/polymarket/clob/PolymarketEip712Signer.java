package cz.polymarket.bot.connector.polymarket.clob;

import org.bouncycastle.asn1.sec.SECNamedCurves;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.signers.HMacDSAKCalculator;
import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.math.ec.FixedPointCombMultiplier;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Cryptographic EIP-712 + EIP-1271 Deposit Wallet ({@code signatureType = 3}) order signer
 * for the Polymarket CTF Exchange on Polygon Mainnet ({@code chainId = 137}).
 * <p>
 * Produces deterministic RFC 6979 recoverable {@code secp256k1} signatures with the 252-byte
 * EIP-1271 DepositWallet trailer (total 317 bytes = {@code "0x"} + 634 lowercase hex characters),
 * matching the verified Rust reference implementation byte-for-byte.
 */
public final class PolymarketEip712Signer {

    /** Polygon Mainnet chain ID (137). */
    public static final long POLYGON_CHAIN_ID = 137L;

    /** Polymarket CTF Exchange v2 verifying contract on Polygon Mainnet. */
    public static final String POLYMARKET_CTF_EXCHANGE_ADDR = "0xE111180000d2663C0091e4f400237545B87B996B";

    /** Polymarket CTF Exchange EIP-712 protocol name. */
    public static final String PROTOCOL_NAME = "Polymarket CTF Exchange";

    /** Polymarket CTF Exchange EIP-712 protocol version. */
    public static final String PROTOCOL_VERSION = "2";

    /** 32-byte zero hex string used for {@code metadata} and {@code builder}. */
    public static final String BYTES32_ZERO = "0x0000000000000000000000000000000000000000000000000000000000000000";

    /** Polymarket Deposit Wallet EIP-1271 signature type (3). */
    public static final int POLYMARKET_SIGNATURE_TYPE_1271 = 3;

    /** Minimum marketable BUY order size on Polymarket (5 shares). */
    public static final long MIN_BUY_SHARES = 5L;

    /** Minimum marketable BUY order notional on Polymarket ($1.00 = 1,000,000 micro-USDC). */
    public static final long MIN_BUY_USDC_MICRO = 1_000_000L;

    /** Canonical EIP-712 {@code Order} struct type string (186 ASCII bytes = {@code 0x00ba}). */
    public static final String ORDER_TYPE_STRING =
            "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";

    /** Canonical EIP-1271 DepositWallet {@code TypedDataSign} wrapper type string. */
    public static final String TYPED_DATA_SIGN_TYPE_STRING =
            "TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)"
                    + ORDER_TYPE_STRING;

    private static final byte[] ORDER_TYPE_BYTES = ORDER_TYPE_STRING.getBytes(StandardCharsets.US_ASCII);
    private static final X9ECParameters CURVE_PARAMS = SECNamedCurves.getByName("secp256k1");
    private static final ECDomainParameters CURVE = new ECDomainParameters(
            CURVE_PARAMS.getCurve(),
            CURVE_PARAMS.getG(),
            CURVE_PARAMS.getN(),
            CURVE_PARAMS.getH()
    );
    private static final BigInteger HALF_CURVE_ORDER = CURVE_PARAMS.getN().shiftRight(1);

    private static final byte[] DOMAIN_SEPARATOR = computeCtfExchangeDomainSeparator();
    private static final byte[] ORDER_TYPE_HASH = keccak256(ORDER_TYPE_BYTES);
    private static final byte[] TYPED_DATA_SIGN_TYPE_HASH =
            keccak256(TYPED_DATA_SIGN_TYPE_STRING.getBytes(StandardCharsets.US_ASCII));
    private static final byte[] DEPOSIT_WALLET_NAME_HASH =
            keccak256("DepositWallet".getBytes(StandardCharsets.US_ASCII));
    private static final byte[] DEPOSIT_WALLET_VERSION_HASH =
            keccak256("1".getBytes(StandardCharsets.US_ASCII));

    private final BigInteger privateKeyScalar;
    private final String signerEoaAddress;

    private volatile String cachedMakerInput;
    private volatile String cachedMakerChecksummed;
    private volatile byte[] cachedMakerBytes;

    private volatile String cachedTokenId1;
    private volatile byte[] cachedTokenWord1;
    private volatile String cachedTokenId2;
    private volatile byte[] cachedTokenWord2;

    /**
     * Constructs a new {@link PolymarketEip712Signer} from a 32-byte hex-encoded secp256k1 private key.
     *
     * @param privateKeyHex 64-character hex string (with or without {@code 0x} prefix)
     */
    public PolymarketEip712Signer(String privateKeyHex) {
        byte[] keyBytes = decodeHexStrict(privateKeyHex, 32, "privateKeyHex");
        this.privateKeyScalar = new BigInteger(1, keyBytes);
        if (this.privateKeyScalar.signum() <= 0 || this.privateKeyScalar.compareTo(CURVE.getN()) >= 0) {
            throw new IllegalArgumentException("Invalid secp256k1 private key scalar");
        }
        this.signerEoaAddress = deriveEip55AddressFromPrivateKeyScalar(this.privateKeyScalar);
    }

    /**
     * Returns the EIP-55 checksummed EOA address corresponding to this signer's private key.
     *
     * @return EIP-55 checksummed EOA address ({@code 0x...})
     */
    public String getSignerEoaAddress() {
        return signerEoaAddress;
    }

    /**
     * Builds and signs a Polymarket CLOB {@code BUY} order envelope.
     *
     * @param makerAddress EIP-55 Deposit/Maker wallet address
     * @param ownerApiKey  Polymarket CLOB API key UUID
     * @param tokenId      256-bit decimal token ID string
     * @param priceMc      limit price in millicents ({@code 1..999}, where {@code 690 = $0.690})
     * @param shares       requested share quantity (automatically floored to 5 shares and $1.00 minimum notional)
     * @param orderType    order time-in-force ({@code "FAK"}, {@code "FOK"}, or {@code "GTC"})
     * @param salt         order salt
     * @param timestampMs  order timestamp in epoch milliseconds
     * @return signed {@link ClobOrderEnvelope}
     */
    public ClobOrderEnvelope buildSignedBuyOrder(
            String makerAddress,
            String ownerApiKey,
            String tokenId,
            int priceMc,
            long shares,
            String orderType,
            long salt,
            long timestampMs) {
        int clampedPriceMc = Math.max(1, Math.min(999, priceMc));
        long pricePerShareMicroUsdc = clampedPriceMc * 1_000L;
        long minSharesForDollar = (MIN_BUY_USDC_MICRO + pricePerShareMicroUsdc - 1L) / pricePerShareMicroUsdc;
        long effectiveShares = Math.max(shares, Math.max(MIN_BUY_SHARES, minSharesForDollar));

        long takerAmountMicro = effectiveShares * 1_000_000L;
        long makerAmountMicro = effectiveShares * pricePerShareMicroUsdc;

        return buildAndSignOrderEnvelope(
                makerAddress,
                ownerApiKey,
                tokenId,
                String.valueOf(makerAmountMicro),
                String.valueOf(takerAmountMicro),
                "BUY",
                orderType,
                salt,
                timestampMs
        );
    }

    /**
     * Builds and signs a Polymarket CLOB {@code BUY} order envelope using a newly generated random salt
     * and the current epoch millisecond timestamp.
     *
     * @param makerAddress EIP-55 Deposit/Maker wallet address
     * @param ownerApiKey  Polymarket CLOB API key UUID
     * @param tokenId      256-bit decimal token ID string
     * @param priceMc      limit price in millicents ({@code 1..999})
     * @param shares       requested share quantity
     * @param orderType    order time-in-force ({@code "FAK"}, {@code "FOK"}, or {@code "GTC"})
     * @return signed {@link ClobOrderEnvelope}
     */
    public ClobOrderEnvelope buildSignedBuyOrder(
            String makerAddress,
            String ownerApiKey,
            String tokenId,
            int priceMc,
            long shares,
            String orderType) {
        return buildSignedBuyOrder(
                makerAddress,
                ownerApiKey,
                tokenId,
                priceMc,
                shares,
                orderType,
                generateOrderSalt(),
                System.currentTimeMillis()
        );
    }

    /**
     * Builds and signs a Polymarket CLOB {@code SELL} order envelope.
     *
     * @param makerAddress EIP-55 Deposit/Maker wallet address
     * @param ownerApiKey  Polymarket CLOB API key UUID
     * @param tokenId      256-bit decimal token ID string
     * @param limitPriceMc limit price in millicents ({@code 1..999})
     * @param microShares  share quantity in micro-shares ({@code 1 share = 1,000,000}), truncated to 2 decimals
     * @param orderType    order time-in-force ({@code "FAK"}, {@code "FOK"}, or {@code "GTC"})
     * @param salt         order salt
     * @param timestampMs  order timestamp in epoch milliseconds
     * @return signed {@link ClobOrderEnvelope}
     */
    public ClobOrderEnvelope buildSignedSellOrder(
            String makerAddress,
            String ownerApiKey,
            String tokenId,
            int limitPriceMc,
            long microShares,
            String orderType,
            long salt,
            long timestampMs) {
        int clampedPriceMc = Math.max(1, Math.min(999, limitPriceMc));
        long tradableMicroShares = (Math.max(10_000L, microShares) / 10_000L) * 10_000L;
        long makerAmountMicro = tradableMicroShares;
        long takerAmountMicro = (tradableMicroShares * clampedPriceMc) / 1_000L;

        return buildAndSignOrderEnvelope(
                makerAddress,
                ownerApiKey,
                tokenId,
                String.valueOf(makerAmountMicro),
                String.valueOf(takerAmountMicro),
                "SELL",
                orderType,
                salt,
                timestampMs
        );
    }

    /**
     * Builds and signs an arbitrary Polymarket CLOB order envelope.
     *
     * @param makerAddress EIP-55 Deposit/Maker wallet address
     * @param ownerApiKey  Polymarket CLOB API key UUID
     * @param tokenId      256-bit decimal token ID string
     * @param makerAmount  maker amount string in micro-units
     * @param takerAmount  taker amount string in micro-units
     * @param side         {@code "BUY"} or {@code "SELL"}
     * @param orderType    {@code "FAK"}, {@code "FOK"}, or {@code "GTC"}
     * @param salt         order salt
     * @param timestampMs  order timestamp in epoch milliseconds
     * @return signed {@link ClobOrderEnvelope}
     */
    public ClobOrderEnvelope buildAndSignOrderEnvelope(
            String makerAddress,
            String ownerApiKey,
            String tokenId,
            String makerAmount,
            String takerAmount,
            String side,
            String orderType,
            long salt,
            long timestampMs) {
        String checksummedMaker = resolveChecksummedMaker(makerAddress);
        ClobOrderPayload unsignedOrder = new ClobOrderPayload(
                salt,
                checksummedMaker,
                checksummedMaker,
                tokenId.trim(),
                makerAmount,
                takerAmount,
                side,
                "0",
                String.valueOf(timestampMs),
                BYTES32_ZERO,
                BYTES32_ZERO,
                POLYMARKET_SIGNATURE_TYPE_1271,
                ""
        );
        String signatureHex = signOrder(unsignedOrder);
        ClobOrderPayload signedOrder = new ClobOrderPayload(
                unsignedOrder.salt(),
                unsignedOrder.maker(),
                unsignedOrder.signer(),
                unsignedOrder.tokenId(),
                unsignedOrder.makerAmount(),
                unsignedOrder.takerAmount(),
                unsignedOrder.side(),
                unsignedOrder.expiration(),
                unsignedOrder.timestamp(),
                unsignedOrder.metadata(),
                unsignedOrder.builder(),
                unsignedOrder.signatureType(),
                signatureHex
        );
        return new ClobOrderEnvelope(signedOrder, ownerApiKey, orderType, false);
    }

    /**
     * Computes the 636-character EIP-712 + EIP-1271 DepositWallet signature ({@code "0x" + 634 hex chars})
     * for the given {@link ClobOrderPayload}.
     *
     * @param order order payload to sign
     * @return {@code 0x}-prefixed 636-character hex signature
     */
    public String signOrder(ClobOrderPayload order) {
        byte[] contentsHash = computeOrderStructHash(order);
        byte[] signerAddrBytes = resolveAddressBytes(order.signer(), "order.signer");

        // TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)
        byte[] typedDataBuf = new byte[7 * 32];
        System.arraycopy(TYPED_DATA_SIGN_TYPE_HASH, 0, typedDataBuf, 0, 32);
        System.arraycopy(contentsHash, 0, typedDataBuf, 32, 32);
        System.arraycopy(DEPOSIT_WALLET_NAME_HASH, 0, typedDataBuf, 64, 32);
        System.arraycopy(DEPOSIT_WALLET_VERSION_HASH, 0, typedDataBuf, 96, 32);
        encodeUint64(POLYGON_CHAIN_ID, typedDataBuf, 128);
        encodeAddressWord(signerAddrBytes, typedDataBuf, 160);
        // offset 192..224 is bytes32(0)

        byte[] typedDataSignStructHash = keccak256(typedDataBuf);

        byte[] eip712Payload = new byte[2 + 32 + 32];
        eip712Payload[0] = 0x19;
        eip712Payload[1] = 0x01;
        System.arraycopy(DOMAIN_SEPARATOR, 0, eip712Payload, 2, 32);
        System.arraycopy(typedDataSignStructHash, 0, eip712Payload, 34, 32);

        byte[] signingDigest = keccak256(eip712Payload);

        // Deterministic RFC 6979 recoverable secp256k1 signature (65 bytes) + 252-byte EIP-1271 trailer = 317 bytes
        HMacDSAKCalculator kCalculator = new HMacDSAKCalculator(new SHA256Digest());
        kCalculator.init(CURVE.getN(), privateKeyScalar, signingDigest);
        BigInteger n = CURVE.getN();
        BigInteger z = new BigInteger(1, signingDigest);

        BigInteger r;
        BigInteger s;
        int recId;
        while (true) {
            BigInteger k = kCalculator.nextK();
            ECPoint p = new FixedPointCombMultiplier().multiply(CURVE.getG(), k).normalize();
            BigInteger xR = p.getAffineXCoord().toBigInteger();
            BigInteger yR = p.getAffineYCoord().toBigInteger();
            r = xR.mod(n);
            if (r.signum() == 0) {
                continue;
            }
            recId = (yR.testBit(0) ? 1 : 0) | (xR.compareTo(n) >= 0 ? 2 : 0);
            s = k.modInverse(n).multiply(z.add(privateKeyScalar.multiply(r))).mod(n);
            if (s.signum() == 0) {
                continue;
            }
            if (s.compareTo(HALF_CURVE_ORDER) > 0) {
                s = n.subtract(s);
                recId ^= 1;
            }
            break;
        }

        byte[] fullSig = new byte[65 + 32 + 32 + ORDER_TYPE_BYTES.length + 2];
        encodeUint256(r, fullSig, 0);
        encodeUint256(s, fullSig, 32);
        fullSig[64] = (byte) (recId + 27);

        // Append 252-byte EIP-1271 DepositWallet trailer
        System.arraycopy(DOMAIN_SEPARATOR, 0, fullSig, 65, 32);
        System.arraycopy(contentsHash, 0, fullSig, 97, 32);
        System.arraycopy(ORDER_TYPE_BYTES, 0, fullSig, 129, ORDER_TYPE_BYTES.length);
        int lenOffset = 129 + ORDER_TYPE_BYTES.length;
        int typeLen = ORDER_TYPE_BYTES.length;
        fullSig[lenOffset] = (byte) ((typeLen >>> 8) & 0xFF);
        fullSig[lenOffset + 1] = (byte) (typeLen & 0xFF);

        return "0x" + HexFormat.of().formatHex(fullSig);
    }

    /**
     * Generates a 53-bit positive integer salt suitable for Polymarket CLOB JSON numbers.
     *
     * @return random salt in {@code [1, 2^53 - 1]}
     */
    public static long generateOrderSalt() {
        long raw = ThreadLocalRandom.current().nextLong() & ((1L << 53) - 1L);
        return raw == 0L ? 1L : raw;
    }

    /**
     * Derives the EIP-55 checksummed Ethereum address from a 32-byte hex-encoded secp256k1 private key.
     *
     * @param privateKeyHex 64-character hex string (with or without {@code 0x} prefix)
     * @return EIP-55 checksummed address ({@code 0x...})
     */
    public static String deriveEip55AddressFromPrivateKey(String privateKeyHex) {
        byte[] keyBytes = decodeHexStrict(privateKeyHex, 32, "privateKeyHex");
        BigInteger scalar = new BigInteger(1, keyBytes);
        return deriveEip55AddressFromPrivateKeyScalar(scalar);
    }

    /**
     * Formats a 20-byte hex address according to EIP-55 mixed-case checksum rules.
     *
     * @param addressHex 20-byte hex address (with or without {@code 0x} prefix)
     * @return EIP-55 checksummed address string ({@code 0x...})
     */
    public static String toEip55ChecksumAddress(String addressHex) {
        byte[] addrBytes = decodeHexStrict(addressHex, 20, "addressHex");
        String lowerHex = HexFormat.of().formatHex(addrBytes);
        byte[] hash = keccak256(lowerHex.getBytes(StandardCharsets.US_ASCII));

        StringBuilder out = new StringBuilder(42);
        out.append("0x");
        for (int i = 0; i < 40; i++) {
            char c = lowerHex.charAt(i);
            int nibble = (hash[i >>> 1] >>> ((i & 1) == 0 ? 4 : 0)) & 0x0F;
            if (c >= 'a' && c <= 'f' && nibble >= 8) {
                out.append((char) (c - 32));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static String deriveEip55AddressFromPrivateKeyScalar(BigInteger scalar) {
        ECPoint pubPoint = new FixedPointCombMultiplier().multiply(CURVE.getG(), scalar).normalize();
        byte[] uncompressed = pubPoint.getEncoded(false); // 65 bytes: 0x04 || X (32B) || Y (32B)
        byte[] xy = new byte[64];
        System.arraycopy(uncompressed, 1, xy, 0, 64);
        byte[] pubHash = keccak256(xy);
        byte[] addrBytes = new byte[20];
        System.arraycopy(pubHash, 12, addrBytes, 0, 20);
        return toEip55ChecksumAddress(HexFormat.of().formatHex(addrBytes));
    }

    private static byte[] computeCtfExchangeDomainSeparator() {
        byte[] typeHash = keccak256(
                "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
                        .getBytes(StandardCharsets.US_ASCII)
        );
        byte[] nameHash = keccak256(PROTOCOL_NAME.getBytes(StandardCharsets.US_ASCII));
        byte[] versionHash = keccak256(PROTOCOL_VERSION.getBytes(StandardCharsets.US_ASCII));
        byte[] contractBytes = decodeHexStrict(POLYMARKET_CTF_EXCHANGE_ADDR, 20, "verifyingContract");

        byte[] buf = new byte[5 * 32];
        System.arraycopy(typeHash, 0, buf, 0, 32);
        System.arraycopy(nameHash, 0, buf, 32, 32);
        System.arraycopy(versionHash, 0, buf, 64, 32);
        encodeUint256(BigInteger.valueOf(POLYGON_CHAIN_ID), buf, 96);
        encodeAddressWord(contractBytes, buf, 128);
        return keccak256(buf);
    }

    private byte[] computeOrderStructHash(ClobOrderPayload order) {
        byte[] makerBytes = resolveAddressBytes(order.maker(), "order.maker");
        byte[] signerBytes = order.maker().equals(order.signer())
                ? makerBytes
                : resolveAddressBytes(order.signer(), "order.signer");
        byte[] tokenWord = resolveTokenWord(order.tokenId());
        int sideCode = "BUY".equalsIgnoreCase(order.side()) ? 0 : 1;

        byte[] buf = new byte[12 * 32];
        System.arraycopy(ORDER_TYPE_HASH, 0, buf, 0, 32);
        encodeUint64(order.salt(), buf, 32);
        encodeAddressWord(makerBytes, buf, 64);
        encodeAddressWord(signerBytes, buf, 96);
        System.arraycopy(tokenWord, 0, buf, 128, 32);
        encodeDecimalUint256(order.makerAmount(), buf, 160);
        encodeDecimalUint256(order.takerAmount(), buf, 192);
        encodeUint64(sideCode, buf, 224);
        encodeUint64(order.signatureType(), buf, 256);
        encodeDecimalUint256(order.timestamp(), buf, 288);
        if (!BYTES32_ZERO.equals(order.metadata())) {
            byte[] metadataBytes = decodeHexStrict(order.metadata(), 32, "order.metadata");
            System.arraycopy(metadataBytes, 0, buf, 320, 32);
        }
        if (!BYTES32_ZERO.equals(order.builder())) {
            byte[] builderBytes = decodeHexStrict(order.builder(), 32, "order.builder");
            System.arraycopy(builderBytes, 0, buf, 352, 32);
        }
        return keccak256(buf);
    }

    private String resolveChecksummedMaker(String makerAddress) {
        if (makerAddress != null && makerAddress.equals(this.cachedMakerInput)) {
            String cached = this.cachedMakerChecksummed;
            if (cached != null) {
                return cached;
            }
        }
        String checksummed = toEip55ChecksumAddress(makerAddress);
        byte[] bytes = decodeHexStrict(checksummed, 20, "makerAddress");
        this.cachedMakerBytes = bytes;
        this.cachedMakerChecksummed = checksummed;
        this.cachedMakerInput = makerAddress;
        return checksummed;
    }

    private byte[] resolveAddressBytes(String addressHex, String fieldName) {
        if (addressHex != null
                && (addressHex.equals(this.cachedMakerChecksummed) || addressHex.equals(this.cachedMakerInput))) {
            byte[] cached = this.cachedMakerBytes;
            if (cached != null) {
                return cached;
            }
        }
        return decodeHexStrict(addressHex, 20, fieldName);
    }

    private byte[] resolveTokenWord(String tokenId) {
        String clean = tokenId.trim();
        if (clean.equals(this.cachedTokenId1)) {
            byte[] word = this.cachedTokenWord1;
            if (word != null) {
                return word;
            }
        }
        if (clean.equals(this.cachedTokenId2)) {
            byte[] word = this.cachedTokenWord2;
            if (word != null) {
                return word;
            }
        }
        byte[] word = new byte[32];
        encodeUint256(new BigInteger(clean), word, 0);
        this.cachedTokenId2 = this.cachedTokenId1;
        this.cachedTokenWord2 = this.cachedTokenWord1;
        this.cachedTokenId1 = clean;
        this.cachedTokenWord1 = word;
        return word;
    }

    private static void encodeDecimalUint256(String decimalStr, byte[] dest, int offset) {
        String clean = decimalStr.trim();
        int len = clean.length();
        if (len > 0 && len <= 18 && clean.charAt(0) != '-') {
            long val = 0L;
            boolean allDigits = true;
            for (int i = 0; i < len; i++) {
                char c = clean.charAt(i);
                if (c >= '0' && c <= '9') {
                    val = val * 10L + (c - '0');
                } else {
                    allDigits = false;
                    break;
                }
            }
            if (allDigits) {
                encodeUint64(val, dest, offset);
                return;
            }
        }
        encodeUint256(new BigInteger(clean), dest, offset);
    }

    private static void encodeUint64(long value, byte[] dest, int offset) {
        if (value < 0L) {
            encodeUint256(BigInteger.valueOf(value), dest, offset);
            return;
        }
        int base = offset + 24;
        dest[base] = (byte) ((value >>> 56) & 0xFFL);
        dest[base + 1] = (byte) ((value >>> 48) & 0xFFL);
        dest[base + 2] = (byte) ((value >>> 40) & 0xFFL);
        dest[base + 3] = (byte) ((value >>> 32) & 0xFFL);
        dest[base + 4] = (byte) ((value >>> 24) & 0xFFL);
        dest[base + 5] = (byte) ((value >>> 16) & 0xFFL);
        dest[base + 6] = (byte) ((value >>> 8) & 0xFFL);
        dest[base + 7] = (byte) (value & 0xFFL);
    }

    private static void encodeUint256(BigInteger value, byte[] dest, int offset) {
        if (value.signum() < 0) {
            throw new IllegalArgumentException("uint256 cannot be negative: " + value);
        }
        byte[] raw = value.toByteArray();
        if (raw.length == 33 && raw[0] == 0) {
            System.arraycopy(raw, 1, dest, offset, 32);
        } else if (raw.length <= 32) {
            System.arraycopy(raw, 0, dest, offset + (32 - raw.length), raw.length);
        } else {
            throw new IllegalArgumentException("Value overflows uint256: " + value);
        }
    }

    private static void encodeAddressWord(byte[] addr20Bytes, byte[] dest, int offset) {
        System.arraycopy(addr20Bytes, 0, dest, offset + 12, 20);
    }

    private static byte[] decodeHexStrict(String hex, int expectedByteLen, String fieldName) {
        if (hex == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        String clean = hex.trim();
        if (clean.startsWith("0x") || clean.startsWith("0X")) {
            clean = clean.substring(2);
        }
        if (clean.length() != expectedByteLen * 2) {
            throw new IllegalArgumentException(
                    fieldName + " must be " + (expectedByteLen * 2) + " hex chars, got " + clean.length()
            );
        }
        return HexFormat.of().parseHex(clean);
    }

    private static byte[] keccak256(byte[] input) {
        Keccak.Digest256 digest = new Keccak.Digest256();
        return digest.digest(input);
    }
}
