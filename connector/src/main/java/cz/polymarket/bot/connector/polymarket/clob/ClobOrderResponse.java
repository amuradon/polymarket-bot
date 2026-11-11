package cz.polymarket.bot.connector.polymarket.clob;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Response payload returned by Polymarket CLOB {@code POST /order}.
 *
 * @param errorMsg           error message if rejected, or empty string
 * @param orderID            assigned Polymarket CLOB order hash ({@code 0x...})
 * @param takingAmount       filled taker amount string (shares for BUY, USDC for SELL)
 * @param makingAmount       filled maker amount string (USDC for BUY, shares for SELL)
 * @param status             order status ({@code "matched"}, {@code "live"}, {@code "delayed"}, {@code "unmatched"})
 * @param transactionsHashes on-chain settlement transaction hashes, if matched
 * @param success            whether the order submission succeeded
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ClobOrderResponse(
        String errorMsg,
        String orderID,
        String takingAmount,
        String makingAmount,
        String status,
        List<String> transactionsHashes,
        boolean success
) {

    /**
     * Parses a Polymarket CLOB amount string (either human-readable decimal like {@code "5"} / {@code "3.45"}
     * or raw 6-decimal micro-units like {@code "5000000"}) into micro-units ({@code 1.0 = 1,000,000}).
     *
     * @param raw amount string from CLOB response
     * @return amount in micro-units ({@code 10^-6})
     */
    public static long parseMicroUnits(String raw) {
        if (raw == null || raw.isBlank()) {
            return 0L;
        }
        String trimmed = raw.trim();
        if (trimmed.contains(".")) {
            try {
                double val = Double.parseDouble(trimmed);
                return val > 0.0 && Double.isFinite(val) ? Math.round(val * 1_000_000.0) : 0L;
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
        try {
            long val = Long.parseLong(trimmed);
            if (val <= 0L) {
                return 0L;
            }
            return val < 100_000L ? val * 1_000_000L : val;
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * Returns the filled share quantity in micro-shares ({@code 1 share = 1,000,000}) for the given order side.
     *
     * @param side {@code "BUY"} or {@code "SELL"}
     * @return filled micro-shares
     */
    public long filledMicroShares(String side) {
        boolean isBuy = "BUY".equalsIgnoreCase(side);
        return parseMicroUnits(isBuy ? takingAmount : makingAmount);
    }

    /**
     * Returns the filled USDC notional in micro-USDC ({@code $1.00 = 1,000,000}) for the given order side.
     *
     * @param side {@code "BUY"} or {@code "SELL"}
     * @return filled micro-USDC
     */
    public long filledMicroUsdc(String side) {
        boolean isBuy = "BUY".equalsIgnoreCase(side);
        return parseMicroUnits(isBuy ? makingAmount : takingAmount);
    }

    /**
     * Returns the filled share quantity as a decimal {@code double}.
     *
     * @param side {@code "BUY"} or {@code "SELL"}
     * @return filled shares
     */
    public double filledShares(String side) {
        return filledMicroShares(side) / 1_000_000.0;
    }

    /**
     * Computes the average execution price in millicents ({@code 0..1000}, where {@code 690 = $0.690}).
     *
     * @param side {@code "BUY"} or {@code "SELL"}
     * @return average execution price in millicents, or {@code 0} if unfilled
     */
    public int avgPriceMc(String side) {
        long sharesMicro = filledMicroShares(side);
        long usdcMicro = filledMicroUsdc(side);
        if (sharesMicro <= 0L || usdcMicro <= 0L) {
            return 0;
        }
        long mc = (usdcMicro * 1_000L) / sharesMicro;
        return (int) Math.max(0L, Math.min(1_000L, mc));
    }
}
