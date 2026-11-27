package cz.polymarket.bot.backtest.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Fully resolved immutable configuration snapshot of an executed backtest run,
 * capturing both engine parameters and effective strategy parameters/overrides.
 *
 * @param strategyName        evaluated trading strategy name
 * @param symbol              evaluated cryptocurrency symbol
 * @param startDate           inclusive UTC start date ({@code yyyy-MM-dd})
 * @param endDate             inclusive UTC end date ({@code yyyy-MM-dd})
 * @param initialCapital      initial account capital in USD
 * @param orderGatewayDelayMs simulated Polymarket CLOB {@code POST /order} gateway round-trip delay in milliseconds
 * @param binanceDelayMs      simulated Binance one-way feed delay in milliseconds
 * @param krakenDelayMs       simulated Kraken one-way feed delay in milliseconds
 * @param coinbaseDelayMs     simulated Coinbase one-way feed delay in milliseconds
 * @param strategyParams      complete effective strategy parameters map
 * @param strategyOverrides   subset of strategy parameters overridden relative to defaults
 */
public record BacktestRunConfig(
        String strategyName,
        String symbol,
        String startDate,
        String endDate,
        double initialCapital,
        long orderGatewayDelayMs,
        long binanceDelayMs,
        long krakenDelayMs,
        long coinbaseDelayMs,
        Map<String, Object> strategyParams,
        Map<String, Object> strategyOverrides
) {

    private static final ObjectMapper CANONICAL_HASH_MAPPER = JsonMapper.builder()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .build();

    /**
     * Computes the deterministic 64-character lowercase SHA-256 hash over this run's
     * strategy, symbol, engine settings, and effective strategy parameters (excluding date bounds).
     *
     * @return SHA-256 hex digest
     */
    public String configHash() {
        return computeConfigHash(
                strategyName,
                symbol,
                initialCapital,
                orderGatewayDelayMs,
                binanceDelayMs,
                krakenDelayMs,
                coinbaseDelayMs,
                strategyParams
        );
    }

    /**
     * Computes a deterministic 64-character lowercase SHA-256 hash over strategy name, symbol,
     * initial capital, CLOB order gateway delay, exchange feed delays, and normalized
     * strategy parameters. Date bounds and human-readable labels are intentionally excluded so incremental
     * runs over new dates automatically resolve to the same configuration hash.
     *
     * @param strategyName        evaluated trading strategy name
     * @param symbol              evaluated cryptocurrency symbol
     * @param initialCapital      initial account capital in USD
     * @param orderGatewayDelayMs simulated CLOB order gateway delay in milliseconds
     * @param binanceDelayMs      simulated Binance one-way feed delay in milliseconds
     * @param krakenDelayMs       simulated Kraken one-way feed delay in milliseconds
     * @param coinbaseDelayMs     simulated Coinbase one-way feed delay in milliseconds
     * @param strategyParams      complete effective strategy parameters map
     * @return 64-character lowercase hex SHA-256 digest
     */
    public static String computeConfigHash(
            String strategyName,
            String symbol,
            double initialCapital,
            long orderGatewayDelayMs,
            long binanceDelayMs,
            long krakenDelayMs,
            long coinbaseDelayMs,
            Map<String, Object> strategyParams) {
        Map<String, Object> normalizedStrategyParams = new TreeMap<>();
        if (strategyParams != null) {
            for (Map.Entry<String, Object> entry : strategyParams.entrySet()) {
                if (entry.getKey() != null) {
                    normalizedStrategyParams.put(entry.getKey(), normalizeValue(entry.getValue()));
                }
            }
        }

        Map<String, Object> canonicalPayload = new LinkedHashMap<>();
        canonicalPayload.put("strategyName", strategyName != null ? strategyName.trim() : "");
        canonicalPayload.put("symbol", symbol != null ? symbol.trim().toUpperCase(Locale.ROOT) : "");
        canonicalPayload.put("initialCapital", canonicalDouble(initialCapital));
        canonicalPayload.put("orderGatewayDelayMs", orderGatewayDelayMs);
        canonicalPayload.put("binanceDelayMs", binanceDelayMs);
        canonicalPayload.put("krakenDelayMs", krakenDelayMs);
        canonicalPayload.put("coinbaseDelayMs", coinbaseDelayMs);
        canonicalPayload.put("strategyParams", normalizedStrategyParams);

        try {
            String canonicalJson = CANONICAL_HASH_MAPPER.writeValueAsString(canonicalPayload);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(canonicalJson.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed computing configHash for " + strategyName, e);
        }
    }

    private static Object normalizeValue(Object val) {
        if (val instanceof Number num) {
            double d = num.doubleValue();
            if (Double.isFinite(d) && d == Math.rint(d) && d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
                return (long) d;
            }
            return canonicalDouble(d);
        }
        return val;
    }

    private static double canonicalDouble(double val) {
        return val == 0.0 ? 0.0 : val;
    }
}
