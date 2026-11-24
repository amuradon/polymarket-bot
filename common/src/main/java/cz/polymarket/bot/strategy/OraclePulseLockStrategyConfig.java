package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.Timeframe;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Immutable configuration bean for {@link OraclePulseLockStrategy}.
 * <p>
 * Defaults correspond to the calibrated Wave 2 <b>Profile A</b> configuration
 * ({@code maxTauMs = 175_000}, {@code maxRiskPerMarketUsd = 330.0}, strict One-Side Hold-to-Settlement).
 */
@ApplicationScoped
public class OraclePulseLockStrategyConfig {

    private final Timeframe timeframe;
    private final long minTauMs;
    private final long maxTauMs;
    private final long maxQuoteAgeMs;
    private final long cooldownPre60Ms;
    private final long cooldownMidLockMs;
    private final long cooldownLateLockMs;
    private final double spotLeadBeta;
    private final double sigmaFloorLockInUsd;
    private final double minSigma1sBps;
    private final double ofiLogitWeight;
    private final double syncLogitWeight;
    private final double toxicOfiVeto;
    private final double minModelProb;
    private final double minEntryAsk;
    private final double maxEntryAskPre60;
    private final double maxEntryAskLockIn;
    private final double maxSpread;
    private final int minDepthShares;
    private final double minLogitEdgePre60;
    private final double minLogitEdgeLate;
    private final double minBankedCushionSigma;
    private final double minFreshUnrepricedEdge;
    private final double minSpotImpulseSigma;
    private final double takerFeeRate;
    private final double epistemicVariance;
    private final double maxRiskPerMarketUsd;
    private final double kellyBankrollUsd;
    private final int minOrderShares;
    private final int maxOrderShares;
    private final int maxClipsPerMarket;
    private final String defaultMarketId;
    private final String upToken;
    private final String downToken;

    /**
     * Constructs the {@link OraclePulseLockStrategy} configuration with MicroProfile properties.
     *
     * @param timeframeCode          candle timeframe code (default {@code "15m"})
     * @param minTauMs               minimum remaining time before expiry to submit orders in ms (default {@code 2000})
     * @param maxTauMs               maximum remaining time before expiry to evaluate signals in ms (default {@code 175000})
     * @param maxQuoteAgeMs          maximum allowed resting CLOB quote age in ms (default {@code 15000})
     * @param cooldownPre60Ms        inter-clip cooldown above 60s remaining in ms (default {@code 1800})
     * @param cooldownMidLockMs      inter-clip cooldown in (35s, 60s] remaining in ms (default {@code 600})
     * @param cooldownLateLockMs     inter-clip cooldown in [minTau, 35s] remaining in ms (default {@code 180})
     * @param spotLeadBeta           Chainlink nowcast beta on 50ms Direct-USD spot lead (default {@code 0.84})
     * @param sigmaFloorLockInUsd    sub-second oracle basis/roll-out floor inside 60s TWAP lock-in in USD (default {@code 0.55})
     * @param minSigma1sBps          minimum 1-second diffusive volatility floor in basis points (default {@code 0.32})
     * @param ofiLogitWeight         Layer-2 log-odds weight on 1s 3-venue signed OFI (default {@code 0.30})
     * @param syncLogitWeight        Layer-2 log-odds weight on normalized 500ms 3-venue sync impulse (default {@code 0.15})
     * @param toxicOfiVeto           adverse signed OFI veto threshold when not cushion-locked (default {@code -0.18})
     * @param minModelProb           minimum final model win probability for candidate side (default {@code 0.58})
     * @param minEntryAsk            minimum allowed entry ask price (default {@code 0.36})
     * @param maxEntryAskPre60       maximum allowed entry ask price prior to 60s TWAP lock-in (default {@code 0.72})
     * @param maxEntryAskLockIn      maximum allowed entry ask price inside 60s TWAP lock-in (default {@code 0.86})
     * @param maxSpread              maximum allowed bid-ask spread (default {@code 0.026})
     * @param minDepthShares         minimum required 1-cent ask depth in shares (default {@code 15})
     * @param minLogitEdgePre60      minimum log-odds edge in nats above 60s remaining (default {@code 0.80})
     * @param minLogitEdgeLate       minimum log-odds edge in nats inside final 25s remaining (default {@code 0.56})
     * @param minBankedCushionSigma  minimum irreversible banked TWAP integral cushion in sigma units (default {@code 1.15})
     * @param minFreshUnrepricedEdge minimum fresh unrepriced probability jump in (0, 1) (default {@code 0.036})
     * @param minSpotImpulseSigma    minimum favorable Direct-USD spot impulse in sigma units (default {@code 1.95})
     * @param takerFeeRate           Polymarket parabolic crypto taker fee coefficient (default {@code 0.07})
     * @param epistemicVariance      Bayesian epistemic model variance floor in Kelly denominator (default {@code 0.018})
     * @param maxRiskPerMarketUsd    maximum total USD capital risked per 15m market (default {@code 330.0})
     * @param kellyBankrollUsd       reference bankroll scaling epistemic Kelly allocation in USD (default {@code 4620.0})
     * @param minOrderShares         minimum shares per submitted order clip (default {@code 8})
     * @param maxOrderShares         maximum shares per submitted order clip (default {@code 260})
     * @param maxClipsPerMarket      maximum filled BUY clips per market (default {@code 4})
     * @param defaultMarketId        logical market identifier for OrderCommand (default {@code "pm-btc-15m"})
     * @param upToken                logical UP token identifier (default {@code "BTC-UP"})
     * @param downToken              logical DOWN token identifier (default {@code "BTC-DOWN"})
     */
    @Inject
    public OraclePulseLockStrategyConfig(
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.timeframe", defaultValue = "15m")
            String timeframeCode,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-tau-ms", defaultValue = "2000")
            long minTauMs,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.max-tau-ms", defaultValue = "175000")
            long maxTauMs,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.max-quote-age-ms", defaultValue = "15000")
            long maxQuoteAgeMs,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.cooldown-pre60-ms", defaultValue = "1800")
            long cooldownPre60Ms,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.cooldown-mid-lock-ms", defaultValue = "600")
            long cooldownMidLockMs,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.cooldown-late-lock-ms", defaultValue = "180")
            long cooldownLateLockMs,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.spot-lead-beta", defaultValue = "0.84")
            double spotLeadBeta,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.sigma-floor-lock-in-usd", defaultValue = "0.55")
            double sigmaFloorLockInUsd,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-sigma-1s-bps", defaultValue = "0.32")
            double minSigma1sBps,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.ofi-logit-weight", defaultValue = "0.30")
            double ofiLogitWeight,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.sync-logit-weight", defaultValue = "0.15")
            double syncLogitWeight,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.toxic-ofi-veto", defaultValue = "-0.18")
            double toxicOfiVeto,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-model-prob", defaultValue = "0.58")
            double minModelProb,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-entry-ask", defaultValue = "0.36")
            double minEntryAsk,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.max-entry-ask-pre60", defaultValue = "0.72")
            double maxEntryAskPre60,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.max-entry-ask-lock-in", defaultValue = "0.86")
            double maxEntryAskLockIn,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.max-spread", defaultValue = "0.026")
            double maxSpread,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-depth-shares", defaultValue = "15")
            int minDepthShares,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-logit-edge-pre60", defaultValue = "0.80")
            double minLogitEdgePre60,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-logit-edge-late", defaultValue = "0.56")
            double minLogitEdgeLate,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-banked-cushion-sigma", defaultValue = "1.15")
            double minBankedCushionSigma,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-fresh-unrepriced-edge", defaultValue = "0.036")
            double minFreshUnrepricedEdge,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-spot-impulse-sigma", defaultValue = "1.95")
            double minSpotImpulseSigma,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.taker-fee-rate", defaultValue = "0.07")
            double takerFeeRate,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.epistemic-variance", defaultValue = "0.018")
            double epistemicVariance,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.max-risk-per-market-usd", defaultValue = "330.0")
            double maxRiskPerMarketUsd,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.kelly-bankroll-usd", defaultValue = "4620.0")
            double kellyBankrollUsd,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.min-order-shares", defaultValue = "8")
            int minOrderShares,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.max-order-shares", defaultValue = "260")
            int maxOrderShares,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.max-clips-per-market", defaultValue = "4")
            int maxClipsPerMarket,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.market-id", defaultValue = "pm-btc-15m")
            String defaultMarketId,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.up-token", defaultValue = "BTC-UP")
            String upToken,
            @ConfigProperty(name = "polymarket.strategy.oracle-pulse-lock.down-token", defaultValue = "BTC-DOWN")
            String downToken) {

        Timeframe parsedTf = Timeframe.fromCode(timeframeCode != null ? timeframeCode.trim() : "15m");
        long candleMs = parsedTf.getDurationSeconds() * 1_000L;
        if (minTauMs < 500L || maxTauMs <= minTauMs || maxTauMs >= candleMs) {
            throw new IllegalArgumentException("Tau window must satisfy 500 <= minTauMs < maxTauMs < candleDurationMs");
        }
        if (maxQuoteAgeMs <= 0L || cooldownPre60Ms < 0L || cooldownMidLockMs < 0L || cooldownLateLockMs < 0L) {
            throw new IllegalArgumentException("Quote age and cooldown durations must be valid");
        }
        if (!Double.isFinite(spotLeadBeta) || spotLeadBeta < 0.0 || spotLeadBeta > 2.0
                || !Double.isFinite(sigmaFloorLockInUsd) || sigmaFloorLockInUsd < 0.0
                || !Double.isFinite(minSigma1sBps) || minSigma1sBps <= 0.0) {
            throw new IllegalArgumentException("Nowcast beta and volatility floor parameters must be valid");
        }
        if (!Double.isFinite(ofiLogitWeight) || !Double.isFinite(syncLogitWeight) || !Double.isFinite(toxicOfiVeto)) {
            throw new IllegalArgumentException("Layer-2 microstructure weights must be finite");
        }
        if (!Double.isFinite(minModelProb) || minModelProb <= 0.50 || minModelProb >= 0.99
                || !Double.isFinite(minEntryAsk) || minEntryAsk <= 0.01
                || !Double.isFinite(maxEntryAskPre60) || maxEntryAskPre60 < minEntryAsk || maxEntryAskPre60 >= 0.99
                || !Double.isFinite(maxEntryAskLockIn) || maxEntryAskLockIn < minEntryAsk || maxEntryAskLockIn >= 0.99) {
            throw new IllegalArgumentException("Probability and ask bounds must be valid");
        }
        if (!Double.isFinite(maxSpread) || maxSpread <= 0.0 || maxSpread >= 0.50 || minDepthShares < 1) {
            throw new IllegalArgumentException("Spread and depth bounds must be valid");
        }
        if (!Double.isFinite(minLogitEdgePre60) || minLogitEdgePre60 < 0.0
                || !Double.isFinite(minLogitEdgeLate) || minLogitEdgeLate < 0.0
                || !Double.isFinite(minBankedCushionSigma)
                || !Double.isFinite(minFreshUnrepricedEdge)
                || !Double.isFinite(minSpotImpulseSigma)) {
            throw new IllegalArgumentException("Edge, cushion, and impulse thresholds must be valid");
        }
        if (!Double.isFinite(takerFeeRate) || takerFeeRate < 0.0 || takerFeeRate > 0.50
                || !Double.isFinite(epistemicVariance) || epistemicVariance <= 0.0
                || !Double.isFinite(maxRiskPerMarketUsd) || maxRiskPerMarketUsd <= 0.0
                || !Double.isFinite(kellyBankrollUsd) || kellyBankrollUsd <= 0.0) {
            throw new IllegalArgumentException("Fee, epistemic variance, and Kelly risk bounds must be valid");
        }
        if (minOrderShares < 5 || maxOrderShares < minOrderShares || maxClipsPerMarket < 1) {
            throw new IllegalArgumentException("Share and clip bounds must satisfy 5 <= minOrderShares <= maxOrderShares and maxClipsPerMarket >= 1");
        }

        this.timeframe = parsedTf;
        this.minTauMs = minTauMs;
        this.maxTauMs = maxTauMs;
        this.maxQuoteAgeMs = maxQuoteAgeMs;
        this.cooldownPre60Ms = cooldownPre60Ms;
        this.cooldownMidLockMs = cooldownMidLockMs;
        this.cooldownLateLockMs = cooldownLateLockMs;
        this.spotLeadBeta = spotLeadBeta;
        this.sigmaFloorLockInUsd = sigmaFloorLockInUsd;
        this.minSigma1sBps = minSigma1sBps;
        this.ofiLogitWeight = ofiLogitWeight;
        this.syncLogitWeight = syncLogitWeight;
        this.toxicOfiVeto = toxicOfiVeto;
        this.minModelProb = minModelProb;
        this.minEntryAsk = minEntryAsk;
        this.maxEntryAskPre60 = maxEntryAskPre60;
        this.maxEntryAskLockIn = maxEntryAskLockIn;
        this.maxSpread = maxSpread;
        this.minDepthShares = minDepthShares;
        this.minLogitEdgePre60 = minLogitEdgePre60;
        this.minLogitEdgeLate = minLogitEdgeLate;
        this.minBankedCushionSigma = minBankedCushionSigma;
        this.minFreshUnrepricedEdge = minFreshUnrepricedEdge;
        this.minSpotImpulseSigma = minSpotImpulseSigma;
        this.takerFeeRate = takerFeeRate;
        this.epistemicVariance = epistemicVariance;
        this.maxRiskPerMarketUsd = maxRiskPerMarketUsd;
        this.kellyBankrollUsd = kellyBankrollUsd;
        this.minOrderShares = minOrderShares;
        this.maxOrderShares = maxOrderShares;
        this.maxClipsPerMarket = maxClipsPerMarket;
        this.defaultMarketId = (defaultMarketId != null && !defaultMarketId.isBlank()) ? defaultMarketId.trim() : "pm-btc-15m";
        this.upToken = (upToken != null && !upToken.isBlank()) ? upToken.trim() : "BTC-UP";
        this.downToken = (downToken != null && !downToken.isBlank()) ? downToken.trim() : "BTC-DOWN";
    }

    /**
     * Returns the calibrated Wave 2 Profile A default configuration.
     *
     * @return default {@link OraclePulseLockStrategyConfig}
     */
    public static OraclePulseLockStrategyConfig defaults() {
        return new OraclePulseLockStrategyConfig(
                "15m",
                2_000L,
                175_000L,
                15_000L,
                1_800L,
                600L,
                180L,
                0.84,
                0.55,
                0.32,
                0.30,
                0.15,
                -0.18,
                0.58,
                0.36,
                0.72,
                0.86,
                0.026,
                15,
                0.80,
                0.56,
                1.15,
                0.036,
                1.95,
                0.07,
                0.018,
                330.0,
                4_620.0,
                8,
                260,
                4,
                "pm-btc-15m",
                "BTC-UP",
                "BTC-DOWN"
        );
    }

    /**
     * Unwraps any CDI client proxy into a direct value instance for zero-overhead field access on the hot path.
     *
     * @param config injected configuration instance
     * @return direct {@link OraclePulseLockStrategyConfig} instance
     */
    public static OraclePulseLockStrategyConfig unwrap(OraclePulseLockStrategyConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        return config.getClass() == OraclePulseLockStrategyConfig.class
                ? config
                : new OraclePulseLockStrategyConfig(
                        config.timeframe().getCode(),
                        config.minTauMs(),
                        config.maxTauMs(),
                        config.maxQuoteAgeMs(),
                        config.cooldownPre60Ms(),
                        config.cooldownMidLockMs(),
                        config.cooldownLateLockMs(),
                        config.spotLeadBeta(),
                        config.sigmaFloorLockInUsd(),
                        config.minSigma1sBps(),
                        config.ofiLogitWeight(),
                        config.syncLogitWeight(),
                        config.toxicOfiVeto(),
                        config.minModelProb(),
                        config.minEntryAsk(),
                        config.maxEntryAskPre60(),
                        config.maxEntryAskLockIn(),
                        config.maxSpread(),
                        config.minDepthShares(),
                        config.minLogitEdgePre60(),
                        config.minLogitEdgeLate(),
                        config.minBankedCushionSigma(),
                        config.minFreshUnrepricedEdge(),
                        config.minSpotImpulseSigma(),
                        config.takerFeeRate(),
                        config.epistemicVariance(),
                        config.maxRiskPerMarketUsd(),
                        config.kellyBankrollUsd(),
                        config.minOrderShares(),
                        config.maxOrderShares(),
                        config.maxClipsPerMarket(),
                        config.defaultMarketId(),
                        config.upToken(),
                        config.downToken());
    }

    public Timeframe timeframe() { return timeframe; }
    public long minTauMs() { return minTauMs; }
    public long maxTauMs() { return maxTauMs; }
    public long maxQuoteAgeMs() { return maxQuoteAgeMs; }
    public long cooldownPre60Ms() { return cooldownPre60Ms; }
    public long cooldownMidLockMs() { return cooldownMidLockMs; }
    public long cooldownLateLockMs() { return cooldownLateLockMs; }
    public double spotLeadBeta() { return spotLeadBeta; }
    public double sigmaFloorLockInUsd() { return sigmaFloorLockInUsd; }
    public double minSigma1sBps() { return minSigma1sBps; }
    public double ofiLogitWeight() { return ofiLogitWeight; }
    public double syncLogitWeight() { return syncLogitWeight; }
    public double toxicOfiVeto() { return toxicOfiVeto; }
    public double minModelProb() { return minModelProb; }
    public double minEntryAsk() { return minEntryAsk; }
    public double maxEntryAskPre60() { return maxEntryAskPre60; }
    public double maxEntryAskLockIn() { return maxEntryAskLockIn; }
    public double maxSpread() { return maxSpread; }
    public int minDepthShares() { return minDepthShares; }
    public double minLogitEdgePre60() { return minLogitEdgePre60; }
    public double minLogitEdgeLate() { return minLogitEdgeLate; }
    public double minBankedCushionSigma() { return minBankedCushionSigma; }
    public double minFreshUnrepricedEdge() { return minFreshUnrepricedEdge; }
    public double minSpotImpulseSigma() { return minSpotImpulseSigma; }
    public double takerFeeRate() { return takerFeeRate; }
    public double epistemicVariance() { return epistemicVariance; }
    public double maxRiskPerMarketUsd() { return maxRiskPerMarketUsd; }
    public double kellyBankrollUsd() { return kellyBankrollUsd; }
    public int minOrderShares() { return minOrderShares; }
    public int maxOrderShares() { return maxOrderShares; }
    public int maxClipsPerMarket() { return maxClipsPerMarket; }
    public String defaultMarketId() { return defaultMarketId; }
    public String upToken() { return upToken; }
    public String downToken() { return downToken; }
}
