package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Deterministic synthetic unit tests for {@link RoundMainstreamStrategy} and {@link RoundMainstreamStrategyConfig}.
 */
class RoundMainstreamStrategyTest {

    private static final long CANDLE_START_SEC = 1_794_131_100L;
    private static final long CANDLE_END_SEC = CANDLE_START_SEC + 900L;
    private static final long CANDLE_START_MS = CANDLE_START_SEC * 1_000L;
    private static final long CANDLE_END_MS = CANDLE_END_SEC * 1_000L;

    private RoundMainstreamStrategyConfig config;
    private RoundMainstreamStrategy strategy;
    private List<OrderCommand> submittedOrders;
    private int balanceRefreshCount;
    private StrategyContext testContext;

    @BeforeEach
    void setUp() {
        config = RoundMainstreamStrategyConfig.defaultModelV1();
        strategy = new RoundMainstreamStrategy(config);
        submittedOrders = new ArrayList<>();
        balanceRefreshCount = 0;

        ExecutionRouter router = new ExecutionRouter() {
            @Override
            public void submitOrder(OrderCommand command) {
                submittedOrders.add(command);
            }

            @Override
            public void cancelOrder(String clientOrderId) {
            }
        };

        testContext = new StrategyContext() {
            @Override
            public ExecutionRouter getExecutionRouter() {
                return router;
            }

            @Override
            public void refreshAccountBalance() {
                balanceRefreshCount++;
            }
        };

        strategy.init(testContext);
    }

    @Test
    @DisplayName("Should validate immutable defaults and enforce fail-fast configuration invariants")
    void shouldValidateConfigDefaultsAndFailFastInvariants() {
        assertThat(strategy.getConfig()).isSameAs(config);
        assertThat(config.minOrderShares()).isEqualTo(5);
        assertThat(config.maxOrderShares()).isGreaterThanOrEqualTo(config.minOrderShares());
        assertThat(config.minTauMs()).isGreaterThan(0L);
        assertThat(config.maxTauMs()).isGreaterThan(config.minTauMs());
        assertThat(config.defaultMarketId()).isEqualTo("pm-btc-15m");
        assertThat(config.upToken()).isEqualTo("BTC-UP");
        assertThat(config.downToken()).isEqualTo("BTC-DOWN");

        assertThatThrownBy(() -> new RoundMainstreamStrategy(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strategy.init(null))
                .isInstanceOf(IllegalArgumentException.class);

        // Invalid tau window (minTauMs >= maxTauMs)
        assertThatThrownBy(() -> new RoundMainstreamStrategyConfig(
                500_000L, 100_000L, 3_000L, 2_500L,
                8, 15, 26, 10, 80, 42, 90,
                4_500L, 30, 20, 20, 2_100, 1_800, 4_200,
                220, 880, 60, 320, 840, 1_080, 20, 700,
                5, 650, 180, 4_200, 1_800,
                "pm-btc-15m", "BTC-UP", "BTC-DOWN"))
                .isInstanceOf(IllegalArgumentException.class);

        // Invalid order shares (< 5 Polymarket minimum)
        assertThatThrownBy(() -> new RoundMainstreamStrategyConfig(
                15_000L, 875_000L, 3_000L, 2_500L,
                8, 15, 26, 10, 80, 42, 90,
                4_500L, 30, 20, 20, 2_100, 1_800, 4_200,
                220, 880, 60, 320, 840, 1_080, 20, 700,
                4, 650, 180, 4_200, 1_800,
                "pm-btc-15m", "BTC-UP", "BTC-DOWN"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should unwrap CDI config proxy, expose bounded CLOB lookback, and isolate worker instances")
    void shouldUnwrapCdiConfigProxyAndCreateIsolatedWorkerInstances() {
        class ProxiedConfig extends RoundMainstreamStrategyConfig {
            ProxiedConfig(RoundMainstreamStrategyConfig delegate) {
                super(
                        delegate.minTauMs(),
                        delegate.maxTauMs(),
                        delegate.sameSideCooldownMs(),
                        delegate.oppositeSideCooldownMs(),
                        delegate.maxRange5mBps(),
                        delegate.maxCandleRangeBps(),
                        delegate.maxSigma1sCentiBps(),
                        delegate.maxTargetDistanceBps(),
                        delegate.maxAdverseSpotMoveCentiBps(),
                        delegate.minCompressionEdgeMc(),
                        delegate.minWarmupSamples(),
                        delegate.sweepWindowMs(),
                        delegate.minEntryAskAdvanceMc(),
                        delegate.minEntryBidAdvanceMc(),
                        delegate.maxSpreadMc(),
                        delegate.minEffectiveSweepShares(),
                        delegate.minBidWallShares(),
                        delegate.minWallDepletionShares(),
                        delegate.minOpenPriceMc(),
                        delegate.maxOpenPriceMc(),
                        delegate.minOffsetTpPriceMc(),
                        delegate.maxOffsetTpPriceMc(),
                        delegate.maxOffsetTpPairCostMc(),
                        delegate.minOffsetSlPairCostMc(),
                        delegate.maxSlippageMc(),
                        delegate.takerFeeRateBps(),
                        delegate.minOrderShares(),
                        delegate.maxOrderShares(),
                        delegate.sweepShareScaleBps(),
                        delegate.convexWhaleSweepThresholdShares(),
                        delegate.maxInventorySharesPerSide(),
                        delegate.defaultMarketId(),
                        delegate.upToken(),
                        delegate.downToken()
                );
            }
        }

        RoundMainstreamStrategy proxiedStrategy = new RoundMainstreamStrategy(new ProxiedConfig(config));
        assertThat(proxiedStrategy.getConfig().getClass()).isEqualTo(RoundMainstreamStrategyConfig.class);
        assertThat(proxiedStrategy.getName()).isEqualTo("RoundMainstreamStrategy");
        assertThat(proxiedStrategy.getTimeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(proxiedStrategy.getRequiredClobLookbackMs()).isBetween(config.maxTauMs(), 899_000L);

        TradingStrategy workerCopy = proxiedStrategy.createInstance();
        assertThat(workerCopy).isNotSameAs(proxiedStrategy);
        assertThat(workerCopy).isInstanceOf(RoundMainstreamStrategy.class);
    }

    @Test
    @DisplayName("Should skip market when target TWAP is missing or underlying volatility exceeds compression regime")
    void shouldSkipMarketWhenTargetPriceMissingOrVolatilityExceedsCompressionRegime() {
        long t0 = CANDLE_START_MS + 180_000L;
        // 1. Missing target price: feed calm spot and a strong UP sweep -> must warn once and emit zero orders
        feedCalmUnderlying(CANDLE_START_MS + 1_000L, t0, 90_025.00, "btcusd");
        feedUpWhaleSweep(t0, 0.52, 0.57, 2_400.0, 2_800.0);

        assertThat(strategy.isMissingTargetPriceWarned()).isTrue();
        assertThat(submittedOrders).isEmpty();

        // 2. High-volatility candle (BTC swings $320 = ~35.5 bps on $90k): must block entry despite whale sweep
        strategy.reset();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        for (int s = 1; s <= 100; s++) {
            long ts = CANDLE_START_MS + s * 1_000L;
            double volatilePrice = 90_000.00 + ((s % 2 == 0) ? 160.00 : -160.00);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", volatilePrice, ts);
        }
        long tHighVol = CANDLE_START_MS + 105_000L;
        feedUpWhaleSweep(tHighVol, 0.52, 0.58, 2_600.0, 3_000.0);
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should ignore HFT depth flicker, MM symmetric re-quote without bid wall, and one-sided ask vacuum")
    void shouldIgnoreHftQuoteFlickerAndAskVacuumWithoutBidAdvance() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long t0 = CANDLE_START_MS + 180_000L;
        feedCalmUnderlying(CANDLE_START_MS + 1_000L, t0, 90_022.00, "btcusd");

        // 1. Same-tick HFT cancel/replace flicker: 2800 -> 800 -> 2800 -> 850 -> 2800
        strategy.onOrderBookQuote(new OrderBookQuote(0.54, 0.53, 0.47, 0.46, 2_800.0, 400.0, 0.54, 0.47, t0));
        strategy.onOrderBookQuote(new OrderBookQuote(0.54, 0.53, 0.47, 0.46, 800.0, 400.0, 0.54, 0.47, t0 + 80L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.54, 0.53, 0.47, 0.46, 2_800.0, 400.0, 0.54, 0.47, t0 + 160L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.54, 0.53, 0.47, 0.46, 850.0, 400.0, 0.54, 0.47, t0 + 240L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.54, 0.53, 0.47, 0.46, 2_800.0, 400.0, 0.54, 0.47, t0 + 320L));
        assertThat(submittedOrders).isEmpty();

        // 2. Routine MM quote shift (2,200 sh ask thinned to 400 sh, but bidWall = depthDown is only 500 < minBidWallShares)
        long t1 = t0 + 2_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(0.52, 0.51, 0.49, 0.48, 2_200.0, 500.0, 0.52, 0.49, t1));
        strategy.onOrderBookQuote(new OrderBookQuote(0.55, 0.54, 0.46, 0.45, 400.0, 500.0, 0.55, 0.46, t1 + 300L));
        assertThat(submittedOrders).isEmpty();

        // 3. One-sided MM ask pull vacuum (ask jumps 0.54 -> 0.64 while bid stays at 0.53 -> wide 110 mc spread)
        long t2 = t1 + 2_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(0.54, 0.53, 0.47, 0.46, 2_400.0, 2_500.0, 0.54, 0.47, t2));
        strategy.onOrderBookQuote(new OrderBookQuote(0.64, 0.53, 0.47, 0.36, 200.0, 2_500.0, 0.64, 0.47, t2 + 300L));
        assertThat(submittedOrders).isEmpty();
    }

    @Test
    @DisplayName("Should execute immediately without artificial delay on confirmed whale sweep across ETH and BTC scales")
    void shouldExecuteImmediatelyWithoutBlockchainDelayOnPriceAdvancingWhaleSweep() {
        // Verify asset-agnostic bps behavior on ETH ($3,000.00 target, calm ETH +$1.20 = +4.0 bps compression advantage)
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "ethusd", "chainlink", 3_000.00, 60, CANDLE_START_MS);
        long tSweep = CANDLE_START_MS + 660_000L; // tau = 240s
        feedCalmUnderlying(CANDLE_START_MS + 540_000L, tSweep, 3_001.20, "ethusd");

        // Whale sweeps UP book from 0.51/0.50 (1,600 sh) -> 0.54/0.53 (900 sh) -> 0.56/0.55 (220 sh) with 2,400 sh mirrored bid wall
        strategy.onOrderBookQuote(new OrderBookQuote(0.51, 0.50, 0.50, 0.49, 1_600.0, 700.0, 0.51, 0.50, tSweep - 800L));
        assertThat(submittedOrders).isEmpty();

        strategy.onOrderBookQuote(new OrderBookQuote(0.54, 0.53, 0.47, 0.46, 900.0, 1_900.0, 0.54, 0.47, tSweep - 300L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.56, 0.55, 0.45, 0.44, 220.0, 2_400.0, 0.56, 0.45, tSweep));

        // Immediate execution on signal tick (zero artificial blockchain reaction delay!)
        assertThat(submittedOrders).hasSize(1);
        OrderCommand firstOrder = submittedOrders.getFirst();
        assertThat(firstOrder.token()).isEqualTo("BTC-UP");
        assertThat(firstOrder.side()).isEqualTo("BUY");
        assertThat(firstOrder.timeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(firstOrder.size().intValue()).isBetween(config.minOrderShares(), config.maxOrderShares());
        assertThat(balanceRefreshCount).isEqualTo(1);

        // Fill the order and verify sameSideCooldownMs prevents sub-second spam
        strategy.onExecutionReport(new ExecutionReport(
                firstOrder.clientOrderId(), "fill-1", "FILLED",
                BigDecimal.valueOf(0.56), firstOrder.size(), tSweep + 20L, "Filled"
        ));
        strategy.onOrderBookQuote(new OrderBookQuote(0.58, 0.57, 0.43, 0.42, 200.0, 2_500.0, 0.58, 0.43, tSweep + 400L));
        assertThat(submittedOrders).hasSize(1);

        // After sameSideCooldownMs elapses, continued sweep scales ADD_SAME_DIR
        long tScale = tSweep + config.sameSideCooldownMs() + 200L;
        strategy.onPolymarketCryptoPrice(tScale - 100L, "ethusd", "chainlink", 3_001.30, tScale - 100L);
        strategy.onOrderBookQuote(new OrderBookQuote(0.58, 0.57, 0.43, 0.42, 2_100.0, 1_500.0, 0.58, 0.43, tScale - 500L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.62, 0.61, 0.39, 0.38, 220.0, 2_600.0, 0.62, 0.39, tScale));
        assertThat(submittedOrders).hasSize(2);
        assertThat(submittedOrders.get(1).token()).isEqualTo("BTC-UP");
    }

    @Test
    @DisplayName("Should detect persistent ask-wall depletion at constant BBO and scale clip convexly with whale size")
    void shouldDetectPersistentAskWallDepletionAndScaleConvexlyForWhaleSyndicate() {
        // 1. Threshold whale sweep (~2,300 sh -> small clip ~8-15 sh)
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tMod = CANDLE_START_MS + 660_000L;
        feedCalmUnderlying(CANDLE_START_MS + 540_000L, tMod, 90_028.00, "btcusd");
        feedUpWhaleSweep(tMod, 0.52, 0.55, 1_900.0, 2_100.0);
        assertThat(submittedOrders).hasSize(1);
        int moderateShares = submittedOrders.getFirst().size().intValue();
        assertThat(moderateShares).isBetween(5, 35);

        // 2. Massive ask-wall iceberg depletion (~6,200 sh eaten at constant 0.60/0.59 BBO -> convex whale clip > 100 sh)
        strategy.reset();
        submittedOrders.clear();
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tWall = CANDLE_START_MS + 660_000L;
        feedCalmUnderlying(CANDLE_START_MS + 540_000L, tWall, 90_028.00, "btcusd");

        strategy.onOrderBookQuote(new OrderBookQuote(0.60, 0.59, 0.41, 0.40, 8_000.0, 2_200.0, 0.60, 0.41, tWall - 1_800L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.60, 0.59, 0.41, 0.40, 5_800.0, 2_300.0, 0.60, 0.41, tWall - 1_200L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.60, 0.59, 0.41, 0.40, 3_700.0, 2_400.0, 0.60, 0.41, tWall - 600L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.60, 0.59, 0.41, 0.40, 1_800.0, 2_600.0, 0.60, 0.41, tWall));

        assertThat(submittedOrders).isNotEmpty();
        int whaleWallShares = submittedOrders.getLast().size().intValue();
        assertThat(whaleWallShares).isGreaterThan(moderateShares * 3);
    }

    @Test
    @DisplayName("Should execute synthetic OFFSET_TP by buying cheap opposite token when whale bounce locks profit")
    void shouldExecuteSyntheticOffsetTakeProfitViaOppositeTokenBuy() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tOpen = CANDLE_START_MS + 600_000L;
        feedCalmUnderlying(CANDLE_START_MS + 480_000L, tOpen, 90_030.00, "btcusd");
        feedUpWhaleSweep(tOpen, 0.50, 0.54, 2_200.0, 2_400.0);
        assertThat(submittedOrders).hasSize(1);

        OrderCommand openCmd = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                openCmd.clientOrderId(), "fill-open", "FILLED",
                BigDecimal.valueOf(0.54), BigDecimal.valueOf(120), tOpen + 20L, "Filled"
        ));
        assertThat(strategy.getNetSharesUp()).isEqualTo(120);

        // Later at tau = 180s, UP rallies to 0.80 so DOWN is cheap at 0.22 (pairCost = 540 + 220 = 760 mc <= maxOffsetTpPairCostMc)
        // and DOWN gets swept 0.19 -> 0.22 with 1,200 sh mirrored DOWN bid wall (depthUp = 1,200)
        long tTp = CANDLE_END_MS - 180_000L;
        strategy.onPolymarketCryptoPrice(tTp - 1_000L, "btcusd", "chainlink", 90_035.00, tTp - 1_000L);
        strategy.onPolymarketCryptoPrice(tTp, "btcusd", "chainlink", 90_034.80, tTp);
        strategy.onOrderBookQuote(new OrderBookQuote(0.82, 0.81, 0.19, 0.18, 900.0, 900.0, 0.82, 0.19, tTp - 600L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.79, 0.78, 0.22, 0.21, 1_200.0, 200.0, 0.79, 0.22, tTp));

        assertThat(submittedOrders).hasSize(2);
        OrderCommand tpCmd = submittedOrders.get(1);
        assertThat(tpCmd.token()).isEqualTo("BTC-DOWN");
        assertThat(tpCmd.side()).isEqualTo("BUY");
        assertThat(tpCmd.size().intValue()).isBetween(config.minOrderShares(), 120);
    }

    @Test
    @DisplayName("Should execute single-cycle synthetic OFFSET_SL via opposite token BUY without ping-pong churn")
    void shouldExecuteSyntheticOffsetStopLossWithoutPingPongChurn() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 90_000.00, 60, CANDLE_START_MS);
        long tOpen = CANDLE_START_MS + 600_000L;
        feedCalmUnderlying(CANDLE_START_MS + 480_000L, tOpen, 90_028.00, "btcusd");
        feedUpWhaleSweep(tOpen, 0.51, 0.55, 2_200.0, 2_400.0);
        assertThat(submittedOrders).hasSize(1);

        OrderCommand openCmd = submittedOrders.getFirst();
        strategy.onExecutionReport(new ExecutionReport(
                openCmd.clientOrderId(), "fill-open-up", "FILLED",
                BigDecimal.valueOf(0.55), BigDecimal.valueOf(80), tOpen + 25L, "Filled"
        ));
        assertThat(strategy.getNetSharesUp()).isEqualTo(80);

        // Underlying reverses below target ($89,970) and opposing whale sweeps DOWN from 0.46/0.45 -> 0.59/0.58
        long tSl = tOpen + 25_000L;
        strategy.onPolymarketCryptoPrice(tSl - 2_000L, "btcusd", "chainlink", 89_970.00, tSl - 2_000L);
        strategy.onPolymarketCryptoPrice(tSl - 1_000L, "btcusd", "chainlink", 89_969.80, tSl - 1_000L);
        strategy.onPolymarketCryptoPrice(tSl, "btcusd", "chainlink", 89_969.60, tSl);
        strategy.onOrderBookQuote(new OrderBookQuote(0.55, 0.54, 0.46, 0.45, 900.0, 2_200.0, 0.55, 0.46, tSl - 900L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.46, 0.45, 0.55, 0.54, 1_900.0, 900.0, 0.46, 0.55, tSl - 400L));
        strategy.onOrderBookQuote(new OrderBookQuote(0.42, 0.41, 0.59, 0.58, 2_500.0, 220.0, 0.42, 0.59, tSl));

        assertThat(submittedOrders).hasSize(2);
        OrderCommand slCmd = submittedOrders.get(1);
        assertThat(slCmd.token()).isEqualTo("BTC-DOWN");
        assertThat(slCmd.side()).isEqualTo("BUY");
        strategy.onExecutionReport(new ExecutionReport(
                slCmd.clientOrderId(), "fill-sl-down", "FILLED",
                BigDecimal.valueOf(0.59), slCmd.size(), tSl + 20L, "Filled"
        ));

        // Subsequent whipsaw back UP must NOT trigger endless ping-pong churn
        long tWhipsaw = tSl + 15_000L;
        strategy.onPolymarketCryptoPrice(tWhipsaw, "btcusd", "chainlink", 90_028.00, tWhipsaw);
        feedUpWhaleSweep(tWhipsaw, 0.50, 0.58, 2_400.0, 2_800.0);
        assertThat(submittedOrders).hasSize(2);
    }

    @Test
    @DisplayName("Should resolve RoundMainstreamStrategy in StrategyRegistry by exact and normalized names")
    void shouldResolveInStrategyRegistryByExactAndNormalizedNames() {
        StrategyRegistry registry = new StrategyRegistry(Map.of(strategy.getName(), strategy));

        assertThat(registry.getStrategy("RoundMainstreamStrategy")).isSameAs(strategy);
        assertThat(registry.getStrategy("round-mainstream")).isSameAs(strategy);
        assertThat(registry.getStrategy("round_mainstream")).isSameAs(strategy);
        assertThat(registry.getStrategy("RoundMainstream")).isSameAs(strategy);
    }

    private void feedCalmUnderlying(long startMs, long endMs, double basePrice, String symbol) {
        double tickStep = basePrice * 0.000005; // 0.05 bps realistic quiet oscillation
        int i = 0;
        for (long ts = startMs; ts <= endMs; ts += 1_000L) {
            double p = basePrice + ((i++ % 2 == 0) ? tickStep : -tickStep);
            strategy.onSpotTwap(ts - 100L, symbol, p, basePrice);
            strategy.onPolymarketCryptoPrice(ts, symbol, "chainlink", p, ts);
        }
    }

    private void feedUpWhaleSweep(
            long endTsMs,
            double startAsk,
            double endAsk,
            double startAskDepthShares,
            double endMirroredBidWallShares) {
        double startBid = Math.round((startAsk - 0.01) * 100.0) / 100.0;
        double midAsk = Math.round(((startAsk + endAsk) * 0.5) * 100.0) / 100.0;
        double midBid = Math.round((midAsk - 0.01) * 100.0) / 100.0;
        double endBid = Math.round((endAsk - 0.01) * 100.0) / 100.0;

        strategy.onOrderBookQuote(new OrderBookQuote(
                startAsk, startBid, 1.0 - startBid, 1.0 - startAsk,
                startAskDepthShares, 600.0, startAsk, 1.0 - startBid, endTsMs - 800L
        ));
        strategy.onOrderBookQuote(new OrderBookQuote(
                midAsk, midBid, 1.0 - midBid, 1.0 - midAsk,
                startAskDepthShares * 0.5, endMirroredBidWallShares * 0.8, midAsk, 1.0 - midBid, endTsMs - 350L
        ));
        strategy.onOrderBookQuote(new OrderBookQuote(
                endAsk, endBid, 1.0 - endBid, 1.0 - endAsk,
                220.0, endMirroredBidWallShares, endAsk, 1.0 - endBid, endTsMs
        ));
    }
}
