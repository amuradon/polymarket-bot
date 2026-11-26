package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.OrderBookQuote;
import cz.polymarket.bot.domain.Timeframe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Deterministic synthetic unit tests for {@link OraclePulseLockStrategy} and {@link OraclePulseLockStrategyConfig}.
 */
class OraclePulseLockStrategyTest {

    private static final long CANDLE_START_MS = 1_785_542_400_000L;
    private static final long CANDLE_END_MS = CANDLE_START_MS + 900_000L;

    private OraclePulseLockStrategy strategy;
    private List<OrderCommand> submittedOrders;

    @BeforeEach
    void setUp() {
        strategy = new OraclePulseLockStrategy(OraclePulseLockStrategyConfig.defaults());
        submittedOrders = new ArrayList<>();
        strategy.init(() -> new ExecutionRouter() {
            @Override
            public void submitOrder(OrderCommand command) {
                submittedOrders.add(command);
            }

            @Override
            public void cancelOrder(String clientOrderId) {
            }
        });
    }

    @Test
    @DisplayName("Should validate config invariants, expose metadata, and isolate parallel instances")
    void shouldValidateConfigAndCreateIsolatedInstances() {
        assertThat(strategy.getName()).isEqualTo("OraclePulseLockStrategy");
        assertThat(strategy.getTimeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(strategy.getRequiredClobLookbackMs()).isEqualTo(190_000L);
        assertThat(strategy.createInstance()).isNotSameAs(strategy);

        assertThatThrownBy(() -> OraclePulseLockStrategyConfig.unwrap(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> strategy.init(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should reject static quotes without fresh impulse and fire FAK BUY on unrepriced Direct-USD surge")
    void shouldRejectStaticQuotesAndFireOnUnrepricedDirectUsdSurge() {
        // Seed opening 60s TWAP target at 70,000.00
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 70_000.0, 60, CANDLE_START_MS);

        // Warm up calm 2s volatility around 70,000 up to tau = 100s
        long tBase = CANDLE_END_MS - 105_000L;
        for (int i = 0; i < 10; i++) {
            long ts = tBase + i * 500L;
            double px = 70_018.0 + (i % 2 == 0 ? 0.5 : -0.5);
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", px, ts - 1_000L);
            strategy.onPolymarketCryptoTwap60s(ts, "btcusd", "chainlink", px, 60, ts - 500L);
            strategy.onFastSpotMicro(ts, "btcusd", px, 0.05, 0.0);
            strategy.onOrderBookQuote(new OrderBookQuote(0.50, 0.49, 0.51, 0.50, 200.0, 200.0, ts));
        }

        // Static quote with zero fresh spot impulse must be rejected (Glosten-Milgrom protection)
        assertThat(submittedOrders).isEmpty();

        // Now trigger a fresh 50ms Direct-USD surge (+40 USD) with strong OFI (+0.85) & 3-venue sync (+20 USD)
        // while Polymarket CLOB Best Ask remains stale at 0.51!
        long surgeTs = tBase + 5_200L;
        strategy.onFastSpotMicro(surgeTs, "btcusd", 70_058.0, 0.85, 20.0);
        assertThat(submittedOrders).hasSize(1);

        OrderCommand buyCmd = submittedOrders.getFirst();
        assertThat(buyCmd.side()).isEqualTo("BUY");
        assertThat(buyCmd.token()).isEqualTo("BTC-UP");
        assertThat(buyCmd.price()).isEqualByComparingTo(new BigDecimal("0.510"));
        assertThat(buyCmd.size().intValue()).isBetween(8, 260);

        // While order is in flight, duplicate triggers must not submit another order
        strategy.onFastSpotMicro(surgeTs + 300L, "btcusd", 70_065.0, 0.90, 22.0);
        assertThat(submittedOrders).hasSize(1);

        // Deliver fill report -> locks primary side to UP; opposing DOWN surge must be ignored (One-Side Hold-to-Settlement)
        strategy.onExecutionReport(new ExecutionReport(
                buyCmd.clientOrderId(), "ex-1", "FILLED",
                new BigDecimal("0.50"), buyCmd.size(), surgeTs + 15L, "Filled"));

        long reverseTs = surgeTs + 2_500L;
        strategy.onOrderBookQuote(new OrderBookQuote(0.65, 0.64, 0.48, 0.47, 200.0, 200.0, reverseTs));
        strategy.onFastSpotMicro(reverseTs + 100L, "btcusd", 69_940.0, -0.90, -25.0);
        assertThat(submittedOrders).hasSize(1);
    }

    @Test
    @DisplayName("Should execute Banked TWAP Cushion BUY inside final 45s even during a mild spot pullback")
    void shouldExecuteBankedTwapCushionBuyDuringMildPullback() {
        strategy.onPolymarketCryptoTwap60s(CANDLE_START_MS, "btcusd", "chainlink", 70_000.0, 60, CANDLE_START_MS);

        // Build calm pre-window baseline, then strong +36 USD banked TWAP from tau=60s down to tau=20s
        long winStart = CANDLE_END_MS - 60_000L;
        for (int s = -15; s <= 40; s++) {
            long ts = winStart + s * 1_000L;
            double px = s < 0 ? 70_002.0 : 70_036.0;
            double twap = s <= 0 ? 70_002.0 : (70_002.0 + 34.0 * (s / 60.0));
            strategy.onPolymarketCryptoPrice(ts, "btcusd", "chainlink", px, ts - 500L);
            strategy.onPolymarketCryptoTwap60s(ts, "btcusd", "chainlink", twap, 60, ts - 500L);
            strategy.onFastSpotMicro(ts, "btcusd", px, 0.10, 0.0);
        }

        // At tau = 19s, spot pulls back mildly to 70,031 (negative 1.5s impulse, zero fresh surge!),
        // and spot-watching traders offer UP at 0.78 while banked TWAP cushion is already locked!
        long tau19 = CANDLE_END_MS - 19_000L;
        strategy.onOrderBookQuote(new OrderBookQuote(0.78, 0.77, 0.23, 0.22, 180.0, 180.0, tau19 - 500L));
        strategy.onFastSpotMicro(tau19, "btcusd", 70_031.0, 0.05, 0.0);

        assertThat(submittedOrders).hasSize(1);
        assertThat(submittedOrders.getFirst().token()).isEqualTo("BTC-UP");
        assertThat(submittedOrders.getFirst().side()).isEqualTo("BUY");
    }
}
