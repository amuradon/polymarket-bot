package cz.polymarket.bot.strategy;

import cz.polymarket.bot.calculator.KellyPositionSizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class NextCandleProbabilityModelTest {

    private NextCandleProbabilityModel model;
    private KellyPositionSizer sizer;

    @BeforeEach
    void setUp() {
        sizer = new KellyPositionSizer(10000.0, 0.25, 50.0, 300.0);
        model = new NextCandleProbabilityModel(sizer);
    }

    @Test
    @DisplayName("Should generate UP signal when strong bullish mean-reversion, futures flow, and basis are aligned")
    void shouldGenerateUpSignalWhenBullish() {
        // zVwap < -1.2 (+0.6 score), futDelta = +8 (+0.4 score), futH1Delta = +20 (+0.3 score), basis = +2.0 (+0.35 score)
        // total score = 0.6 + 0.4 + 0.3 + 0.35 = 1.65
        // sigmoid(0.7 * 1.65 = 1.155) = 1 / (1 + exp(-1.155)) = 0.7604
        // Market price UP = 0.50 => edge = 0.7604 - 0.50 = 0.2604 >= 0.06
        // Min prob = 0.63 => condition met

        StrategySignal signal = model.evaluate(
                -1.5, // zVwap
                2.0,  // spotDelta
                8.0,  // futDelta
                20.0, // futH1Delta
                2.0,  // basisOpenBps
                0.0,  // distTwap
                0.50, // marketPriceUp
                0.50, // marketPriceDown
                0.50, // execPriceUp
                0.50, // execPriceDown
                false // isLowVolatility
        );

        assertThat(signal.direction()).isEqualTo(TradeDirection.UP);
        assertThat(signal.modelProbability()).isCloseTo(0.7604, within(1e-3));
        assertThat(signal.marketPrice()).isEqualTo(0.50);
        assertThat(signal.edge()).isCloseTo(0.2604, within(1e-3));
        assertThat(signal.suggestedSizeUsd()).isBetween(50.0, 300.0);
    }

    @Test
    @DisplayName("Should trigger Late Oracle Arb with 0.94 probability when distance to TWAP open > 80 USD")
    void shouldTriggerLateOracleArbUp() {
        StrategySignal signal = model.evaluate(
                0.0,  // zVwap
                0.0,  // spotDelta
                0.0,  // futDelta
                0.0,  // futH1Delta
                0.0,  // basisOpenBps
                95.0, // distTwap > 80.0
                0.55, // marketPriceUp
                0.45, // marketPriceDown
                0.55, // execPriceUp
                0.45, // execPriceDown
                false
        );

        assertThat(signal.direction()).isEqualTo(TradeDirection.UP);
        assertThat(signal.modelProbability()).isEqualTo(0.94);
        assertThat(signal.edge()).isCloseTo(0.94 - 0.55, within(1e-6));
        assertThat(signal.suggestedSizeUsd()).isEqualTo(300.0);
    }

    @Test
    @DisplayName("Should trigger Late Oracle Arb DOWN when distance to TWAP open < -80 USD")
    void shouldTriggerLateOracleArbDown() {
        StrategySignal signal = model.evaluate(
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                -110.0, // distTwap < -80.0
                0.40,
                0.60,
                0.40,
                0.60,
                false
        );

        assertThat(signal.direction()).isEqualTo(TradeDirection.DOWN);
        assertThat(signal.modelProbability()).isEqualTo(0.94);
        assertThat(signal.marketPrice()).isEqualTo(0.60);
        assertThat(signal.edge()).isCloseTo(0.94 - 0.60, within(1e-6));
    }

    @Test
    @DisplayName("Should reject trade when slippage exceeds slippage cap of 0.005 USD")
    void shouldRejectWhenSlippageExceedsCap() {
        // Qualified UP signal, but executed price slipped by 0.006 (> 0.005 cap)
        StrategySignal signal = model.evaluate(
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                100.0, // distTwap
                0.50,  // marketPriceUp
                0.50,
                0.506, // execPriceUp slipped by 0.006!
                0.50,
                false
        );

        assertThat(signal.direction()).isEqualTo(TradeDirection.NO_TRADE);
        assertThat(signal.reason()).containsIgnoringCase("slippage");
    }

    @Test
    @DisplayName("Should skip trade when market is in low-volatility regime")
    void shouldSkipTradeInLowVolatility() {
        StrategySignal signal = model.evaluate(
                -2.0,
                5.0,
                10.0,
                20.0,
                3.0,
                0.0,
                0.50,
                0.50,
                0.50,
                0.50,
                true // isLowVolatility
        );

        assertThat(signal.direction()).isEqualTo(TradeDirection.NO_TRADE);
        assertThat(signal.reason()).containsIgnoringCase("volatility");
    }

    @Test
    @DisplayName("Should return NO_TRADE when edge is below minimum edge threshold (0.06)")
    void shouldReturnNoTradeWhenEdgeIsInsufficient() {
        // High probability, but market price is also high => small edge < 0.06
        StrategySignal signal = model.evaluate(
                -1.5,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.65, // marketPriceUp is 0.65, model P ~ 0.68 => edge 0.03 < 0.06
                0.35,
                0.65,
                0.35,
                false
        );

        assertThat(signal.direction()).isEqualTo(TradeDirection.NO_TRADE);
    }
}
