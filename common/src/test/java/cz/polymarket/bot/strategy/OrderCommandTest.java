package cz.polymarket.bot.strategy;

import cz.polymarket.bot.domain.Timeframe;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderCommandTest {

    @Test
    void shouldCreateValidOrderCommand() {
        OrderCommand cmd = new OrderCommand(
                "order-1",
                "market-btc-15m",
                "BTC",
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                new BigDecimal("0.55"),
                new BigDecimal("100.0")
        );

        assertThat(cmd.clientOrderId()).isEqualTo("order-1");
        assertThat(cmd.marketId()).isEqualTo("market-btc-15m");
        assertThat(cmd.token()).isEqualTo("BTC");
        assertThat(cmd.timeframe()).isEqualTo(Timeframe.FIFTEEN_MINUTES);
        assertThat(cmd.side()).isEqualTo("BUY");
        assertThat(cmd.price()).isEqualByComparingTo("0.55");
        assertThat(cmd.size()).isEqualByComparingTo("100.0");
    }

    @Test
    void shouldRejectInvalidOrderCommand() {
        assertThatThrownBy(() -> new OrderCommand(
                "",
                "market-btc-15m",
                "BTC",
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                new BigDecimal("0.55"),
                new BigDecimal("100.0")
        )).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new OrderCommand(
                "order-1",
                "market-btc-15m",
                "BTC",
                Timeframe.FIFTEEN_MINUTES,
                "BUY",
                BigDecimal.ZERO,
                new BigDecimal("100.0")
        )).isInstanceOf(IllegalArgumentException.class);
    }
}
