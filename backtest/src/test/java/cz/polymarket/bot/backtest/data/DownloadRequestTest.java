package cz.polymarket.bot.backtest.data;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DownloadRequestTest {

    @Test
    void shouldCreateValidRequestWithDefaults() {
        DownloadRequest request = DownloadRequest.of("btcusdt", "2026-08-01", "2026-08-05", null);

        assertThat(request.symbol()).isEqualTo("BTCUSDT");
        assertThat(request.startDate()).isEqualTo(LocalDate.of(2026, 8, 1));
        assertThat(request.endDate()).isEqualTo(LocalDate.of(2026, 8, 5));
        assertThat(request.dataTypes()).containsExactlyInAnyOrder(
                DataType.SPOT_TRADES,
                DataType.FUTURES_TRADES,
                DataType.ORDER_BOOK
        );
    }

    @Test
    void shouldAllowCustomDataTypes() {
        DownloadRequest request = DownloadRequest.of(
                "ETHUSDT",
                "2026-08-01",
                "2026-08-01",
                Set.of(DataType.SPOT_TRADES)
        );

        assertThat(request.dataTypes()).containsExactly(DataType.SPOT_TRADES);
    }

    @Test
    void shouldRejectStartAfterEnd() {
        assertThatThrownBy(() -> DownloadRequest.of("BTCUSDT", "2026-08-05", "2026-08-01", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("start date must not be after end date");
    }

    @Test
    void shouldRejectBlankSymbol() {
        assertThatThrownBy(() -> DownloadRequest.of("   ", "2026-08-01", "2026-08-05", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("symbol must not be blank");
    }

    @Test
    void shouldRejectInvalidDateFormat() {
        assertThatThrownBy(() -> DownloadRequest.of("BTCUSDT", "invalid-date", "2026-08-05", null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
