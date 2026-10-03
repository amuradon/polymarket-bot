package cz.polymarket.bot.backtest.data;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetDatasetLoaderTest {

    private ParquetDatasetLoader loader;

    @BeforeEach
    void setUp() {
        loader = new ParquetDatasetLoader();
    }

    @Test
    @DisplayName("Should load 5100 market rows from unified_market_data.parquet")
    void shouldLoadMarketRowsFromParquet() {
        Path datasetPath = Path.of("D:/Polymarket/btc_nextCandle/unified_market_data.parquet");
        if (!Files.exists(datasetPath)) {
            // Skip test if environment doesn't have the dataset file
            return;
        }

        List<BacktestMarketRow> rows = loader.loadDataset(datasetPath.toString());

        assertThat(rows).isNotNull();
        assertThat(rows).hasSize(5100);

        BacktestMarketRow firstRow = rows.get(0);
        assertThat(firstRow.tStart()).isEqualTo(1786060800L); // 2026-08-07 00:00:00 UTC
        assertThat(firstRow.sOpen()).isPositive();
        assertThat(firstRow.actualOutcome()).isIn("UP", "DOWN");

        // Verify conversion to MarketCandle and OrderBookQuote
        assertThat(firstRow.toMarketCandle()).isNotNull();
        assertThat(firstRow.toOrderBookQuote(60)).isNotNull();
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when file path is invalid")
    void shouldThrowWhenFileNotFound() {
        assertThatThrownBy(() -> loader.loadDataset("D:/NonExistentPath/non_existent.parquet"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
