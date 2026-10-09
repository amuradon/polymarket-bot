package cz.polymarket.bot.backtest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link BinanceAggTradesParquetConverter}.
 * Validates deterministic synthetic CSV conversion to Parquet for both Spot and Futures market data.
 */
class BinanceAggTradesParquetConverterTest {

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            DriverManager.registerDriver(new org.duckdb.DuckDBDriver());
        } catch (Exception ignored) {
        }
    }

    @TempDir
    Path tempDir;

    private BinanceAggTradesParquetConverter converter;

    @BeforeEach
    void setUp() {
        converter = new BinanceAggTradesParquetConverter();
    }

    @Test
    @DisplayName("Should convert Spot CSV without header (8 columns) to Parquet with expected schema and rows")
    void shouldConvertSpotCsvWithoutHeaderToParquet() throws Exception {
        Path csvFile = tempDir.resolve("BTCUSDT-aggTrades-2026-08-01.csv");
        Path parquetFile = tempDir.resolve("BTCUSDT-aggTrades-2026-08-01.parquet");

        String syntheticSpotCsv = """
                4000000001,65000.50,0.015,5000000001,5000000002,1785542400100000,true,true
                4000000002,65001.00,0.020,5000000003,5000000003,1785542400200000,false,true
                4000000003,64999.00,0.100,5000000004,5000000005,1785542400300000,true,false
                """;
        Files.writeString(csvFile, syntheticSpotCsv);

        converter.convertSpotCsvToParquet(csvFile, parquetFile);

        assertThat(parquetFile).exists();
        assertThat(Files.size(parquetFile)).isPositive();

        // Verify Parquet content using DuckDB
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            String normParquet = parquetFile.toAbsolutePath().toString().replace('\\', '/');
            try (ResultSet rs = stmt.executeQuery("SELECT count(*), min(price), max(price), min(transact_time), max(transact_time) FROM read_parquet('" + normParquet + "')")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(3L);
                assertThat(rs.getDouble(2)).isEqualTo(64999.00);
                assertThat(rs.getDouble(3)).isEqualTo(65001.00);
                assertThat(rs.getLong(4)).isEqualTo(1785542400100000L);
                assertThat(rs.getLong(5)).isEqualTo(1785542400300000L);
            }

            try (ResultSet rs = stmt.executeQuery("SELECT agg_trade_id, price, quantity, transact_time, is_buyer_maker, is_best_match FROM read_parquet('" + normParquet + "') ORDER BY agg_trade_id ASC")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("agg_trade_id")).isEqualTo(4000000001L);
                assertThat(rs.getDouble("price")).isEqualTo(65000.50);
                assertThat(rs.getDouble("quantity")).isEqualTo(0.015);
                assertThat(rs.getBoolean("is_buyer_maker")).isTrue();
                assertThat(rs.getBoolean("is_best_match")).isTrue();

                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("agg_trade_id")).isEqualTo(4000000002L);
                assertThat(rs.getDouble("price")).isEqualTo(65001.00);
                assertThat(rs.getBoolean("is_buyer_maker")).isFalse();

                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("agg_trade_id")).isEqualTo(4000000003L);
                assertThat(rs.getBoolean("is_best_match")).isFalse();
            }
        }
    }

    @Test
    @DisplayName("Should convert Spot CSV with header to Parquet correctly")
    void shouldConvertSpotCsvWithHeaderToParquet() throws Exception {
        Path csvFile = tempDir.resolve("ETHUSDT-aggTrades-2026-08-01.csv");
        Path parquetFile = tempDir.resolve("ETHUSDT-aggTrades-2026-08-01.parquet");

        String syntheticSpotCsvWithHeader = """
                agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker,is_best_match
                3000000001,3400.25,1.25,4000000001,4000000001,1785542400150000,false,true
                3000000002,3400.50,0.50,4000000002,4000000003,1785542400250000,true,true
                """;
        Files.writeString(csvFile, syntheticSpotCsvWithHeader);

        converter.convertSpotCsvToParquet(csvFile, parquetFile);

        assertThat(parquetFile).exists();

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            String normParquet = parquetFile.toAbsolutePath().toString().replace('\\', '/');
            try (ResultSet rs = stmt.executeQuery("SELECT count(*), avg(price) FROM read_parquet('" + normParquet + "')")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(2L);
                assertThat(rs.getDouble(2)).isEqualTo((3400.25 + 3400.50) / 2.0);
            }
        }
    }

    @Test
    @DisplayName("Should convert Futures CSV with header (7 columns) to Parquet with expected schema and rows")
    void shouldConvertFuturesCsvWithHeaderToParquet() throws Exception {
        Path csvFile = tempDir.resolve("BTCUSDT-futures-aggTrades-2026-08-01.csv");
        Path parquetFile = tempDir.resolve("BTCUSDT-futures-aggTrades-2026-08-01.parquet");

        String syntheticFuturesCsv = """
                agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
                2000000001,65100.2,0.05,3000000001,3000000001,1785542400123,true
                2000000002,65100.5,0.10,3000000002,3000000005,1785542400234,false
                """;
        Files.writeString(csvFile, syntheticFuturesCsv);

        converter.convertFuturesCsvToParquet(csvFile, parquetFile);

        assertThat(parquetFile).exists();
        assertThat(Files.size(parquetFile)).isPositive();

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            String normParquet = parquetFile.toAbsolutePath().toString().replace('\\', '/');
            try (ResultSet rs = stmt.executeQuery("SELECT agg_trade_id, price, quantity, transact_time, is_buyer_maker FROM read_parquet('" + normParquet + "') ORDER BY agg_trade_id ASC")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("agg_trade_id")).isEqualTo(2000000001L);
                assertThat(rs.getDouble("price")).isEqualTo(65100.2);
                assertThat(rs.getDouble("quantity")).isEqualTo(0.05);
                assertThat(rs.getLong("transact_time")).isEqualTo(1785542400123L);
                assertThat(rs.getBoolean("is_buyer_maker")).isTrue();

                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("agg_trade_id")).isEqualTo(2000000002L);
                assertThat(rs.getBoolean("is_buyer_maker")).isFalse();
            }
        }
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException if CSV file does not exist")
    void shouldThrowExceptionWhenCsvDoesNotExist() {
        Path nonExistentCsv = tempDir.resolve("non_existent.csv");
        Path parquetFile = tempDir.resolve("out.parquet");

        assertThatThrownBy(() -> converter.convertSpotCsvToParquet(nonExistentCsv, parquetFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CSV file does not exist");

        assertThatThrownBy(() -> converter.convertFuturesCsvToParquet(nonExistentCsv, parquetFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CSV file does not exist");
    }
}
