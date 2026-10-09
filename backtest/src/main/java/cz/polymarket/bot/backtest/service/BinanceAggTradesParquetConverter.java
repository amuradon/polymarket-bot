package cz.polymarket.bot.backtest.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * High-performance converter for Binance aggTrades CSV files into compressed Parquet format.
 * Utilizes embedded DuckDB engine with ZSTD compression to produce optimized, columnar dataset files.
 */
@ApplicationScoped
public class BinanceAggTradesParquetConverter {

    private static final Logger LOG = Logger.getLogger(BinanceAggTradesParquetConverter.class);

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            DriverManager.registerDriver(new org.duckdb.DuckDBDriver());
        } catch (Exception ignored) {
        }
    }

    /**
     * Default no-args constructor for CDI proxying and injection.
     */
    public BinanceAggTradesParquetConverter() {
    }

    /**
     * Converts a Binance Spot aggTrades CSV file into a compressed Parquet file.
     * Automatically detects whether the CSV includes a header line and maps all 8 spot trade columns.
     *
     * @param csvPath path to the input CSV file
     * @param parquetPath path to the target Parquet destination
     */
    public void convertSpotCsvToParquet(Path csvPath, Path parquetPath) {
        validateInput(csvPath, parquetPath);
        boolean hasHeader = detectHeader(csvPath);

        String normCsv = csvPath.toAbsolutePath().toString().replace('\\', '/');
        Path tempParquet = parquetPath.resolveSibling(parquetPath.getFileName() + ".part");
        String normTempParquet = tempParquet.toAbsolutePath().toString().replace('\\', '/');

        String sql;
        if (hasHeader) {
            sql = "COPY (SELECT " +
                    "CAST(agg_trade_id AS BIGINT) AS agg_trade_id, " +
                    "CAST(price AS DOUBLE) AS price, " +
                    "CAST(quantity AS DOUBLE) AS quantity, " +
                    "CAST(first_trade_id AS BIGINT) AS first_trade_id, " +
                    "CAST(last_trade_id AS BIGINT) AS last_trade_id, " +
                    "CAST(transact_time AS BIGINT) AS transact_time, " +
                    "CAST(is_buyer_maker AS BOOLEAN) AS is_buyer_maker, " +
                    "CAST(is_best_match AS BOOLEAN) AS is_best_match " +
                    "FROM read_csv('" + normCsv + "', header=true)) " +
                    "TO '" + normTempParquet + "' (FORMAT PARQUET, COMPRESSION ZSTD)";
        } else {
            sql = "COPY (SELECT " +
                    "CAST(column0 AS BIGINT) AS agg_trade_id, " +
                    "CAST(column1 AS DOUBLE) AS price, " +
                    "CAST(column2 AS DOUBLE) AS quantity, " +
                    "CAST(column3 AS BIGINT) AS first_trade_id, " +
                    "CAST(column4 AS BIGINT) AS last_trade_id, " +
                    "CAST(column5 AS BIGINT) AS transact_time, " +
                    "CAST(column6 AS BOOLEAN) AS is_buyer_maker, " +
                    "CAST(column7 AS BOOLEAN) AS is_best_match " +
                    "FROM read_csv('" + normCsv + "', header=false)) " +
                    "TO '" + normTempParquet + "' (FORMAT PARQUET, COMPRESSION ZSTD)";
        }

        executeConversion(sql, tempParquet, parquetPath, "spot");
    }

    /**
     * Converts a Binance Futures aggTrades CSV file into a compressed Parquet file.
     * Automatically detects whether the CSV includes a header line and maps all 7 futures trade columns.
     *
     * @param csvPath path to the input CSV file
     * @param parquetPath path to the target Parquet destination
     */
    public void convertFuturesCsvToParquet(Path csvPath, Path parquetPath) {
        validateInput(csvPath, parquetPath);
        boolean hasHeader = detectHeader(csvPath);

        String normCsv = csvPath.toAbsolutePath().toString().replace('\\', '/');
        Path tempParquet = parquetPath.resolveSibling(parquetPath.getFileName() + ".part");
        String normTempParquet = tempParquet.toAbsolutePath().toString().replace('\\', '/');

        String sql;
        if (hasHeader) {
            sql = "COPY (SELECT " +
                    "CAST(agg_trade_id AS BIGINT) AS agg_trade_id, " +
                    "CAST(price AS DOUBLE) AS price, " +
                    "CAST(quantity AS DOUBLE) AS quantity, " +
                    "CAST(first_trade_id AS BIGINT) AS first_trade_id, " +
                    "CAST(last_trade_id AS BIGINT) AS last_trade_id, " +
                    "CAST(transact_time AS BIGINT) AS transact_time, " +
                    "CAST(is_buyer_maker AS BOOLEAN) AS is_buyer_maker " +
                    "FROM read_csv('" + normCsv + "', header=true)) " +
                    "TO '" + normTempParquet + "' (FORMAT PARQUET, COMPRESSION ZSTD)";
        } else {
            sql = "COPY (SELECT " +
                    "CAST(column0 AS BIGINT) AS agg_trade_id, " +
                    "CAST(column1 AS DOUBLE) AS price, " +
                    "CAST(column2 AS DOUBLE) AS quantity, " +
                    "CAST(column3 AS BIGINT) AS first_trade_id, " +
                    "CAST(column4 AS BIGINT) AS last_trade_id, " +
                    "CAST(column5 AS BIGINT) AS transact_time, " +
                    "CAST(column6 AS BOOLEAN) AS is_buyer_maker " +
                    "FROM read_csv('" + normCsv + "', header=false)) " +
                    "TO '" + normTempParquet + "' (FORMAT PARQUET, COMPRESSION ZSTD)";
        }

        executeConversion(sql, tempParquet, parquetPath, "futures");
    }

    private void validateInput(Path csvPath, Path parquetPath) {
        if (csvPath == null || !Files.exists(csvPath) || !Files.isRegularFile(csvPath)) {
            throw new IllegalArgumentException("CSV file does not exist: " + csvPath);
        }
        if (parquetPath == null) {
            throw new IllegalArgumentException("Target Parquet path cannot be null");
        }

        try {
            Path parent = parquetPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create parent directories for " + parquetPath, e);
        }
    }

    private boolean detectHeader(Path csvPath) {
        try (BufferedReader br = Files.newBufferedReader(csvPath)) {
            String firstLine = br.readLine();
            if (firstLine == null || firstLine.isBlank()) {
                return false;
            }
            char firstChar = firstLine.trim().charAt(0);
            return Character.isLetter(firstChar);
        } catch (IOException e) {
            LOG.warnf("Failed to read first line of %s for header detection: %s", csvPath, e.getMessage());
            return false;
        }
    }

    private void executeConversion(String sql, Path tempParquet, Path targetParquet, String marketType) {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            stmt.execute(sql);

            try {
                Files.move(tempParquet, targetParquet, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                Files.move(tempParquet, targetParquet, StandardCopyOption.REPLACE_EXISTING);
            }

            LOG.infof("Successfully converted %s aggTrades CSV to Parquet: %s", marketType, targetParquet);
        } catch (SQLException | IOException e) {
            try {
                Files.deleteIfExists(tempParquet);
            } catch (IOException ignored) {
            }
            throw new RuntimeException("Failed to convert " + marketType + " CSV to Parquet (" + targetParquet + "): " + e.getMessage(), e);
        }
    }
}
