package cz.polymarket.bot.backtest.data;

import jakarta.enterprise.context.ApplicationScoped;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Loads unified market data from Parquet files using DuckDB JDBC.
 * Provides zero-leakage, high-performance in-memory dataset reading for the backtest engine.
 */
@ApplicationScoped
public class ParquetDatasetLoader {

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            DriverManager.registerDriver(new org.duckdb.DuckDBDriver());
        } catch (Exception ignored) {
        }
    }

    public List<BacktestMarketRow> loadDataset(String filePath) {
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException("Dataset path cannot be null or blank");
        }

        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            throw new IllegalArgumentException("Dataset file does not exist: " + filePath);
        }

        String normalizedPath = file.getAbsolutePath().replace('\\', '/');
        String query = "SELECT * FROM read_parquet('" + normalizedPath + "') ORDER BY t_start ASC";

        List<BacktestMarketRow> rows = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                long tStart = rs.getLong("t_start");
                long tEnd = rs.getLong("t_end");
                String datetimeUtc = rs.getString("datetime_utc");
                String dateStr = rs.getString("date_str");
                double twapOpen = getDouble(rs, "twap_open", 0.0);
                double twapClose = getDouble(rs, "twap_close", 0.0);
                String actualOutcome = rs.getString("actual_outcome");

                double sOpen = getDouble(rs, "s_open", 0.0);
                double sHigh = getDouble(rs, "s_high", sOpen);
                double sLow = getDouble(rs, "s_low", sOpen);
                double sClose = getDouble(rs, "s_close", sOpen);
                double sVolBtc = getDouble(rs, "s_vol_btc", 0.0);
                double sVolUsd = getDouble(rs, "s_vol_usd", 0.0);
                double sDeltaBtc = getDouble(rs, "s_delta_btc", 0.0);

                double fOpen = getDouble(rs, "f_open", sOpen);
                double fClose = getDouble(rs, "f_close", sClose);
                double fVolBtc = getDouble(rs, "f_vol_btc", 0.0);
                double fVolUsd = getDouble(rs, "f_vol_usd", 0.0);
                double fDeltaBtc = getDouble(rs, "f_delta_btc", 0.0);

                double basisOpenBps = getDouble(rs, "basis_open_bps", 0.0);
                double basisCloseBps = getDouble(rs, "basis_close_bps", 0.0);

                double pmAsk0 = getDouble(rs, "pm_ask_0", 0.50);
                double pmBid0 = getDouble(rs, "pm_bid_0", 0.49);
                double pmAsk60 = getDouble(rs, "pm_ask_60", pmAsk0);
                double pmBid60 = getDouble(rs, "pm_bid_60", pmBid0);
                double pmAsk180 = getDouble(rs, "pm_ask_180", pmAsk60);
                double pmBid180 = getDouble(rs, "pm_bid_180", pmBid60);
                double pmAsk300 = getDouble(rs, "pm_ask_300", pmAsk180);
                double pmBid300 = getDouble(rs, "pm_bid_300", pmBid180);

                double pmFill100Up = getDouble(rs, "pm_fill_100_up", pmAsk0);
                double pmFee100Up = getDouble(rs, "pm_fee_100_up", 0.0);
                double pmFill100Down = getDouble(rs, "pm_fill_100_down", Math.max(0.01, 1.0 - pmBid0));
                double pmFee100Down = getDouble(rs, "pm_fee_100_down", 0.0);

                double pmMaxPrice = getDouble(rs, "pm_max_price", pmAsk0);
                double pmMinPrice = getDouble(rs, "pm_min_price", pmBid0);
                double pmDepth1cUp = getDouble(rs, "pm_depth_1c_up", 300.0);
                double pmDepth1cDown = getDouble(rs, "pm_depth_1c_down", 300.0);

                rows.add(new BacktestMarketRow(
                        tStart,
                        tEnd,
                        datetimeUtc,
                        dateStr,
                        twapOpen,
                        twapClose,
                        actualOutcome,
                        sOpen,
                        sHigh,
                        sLow,
                        sClose,
                        sVolBtc,
                        sVolUsd,
                        sDeltaBtc,
                        fOpen,
                        fClose,
                        fVolBtc,
                        fVolUsd,
                        fDeltaBtc,
                        basisOpenBps,
                        basisCloseBps,
                        pmAsk0,
                        pmBid0,
                        pmAsk60,
                        pmBid60,
                        pmAsk180,
                        pmBid180,
                        pmAsk300,
                        pmBid300,
                        pmFill100Up,
                        pmFee100Up,
                        pmFill100Down,
                        pmFee100Down,
                        pmMaxPrice,
                        pmMinPrice,
                        pmDepth1cUp,
                        pmDepth1cDown
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read Parquet dataset using DuckDB: " + filePath, e);
        }

        return Collections.unmodifiableList(rows);
    }

    private double getDouble(ResultSet rs, String col, double defaultVal) {
        try {
            double val = rs.getDouble(col);
            return rs.wasNull() ? defaultVal : val;
        } catch (SQLException e) {
            return defaultVal;
        }
    }
}
