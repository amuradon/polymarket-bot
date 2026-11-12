package cz.polymarket.bot.backtest;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Deterministic test fixture builder for the 10-column typed Polymarket CLOB Parquet schema
 * ({@code ts, server_ts, frame_id, event_code, is_up, is_bid, tick, size, up_token_id, down_token_id}).
 */
public final class TypedClobParquetTestHelper {

    private TypedClobParquetTestHelper() {
    }

    /**
     * Represents a single row in the 10-column typed CLOB Parquet schema.
     *
     * @param ts local EC2 receipt timestamp in milliseconds UTC
     * @param serverTs exchange server timestamp in milliseconds UTC
     * @param frameId monotonic WebSocket frame identifier
     * @param eventCode {@code 0 = market_discovered}, {@code 1 = book}, {@code 2 = price_change}, {@code 3 = last_trade_price}
     * @param isUp nullable UP/DOWN token flag
     * @param isBid nullable bid/ask side flag
     * @param tick nullable integer price tick {@code 0..10000}
     * @param size nullable share depth
     * @param upTokenId nullable UP token ID (populated on {@code eventCode == 0})
     * @param downTokenId nullable DOWN token ID (populated on {@code eventCode == 0})
     */
    public record ClobRow(
            long ts,
            long serverTs,
            int frameId,
            int eventCode,
            Boolean isUp,
            Boolean isBid,
            Integer tick,
            Double size,
            String upTokenId,
            String downTokenId
    ) {
        /**
         * Creates a {@code market_discovered} ({@code event_code = 0}) metadata row.
         */
        public static ClobRow discovered(long ts, String upTokenId, String downTokenId) {
            return new ClobRow(ts, 0L, 0, 0, null, null, null, null, upTokenId, downTokenId);
        }

        /**
         * Creates a {@code book} ({@code event_code = 1}) snapshot level row.
         */
        public static ClobRow bookLevel(long ts, long serverTs, int frameId, boolean isUp, boolean isBid, int tick, double size) {
            return new ClobRow(ts, serverTs, frameId, 1, isUp, isBid, tick, size, null, null);
        }

        /**
         * Creates an empty {@code book} ({@code event_code = 1}) snapshot sentinel row clearing a token side.
         */
        public static ClobRow emptyBook(long ts, long serverTs, int frameId, boolean isUp) {
            return new ClobRow(ts, serverTs, frameId, 1, isUp, null, null, 0.0, null, null);
        }

        /**
         * Creates a {@code price_change} ({@code event_code = 2}) delta item row.
         */
        public static ClobRow priceChange(long ts, long serverTs, int frameId, boolean isUp, boolean isBid, int tick, double size) {
            return new ClobRow(ts, serverTs, frameId, 2, isUp, isBid, tick, size, null, null);
        }

        /**
         * Creates a {@code last_trade_price} ({@code event_code = 3}) row.
         */
        public static ClobRow lastTrade(long ts, long serverTs, int frameId) {
            return new ClobRow(ts, serverTs, frameId, 3, null, null, null, null, null, null);
        }
    }

    /**
     * Writes sorted typed CLOB rows to a Parquet file using default {@code ROW_GROUP_SIZE = 100000}.
     *
     * @param stmt active DuckDB statement
     * @param destFile target Parquet path
     * @param rows synthetic typed CLOB rows
     * @throws SQLException if Parquet generation fails
     */
    public static void writeParquet(Statement stmt, Path destFile, List<ClobRow> rows) throws SQLException {
        writeParquet(stmt, destFile, rows, 100_000);
    }

    /**
     * Writes sorted typed CLOB rows to a Parquet file using an explicit {@code rowGroupSize}.
     *
     * @param stmt active DuckDB statement
     * @param destFile target Parquet path
     * @param rows synthetic typed CLOB rows
     * @param rowGroupSize maximum rows per Parquet row group
     * @throws SQLException if Parquet generation fails
     */
    public static void writeParquet(Statement stmt, Path destFile, List<ClobRow> rows, int rowGroupSize) throws SQLException {
        List<ClobRow> sorted = new ArrayList<>(rows);
        sorted.sort(Comparator.comparingLong(ClobRow::ts).thenComparingInt(ClobRow::frameId));

        String normPath = destFile.toAbsolutePath().toString().replace('\\', '/');
        if (sorted.isEmpty()) {
            stmt.execute("COPY (SELECT " +
                    "0::UBIGINT AS ts, 0::UBIGINT AS server_ts, 0::UINTEGER AS frame_id, 0::UTINYINT AS event_code, " +
                    "NULL::BOOLEAN AS is_up, NULL::BOOLEAN AS is_bid, NULL::USMALLINT AS tick, NULL::DOUBLE AS size, " +
                    "NULL::VARCHAR AS up_token_id, NULL::VARCHAR AS down_token_id WHERE false) " +
                    "TO '" + normPath + "' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE " + rowGroupSize + ")");
            return;
        }

        StringBuilder sb = new StringBuilder(sorted.size() * 96);
        sb.append("COPY (SELECT * FROM (VALUES ");
        for (int i = 0; i < sorted.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            ClobRow r = sorted.get(i);
            sb.append('(')
                    .append(r.ts()).append("::UBIGINT, ")
                    .append(r.serverTs()).append("::UBIGINT, ")
                    .append(r.frameId()).append("::UINTEGER, ")
                    .append(r.eventCode()).append("::UTINYINT, ")
                    .append(r.isUp() != null ? r.isUp() + "::BOOLEAN" : "NULL::BOOLEAN").append(", ")
                    .append(r.isBid() != null ? r.isBid() + "::BOOLEAN" : "NULL::BOOLEAN").append(", ")
                    .append(r.tick() != null ? r.tick() + "::USMALLINT" : "NULL::USMALLINT").append(", ")
                    .append(r.size() != null ? r.size() + "::DOUBLE" : "NULL::DOUBLE").append(", ")
                    .append(r.upTokenId() != null ? "'" + r.upTokenId() + "'::VARCHAR" : "NULL::VARCHAR").append(", ")
                    .append(r.downTokenId() != null ? "'" + r.downTokenId() + "'::VARCHAR" : "NULL::VARCHAR")
                    .append(')');
        }
        sb.append(") AS t(ts, server_ts, frame_id, event_code, is_up, is_bid, tick, size, up_token_id, down_token_id)) ")
                .append("TO '").append(normPath).append("' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ")
                .append(rowGroupSize).append(')');
        stmt.execute(sb.toString());
    }

    /**
     * Convenience overload opening a temporary DuckDB connection to write sorted typed CLOB rows.
     *
     * @param destFile target Parquet path
     * @param rows synthetic typed CLOB rows
     * @throws SQLException if Parquet generation fails
     */
    public static void writeParquet(Path destFile, List<ClobRow> rows) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            writeParquet(stmt, destFile, rows, 100_000);
        }
    }
}
