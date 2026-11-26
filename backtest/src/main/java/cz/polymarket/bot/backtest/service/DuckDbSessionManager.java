package cz.polymarket.bot.backtest.service;

import io.quarkus.logging.Log;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDriver;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Application-scoped manager maintaining a single shared native DuckDB database instance
 * with configurable C++ buffer pool memory ({@code polymarket.backtest.duckdb.memory-limit}),
 * worker thread parallelism ({@code polymarket.backtest.duckdb.threads}), local spill directory
 * ({@code polymarket.backtest.duckdb.temp-dir}), and lazy streaming cursors ({@code jdbc_stream_results = true}).
 * <p>
 * Backtest event streamers call {@link #openSession()} to obtain lightweight duplicated
 * client sessions ({@link DuckDBConnection#duplicate()}) sharing the single native buffer manager
 * without allocating separate C++ database instances.
 */
@ApplicationScoped
public class DuckDbSessionManager implements AutoCloseable {

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            Log.fatal("Fatal error: DuckDB JDBC driver not found on classpath; backtesting engine cannot run", e);
        }
    }

    private final String memoryLimit;
    private final int threads;
    private final Path tempDir;
    private DuckDBConnection rootConnection;

    /**
     * Constructs the shared DuckDB session manager with configurable native buffer pool memory,
     * worker thread count, and temporary spill directory.
     *
     * @param memoryLimit DuckDB {@code memory_limit} setting (e.g. {@code "6GB"})
     * @param threads     DuckDB {@code threads} setting ({@code >= 1})
     * @param tempDir     local directory for DuckDB temporary spill files (e.g. {@code "/tmp/polymarket-duckdb"})
     */
    @Inject
    public DuckDbSessionManager(
            @ConfigProperty(name = "polymarket.backtest.duckdb.memory-limit", defaultValue = "6GB")
            String memoryLimit,
            @ConfigProperty(name = "polymarket.backtest.duckdb.threads", defaultValue = "4")
            int threads,
            @ConfigProperty(name = "polymarket.backtest.duckdb.temp-dir", defaultValue = "/tmp/polymarket-duckdb")
            String tempDir) {
        if (memoryLimit == null || memoryLimit.isBlank()) {
            Log.error("Invalid configuration in DuckDbSessionManager: memoryLimit cannot be null or blank");
            throw new IllegalArgumentException("memoryLimit cannot be null or blank");
        }
        if (threads < 1) {
            Log.errorf("Invalid configuration in DuckDbSessionManager: threads must be >= 1 (received %d)", threads);
            throw new IllegalArgumentException("threads must be >= 1");
        }
        if (tempDir == null || tempDir.isBlank()) {
            Log.error("Invalid configuration in DuckDbSessionManager: tempDir cannot be null or blank");
            throw new IllegalArgumentException("tempDir cannot be null or blank");
        }
        this.memoryLimit = memoryLimit.trim();
        this.threads = threads;
        this.tempDir = Path.of(tempDir.trim()).toAbsolutePath();
    }

    /**
     * Opens a lightweight duplicated DuckDB {@link Connection} sharing the single native buffer pool
     * and configured with lazy vector result streaming ({@code jdbc_stream_results = true}).
     *
     * @return isolated DuckDB client session connection backed by the shared database instance
     * @throws SQLException if initializing or duplicating the DuckDB connection fails
     */
    public synchronized Connection openSession() throws SQLException {
        if (rootConnection == null || rootConnection.isClosed()) {
            try {
                Files.createDirectories(tempDir);
            } catch (IOException e) {
                throw new SQLException("Failed creating DuckDB temp directory " + tempDir, e);
            }
            Properties props = new Properties();
            props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, "true");
            Connection raw = DriverManager.getConnection("jdbc:duckdb:", props);
            DuckDBConnection duckConn = raw.isWrapperFor(DuckDBConnection.class)
                    ? raw.unwrap(DuckDBConnection.class)
                    : (DuckDBConnection) raw;

            String sanitizedLimit = memoryLimit.replace("'", "");
            String sanitizedTempDir = tempDir.toString().replace('\\', '/').replace("'", "''");
            try (Statement stmt = duckConn.createStatement()) {
                stmt.execute("SET memory_limit = '" + sanitizedLimit + "';");
                stmt.execute("SET threads = " + threads + ";");
                stmt.execute("SET temp_directory = '" + sanitizedTempDir + "';");
            }
            this.rootConnection = duckConn;
            if (Log.isDebugEnabled()) {
                Log.debugf("Initialized shared DuckDB instance (memory_limit=%s, threads=%d, temp_directory=%s, jdbc_stream_results=true)",
                        sanitizedLimit, threads, sanitizedTempDir);
            }
        }
        return rootConnection.duplicate();
    }

    /**
     * Closes the underlying shared root DuckDB connection on application shutdown or test teardown.
     */
    @PreDestroy
    @Override
    public synchronized void close() {
        if (rootConnection != null) {
            try {
                rootConnection.close();
            } catch (SQLException e) {
                Log.warnf("Failed closing root DuckDB connection cleanly: %s", e.getMessage());
            } finally {
                rootConnection = null;
            }
        }
    }
}
