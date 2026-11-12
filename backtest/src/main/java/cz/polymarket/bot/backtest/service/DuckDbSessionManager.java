package cz.polymarket.bot.backtest.service;

import io.quarkus.logging.Log;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDriver;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Application-scoped manager maintaining a single shared native DuckDB database instance
 * with bounded C++ buffer pool memory ({@code polymarket.backtest.duckdb.memory-limit})
 * and lazy streaming cursors ({@code jdbc_stream_results = true}, {@code threads = 1}).
 * <p>
 * Parallel backtest workers call {@link #openSession()} to obtain lightweight duplicated
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
    private DuckDBConnection rootConnection;

    /**
     * Constructs the shared DuckDB session manager with a configurable native buffer pool memory limit.
     *
     * @param memoryLimit DuckDB {@code memory_limit} setting (e.g. {@code "512MB"}, {@code "1GB"})
     */
    @Inject
    public DuckDbSessionManager(
            @ConfigProperty(name = "polymarket.backtest.duckdb.memory-limit", defaultValue = "512MB")
            String memoryLimit) {
        if (memoryLimit == null || memoryLimit.isBlank()) {
            Log.error("Invalid configuration in DuckDbSessionManager: memoryLimit cannot be null or blank");
            throw new IllegalArgumentException("memoryLimit cannot be null or blank");
        }
        this.memoryLimit = memoryLimit.trim();
    }

    /**
     * Opens a lightweight duplicated DuckDB {@link Connection} sharing the single native buffer pool
     * and configured with lazy vector result streaming ({@code jdbc_stream_results = true}, {@code threads = 1}).
     *
     * @return isolated DuckDB client session connection backed by the shared database instance
     * @throws SQLException if initializing or duplicating the DuckDB connection fails
     */
    public synchronized Connection openSession() throws SQLException {
        if (rootConnection == null || rootConnection.isClosed()) {
            Properties props = new Properties();
            props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, "true");
            Connection raw = DriverManager.getConnection("jdbc:duckdb:", props);
            DuckDBConnection duckConn = raw.isWrapperFor(DuckDBConnection.class)
                    ? raw.unwrap(DuckDBConnection.class)
                    : (DuckDBConnection) raw;

            String sanitizedLimit = memoryLimit.replace("'", "");
            try (Statement stmt = duckConn.createStatement()) {
                stmt.execute("SET memory_limit = '" + sanitizedLimit + "';");
                stmt.execute("SET threads = 1;");
            }
            this.rootConnection = duckConn;
            if (Log.isDebugEnabled()) {
                Log.debugf("Initialized shared DuckDB instance (memory_limit=%s, threads=1, jdbc_stream_results=true)",
                        sanitizedLimit);
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
