package cz.polymarket.bot.backtest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.polymarket.bot.backtest.data.BacktestRunConfig;
import cz.polymarket.bot.backtest.data.MarketEquityPoint;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.calculator.PerformanceMetricsCalculator;
import cz.polymarket.bot.domain.TradeRecord;
import cz.polymarket.bot.strategy.TradeDirection;
import io.quarkus.logging.Log;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Persistent DuckDB repository storing backtest runs, full engine/strategy configuration snapshots,
 * per-market equity progression, and trade histories, and serving fast time-sliced comparisons
 * rebased from zero with recalculated quantitative metrics.
 */
@ApplicationScoped
public class BacktestHistoryRepository implements AutoCloseable {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC);

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            Log.fatal("Fatal error: DuckDB JDBC driver not found on classpath; history repository cannot run", e);
        }
    }

    private final Path historyDbPath;
    private final ObjectMapper objectMapper;
    private final PerformanceMetricsCalculator metricsCalculator;
    private final ReentrantLock dbLock = new ReentrantLock();
    private Connection connection;
    private boolean schemaInitialized;

    /**
     * CDI constructor injecting Jackson mapper, metrics calculator, and configured DuckDB file path.
     *
     * @param objectMapper Jackson mapper for configuration JSON serialization
     * @param metricsCalculator quantitative performance metrics calculator
     * @param historyDbPath filesystem path to the persistent DuckDB database file
     */
    @Inject
    public BacktestHistoryRepository(
            ObjectMapper objectMapper,
            PerformanceMetricsCalculator metricsCalculator,
            @ConfigProperty(name = "polymarket.backtest.history-db-path",
                    defaultValue = "${user.home}/.local/share/polymarket-bot/backtest-history.duckdb")
            String historyDbPath) {
        this(
                Path.of(validateNonBlank(historyDbPath, "historyDbPath")),
                objectMapper,
                metricsCalculator
        );
    }

    /**
     * Programmatic constructor for isolated unit tests using temporary DuckDB files.
     *
     * @param historyDbPath filesystem path to the persistent DuckDB database file
     * @param objectMapper Jackson mapper
     * @param metricsCalculator quantitative performance metrics calculator
     */
    public BacktestHistoryRepository(
            Path historyDbPath,
            ObjectMapper objectMapper,
            PerformanceMetricsCalculator metricsCalculator) {
        if (historyDbPath == null) {
            throw new IllegalArgumentException("historyDbPath cannot be null");
        }
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper cannot be null");
        }
        if (metricsCalculator == null) {
            throw new IllegalArgumentException("metricsCalculator cannot be null");
        }
        this.historyDbPath = historyDbPath.toAbsolutePath();
        this.objectMapper = objectMapper;
        this.metricsCalculator = metricsCalculator;
    }

    /**
     * Returns the absolute path to the persistent DuckDB history database file.
     *
     * @return database file path
     */
    public Path getHistoryDbPath() {
        return historyDbPath;
    }

    /**
     * Persists a completed backtest run together with all evaluated market points and executed trades
     * atomically inside a single DuckDB transaction.
     *
     * @param result completed {@link BacktestResult}
     */
    public void saveRun(BacktestResult result) {
        if (result == null) {
            throw new IllegalArgumentException("result cannot be null");
        }
        BacktestRunConfig cfg = result.config();
        PerformanceMetrics m = result.metrics();
        List<MarketEquityPoint> marketPoints = result.marketPoints() != null ? result.marketPoints() : List.of();
        List<TradeRecord> trades = result.trades() != null ? result.trades() : List.of();

        String paramsJson = serializeMap(cfg != null ? cfg.strategyParams() : Map.of());
        String overridesJson = serializeMap(cfg != null ? cfg.strategyOverrides() : Map.of());
        String configHash = cfg != null ? cfg.configHash() : "";

        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            boolean prevAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                String insertRunSql = """
                        INSERT INTO backtest_runs (
                            run_id, label, strategy_name, symbol, start_date, end_date,
                            initial_capital, latency_factor, order_gateway_delay_ms,
                            binance_delay_ms, kraken_delay_ms, coinbase_delay_ms,
                            strategy_params_json, strategy_overrides_json,
                            total_markets, total_trades, winning_trades, losing_trades,
                            win_rate_pct, total_net_pnl, total_fees, gross_pnl, gross_profit, gross_loss,
                            profit_factor, max_drawdown_usd, max_drawdown_pct, expected_value_per_trade,
                            sharpe_ratio, sortino_ratio, brier_score, final_balance,
                            execution_time_utc, duration_ms, config_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """;
                try (PreparedStatement ps = conn.prepareStatement(insertRunSql)) {
                    ps.setString(1, result.runId());
                    ps.setString(2, result.label());
                    ps.setString(3, result.strategyName());
                    ps.setString(4, result.symbol());
                    ps.setString(5, cfg != null ? cfg.startDate() : "");
                    ps.setString(6, cfg != null ? cfg.endDate() : "");
                    ps.setDouble(7, m.initialCapital());
                    ps.setDouble(8, cfg != null ? cfg.latencyFactor() : 1.0);
                    ps.setLong(9, cfg != null ? cfg.orderGatewayDelayMs() : 220L);
                    ps.setLong(10, cfg != null ? cfg.binanceDelayMs() : 100L);
                    ps.setLong(11, cfg != null ? cfg.krakenDelayMs() : 50L);
                    ps.setLong(12, cfg != null ? cfg.coinbaseDelayMs() : 50L);
                    ps.setString(13, paramsJson);
                    ps.setString(14, overridesJson);
                    ps.setInt(15, result.totalMarkets());
                    ps.setInt(16, m.totalTrades());
                    ps.setInt(17, m.winningTrades());
                    ps.setInt(18, m.losingTrades());
                    ps.setDouble(19, m.winRatePct());
                    ps.setDouble(20, m.totalNetPnl());
                    ps.setDouble(21, m.totalFees());
                    ps.setDouble(22, m.grossPnl());
                    ps.setDouble(23, m.grossProfit());
                    ps.setDouble(24, m.grossLoss());
                    ps.setDouble(25, m.profitFactor());
                    ps.setDouble(26, m.maxDrawdownUsd());
                    ps.setDouble(27, m.maxDrawdownPct());
                    ps.setDouble(28, m.expectedValuePerTrade());
                    ps.setDouble(29, m.sharpeRatio());
                    ps.setDouble(30, m.sortinoRatio());
                    ps.setDouble(31, m.brierScore());
                    ps.setDouble(32, m.finalBalance());
                    ps.setString(33, result.executionTimeUtc().toString());
                    ps.setLong(34, result.durationMs());
                    ps.setString(35, configHash);
                    ps.executeUpdate();
                }

                insertMarketPoints(conn, result.runId(), marketPoints);
                insertTrades(conn, result.runId(), trades);

                conn.commit();
                if (Log.isInfoEnabled()) {
                    Log.infof("Saved backtest run %s (%s) to DuckDB history: markets=%d, trades=%d",
                            result.runId(), result.label(), marketPoints.size(), trades.size());
                }
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException ignored) {
                }
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(prevAutoCommit);
                } catch (SQLException ignored) {
                }
            }
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed saving backtest run %s to DuckDB: %s", result.runId(), e.getMessage());
            throw new IllegalStateException("Failed saving backtest run to DuckDB: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Lists persisted backtest runs ordered newest first, optionally filtered by strategy and symbol.
     *
     * @param strategyFilter optional strategy name filter
     * @param symbolFilter optional symbol filter
     * @return list of {@link RunSummaryDto}
     */
    public List<RunSummaryDto> listRuns(String strategyFilter, String symbolFilter) {
        StringBuilder sql = new StringBuilder("""
                SELECT run_id, label, strategy_name, symbol, start_date, end_date,
                       initial_capital, latency_factor, COALESCE(order_gateway_delay_ms, 220) AS order_gateway_delay_ms,
                       binance_delay_ms, kraken_delay_ms, coinbase_delay_ms,
                       strategy_overrides_json, total_markets, total_trades, winning_trades, losing_trades,
                       win_rate_pct, total_net_pnl, total_fees, gross_pnl, gross_profit, gross_loss,
                       profit_factor, max_drawdown_usd, max_drawdown_pct, expected_value_per_trade,
                       sharpe_ratio, sortino_ratio, brier_score, final_balance, execution_time_utc, duration_ms
                FROM backtest_runs
                WHERE 1=1
                """);
        List<Object> params = new ArrayList<>(2);
        if (strategyFilter != null && !strategyFilter.isBlank()) {
            sql.append(" AND strategy_name = ?");
            params.add(strategyFilter.trim());
        }
        if (symbolFilter != null && !symbolFilter.isBlank()) {
            sql.append(" AND symbol = ?");
            params.add(symbolFilter.trim().toUpperCase(Locale.ROOT));
        }
        sql.append(" ORDER BY execution_time_utc DESC");

        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }
                List<RunSummaryDto> runs = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        runs.add(mapSummaryRow(rs));
                    }
                }
                return Collections.unmodifiableList(runs);
            }
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed listing backtest runs from DuckDB: %s", e.getMessage());
            throw new IllegalStateException("Failed listing backtest runs: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Loads full metadata and resolved configuration for a single persisted run.
     *
     * @param runId unique run identifier
     * @return optional {@link RunDetailDto}
     */
    public Optional<RunDetailDto> getRun(String runId) {
        if (runId == null || runId.isBlank()) {
            return Optional.empty();
        }
        String sql = """
                SELECT run_id, label, strategy_name, symbol, start_date, end_date,
                       initial_capital, latency_factor, COALESCE(order_gateway_delay_ms, 220) AS order_gateway_delay_ms,
                       binance_delay_ms, kraken_delay_ms, coinbase_delay_ms,
                       strategy_params_json, strategy_overrides_json,
                       total_markets, total_trades, winning_trades, losing_trades,
                       win_rate_pct, total_net_pnl, total_fees, gross_pnl, gross_profit, gross_loss,
                       profit_factor, max_drawdown_usd, max_drawdown_pct, expected_value_per_trade,
                       sharpe_ratio, sortino_ratio, brier_score, final_balance, execution_time_utc, duration_ms
                FROM backtest_runs
                WHERE run_id = ?
                """;
        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, runId.trim());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(mapDetailRow(rs));
                }
            }
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed loading backtest run %s: %s", runId, e.getMessage());
            throw new IllegalStateException("Failed loading backtest run: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Finds the newest persisted run matching the given deterministic configuration hash.
     *
     * @param configHash 64-character SHA-256 configuration hash
     * @return optional matching {@link RunSummaryDto}
     */
    public Optional<RunSummaryDto> findLatestRunByConfigHash(String configHash) {
        if (configHash == null || configHash.isBlank()) {
            return Optional.empty();
        }
        String sql = """
                SELECT run_id, label, strategy_name, symbol, start_date, end_date,
                       initial_capital, latency_factor, COALESCE(order_gateway_delay_ms, 220) AS order_gateway_delay_ms,
                       binance_delay_ms, kraken_delay_ms, coinbase_delay_ms,
                       strategy_overrides_json, total_markets, total_trades, winning_trades, losing_trades,
                       win_rate_pct, total_net_pnl, total_fees, gross_pnl, gross_profit, gross_loss,
                       profit_factor, max_drawdown_usd, max_drawdown_pct, expected_value_per_trade,
                       sharpe_ratio, sortino_ratio, brier_score, final_balance, execution_time_utc, duration_ms
                FROM backtest_runs
                WHERE config_hash = ?
                ORDER BY execution_time_utc DESC
                LIMIT 1
                """;
        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, configHash.trim());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(mapSummaryRow(rs));
                }
            }
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed querying run by configHash %s: %s", configHash, e.getMessage());
            throw new IllegalStateException("Failed querying run by configHash: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Returns the set of distinct UTC dates ({@code yyyy-MM-dd}) already stored in {@code backtest_market_points}
     * for the specified run.
     *
     * @param runId target run identifier
     * @return set of stored UTC date strings
     */
    public Set<String> getStoredDatesForRun(String runId) {
        if (runId == null || runId.isBlank()) {
            return Set.of();
        }
        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            Set<String> dates = new LinkedHashSet<>();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT DISTINCT date_utc FROM backtest_market_points WHERE run_id = ?")) {
                ps.setString(1, runId.trim());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String d = rs.getString(1);
                        if (d != null && !d.isBlank()) {
                            dates.add(d);
                        }
                    }
                }
            }
            return Collections.unmodifiableSet(dates);
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed loading stored dates for run %s: %s", runId, e.getMessage());
            throw new IllegalStateException("Failed loading stored dates: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Loads a complete {@link BacktestResult} (summary, config, market points, and trades) from DuckDB
     * for an existing run without re-running simulation.
     *
     * @param runId target run identifier
     * @param mergedIntoExisting whether this result reflects an incremental merge operation
     * @param skippedDays count of UTC days skipped as already present
     * @return optional {@link BacktestResult}
     */
    public Optional<BacktestResult> loadFullResult(String runId, boolean mergedIntoExisting, int skippedDays) {
        if (runId == null || runId.isBlank()) {
            return Optional.empty();
        }
        String trimmed = runId.trim();
        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            Optional<RunDetailDto> detailOpt = loadRunDetailInternal(conn, trimmed);
            if (detailOpt.isEmpty()) {
                return Optional.empty();
            }
            RunDetailDto detail = detailOpt.get();
            List<MarketEquityPoint> points = loadMarketPointsInternal(conn, trimmed, Set.of());
            List<TradeRecord> trades = loadTradesInternal(conn, trimmed, Set.of());
            RunSummaryDto s = detail.summary();
            return Optional.of(new BacktestResult(
                    s.runId(),
                    s.label(),
                    s.strategyName(),
                    s.symbol(),
                    Instant.parse(s.executionTimeUtc()),
                    s.durationMs(),
                    s.totalMarkets(),
                    detail.config(),
                    s.metrics(),
                    Collections.unmodifiableList(points),
                    Collections.unmodifiableList(trades),
                    mergedIntoExisting,
                    skippedDays
            ));
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed loading full result for run %s: %s", runId, e.getMessage());
            throw new IllegalStateException("Failed loading full result: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Merges newly simulated market points and trades into an existing persisted run atomically,
     * optionally overwriting specified UTC days, chronologically replaying account balances,
     * and recomputing full-period {@link PerformanceMetrics}.
     *
     * @param existingRunId target run identifier in DuckDB
     * @param explicitLabel optional explicit user label (when null/blank, preserves custom label or refreshes auto-generated date span)
     * @param latestConfig resolved configuration snapshot of the incremental execution
     * @param newMarketPoints newly simulated per-market equity points
     * @param newTrades newly simulated trade records
     * @param overwrittenDates UTC dates to replace rather than preserve from existing storage
     * @param durationMs incremental simulation wall-clock duration in milliseconds
     * @param skippedDays count of UTC days skipped because they were already stored
     * @return updated full {@link BacktestResult} reflecting the merged timeline
     */
    public BacktestResult mergeRunDays(
            String existingRunId,
            String explicitLabel,
            BacktestRunConfig latestConfig,
            List<MarketEquityPoint> newMarketPoints,
            List<TradeRecord> newTrades,
            Set<String> overwrittenDates,
            long durationMs,
            int skippedDays) {
        if (existingRunId == null || existingRunId.isBlank()) {
            throw new IllegalArgumentException("existingRunId cannot be null or blank");
        }
        if (latestConfig == null) {
            throw new IllegalArgumentException("latestConfig cannot be null");
        }
        String runId = existingRunId.trim();
        Set<String> excludedDates = overwrittenDates != null ? overwrittenDates : Set.of();

        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            RunDetailDto existingDetail = loadRunDetailInternal(conn, runId)
                    .orElseThrow(() -> new IllegalArgumentException("Run not found for merge: " + runId));

            List<MarketEquityPoint> existingPoints = loadMarketPointsInternal(conn, runId, excludedDates);
            List<TradeRecord> existingTrades = loadTradesInternal(conn, runId, excludedDates);

            // Merge market points chronologically by tStart (new overwrites existing on same tStart)
            Map<Long, MarketEquityPoint> mergedPointsByTStart = new LinkedHashMap<>();
            for (MarketEquityPoint mp : existingPoints) {
                mergedPointsByTStart.put(mp.tStart(), mp);
            }
            Set<Long> newMarketTStarts = new LinkedHashSet<>();
            if (newMarketPoints != null) {
                for (MarketEquityPoint mp : newMarketPoints) {
                    mergedPointsByTStart.put(mp.tStart(), mp);
                    newMarketTStarts.add(mp.tStart());
                }
            }
            List<MarketEquityPoint> sortedRawPoints = new ArrayList<>(mergedPointsByTStart.values());
            sortedRawPoints.sort(java.util.Comparator.comparingLong(MarketEquityPoint::tStart));

            // Group existing & new trades by intervalStartSec (new trades replace same-market trades)
            Map<Long, List<TradeRecord>> tradesByMarket = new LinkedHashMap<>();
            for (TradeRecord tr : existingTrades) {
                if (!newMarketTStarts.contains(tr.intervalStartSec())) {
                    tradesByMarket.computeIfAbsent(tr.intervalStartSec(), k -> new ArrayList<>()).add(tr);
                }
            }
            if (newTrades != null) {
                for (TradeRecord tr : newTrades) {
                    tradesByMarket.computeIfAbsent(tr.intervalStartSec(), k -> new ArrayList<>()).add(tr);
                }
            }

            // Deterministic chronological balance & Brier score replay across merged timeline
            double initialCapital = latestConfig.initialCapital();
            double runningBalance = initialCapital;
            double brierSum = 0.0;
            int evaluatedCount = 0;

            List<MarketEquityPoint> finalizedPoints = new ArrayList<>(sortedRawPoints.size());
            List<TradeRecord> finalizedTrades = new ArrayList<>();
            String mergedStartDate = null;
            String mergedEndDate = null;

            for (MarketEquityPoint mp : sortedRawPoints) {
                if (mergedStartDate == null || mp.dateUtc().compareTo(mergedStartDate) < 0) {
                    mergedStartDate = mp.dateUtc();
                }
                if (mergedEndDate == null || mp.dateUtc().compareTo(mergedEndDate) > 0) {
                    mergedEndDate = mp.dateUtc();
                }

                List<TradeRecord> mTrades = tradesByMarket.remove(mp.tStart());
                if (mTrades != null) {
                    for (TradeRecord tr : mTrades) {
                        runningBalance += tr.netPnl();
                        finalizedTrades.add(new TradeRecord(
                                tr.entryTimeUtc(),
                                tr.intervalStartSec(),
                                tr.side(),
                                tr.entryPrice(),
                                tr.exitPrice(),
                                tr.sizeUsd(),
                                tr.shares(),
                                tr.feeUsd(),
                                tr.netPnl(),
                                tr.exitReason(),
                                tr.modelProbability(),
                                tr.marketPrice(),
                                tr.edge(),
                                tr.isWin(),
                                runningBalance
                        ));
                    }
                }

                if (mp.evaluated()) {
                    brierSum += mp.brierDiffSq();
                    evaluatedCount++;
                }
                finalizedPoints.add(new MarketEquityPoint(
                        mp.tStart(),
                        mp.dateUtc(),
                        mp.tradesCount(),
                        mp.winningTrades(),
                        mp.netPnl(),
                        mp.feesUsd(),
                        mp.brierDiffSq(),
                        mp.evaluated(),
                        runningBalance
                ));
            }

            // Preserve any orphan trades not matched to a market point in chronological order
            if (!tradesByMarket.isEmpty()) {
                List<Long> remainingStarts = new ArrayList<>(tradesByMarket.keySet());
                Collections.sort(remainingStarts);
                for (Long tStart : remainingStarts) {
                    for (TradeRecord tr : tradesByMarket.get(tStart)) {
                        runningBalance += tr.netPnl();
                        finalizedTrades.add(new TradeRecord(
                                tr.entryTimeUtc(),
                                tr.intervalStartSec(),
                                tr.side(),
                                tr.entryPrice(),
                                tr.exitPrice(),
                                tr.sizeUsd(),
                                tr.shares(),
                                tr.feeUsd(),
                                tr.netPnl(),
                                tr.exitReason(),
                                tr.modelProbability(),
                                tr.marketPrice(),
                                tr.edge(),
                                tr.isWin(),
                                runningBalance
                        ));
                    }
                }
            }

            if (mergedStartDate == null) {
                mergedStartDate = latestConfig.startDate();
            }
            if (mergedEndDate == null) {
                mergedEndDate = latestConfig.endDate();
            }

            double durationDays = Math.max(
                    1.0,
                    ChronoUnit.DAYS.between(LocalDate.parse(mergedStartDate), LocalDate.parse(mergedEndDate)) + 1.0
            );
            double avgBrier = evaluatedCount > 0 ? (brierSum / evaluatedCount) : 0.25;
            PerformanceMetrics mergedMetrics = metricsCalculator.calculate(
                    finalizedTrades, initialCapital, avgBrier, durationDays);

            String existingLabel = existingDetail.summary().label();
            String autoPrefix = latestConfig.strategyName() + " (";
            String resolvedLabel;
            if (explicitLabel != null && !explicitLabel.isBlank()) {
                resolvedLabel = explicitLabel.trim();
            } else if (existingLabel != null && existingLabel.startsWith(autoPrefix) && existingLabel.endsWith(")")) {
                resolvedLabel = latestConfig.strategyName() + " (" + mergedStartDate + " .. " + mergedEndDate + ")";
            } else {
                resolvedLabel = (existingLabel != null && !existingLabel.isBlank())
                        ? existingLabel
                        : latestConfig.strategyName() + " (" + mergedStartDate + " .. " + mergedEndDate + ")";
            }

            Instant completedAt = Instant.now();
            BacktestRunConfig mergedConfig = new BacktestRunConfig(
                    latestConfig.strategyName(),
                    latestConfig.symbol(),
                    mergedStartDate,
                    mergedEndDate,
                    latestConfig.initialCapital(),
                    latestConfig.latencyFactor(),
                    latestConfig.orderGatewayDelayMs(),
                    latestConfig.binanceDelayMs(),
                    latestConfig.krakenDelayMs(),
                    latestConfig.coinbaseDelayMs(),
                    latestConfig.strategyParams(),
                    latestConfig.strategyOverrides()
            );

            boolean prevAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM backtest_trades WHERE run_id = ?")) {
                    ps.setString(1, runId);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM backtest_market_points WHERE run_id = ?")) {
                    ps.setString(1, runId);
                    ps.executeUpdate();
                }
                String updateRunSql = """
                        UPDATE backtest_runs SET
                            label = ?, start_date = ?, end_date = ?,
                            total_markets = ?, total_trades = ?, winning_trades = ?, losing_trades = ?,
                            win_rate_pct = ?, total_net_pnl = ?, total_fees = ?, gross_pnl = ?, gross_profit = ?, gross_loss = ?,
                            profit_factor = ?, max_drawdown_usd = ?, max_drawdown_pct = ?, expected_value_per_trade = ?,
                            sharpe_ratio = ?, sortino_ratio = ?, brier_score = ?, final_balance = ?,
                            execution_time_utc = ?, duration_ms = ?, config_hash = ?
                        WHERE run_id = ?
                        """;
                try (PreparedStatement ps = conn.prepareStatement(updateRunSql)) {
                    ps.setString(1, resolvedLabel);
                    ps.setString(2, mergedStartDate);
                    ps.setString(3, mergedEndDate);
                    ps.setInt(4, finalizedPoints.size());
                    ps.setInt(5, mergedMetrics.totalTrades());
                    ps.setInt(6, mergedMetrics.winningTrades());
                    ps.setInt(7, mergedMetrics.losingTrades());
                    ps.setDouble(8, mergedMetrics.winRatePct());
                    ps.setDouble(9, mergedMetrics.totalNetPnl());
                    ps.setDouble(10, mergedMetrics.totalFees());
                    ps.setDouble(11, mergedMetrics.grossPnl());
                    ps.setDouble(12, mergedMetrics.grossProfit());
                    ps.setDouble(13, mergedMetrics.grossLoss());
                    ps.setDouble(14, mergedMetrics.profitFactor());
                    ps.setDouble(15, mergedMetrics.maxDrawdownUsd());
                    ps.setDouble(16, mergedMetrics.maxDrawdownPct());
                    ps.setDouble(17, mergedMetrics.expectedValuePerTrade());
                    ps.setDouble(18, mergedMetrics.sharpeRatio());
                    ps.setDouble(19, mergedMetrics.sortinoRatio());
                    ps.setDouble(20, mergedMetrics.brierScore());
                    ps.setDouble(21, mergedMetrics.finalBalance());
                    ps.setString(22, completedAt.toString());
                    ps.setLong(23, durationMs);
                    ps.setString(24, mergedConfig.configHash());
                    ps.setString(25, runId);
                    ps.executeUpdate();
                }

                insertMarketPoints(conn, runId, finalizedPoints);
                insertTrades(conn, runId, finalizedTrades);

                conn.commit();
                if (Log.isInfoEnabled()) {
                    Log.infof("Merged incremental backtest into run %s (%s): span=%s..%s, markets=%d, trades=%d, skippedDays=%d",
                            runId, resolvedLabel, mergedStartDate, mergedEndDate,
                            finalizedPoints.size(), finalizedTrades.size(), skippedDays);
                }
                return new BacktestResult(
                        runId,
                        resolvedLabel,
                        mergedConfig.strategyName(),
                        mergedConfig.symbol(),
                        completedAt,
                        durationMs,
                        finalizedPoints.size(),
                        mergedConfig,
                        mergedMetrics,
                        Collections.unmodifiableList(finalizedPoints),
                        Collections.unmodifiableList(finalizedTrades),
                        true,
                        skippedDays
                );
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException ignored) {
                }
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(prevAutoCommit);
                } catch (SQLException ignored) {
                }
            }
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed merging incremental days into run %s: %s", runId, e.getMessage());
            throw new IllegalStateException("Failed merging incremental days: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Updates the human-readable label of an existing persisted run.
     *
     * @param runId unique run identifier
     * @param newLabel new non-blank label
     * @return {@code true} if the run existed and was updated
     */
    public boolean updateLabel(String runId, String newLabel) {
        if (runId == null || runId.isBlank()) {
            return false;
        }
        if (newLabel == null || newLabel.isBlank()) {
            throw new IllegalArgumentException("label must not be blank");
        }
        String trimmed = newLabel.trim();

        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            try (PreparedStatement ps = conn.prepareStatement("UPDATE backtest_runs SET label = ? WHERE run_id = ?")) {
                ps.setString(1, trimmed);
                ps.setString(2, runId.trim());
                return ps.executeUpdate() > 0;
            }
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed updating label for run %s: %s", runId, e.getMessage());
            throw new IllegalStateException("Failed updating label: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Deletes a persisted backtest run and all its associated market points and trade records.
     *
     * @param runId unique run identifier
     * @return {@code true} if the run existed and was deleted
     */
    public boolean deleteRun(String runId) {
        if (runId == null || runId.isBlank()) {
            return false;
        }
        String trimmed = runId.trim();

        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            boolean prevAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM backtest_trades WHERE run_id = ?")) {
                    ps.setString(1, trimmed);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM backtest_market_points WHERE run_id = ?")) {
                    ps.setString(1, trimmed);
                    ps.executeUpdate();
                }
                int deletedRuns;
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM backtest_runs WHERE run_id = ?")) {
                    ps.setString(1, trimmed);
                    deletedRuns = ps.executeUpdate();
                }
                conn.commit();
                return deletedRuns > 0;
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException ignored) {
                }
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(prevAutoCommit);
                } catch (SQLException ignored) {
                }
            }
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed deleting backtest run %s: %s", runId, e.getMessage());
            throw new IllegalStateException("Failed deleting backtest run: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Deletes all persisted backtest runs together with all associated market points and trade records
     * atomically inside a single DuckDB transaction.
     *
     * @return count of deleted runs
     */
    public int deleteAllRuns() {
        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            boolean prevAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DELETE FROM backtest_trades");
                stmt.executeUpdate("DELETE FROM backtest_market_points");
                int deletedRuns = stmt.executeUpdate("DELETE FROM backtest_runs");
                conn.commit();
                if (Log.isInfoEnabled()) {
                    Log.infof("Deleted all backtest runs from DuckDB history: deletedRuns=%d", deletedRuns);
                }
                return deletedRuns;
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException ignored) {
                }
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(prevAutoCommit);
                } catch (SQLException ignored) {
                }
            }
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed deleting all backtest runs: %s", e.getMessage());
            throw new IllegalStateException("Failed deleting all backtest runs: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Compares one or more persisted runs over an optional UTC date slice at {@code DAY} or {@code MARKET}
     * granularity. Every series in the selected slice is rebased strictly from {@code 0.0} cumulative PnL
     * and quantitative metrics are recomputed for the slice window via {@link PerformanceMetricsCalculator}.
     *
     * @param runIds ordered list of run IDs to compare
     * @param granularity curve aggregation granularity ({@code DAY} or {@code MARKET})
     * @param fromDate optional inclusive start date ({@code yyyy-MM-dd})
     * @param toDate optional inclusive end date ({@code yyyy-MM-dd})
     * @return {@link ComparisonResponse} containing rebased curves, slice metrics, and parameter diffs
     */
    public ComparisonResponse compareRuns(
            List<String> runIds,
            EquityGranularity granularity,
            String fromDate,
            String toDate) {
        if (runIds == null || runIds.isEmpty()) {
            throw new IllegalArgumentException("runIds must contain at least one run ID");
        }
        EquityGranularity resolvedGranularity = granularity != null ? granularity : EquityGranularity.DAY;
        String resolvedFrom = parseOptionalIsoDate(fromDate, "fromDate");
        String resolvedTo = parseOptionalIsoDate(toDate, "toDate");
        if (resolvedFrom != null && resolvedTo != null && LocalDate.parse(resolvedFrom).isAfter(LocalDate.parse(resolvedTo))) {
            throw new IllegalArgumentException("fromDate must not be after toDate");
        }

        List<String> uniqueIds = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (String id : runIds) {
            if (id != null && !id.isBlank() && seen.add(id.trim())) {
                uniqueIds.add(id.trim());
            }
        }
        if (uniqueIds.isEmpty()) {
            throw new IllegalArgumentException("runIds must contain at least one valid run ID");
        }

        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            List<RunDetailDto> details = new ArrayList<>(uniqueIds.size());
            List<ComparedRunSeries> seriesList = new ArrayList<>(uniqueIds.size());
            String globalMinDate = null;
            String globalMaxDate = null;

            for (String runId : uniqueIds) {
                RunDetailDto detail = loadRunDetailInternal(conn, runId)
                        .orElseThrow(() -> new IllegalArgumentException("Run not found: " + runId));
                details.add(detail);

                if (globalMinDate == null || detail.summary().startDate().compareTo(globalMinDate) < 0) {
                    globalMinDate = detail.summary().startDate();
                }
                if (globalMaxDate == null || detail.summary().endDate().compareTo(globalMaxDate) > 0) {
                    globalMaxDate = detail.summary().endDate();
                }

                ComparedRunSeries series = buildComparedSeries(
                        conn, detail, resolvedGranularity, resolvedFrom, resolvedTo);
                seriesList.add(series);
            }

            List<ParameterDiffRow> parameterDiffs = computeParameterDiffs(details);
            return new ComparisonResponse(
                    resolvedGranularity.name(),
                    resolvedFrom,
                    resolvedTo,
                    globalMinDate,
                    globalMaxDate,
                    Collections.unmodifiableList(seriesList),
                    Collections.unmodifiableList(parameterDiffs)
            );
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed comparing backtest runs: %s", e.getMessage());
            throw new IllegalStateException("Failed comparing backtest runs: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Queries executed trades for a persisted run with optional date-slice, side, outcome, and text filters.
     *
     * @param runId target run identifier
     * @param fromDate optional inclusive start date ({@code yyyy-MM-dd})
     * @param toDate optional inclusive end date ({@code yyyy-MM-dd})
     * @param side optional direction filter ({@code UP} or {@code DOWN})
     * @param outcome optional win/loss filter ({@code WIN} or {@code LOSS})
     * @param search optional case-insensitive search substring across exit reason, timestamp, or market epoch
     * @param limit max rows per page (clamped to {@code [1, 1000]})
     * @param offset zero-based pagination offset
     * @return optional {@link TradesPageResponse}, or empty if run ID does not exist
     */
    public Optional<TradesPageResponse> queryTrades(
            String runId,
            String fromDate,
            String toDate,
            String side,
            String outcome,
            String search,
            int limit,
            int offset) {
        if (runId == null || runId.isBlank()) {
            return Optional.empty();
        }
        String resolvedFrom = parseOptionalIsoDate(fromDate, "fromDate");
        String resolvedTo = parseOptionalIsoDate(toDate, "toDate");
        if (resolvedFrom != null && resolvedTo != null && LocalDate.parse(resolvedFrom).isAfter(LocalDate.parse(resolvedTo))) {
            throw new IllegalArgumentException("fromDate must not be after toDate");
        }
        int clampedLimit = Math.min(1000, Math.max(1, limit));
        int clampedOffset = Math.max(0, offset);

        dbLock.lock();
        try {
            Connection conn = ensureConnected();
            if (!runExistsInternal(conn, runId.trim())) {
                return Optional.empty();
            }

            StringBuilder where = new StringBuilder(" WHERE run_id = ?");
            List<Object> params = new ArrayList<>();
            params.add(runId.trim());

            if (resolvedFrom != null) {
                where.append(" AND date_utc >= ?");
                params.add(resolvedFrom);
            }
            if (resolvedTo != null) {
                where.append(" AND date_utc <= ?");
                params.add(resolvedTo);
            }
            if (side != null && !side.isBlank() && !"ALL".equalsIgnoreCase(side.trim())) {
                where.append(" AND UPPER(side) = ?");
                params.add(side.trim().toUpperCase(Locale.ROOT));
            }
            if (outcome != null && !outcome.isBlank() && !"ALL".equalsIgnoreCase(outcome.trim())) {
                String normOutcome = outcome.trim().toUpperCase(Locale.ROOT);
                if ("WIN".equals(normOutcome)) {
                    where.append(" AND is_win = TRUE");
                } else if ("LOSS".equals(normOutcome)) {
                    where.append(" AND is_win = FALSE");
                }
            }
            if (search != null && !search.isBlank()) {
                where.append(" AND (LOWER(exit_reason) LIKE ? OR LOWER(entry_time_utc) LIKE ? OR CAST(interval_start_sec AS VARCHAR) LIKE ?)");
                String pattern = "%" + search.trim().toLowerCase(Locale.ROOT) + "%";
                params.add(pattern);
                params.add(pattern);
                params.add(pattern);
            }

            int totalCount = 0;
            String countSql = "SELECT COUNT(*) FROM backtest_trades" + where;
            try (PreparedStatement ps = conn.prepareStatement(countSql)) {
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        totalCount = rs.getInt(1);
                    }
                }
            }

            String dataSql = """
                    SELECT entry_time_utc, interval_start_sec, side, entry_price, exit_price,
                           size_usd, shares, fee_usd, net_pnl, exit_reason, model_probability,
                           market_price, edge, is_win, balance_after_trade
                    FROM backtest_trades
                    """ + where + " ORDER BY trade_idx ASC LIMIT ? OFFSET ?";
            List<TradeRecord> trades = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(dataSql)) {
                int idx = 1;
                for (Object param : params) {
                    ps.setObject(idx++, param);
                }
                ps.setInt(idx++, clampedLimit);
                ps.setInt(idx, clampedOffset);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        trades.add(mapTradeRow(rs));
                    }
                }
            }

            return Optional.of(new TradesPageResponse(
                    runId.trim(),
                    totalCount,
                    clampedLimit,
                    clampedOffset,
                    Collections.unmodifiableList(trades)
            ));
        } catch (SQLException e) {
            resetConnectionIfFatal(e);
            Log.errorf(e, "Failed querying trades for run %s: %s", runId, e.getMessage());
            throw new IllegalStateException("Failed querying trades: " + e.getMessage(), e);
        } finally {
            dbLock.unlock();
        }
    }

    /**
     * Closes the underlying persistent DuckDB connection cleanly on shutdown.
     */
    @PreDestroy
    @Override
    public void close() {
        dbLock.lock();
        try {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    Log.warnf("Failed closing DuckDB history connection cleanly: %s", e.getMessage());
                } finally {
                    connection = null;
                    schemaInitialized = false;
                }
            }
        } finally {
            dbLock.unlock();
        }
    }

    private Connection ensureConnected() throws SQLException {
        if (connection == null || connection.isClosed()) {
            Path parent = historyDbPath.getParent();
            if (parent != null) {
                try {
                    Files.createDirectories(parent);
                } catch (IOException e) {
                    throw new SQLException("Failed creating directory for DuckDB history file " + parent, e);
                }
            }
            connection = DriverManager.getConnection("jdbc:duckdb:" + historyDbPath);
            schemaInitialized = false;
        }
        if (!schemaInitialized) {
            try {
                initializeSchema(connection);
                schemaInitialized = true;
            } catch (SQLException e) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                } finally {
                    connection = null;
                    schemaInitialized = false;
                }
                throw e;
            }
        }
        return connection;
    }

    private void resetConnectionIfFatal(SQLException e) {
        if (connection == null) {
            schemaInitialized = false;
            return;
        }
        boolean shouldReset = false;
        try {
            shouldReset = connection.isClosed();
        } catch (SQLException ignored) {
            shouldReset = true;
        }
        if (!shouldReset && e != null && e.getMessage() != null) {
            String msg = e.getMessage().toLowerCase(Locale.ROOT);
            if (msg.contains("fatal") || msg.contains("invalidated")) {
                shouldReset = true;
            }
        }
        if (shouldReset) {
            Log.warnf("Resetting invalidated DuckDB history connection after fatal error: %s",
                    e != null ? e.getMessage() : "unknown");
            try {
                connection.close();
            } catch (SQLException ignored) {
            } finally {
                connection = null;
                schemaInitialized = false;
            }
        }
    }

    private void initializeSchema(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS backtest_runs (
                        run_id VARCHAR PRIMARY KEY,
                        label VARCHAR NOT NULL,
                        strategy_name VARCHAR NOT NULL,
                        symbol VARCHAR NOT NULL,
                        start_date VARCHAR NOT NULL,
                        end_date VARCHAR NOT NULL,
                        initial_capital DOUBLE NOT NULL,
                        latency_factor DOUBLE NOT NULL,
                        order_gateway_delay_ms BIGINT NOT NULL DEFAULT 220,
                        binance_delay_ms BIGINT NOT NULL,
                        kraken_delay_ms BIGINT NOT NULL,
                        coinbase_delay_ms BIGINT NOT NULL,
                        strategy_params_json VARCHAR NOT NULL,
                        strategy_overrides_json VARCHAR NOT NULL,
                        total_markets INTEGER NOT NULL,
                        total_trades INTEGER NOT NULL,
                        winning_trades INTEGER NOT NULL,
                        losing_trades INTEGER NOT NULL,
                        win_rate_pct DOUBLE NOT NULL,
                        total_net_pnl DOUBLE NOT NULL,
                        total_fees DOUBLE NOT NULL,
                        gross_pnl DOUBLE NOT NULL,
                        gross_profit DOUBLE NOT NULL,
                        gross_loss DOUBLE NOT NULL,
                        profit_factor DOUBLE NOT NULL,
                        max_drawdown_usd DOUBLE NOT NULL,
                        max_drawdown_pct DOUBLE NOT NULL,
                        expected_value_per_trade DOUBLE NOT NULL,
                        sharpe_ratio DOUBLE NOT NULL,
                        sortino_ratio DOUBLE NOT NULL,
                        brier_score DOUBLE NOT NULL,
                        final_balance DOUBLE NOT NULL,
                        execution_time_utc VARCHAR NOT NULL,
                        duration_ms BIGINT NOT NULL,
                        config_hash VARCHAR
                    )
                    """);
            stmt.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS config_hash VARCHAR");
            stmt.execute("ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS order_gateway_delay_ms BIGINT DEFAULT 220");
            stmt.execute("UPDATE backtest_runs SET order_gateway_delay_ms = 220 WHERE order_gateway_delay_ms IS NULL");
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS backtest_market_points (
                        run_id VARCHAR NOT NULL,
                        market_idx INTEGER NOT NULL,
                        t_start BIGINT NOT NULL,
                        date_utc VARCHAR NOT NULL,
                        trades_count INTEGER NOT NULL,
                        winning_trades INTEGER NOT NULL,
                        net_pnl DOUBLE NOT NULL,
                        fees_usd DOUBLE NOT NULL,
                        brier_diff_sq DOUBLE NOT NULL,
                        evaluated BOOLEAN NOT NULL,
                        balance_after DOUBLE NOT NULL
                    )
                    """);
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS backtest_trades (
                        run_id VARCHAR NOT NULL,
                        trade_idx INTEGER NOT NULL,
                        entry_time_utc VARCHAR NOT NULL,
                        interval_start_sec BIGINT NOT NULL,
                        date_utc VARCHAR NOT NULL,
                        side VARCHAR NOT NULL,
                        entry_price DOUBLE NOT NULL,
                        exit_price DOUBLE NOT NULL,
                        size_usd DOUBLE NOT NULL,
                        shares DOUBLE NOT NULL,
                        fee_usd DOUBLE NOT NULL,
                        net_pnl DOUBLE NOT NULL,
                        exit_reason VARCHAR NOT NULL,
                        model_probability DOUBLE NOT NULL,
                        market_price DOUBLE NOT NULL,
                        edge DOUBLE NOT NULL,
                        is_win BOOLEAN NOT NULL,
                        balance_after_trade DOUBLE NOT NULL
                    )
                    """);
            // Drop secondary ART indexes: DuckDB's columnar min/max zonemaps handle run_id/date_utc scans natively,
            // whereas secondary ART indexes fail on bulk DELETE/UPDATE (e.g. 400+ rows in backtest_trades).
            stmt.execute("DROP INDEX IF EXISTS idx_bt_points_run");
            stmt.execute("DROP INDEX IF EXISTS idx_bt_trades_run");
            stmt.execute("DROP INDEX IF EXISTS idx_bt_runs_config_hash");
        }
        backfillMissingConfigHashes(conn);
    }

    private void backfillMissingConfigHashes(Connection conn) throws SQLException {
        List<Map.Entry<String, String>> updates = new ArrayList<>();
        String selectSql = """
                SELECT run_id, strategy_name, symbol, initial_capital, latency_factor,
                       COALESCE(order_gateway_delay_ms, 220),
                       binance_delay_ms, kraken_delay_ms, coinbase_delay_ms, strategy_params_json
                FROM backtest_runs
                WHERE config_hash IS NULL OR config_hash = ''
                """;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectSql)) {
            while (rs.next()) {
                String runId = rs.getString(1);
                Map<String, Object> params = deserializeMap(rs.getString(10));
                String hash = BacktestRunConfig.computeConfigHash(
                        rs.getString(2),
                        rs.getString(3),
                        rs.getDouble(4),
                        rs.getDouble(5),
                        rs.getLong(6),
                        rs.getLong(7),
                        rs.getLong(8),
                        rs.getLong(9),
                        params
                );
                updates.add(Map.entry(runId, hash));
            }
        }
        if (!updates.isEmpty()) {
            try (PreparedStatement ps = conn.prepareStatement("UPDATE backtest_runs SET config_hash = ? WHERE run_id = ?")) {
                for (Map.Entry<String, String> entry : updates) {
                    ps.setString(1, entry.getValue());
                    ps.setString(2, entry.getKey());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    private static void insertMarketPoints(Connection conn, String runId, List<MarketEquityPoint> marketPoints)
            throws SQLException {
        if (marketPoints == null || marketPoints.isEmpty()) {
            return;
        }
        String insertPointSql = """
                INSERT INTO backtest_market_points (
                    run_id, market_idx, t_start, date_utc, trades_count, winning_trades,
                    net_pnl, fees_usd, brier_diff_sq, evaluated, balance_after
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = conn.prepareStatement(insertPointSql)) {
            for (int i = 0; i < marketPoints.size(); i++) {
                MarketEquityPoint mp = marketPoints.get(i);
                ps.setString(1, runId);
                ps.setInt(2, i);
                ps.setLong(3, mp.tStart());
                ps.setString(4, mp.dateUtc());
                ps.setInt(5, mp.tradesCount());
                ps.setInt(6, mp.winningTrades());
                ps.setDouble(7, mp.netPnl());
                ps.setDouble(8, mp.feesUsd());
                ps.setDouble(9, mp.brierDiffSq());
                ps.setBoolean(10, mp.evaluated());
                ps.setDouble(11, mp.balanceAfter());
                ps.addBatch();
                if ((i + 1) % 2000 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
    }

    private static void insertTrades(Connection conn, String runId, List<TradeRecord> trades)
            throws SQLException {
        if (trades == null || trades.isEmpty()) {
            return;
        }
        String insertTradeSql = """
                INSERT INTO backtest_trades (
                    run_id, trade_idx, entry_time_utc, interval_start_sec, date_utc,
                    side, entry_price, exit_price, size_usd, shares, fee_usd, net_pnl,
                    exit_reason, model_probability, market_price, edge, is_win, balance_after_trade
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = conn.prepareStatement(insertTradeSql)) {
            for (int i = 0; i < trades.size(); i++) {
                TradeRecord tr = trades.get(i);
                String dateUtc = DATE_FMT.format(Instant.ofEpochSecond(tr.intervalStartSec()));
                ps.setString(1, runId);
                ps.setInt(2, i);
                ps.setString(3, tr.entryTimeUtc());
                ps.setLong(4, tr.intervalStartSec());
                ps.setString(5, dateUtc);
                ps.setString(6, tr.side() != null ? tr.side().name() : TradeDirection.UP.name());
                ps.setDouble(7, tr.entryPrice());
                ps.setDouble(8, tr.exitPrice());
                ps.setDouble(9, tr.sizeUsd());
                ps.setDouble(10, tr.shares());
                ps.setDouble(11, tr.feeUsd());
                ps.setDouble(12, tr.netPnl());
                ps.setString(13, tr.exitReason() != null ? tr.exitReason() : "");
                ps.setDouble(14, tr.modelProbability());
                ps.setDouble(15, tr.marketPrice());
                ps.setDouble(16, tr.edge());
                ps.setBoolean(17, tr.isWin());
                ps.setDouble(18, tr.balanceAfterTrade());
                ps.addBatch();
                if ((i + 1) % 2000 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
    }

    private static List<MarketEquityPoint> loadMarketPointsInternal(
            Connection conn,
            String runId,
            Set<String> excludedDates) throws SQLException {
        String sql = """
                SELECT t_start, date_utc, trades_count, winning_trades,
                       net_pnl, fees_usd, brier_diff_sq, evaluated, balance_after
                FROM backtest_market_points
                WHERE run_id = ?
                ORDER BY market_idx ASC
                """;
        List<MarketEquityPoint> points = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String dateUtc = rs.getString(2);
                    if (excludedDates != null && excludedDates.contains(dateUtc)) {
                        continue;
                    }
                    points.add(new MarketEquityPoint(
                            rs.getLong(1),
                            dateUtc,
                            rs.getInt(3),
                            rs.getInt(4),
                            rs.getDouble(5),
                            rs.getDouble(6),
                            rs.getDouble(7),
                            rs.getBoolean(8),
                            rs.getDouble(9)
                    ));
                }
            }
        }
        return points;
    }

    private static List<TradeRecord> loadTradesInternal(
            Connection conn,
            String runId,
            Set<String> excludedDates) throws SQLException {
        String sql = """
                SELECT entry_time_utc, interval_start_sec, side, entry_price, exit_price,
                       size_usd, shares, fee_usd, net_pnl, exit_reason, model_probability,
                       market_price, edge, is_win, balance_after_trade, date_utc
                FROM backtest_trades
                WHERE run_id = ?
                ORDER BY trade_idx ASC
                """;
        List<TradeRecord> trades = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String dateUtc = rs.getString(16);
                    if (excludedDates != null && excludedDates.contains(dateUtc)) {
                        continue;
                    }
                    trades.add(mapTradeRow(rs));
                }
            }
        }
        return trades;
    }

    private ComparedRunSeries buildComparedSeries(
            Connection conn,
            RunDetailDto detail,
            EquityGranularity granularity,
            String fromDate,
            String toDate) throws SQLException {
        String runId = detail.summary().runId();
        double initialCapital = detail.summary().initialCapital();

        StringBuilder pointWhere = new StringBuilder(" WHERE run_id = ?");
        List<Object> params = new ArrayList<>(3);
        params.add(runId);
        if (fromDate != null) {
            pointWhere.append(" AND date_utc >= ?");
            params.add(fromDate);
        }
        if (toDate != null) {
            pointWhere.append(" AND date_utc <= ?");
            params.add(toDate);
        }

        String pointsSql = (granularity == EquityGranularity.DAY)
                ? """
                  SELECT MIN(t_start), date_utc, COUNT(*), SUM(trades_count), SUM(winning_trades),
                         SUM(net_pnl), SUM(fees_usd)
                  FROM backtest_market_points
                  """ + pointWhere + " GROUP BY date_utc ORDER BY date_utc ASC"
                : """
                  SELECT t_start, date_utc, 1, trades_count, winning_trades, net_pnl, fees_usd
                  FROM backtest_market_points
                  """ + pointWhere + " ORDER BY market_idx ASC";

        List<EquityCurvePointDto> points = new ArrayList<>();
        double cumPnl = 0.0;
        double peakEquity = initialCapital;
        String firstDate = null;
        String lastDate = null;
        int sliceMarketsCount = 0;

        try (PreparedStatement ps = conn.prepareStatement(pointsSql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long tStart = rs.getLong(1);
                    String dateUtc = rs.getString(2);
                    int marketsCount = rs.getInt(3);
                    int tradesCount = rs.getInt(4);
                    int winningTrades = rs.getInt(5);
                    double periodPnl = rs.getDouble(6);

                    if (firstDate == null) {
                        firstDate = dateUtc;
                    }
                    lastDate = dateUtc;
                    sliceMarketsCount += marketsCount;

                    cumPnl += periodPnl;
                    double equity = initialCapital + cumPnl;
                    if (equity > peakEquity) {
                        peakEquity = equity;
                    }
                    double ddUsd = peakEquity - equity;
                    double ddPct = peakEquity > 0.0 ? (ddUsd / peakEquity * 100.0) : 0.0;
                    double returnPct = initialCapital > 0.0 ? (cumPnl / initialCapital * 100.0) : 0.0;

                    long timestampMs = (granularity == EquityGranularity.DAY)
                            ? LocalDate.parse(dateUtc).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
                            : tStart * 1000L;
                    String label = (granularity == EquityGranularity.DAY)
                            ? dateUtc
                            : Instant.ofEpochSecond(tStart).toString();

                    points.add(new EquityCurvePointDto(
                            timestampMs,
                            label,
                            dateUtc,
                            cumPnl,
                            returnPct,
                            periodPnl,
                            ddUsd,
                            ddPct,
                            tradesCount,
                            winningTrades,
                            marketsCount
                    ));
                }
            }
        }

        // Compute slice Brier score over evaluated markets in the slice window
        double brierSum = 0.0;
        int evaluatedCount = 0;
        String brierSql = "SELECT COALESCE(SUM(CASE WHEN evaluated THEN brier_diff_sq ELSE 0.0 END), 0.0), " +
                "COALESCE(SUM(CASE WHEN evaluated THEN 1 ELSE 0 END), 0) FROM backtest_market_points" + pointWhere;
        try (PreparedStatement ps = conn.prepareStatement(brierSql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    brierSum = rs.getDouble(1);
                    evaluatedCount = rs.getInt(2);
                }
            }
        }
        double avgBrier = evaluatedCount > 0 ? (brierSum / evaluatedCount) : 0.25;

        // Query trades in the exact same slice window and recompute metrics from zero
        String tradesSql = """
                SELECT entry_time_utc, interval_start_sec, side, entry_price, exit_price,
                       size_usd, shares, fee_usd, net_pnl, exit_reason, model_probability,
                       market_price, edge, is_win, balance_after_trade
                FROM backtest_trades
                """ + pointWhere + " ORDER BY trade_idx ASC";
        List<TradeRecord> sliceTrades = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(tradesSql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sliceTrades.add(mapTradeRow(rs));
                }
            }
        }

        double sliceDays = 1.0;
        if (firstDate != null && lastDate != null) {
            sliceDays = Math.max(1.0, ChronoUnit.DAYS.between(LocalDate.parse(firstDate), LocalDate.parse(lastDate)) + 1.0);
        } else if (fromDate != null && toDate != null) {
            sliceDays = Math.max(1.0, ChronoUnit.DAYS.between(LocalDate.parse(fromDate), LocalDate.parse(toDate)) + 1.0);
        }

        PerformanceMetrics sliceMetrics = metricsCalculator.calculate(sliceTrades, initialCapital, avgBrier, sliceDays);

        return new ComparedRunSeries(
                detail.summary(),
                sliceMarketsCount,
                sliceMetrics,
                Collections.unmodifiableList(points)
        );
    }

    private List<ParameterDiffRow> computeParameterDiffs(List<RunDetailDto> details) {
        if (details.isEmpty()) {
            return List.of();
        }
        List<ParameterDiffRow> rows = new ArrayList<>();

        // Engine parameters
        addDiffRow(rows, "ENGINE", "strategyName", details, d -> d.config().strategyName());
        addDiffRow(rows, "ENGINE", "symbol", details, d -> d.config().symbol());
        addDiffRow(rows, "ENGINE", "startDate", details, d -> d.config().startDate());
        addDiffRow(rows, "ENGINE", "endDate", details, d -> d.config().endDate());
        addDiffRow(rows, "ENGINE", "initialCapital", details, d -> d.config().initialCapital());
        addDiffRow(rows, "ENGINE", "latencyFactor", details, d -> d.config().latencyFactor());
        addDiffRow(rows, "ENGINE", "orderGatewayDelayMs", details, d -> d.config().orderGatewayDelayMs());
        addDiffRow(rows, "ENGINE", "binanceDelayMs", details, d -> d.config().binanceDelayMs());
        addDiffRow(rows, "ENGINE", "krakenDelayMs", details, d -> d.config().krakenDelayMs());
        addDiffRow(rows, "ENGINE", "coinbaseDelayMs", details, d -> d.config().coinbaseDelayMs());

        // Strategy parameters union across all compared runs
        Set<String> strategyKeys = new LinkedHashSet<>();
        for (RunDetailDto detail : details) {
            if (detail.config().strategyParams() != null) {
                strategyKeys.addAll(detail.config().strategyParams().keySet());
            }
        }
        for (String key : strategyKeys) {
            addDiffRow(rows, "STRATEGY", key, details, d ->
                    d.config().strategyParams() != null ? d.config().strategyParams().get(key) : null);
        }

        return Collections.unmodifiableList(rows);
    }

    private static void addDiffRow(
            List<ParameterDiffRow> rows,
            String scope,
            String key,
            List<RunDetailDto> details,
            java.util.function.Function<RunDetailDto, Object> extractor) {
        Map<String, Object> valuesByRunId = new LinkedHashMap<>(details.size());
        Set<String> canonicalValues = new LinkedHashSet<>();
        for (RunDetailDto detail : details) {
            Object val = extractor.apply(detail);
            valuesByRunId.put(detail.summary().runId(), val);
            canonicalValues.add(canonicalValueKey(val));
        }
        boolean differs = canonicalValues.size() > 1;
        rows.add(new ParameterDiffRow(scope, key, differs, Collections.unmodifiableMap(valuesByRunId)));
    }

    private static String canonicalValueKey(Object val) {
        if (val == null) {
            return "<absent>";
        }
        if (val instanceof Number num) {
            double d = num.doubleValue();
            if (Double.isFinite(d) && d == Math.rint(d)) {
                return Long.toString((long) d);
            }
            return Double.toString(d);
        }
        return String.valueOf(val);
    }

    private Optional<RunDetailDto> loadRunDetailInternal(Connection conn, String runId) throws SQLException {
        String sql = """
                SELECT run_id, label, strategy_name, symbol, start_date, end_date,
                       initial_capital, latency_factor, COALESCE(order_gateway_delay_ms, 220) AS order_gateway_delay_ms,
                       binance_delay_ms, kraken_delay_ms, coinbase_delay_ms,
                       strategy_params_json, strategy_overrides_json,
                       total_markets, total_trades, winning_trades, losing_trades,
                       win_rate_pct, total_net_pnl, total_fees, gross_pnl, gross_profit, gross_loss,
                       profit_factor, max_drawdown_usd, max_drawdown_pct, expected_value_per_trade,
                       sharpe_ratio, sortino_ratio, brier_score, final_balance, execution_time_utc, duration_ms
                FROM backtest_runs
                WHERE run_id = ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapDetailRow(rs));
            }
        }
    }

    private boolean runExistsInternal(Connection conn, String runId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM backtest_runs WHERE run_id = ? LIMIT 1")) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private RunSummaryDto mapSummaryRow(ResultSet rs) throws SQLException {
        Map<String, Object> overrides = deserializeMap(rs.getString("strategy_overrides_json"));
        PerformanceMetrics metrics = new PerformanceMetrics(
                rs.getInt("total_trades"),
                rs.getInt("winning_trades"),
                rs.getInt("losing_trades"),
                rs.getDouble("win_rate_pct"),
                rs.getDouble("total_net_pnl"),
                rs.getDouble("total_fees"),
                rs.getDouble("gross_pnl"),
                rs.getDouble("gross_profit"),
                rs.getDouble("gross_loss"),
                rs.getDouble("profit_factor"),
                rs.getDouble("max_drawdown_usd"),
                rs.getDouble("max_drawdown_pct"),
                rs.getDouble("expected_value_per_trade"),
                rs.getDouble("sharpe_ratio"),
                rs.getDouble("sortino_ratio"),
                rs.getDouble("brier_score"),
                rs.getDouble("initial_capital"),
                rs.getDouble("final_balance")
        );
        return new RunSummaryDto(
                rs.getString("run_id"),
                rs.getString("label"),
                rs.getString("strategy_name"),
                rs.getString("symbol"),
                rs.getString("start_date"),
                rs.getString("end_date"),
                rs.getDouble("initial_capital"),
                rs.getDouble("latency_factor"),
                rs.getLong("order_gateway_delay_ms"),
                rs.getLong("binance_delay_ms"),
                rs.getLong("kraken_delay_ms"),
                rs.getLong("coinbase_delay_ms"),
                rs.getInt("total_markets"),
                overrides,
                metrics,
                rs.getString("execution_time_utc"),
                rs.getLong("duration_ms")
        );
    }

    private RunDetailDto mapDetailRow(ResultSet rs) throws SQLException {
        RunSummaryDto summary = mapSummaryRow(rs);
        Map<String, Object> params = deserializeMap(rs.getString("strategy_params_json"));
        BacktestRunConfig config = new BacktestRunConfig(
                summary.strategyName(),
                summary.symbol(),
                summary.startDate(),
                summary.endDate(),
                summary.initialCapital(),
                summary.latencyFactor(),
                summary.orderGatewayDelayMs(),
                summary.binanceDelayMs(),
                summary.krakenDelayMs(),
                summary.coinbaseDelayMs(),
                params,
                summary.strategyOverrides()
        );
        return new RunDetailDto(summary, config);
    }

    private static TradeRecord mapTradeRow(ResultSet rs) throws SQLException {
        String sideRaw = rs.getString(3);
        TradeDirection side = TradeDirection.DOWN.name().equalsIgnoreCase(sideRaw)
                ? TradeDirection.DOWN
                : TradeDirection.UP;
        return new TradeRecord(
                rs.getString(1),
                rs.getLong(2),
                side,
                rs.getDouble(4),
                rs.getDouble(5),
                rs.getDouble(6),
                rs.getDouble(7),
                rs.getDouble(8),
                rs.getDouble(9),
                rs.getString(10),
                rs.getDouble(11),
                rs.getDouble(12),
                rs.getDouble(13),
                rs.getBoolean(14),
                rs.getDouble(15)
        );
    }

    private String serializeMap(Map<String, Object> map) {
        try {
            return objectMapper.writeValueAsString(map != null ? map : Map.of());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed serializing strategy parameter map: " + e.getMessage(), e);
        }
    }

    private Map<String, Object> deserializeMap(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, Object> map = objectMapper.readValue(json, MAP_TYPE);
            return map != null ? Collections.unmodifiableMap(new LinkedHashMap<>(map)) : Map.of();
        } catch (JsonProcessingException e) {
            Log.warnf("Failed deserializing strategy parameter JSON: %s", e.getMessage());
            return Map.of();
        }
    }

    private static String parseOptionalIsoDate(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String trimmed = value.trim();
        try {
            LocalDate.parse(trimmed);
            return trimmed;
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid " + fieldName + " format: " + trimmed, e);
        }
    }

    private static String validateNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be null or blank");
        }
        return value.trim();
    }

    /**
     * Granularity options for aggregating equity curves in comparison queries.
     */
    public enum EquityGranularity {
        DAY,
        MARKET
    }

    /**
     * Persisted backtest run summary displayed in run lists and comparison headers.
     */
    public record RunSummaryDto(
            String runId,
            String label,
            String strategyName,
            String symbol,
            String startDate,
            String endDate,
            double initialCapital,
            double latencyFactor,
            long orderGatewayDelayMs,
            long binanceDelayMs,
            long krakenDelayMs,
            long coinbaseDelayMs,
            int totalMarkets,
            Map<String, Object> strategyOverrides,
            PerformanceMetrics metrics,
            String executionTimeUtc,
            long durationMs
    ) {
    }

    /**
     * Full persisted run detail including complete effective engine and strategy configuration.
     */
    public record RunDetailDto(
            RunSummaryDto summary,
            BacktestRunConfig config
    ) {
    }

    /**
     * Single point on a time-sliced equity curve rebased from zero at the start of the slice.
     */
    public record EquityCurvePointDto(
            long timestampMs,
            String label,
            String dateUtc,
            double cumulativePnlUsd,
            double returnPct,
            double periodPnlUsd,
            double drawdownUsd,
            double drawdownPct,
            int tradesCount,
            int winningTrades,
            int marketsCount
    ) {
    }

    /**
     * Complete time-sliced equity curve and recalculated performance metrics for one compared run.
     */
    public record ComparedRunSeries(
            RunSummaryDto run,
            int sliceMarketsCount,
            PerformanceMetrics sliceMetrics,
            List<EquityCurvePointDto> points
    ) {
    }

    /**
     * Row in the engine and strategy parameter diff matrix across compared runs.
     */
    public record ParameterDiffRow(
            String scope,
            String parameter,
            boolean differs,
            Map<String, Object> valuesByRunId
    ) {
    }

    /**
     * Multi-run comparison payload returned to the UI.
     */
    public record ComparisonResponse(
            String granularity,
            String fromDate,
            String toDate,
            String availableMinDate,
            String availableMaxDate,
            List<ComparedRunSeries> series,
            List<ParameterDiffRow> parameterDiffs
    ) {
    }

    /**
     * Paginated filtered trade list within an optional time slice.
     */
    public record TradesPageResponse(
            String runId,
            int totalCount,
            int limit,
            int offset,
            List<TradeRecord> trades
    ) {
    }
}
