package cz.polymarket.bot.backtest;

import cz.polymarket.bot.backtest.engine.BacktestEngine;
import cz.polymarket.bot.backtest.engine.BacktestResult;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.Instant;
import java.util.Map;

/**
 * REST controller exposing endpoints for backtest engine status checks and simulation execution.
 */
@Path("/backtest")
@Tag(name = "Backtest Engine", description = "Backtest simulation engine status and lifecycle controls")
public class BacktestResource {

    private final BacktestEngine backtestEngine;

    /**
     * Constructs a new {@link BacktestResource} with the injected backtest simulation engine.
     *
     * @param backtestEngine the backtesting engine responsible for orchestrating historical simulations
     */
    @Inject
    public BacktestResource(BacktestEngine backtestEngine) {
        this.backtestEngine = backtestEngine;
    }

    /**
     * Returns the operational readiness status and identification of the simulation backtesting engine.
     *
     * @return engine readiness response object
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get backtest engine status",
            description = "Returns the operational readiness and identification of the in-memory simulation backtesting engine."
    )
    @APIResponse(
            responseCode = "200",
            description = "Backtest engine readiness status",
            content = @Content(schema = @Schema(implementation = BacktestStatusResponse.class))
    )
    public BacktestStatusResponse status() {
        return new BacktestStatusResponse("READY", "backtest", "In-Memory Simulation Engine");
    }

    /**
     * Executes an end-to-end backtest simulation for a specified trading strategy and parameter set.
     *
     * @param request parameter payload containing strategy name, symbol, date range, initial capital, and output dir
     * @return HTTP 200 with backtest performance summary on success, or HTTP 400 on validation/argument error
     */
    @POST
    @Path("/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Execute backtest simulation",
            description = "Executes backtest for a mandatory specified strategy against cached binary market data, computes performance metrics, and exports full trade logs to JSON."
    )
    @APIResponse(
            responseCode = "200",
            description = "Backtest simulation completed successfully",
            content = @Content(schema = @Schema(implementation = BacktestRunResponse.class))
    )
    @APIResponse(
            responseCode = "400",
            description = "Invalid request (missing mandatory strategyName or invalid dataset)"
    )
    public Response runBacktest(BacktestRunRequest request) {
        if (request == null || request.strategyName() == null || request.strategyName().isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "strategyName is mandatory"))
                    .build();
        }

        try {
            BacktestResult result = backtestEngine.runBacktest(
                    request.strategyName(),
                    request.symbol(),
                    request.startDate(),
                    request.endDate(),
                    request.initialCapital(),
                    request.outputDirectory()
            );

            BacktestRunResponse response = new BacktestRunResponse(
                    result.strategyName(),
                    result.symbol(),
                    result.executionTimeUtc(),
                    result.totalMarkets(),
                    result.metrics(),
                    result.trades().size(),
                    result.jsonFilePath()
            );
            return Response.ok(response).build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    /**
     * Response payload representing the operational readiness status of the backtesting engine.
     *
     * @param status status indicator (e.g., READY)
     * @param module submodule identifier
     * @param engine description of the simulation engine
     */
    @Schema(name = "BacktestStatusResponse", description = "Readiness status of the backtesting engine.")
    public record BacktestStatusResponse(
            @Schema(description = "Engine status", example = "READY")
            String status,

            @Schema(description = "Submodule name", example = "backtest")
            String module,

            @Schema(description = "Simulation engine description", example = "In-Memory Simulation Engine")
            String engine
    ) {}

    /**
     * Request payload for launching a backtest simulation run.
     *
     * @param strategyName mandatory name of the registered trading strategy to test
     * @param symbol trading pair symbol (defaults to BTCUSDT if null)
     * @param startDate optional ISO date string (yyyy-MM-dd) defining the simulation start bound
     * @param endDate optional ISO date string (yyyy-MM-dd) defining the simulation end bound
     * @param initialCapital optional starting account balance in USD (defaults to 10000.0)
     * @param outputDirectory optional directory path where exported trade logs and metrics should be saved
     */
    @Schema(name = "BacktestRunRequest", description = "Parameters for executing a backtest simulation run")
    public record BacktestRunRequest(
            @Schema(description = "Mandatory trading strategy name to execute", example = "TWAPArbitrageStrategy", required = true)
            String strategyName,

            @Schema(description = "Optional symbol to evaluate (default BTCUSDT)", example = "BTCUSDT")
            String symbol,

            @Schema(description = "Optional start date filter (yyyy-MM-dd)", example = "2026-08-07")
            String startDate,

            @Schema(description = "Optional end date filter (yyyy-MM-dd)", example = "2026-10-06")
            String endDate,

            @Schema(description = "Optional initial capital in USD", example = "10000.0")
            Double initialCapital,

            @Schema(description = "Optional directory to store exported JSON results", example = "D:/Crypto/data/Polymarket/backtesting")
            String outputDirectory
    ) {}

    /**
     * Response payload returning summary performance results and output file reference for a completed backtest run.
     *
     * @param strategyName name of the strategy evaluated
     * @param symbol evaluated market asset pair
     * @param executionTimeUtc timestamp when the simulation finished
     * @param totalMarkets count of processed 15-minute market intervals
     * @param metrics calculated financial and risk metrics (PnL, Sharpe ratio, win rate, etc.)
     * @param tradeCount count of executed trades
     * @param jsonFilePath absolute path to the generated JSON report file
     */
    @Schema(name = "BacktestRunResponse", description = "Backtest simulation run results including performance metrics and output file location")
    public record BacktestRunResponse(
            @Schema(description = "Executed strategy name", example = "TWAPArbitrageStrategy")
            String strategyName,

            @Schema(description = "Evaluated symbol", example = "BTCUSDT")
            String symbol,

            @Schema(description = "Execution timestamp", example = "2026-10-03T11:45:00Z")
            Instant executionTimeUtc,

            @Schema(description = "Total number of market intervals evaluated", example = "5100")
            int totalMarkets,

            @Schema(description = "Summary performance metrics")
            PerformanceMetrics metrics,

            @Schema(description = "Total number of trades executed", example = "2801")
            int tradeCount,

            @Schema(description = "Absolute path to the exported JSON file", example = "D:/Crypto/data/Polymarket/backtesting/backtest_TWAPArbitrageStrategy_20261003_114500.json")
            String jsonFilePath
    ) {}
}
