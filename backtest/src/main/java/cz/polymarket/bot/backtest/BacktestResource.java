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

@Path("/backtest")
@Tag(name = "Backtest Engine", description = "Backtest simulation engine status and lifecycle controls")
public class BacktestResource {

    private final BacktestEngine backtestEngine;

    @Inject
    public BacktestResource(BacktestEngine backtestEngine) {
        this.backtestEngine = backtestEngine;
    }

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

    @POST
    @Path("/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Execute backtest simulation",
            description = "Executes backtest for a mandatory specified strategy against historical parquet data, computes performance metrics, and exports full trade logs to JSON."
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
                    request.datasetPath(),
                    request.initialCapital(),
                    request.outputDirectory()
            );

            BacktestRunResponse response = new BacktestRunResponse(
                    result.strategyName(),
                    result.datasetPath(),
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

    @Schema(name = "BacktestStatusResponse", description = "Readiness status of the backtesting engine.")
    public record BacktestStatusResponse(
            @Schema(description = "Engine status", example = "READY")
            String status,

            @Schema(description = "Submodule name", example = "backtest")
            String module,

            @Schema(description = "Simulation engine description", example = "In-Memory Simulation Engine")
            String engine
    ) {}

    @Schema(name = "BacktestRunRequest", description = "Parameters for executing a backtest simulation run")
    public record BacktestRunRequest(
            @Schema(description = "Mandatory trading strategy name to execute", example = "TWAPArbitrageStrategy", required = true)
            String strategyName,

            @Schema(description = "Optional path to the Parquet dataset file", example = "D:/Polymarket/btc_nextCandle/unified_market_data.parquet")
            String datasetPath,

            @Schema(description = "Optional initial capital in USD", example = "10000.0")
            Double initialCapital,

            @Schema(description = "Optional directory to store exported JSON results", example = "D:/Crypto/data/Polymarket/backtesting")
            String outputDirectory
    ) {}

    @Schema(name = "BacktestRunResponse", description = "Backtest simulation run results including performance metrics and output file location")
    public record BacktestRunResponse(
            @Schema(description = "Executed strategy name", example = "TWAPArbitrageStrategy")
            String strategyName,

            @Schema(description = "Dataset path used", example = "D:/Polymarket/btc_nextCandle/unified_market_data.parquet")
            String datasetPath,

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
