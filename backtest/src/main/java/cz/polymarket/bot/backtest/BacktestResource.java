package cz.polymarket.bot.backtest;

import com.fasterxml.jackson.annotation.JsonInclude;
import cz.polymarket.bot.backtest.data.BacktestJob;
import cz.polymarket.bot.backtest.data.BacktestJobStatus;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.service.BacktestJobManager;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.strategy.StrategyRegistry;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;

/**
 * REST controller exposing endpoints for backtest engine status checks and asynchronous simulation job execution.
 */
@Path("/api")
@Tag(name = "Backtest Engine", description = "Backtest simulation engine status, asynchronous execution, and lifecycle controls")
public class BacktestResource {

    private final BacktestJobManager jobManager;
    private final StrategyRegistry strategyRegistry;

    /**
     * Constructs a new {@link BacktestResource} with the injected job manager and strategy registry.
     *
     * @param jobManager orchestrator for asynchronous backtesting job execution
     * @param strategyRegistry registry resolving requested trading strategies
     */
    @Inject
    public BacktestResource(BacktestJobManager jobManager, StrategyRegistry strategyRegistry) {
        this.jobManager = Objects.requireNonNull(jobManager, "jobManager must not be null");
        this.strategyRegistry = Objects.requireNonNull(strategyRegistry, "strategyRegistry must not be null");
    }

    /**
     * Returns the operational readiness status and identification of the simulation backtesting engine.
     *
     * @return engine readiness response object
     */
    @GET
    @Path("/status")
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
     * Triggers asynchronous backtest simulation execution after validating request parameters.
     *
     * @param request parameter payload containing strategy name, symbol, date range, initial capital, and output dir
     * @return HTTP 202 with job identifier upon successful validation and queuing, or HTTP 400 on validation error
     */
    @POST
    @Path("/backtest/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Trigger asynchronous backtest simulation",
            description = "Validates request inputs and enqueues an asynchronous simulation job, returning HTTP 202 with the assigned job ID."
    )
    @RequestBody(
            description = "Parameters specifying trading strategy, asset pair, and date intervals for simulation",
            required = true,
            content = @Content(schema = @Schema(implementation = BacktestRunRequest.class))
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "202",
                    description = "Backtest job successfully validated and queued for background execution",
                    content = @Content(schema = @Schema(implementation = BacktestJobSubmitResponse.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Invalid request payload (missing mandatory strategyName, unknown strategy, or invalid dates)"
            )
    })
    public Response runBacktest(BacktestRunRequest request) {
        if (request == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Request body must not be null"))
                    .build();
        }

        if (request.strategyName() == null || request.strategyName().isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "strategyName is mandatory"))
                    .build();
        }

        try {
            strategyRegistry.getStrategy(request.strategyName());
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }

        if (request.startDate() != null && !request.startDate().isBlank()) {
            try {
                LocalDate.parse(request.startDate());
            } catch (DateTimeParseException e) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Invalid startDate format: " + request.startDate()))
                        .build();
            }
        }

        if (request.endDate() != null && !request.endDate().isBlank()) {
            try {
                LocalDate.parse(request.endDate());
            } catch (DateTimeParseException e) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Invalid endDate format: " + request.endDate()))
                        .build();
            }
        }

        if (request.startDate() != null && request.endDate() != null
                && !request.startDate().isBlank() && !request.endDate().isBlank()) {
            LocalDate start = LocalDate.parse(request.startDate());
            LocalDate end = LocalDate.parse(request.endDate());
            if (start.isAfter(end)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "startDate must not be after endDate"))
                        .build();
            }
        }

        if (request.initialCapital() != null && request.initialCapital() <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "initialCapital must be greater than 0"))
                    .build();
        }

        BacktestJob job = jobManager.submitJob(request);

        BacktestJobSubmitResponse response = new BacktestJobSubmitResponse(
                job.jobId(),
                job.status().name(),
                request.strategyName(),
                request.symbol() != null ? request.symbol() : "BTCUSDT",
                job.createdAt().toString()
        );
        return Response.status(Response.Status.ACCEPTED).entity(response).build();
    }

    /**
     * Retrieves the current progress, execution status, and results of a backtest simulation job.
     *
     * @param jobId UUID string identifying the backtest job
     * @return HTTP 200 with job status and result file path if completed, or HTTP 404 if not found
     */
    @GET
    @Path("/backtest/run/{jobId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get backtest job status",
            description = "Returns current progress and lifecycle state for a specific backtest job, including the file system path of the exported results once completed."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Backtest job found and status returned",
                    content = @Content(schema = @Schema(implementation = BacktestJobStatusResponse.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Backtest job ID not found"
            )
    })
    public Response getJobStatus(
            @Parameter(description = "UUID of the backtest job", required = true, example = "a1b2c3d4-...")
            @PathParam("jobId") String jobId
    ) {
        return jobManager.getJob(jobId)
                .map(job -> {
                    String resultPath = (job.result() != null) ? job.result().jsonFilePath() : null;
                    Integer totalMarkets = (job.result() != null) ? job.result().totalMarkets() : null;
                    Integer tradeCount = (job.result() != null) ? job.result().trades().size() : null;
                    PerformanceMetrics metrics = (job.result() != null) ? job.result().metrics() : null;
                    String completedAtStr = (job.completedAt() != null) ? job.completedAt().toString() : null;

                    BacktestJobStatusResponse response = new BacktestJobStatusResponse(
                            job.jobId(),
                            job.status().name(),
                            job.request().strategyName(),
                            job.request().symbol() != null ? job.request().symbol() : "BTCUSDT",
                            resultPath,
                            resultPath,
                            job.error(),
                            job.createdAt().toString(),
                            completedAtStr,
                            totalMarkets,
                            tradeCount,
                            metrics
                    );
                    return Response.ok(response).build();
                })
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Job not found: " + jobId))
                        .build());
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
     * Response payload returned when an asynchronous backtest simulation job is accepted and queued.
     *
     * @param jobId unique identifier of the submitted backtest job
     * @param status lifecycle state (QUEUED)
     * @param strategyName evaluated trading strategy name
     * @param symbol cryptocurrency asset pair
     * @param createdAt timestamp when the job was accepted
     */
    @Schema(name = "BacktestJobSubmitResponse", description = "Response returned when a backtest simulation job is accepted.")
    public record BacktestJobSubmitResponse(
            @Schema(description = "Unique identifier of the backtest job", example = "a1b2c3d4-...")
            String jobId,

            @Schema(description = "Initial lifecycle status of the job", example = "QUEUED")
            String status,

            @Schema(description = "Trading strategy to evaluate", example = "TWAPArbitrageStrategy")
            String strategyName,

            @Schema(description = "Cryptocurrency symbol evaluated", example = "BTCUSDT")
            String symbol,

            @Schema(description = "Timestamp when the job was accepted", example = "2026-10-09T12:00:00Z")
            String createdAt
    ) {}

    /**
     * Response payload returning current progress, lifecycle status, and output file reference for a backtest job.
     *
     * @param jobId unique identifier of the backtest job
     * @param status lifecycle status: QUEUED, IN_PROGRESS, COMPLETED, or FAILED
     * @param strategyName evaluated strategy name
     * @param symbol evaluated cryptocurrency symbol
     * @param resultFilePath absolute file system path to the exported JSON file when completed
     * @param jsonFilePath absolute file system path to the exported JSON file (alias)
     * @param error error message if the job failed
     * @param createdAt timestamp when the job was submitted
     * @param completedAt timestamp when the job finished execution
     * @param totalMarkets count of processed market intervals
     * @param tradeCount count of executed trades
     * @param metrics summary performance metrics
     */
    @Schema(name = "BacktestJobStatusResponse", description = "Current progress, lifecycle status, and results of a backtest job.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record BacktestJobStatusResponse(
            @Schema(description = "Unique identifier of the backtest job", example = "a1b2c3d4-...")
            String jobId,

            @Schema(description = "Lifecycle status: QUEUED, IN_PROGRESS, COMPLETED, or FAILED", example = "COMPLETED")
            String status,

            @Schema(description = "Strategy evaluated", example = "TWAPArbitrageStrategy")
            String strategyName,

            @Schema(description = "Cryptocurrency symbol evaluated", example = "BTCUSDT")
            String symbol,

            @Schema(description = "File system path to the exported JSON results file when completed",
                    example = "D:/Crypto/data/Polymarket/backtesting/backtest_TWAPArbitrageStrategy_20261003_114500.json")
            String resultFilePath,

            @Schema(description = "File system path to the exported JSON results file (alias)",
                    example = "D:/Crypto/data/Polymarket/backtesting/backtest_TWAPArbitrageStrategy_20261003_114500.json")
            String jsonFilePath,

            @Schema(description = "Error description if execution failed", example = "No market rows available")
            String error,

            @Schema(description = "Timestamp when the job was submitted", example = "2026-10-09T12:00:00Z")
            String createdAt,

            @Schema(description = "Timestamp when the job finished execution", example = "2026-10-09T12:01:00Z")
            String completedAt,

            @Schema(description = "Total number of market intervals evaluated", example = "5100")
            Integer totalMarkets,

            @Schema(description = "Total number of executed trades", example = "2801")
            Integer tradeCount,

            @Schema(description = "Summary performance metrics")
            PerformanceMetrics metrics
    ) {}
}
