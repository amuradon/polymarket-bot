package cz.polymarket.bot.backtest;

import com.fasterxml.jackson.annotation.JsonInclude;
import cz.polymarket.bot.backtest.data.BacktestJob;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.service.BacktestJobManager;
import cz.polymarket.bot.calculator.PerformanceMetrics;
import cz.polymarket.bot.strategy.StrategyRegistry;
import io.quarkus.logging.Log;
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
        if (jobManager == null) {
            Log.fatal("Fatal initialization error in BacktestResource: jobManager must not be null");
            throw new NullPointerException("jobManager must not be null");
        }
        if (strategyRegistry == null) {
            Log.fatal("Fatal initialization error in BacktestResource: strategyRegistry must not be null");
            throw new NullPointerException("strategyRegistry must not be null");
        }
        this.jobManager = jobManager;
        this.strategyRegistry = strategyRegistry;
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
        if (Log.isDebugEnabled()) {
            Log.debug("Backtest engine status queried: READY");
        }
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
            Log.error("Cannot start backtest run: request body must not be null");
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Request body must not be null"))
                    .build();
        }

        if (request.strategyName() == null || request.strategyName().isBlank()) {
            Log.error("Cannot start backtest run: strategyName is mandatory");
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "strategyName is mandatory"))
                    .build();
        }

        try {
            strategyRegistry.getStrategy(request.strategyName());
        } catch (IllegalArgumentException e) {
            Log.errorf("Cannot start backtest run: %s", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }

        if (request.startDate() != null && !request.startDate().isBlank()) {
            try {
                LocalDate.parse(request.startDate());
            } catch (DateTimeParseException e) {
                Log.errorf("Cannot start backtest run: invalid startDate format '%s'", request.startDate());
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Invalid startDate format: " + request.startDate()))
                        .build();
            }
        }

        if (request.endDate() != null && !request.endDate().isBlank()) {
            try {
                LocalDate.parse(request.endDate());
            } catch (DateTimeParseException e) {
                Log.errorf("Cannot start backtest run: invalid endDate format '%s'", request.endDate());
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
                Log.errorf("Cannot start backtest run: startDate %s must not be after endDate %s",
                        request.startDate(), request.endDate());
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "startDate must not be after endDate"))
                        .build();
            }
        }

        if (request.initialCapital() != null && request.initialCapital() <= 0) {
            Log.errorf("Cannot start backtest run: initialCapital must be greater than 0 (received %s)",
                    request.initialCapital());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "initialCapital must be greater than 0"))
                    .build();
        }

        if (request.latencyFactor() != null && (request.latencyFactor().isNaN() || request.latencyFactor() < 0.0)) {
            Log.errorf("Cannot start backtest run: latencyFactor must be non-negative (received %s)",
                    request.latencyFactor());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "latencyFactor must be non-negative"))
                    .build();
        }

        BacktestJob job = jobManager.submitJob(request);
        String resolvedSymbol = request.symbol() != null ? request.symbol() : "BTCUSDT";
        if (Log.isInfoEnabled()) {
            Log.infof("Accepted backtest run request: jobId=%s, strategy=%s, symbol=%s",
                    job.jobId(), request.strategyName(), resolvedSymbol);
        }

        BacktestJobSubmitResponse response = new BacktestJobSubmitResponse(
                job.jobId(),
                job.status().name(),
                request.strategyName(),
                resolvedSymbol,
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
            @Parameter(description = "UUID of the backtest job", required = true, examples = @org.eclipse.microprofile.openapi.annotations.media.ExampleObject(value = "a1b2c3d4-..."))
            @PathParam("jobId") String jobId
    ) {
        return jobManager.getJob(jobId)
                .map(job -> {
                    String resultPath = (job.result() != null) ? job.result().jsonFilePath() : null;
                    Integer totalMarkets = (job.result() != null) ? job.result().totalMarkets() : null;
                    Integer tradeCount = (job.result() != null) ? job.result().trades().size() : null;
                    PerformanceMetrics metrics = (job.result() != null) ? job.result().metrics() : null;
                    String completedAtStr = (job.completedAt() != null) ? job.completedAt().toString() : null;

                    if (Log.isDebugEnabled()) {
                        Log.debugf("Queried backtest job %s: status=%s, totalMarkets=%s, tradeCount=%s",
                                job.jobId(), job.status(), totalMarkets, tradeCount);
                    }

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
                .orElseGet(() -> {
                    Log.warnf("Requested backtest job not found: %s", jobId);
                    return Response.status(Response.Status.NOT_FOUND)
                            .entity(Map.of("error", "Job not found: " + jobId))
                            .build();
                });
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
            @Schema(description = "Engine status", examples = "READY")
            String status,

            @Schema(description = "Submodule name", examples = "backtest")
            String module,

            @Schema(description = "Simulation engine description", examples = "In-Memory Simulation Engine")
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
            @Schema(description = "Unique identifier of the backtest job", examples = "a1b2c3d4-...")
            String jobId,

            @Schema(description = "Initial lifecycle status of the job", examples = "QUEUED")
            String status,

            @Schema(description = "Trading strategy to evaluate", examples = "TWAPArbitrageStrategy")
            String strategyName,

            @Schema(description = "Cryptocurrency symbol evaluated", examples = "BTCUSDT")
            String symbol,

            @Schema(description = "Timestamp when the job was accepted", examples = "2026-10-09T12:00:00Z")
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
            @Schema(description = "Unique identifier of the backtest job", examples = "a1b2c3d4-...")
            String jobId,

            @Schema(description = "Lifecycle status: QUEUED, IN_PROGRESS, COMPLETED, or FAILED", examples = "COMPLETED")
            String status,

            @Schema(description = "Strategy evaluated", examples = "TWAPArbitrageStrategy")
            String strategyName,

            @Schema(description = "Cryptocurrency symbol evaluated", examples = "BTCUSDT")
            String symbol,

            @Schema(description = "File system path to the exported JSON results file when completed",
                    examples = "D:/Crypto/data/Polymarket/backtesting/backtest_TWAPArbitrageStrategy_20261003_114500.json")
            String resultFilePath,

            @Schema(description = "File system path to the exported JSON results file (alias)",
                    examples = "D:/Crypto/data/Polymarket/backtesting/backtest_TWAPArbitrageStrategy_20261003_114500.json")
            String jsonFilePath,

            @Schema(description = "Error description if execution failed", examples = "No market rows available")
            String error,

            @Schema(description = "Timestamp when the job was submitted", examples = "2026-10-09T12:00:00Z")
            String createdAt,

            @Schema(description = "Timestamp when the job finished execution", examples = "2026-10-09T12:01:00Z")
            String completedAt,

            @Schema(description = "Total number of market intervals evaluated", examples = "5100")
            Integer totalMarkets,

            @Schema(description = "Total number of executed trades", examples = "2801")
            Integer tradeCount,

            @Schema(description = "Summary performance metrics")
            PerformanceMetrics metrics
    ) {}
}
