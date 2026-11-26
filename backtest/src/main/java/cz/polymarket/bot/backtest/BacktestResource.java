package cz.polymarket.bot.backtest;

import com.fasterxml.jackson.annotation.JsonInclude;
import cz.polymarket.bot.backtest.data.BacktestJob;
import cz.polymarket.bot.backtest.data.BacktestRunRequest;
import cz.polymarket.bot.backtest.data.DatasetSymbolSummary;
import cz.polymarket.bot.backtest.data.ExchangeDelays;
import cz.polymarket.bot.backtest.data.StrategyMetadata;
import cz.polymarket.bot.backtest.indicator.IndicatorCacheService;
import cz.polymarket.bot.backtest.service.BacktestJobManager;
import cz.polymarket.bot.backtest.service.CollectorParquetDatasetService;
import cz.polymarket.bot.backtest.service.StrategyParameterService;
import cz.polymarket.bot.calculator.PerformanceMetrics;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * REST controller exposing endpoints for backtest engine status checks, strategy/dataset metadata,
 * asynchronous queued simulation job execution, live progress polling, and cooperative cancellation.
 */
@Path("/api")
@Tag(name = "Backtest Engine", description = "Backtest simulation engine status, metadata, asynchronous execution, and lifecycle controls")
public class BacktestResource {

    private final BacktestJobManager jobManager;
    private final StrategyParameterService strategyParameterService;
    private final CollectorParquetDatasetService datasetService;
    private final IndicatorCacheService indicatorCacheService;
    private final String defaultStrategy;
    private final String defaultStartDate;
    private final double defaultInitialCapital;
    private final double defaultLatencyFactor;

    /**
     * Constructs a new {@link BacktestResource} with injected services and default engine settings.
     *
     * @param jobManager orchestrator for queued asynchronous backtesting job execution
     * @param strategyParameterService service introspecting and validating strategy parameters
     * @param datasetService dataset discovery service for available symbols and date bounds
     * @param indicatorCacheService indicator cache service exposing default exchange feed delays
     * @param defaultStrategy default trading strategy name configured in application properties
     * @param defaultStartDate default inclusive start date ({@code yyyy-MM-dd}) configured in application properties
     * @param defaultInitialCapital default initial capital in USD
     * @param defaultLatencyFactor default order execution latency multiplier
     */
    @Inject
    public BacktestResource(
            BacktestJobManager jobManager,
            StrategyParameterService strategyParameterService,
            CollectorParquetDatasetService datasetService,
            IndicatorCacheService indicatorCacheService,
            @ConfigProperty(name = "polymarket.backtest.strategy", defaultValue = "OraclePulseLockStrategy")
            String defaultStrategy,
            @ConfigProperty(name = "polymarket.backtest.from", defaultValue = "2026-10-12")
            String defaultStartDate,
            @ConfigProperty(name = "polymarket.backtest.initial-capital", defaultValue = "10000.0")
            double defaultInitialCapital,
            @ConfigProperty(name = "polymarket.backtest.latency-factor", defaultValue = "1.0")
            double defaultLatencyFactor) {
        if (jobManager == null) {
            Log.fatal("Fatal initialization error in BacktestResource: jobManager must not be null");
            throw new NullPointerException("jobManager must not be null");
        }
        if (strategyParameterService == null) {
            Log.fatal("Fatal initialization error in BacktestResource: strategyParameterService must not be null");
            throw new NullPointerException("strategyParameterService must not be null");
        }
        if (datasetService == null) {
            Log.fatal("Fatal initialization error in BacktestResource: datasetService must not be null");
            throw new NullPointerException("datasetService must not be null");
        }
        if (indicatorCacheService == null) {
            Log.fatal("Fatal initialization error in BacktestResource: indicatorCacheService must not be null");
            throw new NullPointerException("indicatorCacheService must not be null");
        }
        this.jobManager = jobManager;
        this.strategyParameterService = strategyParameterService;
        this.datasetService = datasetService;
        this.indicatorCacheService = indicatorCacheService;
        this.defaultStrategy = (defaultStrategy != null && !defaultStrategy.isBlank())
                ? defaultStrategy.trim()
                : "OraclePulseLockStrategy";
        this.defaultStartDate = (defaultStartDate != null && !defaultStartDate.isBlank())
                ? defaultStartDate.trim()
                : "2026-10-12";
        this.defaultInitialCapital = defaultInitialCapital;
        this.defaultLatencyFactor = defaultLatencyFactor;
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
     * Returns available registered strategies with parameter schemas, discovered collector symbols/date bounds,
     * and default engine configuration values for the Web UI.
     *
     * @return metadata response payload
     */
    @GET
    @Path("/backtest/metadata")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get backtest metadata",
            description = "Returns available strategies with parameter metadata, discovered collector datasets, and default engine settings."
    )
    @APIResponse(
            responseCode = "200",
            description = "Metadata returned",
            content = @Content(schema = @Schema(implementation = BacktestMetadataResponse.class))
    )
    public BacktestMetadataResponse getMetadata() {
        List<StrategyMetadata> strategies = strategyParameterService.listStrategies();
        List<DatasetSymbolSummary> datasets = datasetService.discoverAvailableDatasets();
        ExchangeDelays delays = indicatorCacheService.getDefaultExchangeDelays();
        return new BacktestMetadataResponse(
                defaultStrategy,
                "BTCUSDT",
                defaultStartDate,
                defaultInitialCapital,
                defaultLatencyFactor,
                delays,
                strategies,
                datasets
        );
    }

    /**
     * Triggers asynchronous backtest simulation execution after validating strategy, parameters,
     * dates, and engine configuration.
     *
     * @param request parameter payload containing strategy name, overrides, symbol, date range, and engine settings
     * @return HTTP 202 with job &amp; run identifiers upon successful validation and queuing, or HTTP 400 on validation error
     */
    @POST
    @Path("/backtest/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Trigger asynchronous backtest simulation",
            description = "Validates request inputs and enqueues an asynchronous simulation job, returning HTTP 202 with assigned job and run IDs."
    )
    @RequestBody(
            description = "Parameters specifying trading strategy, parameter overrides, asset pair, date intervals, and engine settings",
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
                    description = "Invalid request payload (unknown strategy, invalid strategy param, or invalid engine/date config)"
            )
    })
    public Response runBacktest(BacktestRunRequest request) {
        if (request == null) {
            Log.error("Cannot start backtest run: request body must not be null");
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Request body must not be null"))
                    .build();
        }

        String resolvedStrategyName = (request.strategyName() != null && !request.strategyName().isBlank())
                ? request.strategyName().trim()
                : defaultStrategy;

        try {
            strategyParameterService.instantiateStrategy(resolvedStrategyName, request.strategyParams());
        } catch (IllegalArgumentException e) {
            Log.errorf("Cannot start backtest run: %s", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }

        String resolvedSymbol = (request.symbol() != null && !request.symbol().isBlank())
                ? request.symbol().trim().toUpperCase(Locale.ROOT)
                : "BTCUSDT";

        String resolvedStartDate;
        String resolvedEndDate;
        if (Boolean.TRUE.equals(request.fullRange())) {
            var boundsOpt = datasetService.resolveDateBounds(resolvedSymbol);
            if (boundsOpt.isEmpty()) {
                String msg = "No market rows available for backtest for symbol " + resolvedSymbol;
                Log.errorf("Cannot start backtest run: %s", msg);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", msg))
                        .build();
            }
            resolvedStartDate = boundsOpt.get().minDate();
            resolvedEndDate = boundsOpt.get().maxDate();
        } else {
            resolvedStartDate = (request.startDate() != null && !request.startDate().isBlank())
                    ? request.startDate().trim()
                    : defaultStartDate;
            resolvedEndDate = (request.endDate() != null && !request.endDate().isBlank())
                    ? request.endDate().trim()
                    : LocalDate.now(ZoneOffset.UTC).toString();

            LocalDate start;
            try {
                start = LocalDate.parse(resolvedStartDate);
            } catch (DateTimeParseException e) {
                Log.errorf("Cannot start backtest run: invalid startDate format '%s'", resolvedStartDate);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Invalid startDate format: " + resolvedStartDate))
                        .build();
            }

            LocalDate end;
            try {
                end = LocalDate.parse(resolvedEndDate);
            } catch (DateTimeParseException e) {
                Log.errorf("Cannot start backtest run: invalid endDate format '%s'", resolvedEndDate);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Invalid endDate format: " + resolvedEndDate))
                        .build();
            }

            if (start.isAfter(end)) {
                Log.errorf("Cannot start backtest run: startDate %s must not be after endDate %s",
                        resolvedStartDate, resolvedEndDate);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "startDate must not be after endDate"))
                        .build();
            }
        }

        if (request.initialCapital() != null && (Double.isNaN(request.initialCapital()) || Double.isInfinite(request.initialCapital()) || request.initialCapital() <= 0)) {
            Log.errorf("Cannot start backtest run: initialCapital must be greater than 0 (received %s)",
                    request.initialCapital());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "initialCapital must be greater than 0"))
                    .build();
        }

        if (request.latencyFactor() != null && (Double.isNaN(request.latencyFactor()) || Double.isInfinite(request.latencyFactor()) || request.latencyFactor() < 0.0)) {
            Log.errorf("Cannot start backtest run: latencyFactor must be non-negative (received %s)",
                    request.latencyFactor());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "latencyFactor must be non-negative"))
                    .build();
        }

        if (request.binanceDelayMs() != null && request.binanceDelayMs() < 0L) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "binanceDelayMs must be non-negative"))
                    .build();
        }
        if (request.krakenDelayMs() != null && request.krakenDelayMs() < 0L) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "krakenDelayMs must be non-negative"))
                    .build();
        }
        if (request.coinbaseDelayMs() != null && request.coinbaseDelayMs() < 0L) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "coinbaseDelayMs must be non-negative"))
                    .build();
        }

        String explicitLabel = (request.label() != null && !request.label().isBlank())
                ? request.label().trim()
                : null;
        String displayLabel = explicitLabel != null
                ? explicitLabel
                : resolvedStrategyName + " (" + resolvedStartDate + " .. " + resolvedEndDate + ")";

        BacktestRunRequest resolvedRequest = new BacktestRunRequest(
                explicitLabel,
                resolvedStrategyName,
                resolvedSymbol,
                resolvedStartDate,
                resolvedEndDate,
                Boolean.TRUE.equals(request.fullRange()),
                request.initialCapital(),
                request.latencyFactor(),
                request.binanceDelayMs(),
                request.krakenDelayMs(),
                request.coinbaseDelayMs(),
                request.strategyParams(),
                Boolean.TRUE.equals(request.forceNewRun()),
                Boolean.TRUE.equals(request.overwriteExistingDays())
        );
        BacktestJob job = jobManager.submitJob(resolvedRequest);
        if (Log.isInfoEnabled()) {
            Log.infof("Accepted backtest run request: jobId=%s, runId=%s, strategy=%s, symbol=%s",
                    job.jobId(), job.runId(), resolvedStrategyName, resolvedSymbol);
        }

        BacktestJobSubmitResponse response = new BacktestJobSubmitResponse(
                job.jobId(),
                job.runId(),
                job.status().name(),
                displayLabel,
                resolvedStrategyName,
                resolvedSymbol,
                job.createdAt().toString()
        );
        return Response.status(Response.Status.ACCEPTED).entity(response).build();
    }

    /**
     * Retrieves the current progress, execution status, and summary metrics of a backtest simulation job.
     *
     * @param jobId UUID string identifying the backtest job
     * @return HTTP 200 with job status and progress, or HTTP 404 if not found
     */
    @GET
    @Path("/backtest/run/{jobId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get backtest job status",
            description = "Returns current progress and lifecycle state for a specific backtest job."
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
                .map(job -> Response.ok(toJobStatusResponse(job)).build())
                .orElseGet(() -> {
                    Log.warnf("Requested backtest job not found: %s", jobId);
                    return Response.status(Response.Status.NOT_FOUND)
                            .entity(Map.of("error", "Job not found: " + jobId))
                            .build();
                });
    }

    /**
     * Lists all queued, active, and recent backtest simulation jobs ordered newest first.
     *
     * @return list of {@link BacktestJobStatusResponse}
     */
    @GET
    @Path("/backtest/jobs")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "List backtest jobs",
            description = "Returns queued, active, completed, failed, and cancelled backtest jobs ordered newest first."
    )
    public List<BacktestJobStatusResponse> listJobs() {
        return jobManager.listJobs().stream()
                .map(BacktestResource::toJobStatusResponse)
                .toList();
    }

    /**
     * Requests cancellation of a queued or currently running backtest job.
     *
     * @param jobId UUID string identifying the backtest job
     * @return HTTP 200 with updated job status, or HTTP 404 if not found
     */
    @POST
    @Path("/backtest/run/{jobId}/cancel")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Cancel a backtest job",
            description = "Cancels a queued job immediately or signals cooperative cancellation for an active job."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Cancellation processed",
                    content = @Content(schema = @Schema(implementation = BacktestJobStatusResponse.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Backtest job ID not found"
            )
    })
    public Response cancelJob(@PathParam("jobId") String jobId) {
        return jobManager.cancelJob(jobId)
                .map(job -> Response.ok(toJobStatusResponse(job)).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Job not found: " + jobId))
                        .build());
    }

    private static BacktestJobStatusResponse toJobStatusResponse(BacktestJob job) {
        Integer totalMarkets = (job.result() != null)
                ? job.result().totalMarkets()
                : (job.totalMarkets() > 0 ? job.totalMarkets() : null);
        Integer tradeCount = (job.result() != null && job.result().trades() != null)
                ? job.result().trades().size()
                : null;
        PerformanceMetrics metrics = (job.result() != null) ? job.result().metrics() : null;
        String completedAtStr = (job.completedAt() != null) ? job.completedAt().toString() : null;
        Long durationMs = job.durationMs() > 0 ? job.durationMs() : null;

        if (Log.isDebugEnabled()) {
            Log.debugf("Queried backtest job %s: status=%s, totalMarkets=%s, tradeCount=%s",
                    job.jobId(), job.status(), totalMarkets, tradeCount);
        }

        return new BacktestJobStatusResponse(
                job.jobId(),
                job.runId(),
                job.label(),
                job.status().name(),
                job.request().strategyName(),
                job.request().symbol() != null ? job.request().symbol() : "BTCUSDT",
                job.error(),
                job.createdAt().toString(),
                completedAtStr,
                durationMs,
                job.totalDays(),
                job.completedDays(),
                totalMarkets,
                job.completedMarkets(),
                job.currentDate(),
                job.progressPct(),
                job.mergedIntoExisting(),
                job.skippedDays(),
                tradeCount,
                metrics
        );
    }

    /**
     * Response payload representing the operational readiness status of the backtesting engine.
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
     * Metadata payload describing available strategies, datasets, and default engine settings.
     */
    @Schema(name = "BacktestMetadataResponse", description = "Available strategies, datasets, and engine defaults.")
    public record BacktestMetadataResponse(
            String defaultStrategy,
            String defaultSymbol,
            String defaultStartDate,
            double defaultInitialCapital,
            double defaultLatencyFactor,
            ExchangeDelays defaultExchangeDelays,
            List<StrategyMetadata> strategies,
            List<DatasetSymbolSummary> datasets
    ) {}

    /**
     * Response payload returned when an asynchronous backtest simulation job is accepted and queued.
     */
    @Schema(name = "BacktestJobSubmitResponse", description = "Response returned when a backtest simulation job is accepted.")
    public record BacktestJobSubmitResponse(
            @Schema(description = "Unique identifier of the backtest job", examples = "a1b2c3d4-...")
            String jobId,
            @Schema(description = "Persistent run identifier stored in DuckDB", examples = "f1e2d3c4-...")
            String runId,
            @Schema(description = "Initial lifecycle status of the job", examples = "QUEUED")
            String status,
            @Schema(description = "Human-readable run label")
            String label,
            @Schema(description = "Trading strategy to evaluate", examples = "OraclePulseLockStrategy")
            String strategyName,
            @Schema(description = "Cryptocurrency symbol evaluated", examples = "BTCUSDT")
            String symbol,
            @Schema(description = "Timestamp when the job was accepted", examples = "2026-10-09T12:00:00Z")
            String createdAt
    ) {}

    /**
     * Response payload returning current progress, lifecycle status, and summary metrics for a backtest job.
     */
    @Schema(name = "BacktestJobStatusResponse", description = "Current progress, lifecycle status, and results of a backtest job.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record BacktestJobStatusResponse(
            String jobId,
            String runId,
            String label,
            String status,
            String strategyName,
            String symbol,
            String error,
            String createdAt,
            String completedAt,
            Long durationMs,
            int totalDays,
            int completedDays,
            Integer totalMarkets,
            int completedMarkets,
            String currentDate,
            double progressPct,
            boolean mergedIntoExisting,
            int skippedDays,
            Integer tradeCount,
            PerformanceMetrics metrics
    ) {}
}
