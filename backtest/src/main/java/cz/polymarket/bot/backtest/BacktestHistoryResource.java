package cz.polymarket.bot.backtest;

import cz.polymarket.bot.backtest.service.BacktestHistoryRepository;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.ComparisonResponse;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.EquityGranularity;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.RunDetailDto;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.RunSummaryDto;
import cz.polymarket.bot.backtest.service.BacktestHistoryRepository.TradesPageResponse;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * REST controller exposing persisted DuckDB backtest run history, time-sliced multi-run equity
 * comparison with parameter diffing, label management, deletion, and paginated trade inspection.
 */
@Path("/api")
@Tag(name = "Backtest History & Comparison", description = "Persisted DuckDB backtest runs, equity curve comparison, parameter diffs, and trade logs")
public class BacktestHistoryResource {

    private final BacktestHistoryRepository historyRepository;

    /**
     * Constructs a {@link BacktestHistoryResource} backed by the persistent DuckDB history repository.
     *
     * @param historyRepository persistent DuckDB history repository
     */
    @Inject
    public BacktestHistoryResource(BacktestHistoryRepository historyRepository) {
        if (historyRepository == null) {
            Log.fatal("Fatal initialization error in BacktestHistoryResource: historyRepository must not be null");
            throw new NullPointerException("historyRepository must not be null");
        }
        this.historyRepository = historyRepository;
    }

    /**
     * Lists persisted backtest runs ordered newest first.
     *
     * @param strategy optional strategy name filter
     * @param symbol optional symbol filter
     * @return list of {@link RunSummaryDto}
     */
    @GET
    @Path("/backtest/runs")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List persisted backtest runs", description = "Returns persisted runs from DuckDB ordered newest first.")
    public List<RunSummaryDto> listRuns(
            @QueryParam("strategy") String strategy,
            @QueryParam("symbol") String symbol) {
        return historyRepository.listRuns(strategy, symbol);
    }

    /**
     * Loads full metadata and resolved configuration for a single persisted run.
     *
     * @param runId unique run identifier
     * @return HTTP 200 with {@link RunDetailDto}, or HTTP 404 if not found
     */
    @GET
    @Path("/backtest/runs/{runId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get persisted backtest run detail", description = "Returns summary metrics and full resolved engine/strategy configuration for a run.")
    public Response getRun(@PathParam("runId") String runId) {
        return historyRepository.getRun(runId)
                .map(detail -> Response.ok(detail).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Run not found: " + runId))
                        .build());
    }

    /**
     * Updates the human-readable label of a persisted backtest run.
     *
     * @param runId unique run identifier
     * @param request body containing the new label
     * @return HTTP 200 with updated run summary, HTTP 400 on blank label, or HTTP 404 if not found
     */
    @PATCH
    @Path("/backtest/runs/{runId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Rename backtest run label", description = "Updates the custom label of a persisted run in DuckDB.")
    public Response updateLabel(@PathParam("runId") String runId, UpdateLabelRequest request) {
        if (request == null || request.label() == null || request.label().isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "label must not be blank"))
                    .build();
        }
        boolean updated = historyRepository.updateLabel(runId, request.label());
        if (!updated) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", "Run not found: " + runId))
                    .build();
        }
        return historyRepository.getRun(runId)
                .map(detail -> Response.ok(detail.summary()).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
    }

    /**
     * Deletes a persisted backtest run and all associated market points and trade records.
     *
     * @param runId unique run identifier
     * @return HTTP 204 if deleted, or HTTP 404 if not found
     */
    @DELETE
    @Path("/backtest/runs/{runId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Delete backtest run", description = "Deletes a persisted run and all its equity points and trades from DuckDB.")
    public Response deleteRun(@PathParam("runId") String runId) {
        boolean deleted = historyRepository.deleteRun(runId);
        if (!deleted) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("error", "Run not found: " + runId))
                    .build();
        }
        return Response.noContent().build();
    }

    /**
     * Compares one or more persisted runs across an optional UTC date slice at {@code DAY} or {@code MARKET}
     * granularity, rebasing every equity series from zero at the start of the slice and computing
     * slice metrics plus parameter differences.
     *
     * @param request comparison request payload
     * @return HTTP 200 with {@link ComparisonResponse}, or HTTP 400 on validation error
     */
    @POST
    @Path("/backtest/compare")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Compare backtest runs",
            description = "Builds rebased equity curves (per day or per market), recalculated slice metrics, and parameter diffs."
    )
    public Response compareRuns(CompareRequest request) {
        if (request == null || request.runIds() == null || request.runIds().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "runIds must contain at least one run ID"))
                    .build();
        }
        EquityGranularity granularity = EquityGranularity.DAY;
        if (request.granularity() != null && !request.granularity().isBlank()) {
            try {
                granularity = EquityGranularity.valueOf(request.granularity().trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Invalid granularity: " + request.granularity() + " (expected DAY or MARKET)"))
                        .build();
            }
        }
        try {
            ComparisonResponse response = historyRepository.compareRuns(
                    request.runIds(), granularity, request.fromDate(), request.toDate());
            return Response.ok(response).build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    /**
     * Queries paginated and filtered executed trades for a persisted run within an optional UTC date slice.
     *
     * @param runId unique run identifier
     * @param fromDate optional inclusive start date ({@code yyyy-MM-dd})
     * @param toDate optional inclusive end date ({@code yyyy-MM-dd})
     * @param side optional side filter ({@code ALL}, {@code UP}, {@code DOWN})
     * @param outcome optional outcome filter ({@code ALL}, {@code WIN}, {@code LOSS})
     * @param search optional search substring
     * @param limit page size (default 100)
     * @param offset zero-based offset (default 0)
     * @return HTTP 200 with {@link TradesPageResponse}, HTTP 400 on invalid dates, or HTTP 404 if run not found
     */
    @GET
    @Path("/backtest/runs/{runId}/trades")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Query trades of a backtest run", description = "Returns paginated and filtered trades for a persisted run within an optional date slice.")
    public Response queryTrades(
            @PathParam("runId") String runId,
            @QueryParam("fromDate") String fromDate,
            @QueryParam("toDate") String toDate,
            @QueryParam("side") @DefaultValue("ALL") String side,
            @QueryParam("outcome") @DefaultValue("ALL") String outcome,
            @QueryParam("search") String search,
            @QueryParam("limit") @DefaultValue("100") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        try {
            return historyRepository.queryTrades(runId, fromDate, toDate, side, outcome, search, limit, offset)
                    .<Response>map(page -> Response.ok(page).build())
                    .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                            .entity(Map.of("error", "Run not found: " + runId))
                            .build());
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    /**
     * Request body for updating a run's label.
     *
     * @param label new non-blank label
     */
    @Schema(name = "UpdateLabelRequest", description = "Payload for renaming a persisted backtest run.")
    public record UpdateLabelRequest(String label) {}

    /**
     * Request body for comparing one or more backtest runs.
     *
     * @param runIds ordered list of persisted run IDs
     * @param granularity curve aggregation granularity ({@code DAY} or {@code MARKET})
     * @param fromDate optional inclusive slice start date ({@code yyyy-MM-dd})
     * @param toDate optional inclusive slice end date ({@code yyyy-MM-dd})
     */
    @Schema(name = "CompareRequest", description = "Parameters for comparing equity curves and metrics across runs.")
    public record CompareRequest(
            List<String> runIds,
            String granularity,
            String fromDate,
            String toDate
    ) {}
}
