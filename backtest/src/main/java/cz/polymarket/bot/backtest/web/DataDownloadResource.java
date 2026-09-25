package cz.polymarket.bot.backtest.web;

import cz.polymarket.bot.backtest.data.DataType;
import cz.polymarket.bot.backtest.data.DownloadJob;
import cz.polymarket.bot.backtest.data.DownloadRequest;
import cz.polymarket.bot.backtest.service.DataDownloadJobManager;
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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("/api/1/data/download")
@Tag(name = "Data Ingestion", description = "Operations for triggering and monitoring Binance historical data downloads")
public class DataDownloadResource {

    private final DataDownloadJobManager jobManager;

    @Inject
    public DataDownloadResource(DataDownloadJobManager jobManager) {
        this.jobManager = jobManager;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Trigger historical market data download",
            description = "Queues an asynchronous ingestion job to download daily Binance spot aggTrades, futures aggTrades, and/or futures orderbook into local storage."
    )
    @RequestBody(
            description = "Download parameters specifying symbol, date range, and optional data types",
            required = true,
            content = @Content(schema = @Schema(implementation = DownloadApiRequest.class))
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "202",
                    description = "Download job successfully queued for execution",
                    content = @Content(schema = @Schema(implementation = DownloadJobResponse.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Invalid request payload or date format",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            )
    })
    public Response triggerDownload(DownloadApiRequest request) {
        if (request == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Request body must not be null"))
                    .build();
        }

        Set<DataType> parsedTypes = null;
        if (request.dataTypes() != null && !request.dataTypes().isEmpty()) {
            parsedTypes = new HashSet<>();
            for (String typeStr : request.dataTypes()) {
                try {
                    parsedTypes.add(DataType.fromString(typeStr));
                } catch (IllegalArgumentException e) {
                    return Response.status(Response.Status.BAD_REQUEST)
                            .entity(Map.of("error", e.getMessage()))
                            .build();
                }
            }
        }

        DownloadRequest downloadRequest;
        try {
            downloadRequest = DownloadRequest.of(request.symbol(), request.start(), request.end(), parsedTypes);
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }

        DownloadJob job = jobManager.submitJob(downloadRequest);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("jobId", job.jobId());
        response.put("status", job.status().name());
        response.put("symbol", downloadRequest.symbol());
        response.put("startDate", downloadRequest.startDate().toString());
        response.put("endDate", downloadRequest.endDate().toString());
        response.put("dataTypes", downloadRequest.dataTypes().stream().map(DataType::getKey).toList());

        return Response.status(Response.Status.ACCEPTED).entity(response).build();
    }

    @GET
    @Path("/{jobId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get download job progress status",
            description = "Fetches the current lifecycle state, counters (downloaded, skipped, failed), and errors for a specific download job."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Job found and status returned",
                    content = @Content(schema = @Schema(implementation = DownloadJobStatusResponse.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Job ID not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))
            )
    })
    public Response getJobStatus(
            @Parameter(description = "UUID of the download job", required = true, example = "b9f5e1a2-...")
            @PathParam("jobId") String jobId
    ) {
        return jobManager.getJob(jobId)
                .map(job -> {
                    Map<String, Object> response = new LinkedHashMap<>();
                    response.put("jobId", job.jobId());
                    response.put("status", job.status().name());
                    response.put("symbol", job.request().symbol());
                    response.put("startDate", job.request().startDate().toString());
                    response.put("endDate", job.request().endDate().toString());
                    response.put("downloadedFiles", job.downloadedFiles());
                    response.put("skippedFiles", job.skippedFiles());
                    response.put("failedFiles", job.failedFiles());
                    response.put("errors", job.errors());
                    response.put("createdAt", job.createdAt().toString());
                    if (job.completedAt() != null) {
                        response.put("completedAt", job.completedAt().toString());
                    }
                    return Response.ok(response).build();
                })
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Job not found: " + jobId))
                        .build());
    }

    @Schema(name = "DownloadApiRequest", description = "Payload specifying parameters for downloading Binance historical daily market data.")
    public record DownloadApiRequest(
            @Schema(description = "Trading symbol to download", example = "BTCUSDT", required = true)
            String symbol,

            @Schema(description = "Start date in yyyy-MM-dd format (inclusive)", example = "2026-08-01", required = true)
            String start,

            @Schema(description = "End date in yyyy-MM-dd format (inclusive)", example = "2026-08-05", required = true)
            String end,

            @Schema(description = "Set of data types to download: spot_trades, futures_trades, orderbook. If omitted, downloads all types.",
                    example = "[\"spot_trades\", \"futures_trades\", \"orderbook\"]")
            Set<String> dataTypes
    ) {}

    @Schema(name = "DownloadJobResponse", description = "Response returned when a download job is accepted and queued.")
    public record DownloadJobResponse(
            @Schema(description = "Unique identifier of the download job", example = "b9f5e1a2-...")
            String jobId,
            @Schema(description = "Current lifecycle status of the job", example = "QUEUED")
            String status,
            @Schema(description = "Target cryptocurrency trading symbol", example = "BTCUSDT")
            String symbol,
            @Schema(description = "Start date (inclusive) in yyyy-MM-dd format", example = "2026-08-01")
            String startDate,
            @Schema(description = "End date (inclusive) in yyyy-MM-dd format", example = "2026-08-05")
            String endDate,
            @Schema(description = "List of data types being ingested", example = "[\"spot_trades\", \"futures_trades\", \"orderbook\"]")
            List<String> dataTypes
    ) {}

    @Schema(name = "DownloadJobStatusResponse", description = "Current progress and metrics of a download job.")
    public record DownloadJobStatusResponse(
            @Schema(description = "Unique identifier of the download job", example = "b9f5e1a2-...")
            String jobId,
            @Schema(description = "Lifecycle status: QUEUED, IN_PROGRESS, COMPLETED, or FAILED", example = "COMPLETED")
            String status,
            @Schema(description = "Target cryptocurrency trading symbol", example = "BTCUSDT")
            String symbol,
            @Schema(description = "Start date in yyyy-MM-dd format", example = "2026-08-01")
            String startDate,
            @Schema(description = "End date in yyyy-MM-dd format", example = "2026-08-05")
            String endDate,
            @Schema(description = "Count of successfully downloaded and extracted files", example = "24")
            int downloadedFiles,
            @Schema(description = "Count of existing files skipped due to idempotency", example = "0")
            int skippedFiles,
            @Schema(description = "Count of failed file downloads", example = "0")
            int failedFiles,
            @Schema(description = "List of error messages encountered during ingestion", example = "[]")
            List<String> errors,
            @Schema(description = "Timestamp when the job was created", example = "2026-09-25T09:00:00Z")
            String createdAt,
            @Schema(description = "Timestamp when the job finished execution (if completed)", example = "2026-09-25T09:05:00Z")
            String completedAt
    ) {}

    @Schema(name = "ErrorResponse", description = "Error details payload.")
    public record ErrorResponse(
            @Schema(description = "Description of the error", example = "symbol must not be blank")
            String error
    ) {}
}
