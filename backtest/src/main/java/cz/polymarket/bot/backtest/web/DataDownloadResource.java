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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@Path("/api/1/data/download")
public class DataDownloadResource {

    private final DataDownloadJobManager jobManager;

    @Inject
    public DataDownloadResource(DataDownloadJobManager jobManager) {
        this.jobManager = jobManager;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
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
    public Response getJobStatus(@PathParam("jobId") String jobId) {
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

    public record DownloadApiRequest(
            String symbol,
            String start,
            String end,
            Set<String> dataTypes
    ) {}
}
