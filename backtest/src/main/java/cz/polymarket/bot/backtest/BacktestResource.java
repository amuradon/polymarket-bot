package cz.polymarket.bot.backtest;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/backtest")
@Tag(name = "Backtest Engine", description = "Backtest simulation engine status and lifecycle controls")
public class BacktestResource {

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

    @Schema(name = "BacktestStatusResponse", description = "Readiness status of the backtesting engine.")
    public record BacktestStatusResponse(
            @Schema(description = "Engine status", example = "READY")
            String status,

            @Schema(description = "Submodule name", example = "backtest")
            String module,

            @Schema(description = "Simulation engine description", example = "In-Memory Simulation Engine")
            String engine
    ) {}
}
