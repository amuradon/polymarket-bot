package cz.polymarket.bot.backtest;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;

@Path("/backtest")
public class BacktestResource {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> status() {
        return Map.of(
                "status", "READY",
                "module", "backtest",
                "engine", "In-Memory Simulation Engine"
        );
    }
}
