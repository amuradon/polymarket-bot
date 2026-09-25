package cz.polymarket.bot.backtest.openapi;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

import java.net.URI;

/**
 * Convenient redirect endpoint from /swagger-ui to /q/swagger-ui/.
 */
@Path("/swagger-ui")
public class SwaggerUiRedirectResource {

    @GET
    @Operation(hidden = true, summary = "Redirect to /q/swagger-ui/")
    @APIResponse(responseCode = "303", description = "Redirect to the active Swagger UI interface")
    public Response redirectToSwaggerUi() {
        return Response.seeOther(URI.create("/q/swagger-ui/")).build();
    }
}
