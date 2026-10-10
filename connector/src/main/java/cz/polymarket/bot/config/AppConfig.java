package cz.polymarket.bot.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import java.net.http.HttpClient;
import java.time.Duration;

/**
 * CDI configuration producer for shared HTTP and JSON serialization clients across connectors.
 */
@ApplicationScoped
public class AppConfig {

    /**
     * Produces a shared, thread-safe {@link HttpClient} with a 10-second connection timeout.
     *
     * @return configured HTTP client instance
     */
    @Produces
    @ApplicationScoped
    public HttpClient httpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Produces a shared Jackson {@link ObjectMapper} instance.
     *
     * @return Jackson object mapper
     */
    @Produces
    @ApplicationScoped
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
