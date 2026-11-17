package cz.polymarket.bot.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CDI configuration producer for shared HTTP and JSON serialization clients across connectors.
 * Uses a bounded daemon thread pool for {@link HttpClient} async TLS/HTTP2 tasks to prevent
 * unbounded worker thread churn and context-switch overhead.
 */
@ApplicationScoped
public class AppConfig {

    private static final int HTTP_CLIENT_POOL_THREADS = 2;

    private final ExecutorService httpExecutor;

    /**
     * Initializes the bounded daemon executor backing the shared {@link HttpClient}.
     */
    public AppConfig() {
        AtomicInteger idx = new AtomicInteger(0);
        this.httpExecutor = Executors.newFixedThreadPool(HTTP_CLIENT_POOL_THREADS, runnable ->
                Thread.ofPlatform()
                        .daemon(true)
                        .priority(Thread.NORM_PRIORITY)
                        .name("polymarket-http-io-" + idx.getAndIncrement())
                        .unstarted(runnable));
    }

    @PreDestroy
    void destroy() {
        httpExecutor.shutdownNow();
    }

    /**
     * Produces a shared, thread-safe {@link HttpClient} with a 10-second connection timeout
     * and a bounded 2-thread daemon executor.
     *
     * @return configured HTTP client instance
     */
    @Produces
    @ApplicationScoped
    public HttpClient httpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .executor(httpExecutor)
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
