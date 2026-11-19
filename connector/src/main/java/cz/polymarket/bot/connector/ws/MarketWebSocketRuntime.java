package cz.polymarket.bot.connector.ws;

import cz.polymarket.bot.connector.os.LinuxCpuAffinity;
import io.netty.handler.ssl.OpenSsl;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.dns.AddressResolverOptions;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Shared single-EventLoop native {@code epoll} / BoringSSL runtime servicing all market-data WebSocket
 * connectors ({@code PolymarketClobClient}, {@code PolymarketPolyBoltClient}, {@code BinanceStreamConnector},
 * {@code KrakenStreamConnector}, and {@code CoinbaseStreamConnector}).
 * <p>
 * Consolidates all market WebSocket file descriptors onto a single {@code epoll_wait} loop
 * ({@code market-ws-io-reader}) pinned exclusively to {@code CPU #1} on 4-core Linux hosts while pinning
 * the single asynchronous DNS worker thread to {@code CPU #0}. Provides sibling-aware IPv4 pool resolution
 * so dual Active-Active connections ({@code PRIMARY} and {@code BACKUP}) always connect to distinct
 * CDN/exchange edge IPv4 addresses on startup and across reconnections while preserving TLS SNI,
 * X.509 hostname verification, and HTTP {@code Host} headers.
 */
@ApplicationScoped
public class MarketWebSocketRuntime {

    /** Dedicated thread name assigned to the consolidated market-data Netty EventLoop on {@code CPU #1}. */
    public static final String DEDICATED_MARKET_IO_THREAD_NAME = "market-ws-io-reader";

    /** Target physical CPU index on 4-vCPU hosts ({@code c8gn.xlarge}) for market WebSocket I/O reading. */
    public static final int MARKET_IO_CPU_INDEX = 1;

    private static final int HOUSEKEEPING_CPU_INDEX = 0;

    private record CachedIpv4Pool(List<String> sortedIpv4s, long expiresAtNanoTime) {
    }

    private final Vertx marketVertx;
    private final long dnsCacheTtlNs;
    private final ConcurrentHashMap<String, CachedIpv4Pool> ipv4PoolCache = new ConcurrentHashMap<>();
    private volatile Thread ioReaderThread;

    /**
     * Package-private no-arg constructor required exclusively by Quarkus ArC client proxies.
     * Allocates zero threads and zero Vert.x resources so no ghost EventLoop is created on {@code CPU #1}.
     */
    MarketWebSocketRuntime() {
        this.marketVertx = null;
        this.dnsCacheTtlNs = 0L;
    }

    /**
     * Constructs and initializes the shared 1-thread market WebSocket {@link Vertx} runtime,
     * pinning the single blocking DNS worker to {@code CPU #0} and the single EventLoop thread
     * ({@link #DEDICATED_MARKET_IO_THREAD_NAME}) exclusively to {@code CPU #1}.
     *
     * @param dnsCacheTtlSec TTL in seconds for cached sorted IPv4 {@code A}-record pools (default 30s)
     */
    @Inject
    public MarketWebSocketRuntime(
            @ConfigProperty(name = "polymarket.connector.dns.cache-ttl-sec", defaultValue = "30")
            long dnsCacheTtlSec) {
        this.dnsCacheTtlNs = TimeUnit.SECONDS.toNanos(Math.max(1L, dnsCacheTtlSec));
        this.marketVertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(1)
                .setWorkerPoolSize(1)
                .setInternalBlockingPoolSize(1)
                .setPreferNativeTransport(true)
                .setAddressResolverOptions(new AddressResolverOptions()
                        .setRoundRobinInetAddress(true)
                        .setRotateServers(true)
                        .setCacheMaxTimeToLive(60)));

        // Eagerly initialize and pin the single worker thread to CPU #0 before CPU #1 EventLoop work runs,
        // preventing lazy worker threads spawned during DNS lookups from inheriting CPU #1 affinity.
        CountDownLatch workerInitLatch = new CountDownLatch(1);
        this.marketVertx.executeBlocking(() -> {
            try {
                Thread worker = Thread.currentThread();
                worker.setPriority(Thread.NORM_PRIORITY);
                LinuxCpuAffinity.pinCurrentThreadIfSupported(HOUSEKEEPING_CPU_INDEX, "vCPU #0: Market DNS Worker");
            } catch (SecurityException ignored) {
            } finally {
                workerInitLatch.countDown();
            }
            return null;
        }, false);

        CountDownLatch ioInitLatch = new CountDownLatch(1);
        this.marketVertx.runOnContext(v -> {
            try {
                Thread current = Thread.currentThread();
                current.setName(DEDICATED_MARKET_IO_THREAD_NAME);
                current.setPriority(Thread.MAX_PRIORITY);
                LinuxCpuAffinity.pinCurrentThreadIfSupported(MARKET_IO_CPU_INDEX, "vCPU #1: Market WS I/O");
                this.ioReaderThread = current;
            } catch (SecurityException ignored) {
                this.ioReaderThread = Thread.currentThread();
            } finally {
                ioInitLatch.countDown();
            }
        });
        try {
            workerInitLatch.await(2L, TimeUnit.SECONDS);
            ioInitLatch.await(2L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!this.marketVertx.isNativeTransportEnabled()) {
            Log.warnf("Native Linux Epoll transport is unavailable on MarketWebSocketRuntime (os=%s); falling back to JDK NIO EventLoop",
                    System.getProperty("os.name", "unknown"));
        } else {
            Log.infof("Initialized consolidated MarketWebSocketRuntime on thread '%s' with native Linux Epoll transport (openSslAvailable=%b)",
                    DEDICATED_MARKET_IO_THREAD_NAME, OpenSsl.isAvailable());
        }
    }

    /**
     * Closes the underlying shared market {@link Vertx} runtime on application shutdown.
     */
    @PreDestroy
    public void destroy() {
        ipv4PoolCache.clear();
        if (marketVertx != null) {
            try {
                marketVertx.close().toCompletionStage().toCompletableFuture().get(3L, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Returns the consolidated 1-EventLoop {@link Vertx} instance for market WebSocket clients.
     *
     * @return market {@link Vertx} instance
     */
    public Vertx vertx() {
        return marketVertx;
    }

    /**
     * Returns the dedicated Netty EventLoop thread ({@code market-ws-io-reader}) servicing market WebSocket I/O.
     *
     * @return dedicated market I/O reader thread
     */
    public Thread getIoReaderThread() {
        return ioReaderThread;
    }

    /**
     * Returns whether native Linux {@code epoll} transport is active on this runtime.
     *
     * @return {@code true} if native transport is enabled
     */
    public boolean isNativeTransportEnabled() {
        return marketVertx != null && marketVertx.isNativeTransportEnabled();
    }

    /**
     * Returns whether native OpenSSL/BoringSSL ({@code netty-tcnative}) is available on the classpath.
     *
     * @return {@code true} if native OpenSSL/BoringSSL is available
     */
    public boolean isOpenSslAvailable() {
        return OpenSsl.isAvailable();
    }

    /**
     * Asynchronously resolves the deterministically sorted list of unique IPv4 {@code A} records for {@code host},
     * using the in-memory TTL cache unless {@code forceRefresh} is {@code true}.
     *
     * @param host         target DNS hostname or IP literal
     * @param forceRefresh {@code true} to bypass the TTL cache (e.g. after a connection drop)
     * @return future yielding an unmodifiable sorted list of IPv4 address strings (empty on lookup failure)
     */
    public Future<List<String>> resolveIpv4Pool(String host, boolean forceRefresh) {
        if (marketVertx == null || host == null || host.isBlank()) {
            return Future.succeededFuture(List.of());
        }
        String normalizedHost = host.trim();
        if (!forceRefresh) {
            long nowNs = System.nanoTime();
            CachedIpv4Pool cached = ipv4PoolCache.get(normalizedHost);
            if (cached != null && nowNs < cached.expiresAtNanoTime() && !cached.sortedIpv4s().isEmpty()) {
                return Future.succeededFuture(cached.sortedIpv4s());
            }
        }

        return marketVertx.executeBlocking(() -> {
            List<String> resolved = lookupSortedIpv4Addresses(normalizedHost);
            if (!resolved.isEmpty()) {
                ipv4PoolCache.put(normalizedHost, new CachedIpv4Pool(resolved, System.nanoTime() + dnsCacheTtlNs));
                return resolved;
            }
            CachedIpv4Pool stale = ipv4PoolCache.get(normalizedHost);
            return stale != null ? stale.sortedIpv4s() : List.of();
        }, false);
    }

    /**
     * Deterministically selects an IPv4 address from a sorted list of {@code A} records for {@code preferredSlot}
     * and {@code reconnectAttempt} while avoiding collision with {@code excludedPeerIp} (whenever at least 2 records
     * exist) and rotating away from {@code lastFailedIp}.
     *
     * @param sortedIpv4s      deterministically sorted unique IPv4 literal strings
     * @param preferredSlot    0-based stream leg slot ({@code >= 0})
     * @param reconnectAttempt consecutive reconnect counter ({@code >= 0})
     * @param excludedPeerIp   IPv4 address currently in use by the sibling leg (may be {@code null} or blank)
     * @param lastFailedIp     IPv4 address that most recently failed on this leg (may be {@code null} or blank)
     * @return selected IPv4 address string, or {@code null} if {@code sortedIpv4s} is null/empty or {@code preferredSlot < 0}
     */
    public static String selectIpv4ForSlot(
            List<String> sortedIpv4s,
            int preferredSlot,
            int reconnectAttempt,
            String excludedPeerIp,
            String lastFailedIp) {
        if (sortedIpv4s == null || sortedIpv4s.isEmpty() || preferredSlot < 0) {
            return null;
        }
        int size = sortedIpv4s.size();
        if (size == 1) {
            return sortedIpv4s.getFirst();
        }
        int startIdx = Math.floorMod(preferredSlot + Math.max(0, reconnectAttempt), size);
        String peerIp = normalizeIp(excludedPeerIp);
        String failedIp = normalizeIp(lastFailedIp);

        if (failedIp != null) {
            for (int i = 0; i < size; i++) {
                String candidate = sortedIpv4s.get((startIdx + i) % size);
                if (!candidate.equals(peerIp) && !candidate.equals(failedIp)) {
                    return candidate;
                }
            }
        }
        if (peerIp != null) {
            for (int i = 0; i < size; i++) {
                String candidate = sortedIpv4s.get((startIdx + i) % size);
                if (!candidate.equals(peerIp)) {
                    return candidate;
                }
            }
        }
        return sortedIpv4s.get(startIdx);
    }

    List<String> lookupSortedIpv4Addresses(String host) {
        try {
            InetAddress[] all = InetAddress.getAllByName(host);
            if (all == null || all.length == 0) {
                return List.of();
            }
            List<Inet4Address> v4List = new ArrayList<>(all.length);
            for (InetAddress addr : all) {
                if (addr instanceof Inet4Address v4) {
                    v4List.add(v4);
                }
            }
            if (v4List.isEmpty()) {
                return List.of();
            }
            v4List.sort(Comparator.comparingLong(MarketWebSocketRuntime::ipv4ToUnsignedLong));
            List<String> uniqueSorted = new ArrayList<>(v4List.size());
            String prev = null;
            for (Inet4Address v4 : v4List) {
                String ip = v4.getHostAddress();
                if (!ip.equals(prev)) {
                    uniqueSorted.add(ip);
                    prev = ip;
                }
            }
            return Collections.unmodifiableList(uniqueSorted);
        } catch (Exception e) {
            Log.debugf("IPv4 DNS pool lookup failed for '%s': %s", host, e.getMessage());
            return List.of();
        }
    }

    private static String normalizeIp(String rawIp) {
        if (rawIp == null) {
            return null;
        }
        String trimmed = rawIp.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        int colonIdx = trimmed.indexOf(':');
        return colonIdx > 0 ? trimmed.substring(0, colonIdx) : trimmed;
    }

    private static long ipv4ToUnsignedLong(Inet4Address addr) {
        byte[] b = addr.getAddress();
        return ((b[0] & 0xFFL) << 24)
                | ((b[1] & 0xFFL) << 16)
                | ((b[2] & 0xFFL) << 8)
                | (b[3] & 0xFFL);
    }
}
