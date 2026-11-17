package cz.polymarket.bot.connector.ws;

import cz.polymarket.bot.connector.os.LinuxCpuAffinity;
import io.netty.handler.ssl.OpenSsl;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.dns.AddressResolverOptions;
import io.vertx.core.net.SocketAddress;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

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
 * ({@code market-ws-io-reader}) pinned to {@code CPU #1} on 4-core Linux hosts, eliminating redundant
 * Netty EventLoop instances, idle blocking worker pools, and cross-core scheduler migrations. Also provides
 * deterministic, non-blocking IPv4 pool resolution so dual Active-Active connections ({@code PRIMARY} and
 * {@code BACKUP}) connect to distinct CDN/exchange edge IPv4 addresses while preserving TLS SNI and HTTP
 * {@code Host} headers.
 */
@ApplicationScoped
public class MarketWebSocketRuntime {

    /** Dedicated thread name assigned to the consolidated market-data Netty EventLoop on {@code CPU #1}. */
    public static final String DEDICATED_MARKET_IO_THREAD_NAME = "market-ws-io-reader";

    /** Target physical CPU index on 4-vCPU hosts ({@code c8gn.xlarge}) for market WebSocket I/O reading. */
    public static final int MARKET_IO_CPU_INDEX = 1;

    private static final long DNS_CACHE_TTL_NS = TimeUnit.SECONDS.toNanos(30L);

    private record CachedIpv4Pool(List<String> sortedIpv4s, long expiresAtNanoTime) {
    }

    private final Vertx marketVertx;
    private final ConcurrentHashMap<String, CachedIpv4Pool> ipv4PoolCache = new ConcurrentHashMap<>();
    private volatile Thread ioReaderThread;

    /**
     * Constructs and initializes the shared 1-thread market WebSocket {@link Vertx} runtime,
     * naming the EventLoop thread {@link #DEDICATED_MARKET_IO_THREAD_NAME}, setting {@link Thread#MAX_PRIORITY},
     * and pinning it to {@code CPU #1} when supported.
     */
    public MarketWebSocketRuntime() {
        this.marketVertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(1)
                .setWorkerPoolSize(1)
                .setInternalBlockingPoolSize(1)
                .setPreferNativeTransport(true)
                .setAddressResolverOptions(new AddressResolverOptions()
                        .setRoundRobinInetAddress(true)
                        .setRotateServers(true)
                        .setCacheMaxTimeToLive(60)));

        CountDownLatch initLatch = new CountDownLatch(1);
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
                initLatch.countDown();
            }
        });
        try {
            initLatch.await(2L, TimeUnit.SECONDS);
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
        try {
            marketVertx.close().toCompletionStage().toCompletableFuture().get(3L, TimeUnit.SECONDS);
        } catch (Exception ignored) {
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
        return marketVertx.isNativeTransportEnabled();
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
     * Asynchronously resolves a deterministic IPv4 target {@link SocketAddress} for the given {@code host},
     * {@code port}, {@code preferredSlot} (e.g. {@code 0} for {@code PRIMARY} and {@code 1} for {@code BACKUP}),
     * and {@code reconnectAttempt}.
     * <p>
     * Sorting the resolved {@code A} records deterministically guarantees that {@code slot 0} and {@code slot 1}
     * select distinct upstream CDN/exchange edge IPv4 addresses whenever at least two {@code A} records exist,
     * while subsequent reconnect attempts rotate across the remaining pool entries.
     *
     * @param host             target DNS hostname or IP literal
     * @param port             target TCP port
     * @param preferredSlot    0-based leg index ({@code 0} for primary, {@code 1} for backup; {@code < 0} disables slot pinning)
     * @param reconnectAttempt consecutive reconnect attempt counter ({@code >= 0})
     * @return future yielding the resolved IPv4 {@link SocketAddress}, or {@code null} to use default host connect
     */
    public Future<SocketAddress> resolveDistinctIpv4Server(
            String host,
            int port,
            int preferredSlot,
            int reconnectAttempt) {
        if (host == null || host.isBlank() || preferredSlot < 0) {
            return Future.succeededFuture(null);
        }
        String normalizedHost = host.trim();
        long nowNs = System.nanoTime();
        CachedIpv4Pool cached = ipv4PoolCache.get(normalizedHost);
        if (cached != null && nowNs < cached.expiresAtNanoTime() && !cached.sortedIpv4s().isEmpty()) {
            String selectedIp = selectIpv4ForSlot(cached.sortedIpv4s(), preferredSlot, reconnectAttempt);
            return Future.succeededFuture(selectedIp != null ? SocketAddress.inetSocketAddress(port, selectedIp) : null);
        }

        return marketVertx.executeBlocking(() -> {
            List<String> resolved = lookupSortedIpv4Addresses(normalizedHost);
            if (!resolved.isEmpty()) {
                ipv4PoolCache.put(normalizedHost, new CachedIpv4Pool(resolved, System.nanoTime() + DNS_CACHE_TTL_NS));
            }
            String selectedIp = selectIpv4ForSlot(resolved, preferredSlot, reconnectAttempt);
            return selectedIp != null ? SocketAddress.inetSocketAddress(port, selectedIp) : null;
        }, false);
    }

    /**
     * Deterministically selects an IPv4 address from a sorted list of {@code A} records for the given
     * {@code preferredSlot} and {@code reconnectAttempt}.
     *
     * @param sortedIpv4s      deterministically sorted unique IPv4 literal strings
     * @param preferredSlot    0-based stream leg slot ({@code >= 0})
     * @param reconnectAttempt consecutive reconnect counter ({@code >= 0})
     * @return selected IPv4 address string, or {@code null} if {@code sortedIpv4s} is null/empty or {@code preferredSlot < 0}
     */
    public static String selectIpv4ForSlot(List<String> sortedIpv4s, int preferredSlot, int reconnectAttempt) {
        if (sortedIpv4s == null || sortedIpv4s.isEmpty() || preferredSlot < 0) {
            return null;
        }
        int size = sortedIpv4s.size();
        int offset = Math.floorMod(preferredSlot + Math.max(0, reconnectAttempt), size);
        return sortedIpv4s.get(offset);
    }

    static List<String> lookupSortedIpv4Addresses(String host) {
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

    private static long ipv4ToUnsignedLong(Inet4Address addr) {
        byte[] b = addr.getAddress();
        return ((b[0] & 0xFFL) << 24)
                | ((b[1] & 0xFFL) << 16)
                | ((b[2] & 0xFFL) << 8)
                | (b[3] & 0xFFL);
    }
}
