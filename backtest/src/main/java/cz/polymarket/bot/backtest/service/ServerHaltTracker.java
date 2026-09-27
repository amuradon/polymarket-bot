package cz.polymarket.bot.backtest.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class ServerHaltTracker {

    private static final Logger LOG = Logger.getLogger(ServerHaltTracker.class);

    private final Map<String, String> haltedServers = new ConcurrentHashMap<>();

    public boolean isHalted(String serverHostOrUrl) {
        String host = extractHost(serverHostOrUrl);
        return !host.isEmpty() && haltedServers.containsKey(host);
    }

    public void haltServer(String serverHostOrUrl, String reason) {
        String host = extractHost(serverHostOrUrl);
        if (!host.isEmpty()) {
            haltedServers.put(host, reason);
            LOG.warnf("Server %s halted: %s", host, reason);
        }
    }

    public Optional<String> getHaltReason(String serverHostOrUrl) {
        String host = extractHost(serverHostOrUrl);
        return Optional.ofNullable(haltedServers.get(host));
    }

    public void reset() {
        haltedServers.clear();
    }

    public static String extractHost(String serverHostOrUrl) {
        if (serverHostOrUrl == null || serverHostOrUrl.isBlank()) {
            return "";
        }
        try {
            if (serverHostOrUrl.contains("://")) {
                URI uri = URI.create(serverHostOrUrl);
                String host = uri.getHost();
                if (host != null && !host.isBlank()) {
                    return host.toLowerCase();
                }
            }
        } catch (Exception ignored) {
        }
        // If not a full URI or getHost() is null, strip port / path if present or return as-is
        String cleaned = serverHostOrUrl.trim().toLowerCase();
        int slashIdx = cleaned.indexOf('/');
        if (slashIdx != -1) {
            cleaned = cleaned.substring(0, slashIdx);
        }
        int colonIdx = cleaned.indexOf(':');
        if (colonIdx != -1) {
            cleaned = cleaned.substring(0, colonIdx);
        }
        return cleaned;
    }
}
