package cz.polymarket.bot.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * Atomically persists and loads {@link RuntimeStateSnapshot} on disk (e.g. {@code /mnt/ebs/polymarket-bot/state/runtime-state.json})
 * so target prices ({@code twapOpenPrice}), runtime {@code StrategyMode}, and open positions survive upgrades and rollbacks.
 */
@ApplicationScoped
public class RuntimeStateStore {

    private final Path stateFilePath;
    private final ObjectMapper objectMapper;

    /**
     * Constructs the runtime state store.
     *
     * @param stateFile    configured path to {@code runtime-state.json}
     * @param objectMapper shared Jackson {@link ObjectMapper}
     */
    @Inject
    public RuntimeStateStore(
            @ConfigProperty(name = "polymarket.runtime.state-file", defaultValue = "data/state/runtime-state.json")
            String stateFile,
            ObjectMapper objectMapper) {
        if (stateFile == null || stateFile.isBlank()) {
            throw new IllegalArgumentException("stateFile cannot be null or blank");
        }
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper cannot be null");
        }
        Path path = Path.of(stateFile);
        this.stateFilePath = path.isAbsolute() ? path : path.toAbsolutePath();
        this.objectMapper = objectMapper;
    }

    /**
     * Atomically writes {@code snapshot} to disk via a sibling {@code .tmp} file and {@code REPLACE_EXISTING} move.
     *
     * @param snapshot runtime state snapshot to persist
     */
    public synchronized void save(RuntimeStateSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        try {
            Path parent = stateFilePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmpPath = stateFilePath.resolveSibling(stateFilePath.getFileName() + ".tmp");
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(snapshot);
            Files.writeString(
                    tmpPath,
                    json,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
            try {
                Files.move(tmpPath, stateFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(tmpPath, stateFilePath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            Log.errorf("Failed to persist RuntimeStateSnapshot to %s: %s", stateFilePath, e.getMessage());
        }
    }

    /**
     * Loads the persisted {@link RuntimeStateSnapshot} from disk if the file exists and is valid JSON.
     *
     * @return loaded {@link RuntimeStateSnapshot}, or {@link Optional#empty()} if absent or unreadable
     */
    public synchronized Optional<RuntimeStateSnapshot> load() {
        if (!Files.exists(stateFilePath)) {
            return Optional.empty();
        }
        try {
            String json = Files.readString(stateFilePath);
            if (json == null || json.isBlank()) {
                return Optional.empty();
            }
            RuntimeStateSnapshot snapshot = objectMapper.readValue(json, RuntimeStateSnapshot.class);
            return Optional.ofNullable(snapshot);
        } catch (Exception e) {
            Log.warnf("Could not load RuntimeStateSnapshot from %s: %s", stateFilePath, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Returns the resolved path of the runtime state file.
     *
     * @return state file path
     */
    public Path getStateFilePath() {
        return stateFilePath;
    }
}
