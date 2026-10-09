package cz.polymarket.bot.backtest.cache;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Memory-mapped binary cache service for market intervals and technical indicators.
 * Provides zero-copy fast reading and memory-mapped file channels matching high-frequency trading bot architecture.
 */
@ApplicationScoped
public class BinaryMarketCacheService {

    private static final Logger LOG = Logger.getLogger(BinaryMarketCacheService.class);

    public static final int IND_MAGIC = 0x494E4431; // 'IND1'
    public static final int MKT_MAGIC = 0x4D4B5431; // 'MKT1'
    public static final int MARKET_RECORD_SIZE = 264; // 33 * 8 bytes

    private final Path cacheDir;

    @Inject
    public BinaryMarketCacheService(
            @ConfigProperty(name = "polymarket.backtest.cache-dir", defaultValue = "D:/Crypto/data/Polymarket/backtesting/cache")
            String cacheDirPath) {
        if (cacheDirPath == null || cacheDirPath.isBlank()) {
            throw new IllegalArgumentException("Cache directory path cannot be blank");
        }
        this.cacheDir = Path.of(cacheDirPath);
        try {
            Files.createDirectories(this.cacheDir);
        } catch (IOException e) {
            LOG.warnf("Could not create cache directory %s: %s", cacheDir, e.getMessage());
        }
    }

    public Path getCacheDir() {
        return cacheDir;
    }

    public File getMarketFile(String symbol, String month) {
        return cacheDir.resolve(String.format("market_%s_%s.bin", symbol.toUpperCase(), month)).toFile();
    }

    public File getIndicatorFile(String symbol, String indicatorId, String month) {
        return cacheDir.resolve(String.format("ind_%s_%s_%s.bin", symbol.toUpperCase(), indicatorId.toLowerCase(), month)).toFile();
    }

    public boolean hasMarketRowsCache(String symbol, String month) {
        File file = getMarketFile(symbol, month);
        return file.exists() && file.isFile() && file.length() >= 8;
    }

    public boolean hasIndicatorCache(String symbol, String indicatorId, String month) {
        File file = getIndicatorFile(symbol, indicatorId, month);
        return file.exists() && file.isFile() && file.length() >= 6;
    }

    public List<String> getAvailableMonths(String symbol) {
        File[] files = cacheDir.toFile().listFiles((dir, name) ->
                name.startsWith(String.format("market_%s_", symbol.toUpperCase())) && name.endsWith(".bin"));
        if (files == null || files.length == 0) {
            return List.of();
        }
        List<String> months = new ArrayList<>();
        String prefix = String.format("market_%s_", symbol.toUpperCase());
        for (File f : files) {
            String name = f.getName();
            String month = name.substring(prefix.length(), name.length() - 4);
            months.add(month);
        }
        Collections.sort(months);
        return Collections.unmodifiableList(months);
    }

    public void writeIndicatorCache(
            String symbol,
            String indicatorId,
            String month,
            List<String> keys,
            Map<Long, Map<String, Double>> records) {

        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("Indicator keys cannot be empty");
        }

        File file = getIndicatorFile(symbol, indicatorId, month);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        try (FileChannel channel = FileChannel.open(
                file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // Build binary header
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeInt(IND_MAGIC);
            dos.writeShort(keys.size());
            for (String key : keys) {
                byte[] kb = key.getBytes(StandardCharsets.UTF_8);
                dos.writeShort(kb.length);
                dos.write(kb);
            }
            dos.flush();
            byte[] headerBytes = baos.toByteArray();

            int recordSize = 8 + keys.size() * 8;
            ByteBuffer buffer = ByteBuffer.allocateDirect(Math.max(8192, headerBytes.length + recordSize * 64));
            buffer.put(headerBytes);

            List<Long> timestamps = new ArrayList<>(records.keySet());
            Collections.sort(timestamps);

            for (Long t : timestamps) {
                if (buffer.remaining() < recordSize) {
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                }
                buffer.putLong(t);
                Map<String, Double> vals = records.get(t);
                for (String k : keys) {
                    Double v = (vals != null) ? vals.get(k) : null;
                    buffer.putDouble(v != null ? v : Double.NaN);
                }
            }

            if (buffer.position() > 0) {
                buffer.flip();
                channel.write(buffer);
            }
            LOG.debugf("Successfully wrote indicator cache %s for %s %s (%d records)", indicatorId, symbol, month, timestamps.size());
        } catch (IOException e) {
            LOG.errorf(e, "Failed to write indicator cache to %s", file.getAbsolutePath());
            throw new RuntimeException("Error writing indicator cache: " + file.getAbsolutePath(), e);
        }
    }

    public Map<Long, Map<String, Double>> readIndicatorCache(String symbol, String indicatorId, String month) {
        File file = getIndicatorFile(symbol, indicatorId, month);
        if (!file.exists() || file.length() == 0) {
            return Map.of();
        }

        Map<Long, Map<String, Double>> result = new LinkedHashMap<>();
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < 6) {
                return Map.of();
            }

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            int magic = buffer.getInt();
            if (magic != IND_MAGIC) {
                throw new IllegalStateException("Invalid indicator binary cache magic in " + file.getAbsolutePath() + ": 0x" + Integer.toHexString(magic));
            }

            int keyCount = buffer.getShort();
            String[] keys = new String[keyCount];
            for (int i = 0; i < keyCount; i++) {
                int len = buffer.getShort();
                byte[] keyBytes = new byte[len];
                buffer.get(keyBytes);
                keys[i] = new String(keyBytes, StandardCharsets.UTF_8);
            }

            int recordSize = 8 + keyCount * 8;
            while (buffer.remaining() >= recordSize) {
                long tStart = buffer.getLong();
                Map<String, Double> valMap = new LinkedHashMap<>(keyCount);
                for (int i = 0; i < keyCount; i++) {
                    double v = buffer.getDouble();
                    if (!Double.isNaN(v)) {
                        valMap.put(keys[i], v);
                    }
                }
                result.put(tStart, valMap);
            }
        } catch (IOException e) {
            LOG.errorf(e, "Failed to read indicator cache from %s", file.getAbsolutePath());
            throw new RuntimeException("Error reading indicator cache: " + file.getAbsolutePath(), e);
        }

        return Collections.unmodifiableMap(result);
    }

    public void writeMarketRowsCache(String symbol, String month, List<CachedMarketRow> rows) {
        if (rows == null) {
            return;
        }

        File file = getMarketFile(symbol, month);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        try (FileChannel channel = FileChannel.open(
                file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer buffer = ByteBuffer.allocateDirect(Math.max(8192, 8 + rows.size() * MARKET_RECORD_SIZE));
            buffer.putInt(MKT_MAGIC);
            buffer.putInt(rows.size());

            for (CachedMarketRow r : rows) {
                if (buffer.remaining() < MARKET_RECORD_SIZE) {
                    buffer.flip();
                    channel.write(buffer);
                    buffer.clear();
                }

                buffer.putLong(r.tStart());
                buffer.putLong(r.tEnd());
                buffer.putDouble(r.sOpen());
                buffer.putDouble(r.sHigh());
                buffer.putDouble(r.sLow());
                buffer.putDouble(r.sClose());
                buffer.putDouble(r.sVolBtc());
                buffer.putDouble(r.sVolUsd());
                buffer.putDouble(r.sDeltaBtc());

                buffer.putDouble(r.fOpen());
                buffer.putDouble(r.fHigh());
                buffer.putDouble(r.fLow());
                buffer.putDouble(r.fClose());
                buffer.putDouble(r.fVolBtc());
                buffer.putDouble(r.fVolUsd());
                buffer.putDouble(r.fDeltaBtc());

                buffer.put((byte) (r.actualOutcomeUp() ? 1 : 0));
                for (int p = 0; p < 7; p++) {
                    buffer.put((byte) 0); // 7-byte padding
                }

                buffer.putDouble(r.pmAsk0());
                buffer.putDouble(r.pmBid0());
                buffer.putDouble(r.pmAsk60());
                buffer.putDouble(r.pmBid60());
                buffer.putDouble(r.pmAsk180());
                buffer.putDouble(r.pmBid180());
                buffer.putDouble(r.pmAsk300());
                buffer.putDouble(r.pmBid300());

                buffer.putDouble(r.pmFill100Up());
                buffer.putDouble(r.pmFee100Up());
                buffer.putDouble(r.pmFill100Down());
                buffer.putDouble(r.pmFee100Down());

                buffer.putDouble(r.pmMaxPrice());
                buffer.putDouble(r.pmMinPrice());
                buffer.putDouble(r.pmDepth1cUp());
                buffer.putDouble(r.pmDepth1cDown());
            }

            if (buffer.position() > 0) {
                buffer.flip();
                channel.write(buffer);
            }
            LOG.debugf("Successfully wrote market rows cache for %s %s (%d records)", symbol, month, rows.size());
        } catch (IOException e) {
            LOG.errorf(e, "Failed to write market rows cache to %s", file.getAbsolutePath());
            throw new RuntimeException("Error writing market rows cache: " + file.getAbsolutePath(), e);
        }
    }

    public List<CachedMarketRow> readMarketRowsCache(String symbol, String month) {
        File file = getMarketFile(symbol, month);
        if (!file.exists() || file.length() == 0) {
            return List.of();
        }

        List<CachedMarketRow> rows = new ArrayList<>();
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < 8) {
                return List.of();
            }

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            int magic = buffer.getInt();
            if (magic != MKT_MAGIC) {
                throw new IllegalStateException("Invalid market binary cache magic in " + file.getAbsolutePath() + ": 0x" + Integer.toHexString(magic));
            }
            int recordCount = buffer.getInt();

            while (buffer.remaining() >= MARKET_RECORD_SIZE) {
                long tStart = buffer.getLong();
                long tEnd = buffer.getLong();
                double sOpen = buffer.getDouble();
                double sHigh = buffer.getDouble();
                double sLow = buffer.getDouble();
                double sClose = buffer.getDouble();
                double sVolBtc = buffer.getDouble();
                double sVolUsd = buffer.getDouble();
                double sDeltaBtc = buffer.getDouble();

                double fOpen = buffer.getDouble();
                double fHigh = buffer.getDouble();
                double fLow = buffer.getDouble();
                double fClose = buffer.getDouble();
                double fVolBtc = buffer.getDouble();
                double fVolUsd = buffer.getDouble();
                double fDeltaBtc = buffer.getDouble();

                byte outcomeByte = buffer.get();
                boolean outcomeUp = (outcomeByte == 1);
                for (int p = 0; p < 7; p++) {
                    buffer.get(); // skip 7-byte padding
                }

                double pmAsk0 = buffer.getDouble();
                double pmBid0 = buffer.getDouble();
                double pmAsk60 = buffer.getDouble();
                double pmBid60 = buffer.getDouble();
                double pmAsk180 = buffer.getDouble();
                double pmBid180 = buffer.getDouble();
                double pmAsk300 = buffer.getDouble();
                double pmBid300 = buffer.getDouble();

                double pmFill100Up = buffer.getDouble();
                double pmFee100Up = buffer.getDouble();
                double pmFill100Down = buffer.getDouble();
                double pmFee100Down = buffer.getDouble();

                double pmMaxPrice = buffer.getDouble();
                double pmMinPrice = buffer.getDouble();
                double pmDepth1cUp = buffer.getDouble();
                double pmDepth1cDown = buffer.getDouble();

                rows.add(new CachedMarketRow(
                        tStart, tEnd,
                        sOpen, sHigh, sLow, sClose, sVolBtc, sVolUsd, sDeltaBtc,
                        fOpen, fHigh, fLow, fClose, fVolBtc, fVolUsd, fDeltaBtc,
                        outcomeUp,
                        pmAsk0, pmBid0, pmAsk60, pmBid60, pmAsk180, pmBid180, pmAsk300, pmBid300,
                        pmFill100Up, pmFee100Up, pmFill100Down, pmFee100Down,
                        pmMaxPrice, pmMinPrice, pmDepth1cUp, pmDepth1cDown
                ));
            }
        } catch (IOException e) {
            LOG.errorf(e, "Failed to read market rows cache from %s", file.getAbsolutePath());
            throw new RuntimeException("Error reading market rows cache: " + file.getAbsolutePath(), e);
        }

        return Collections.unmodifiableList(rows);
    }
}
