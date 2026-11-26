package cz.polymarket.bot.backtest.service;

import cz.polymarket.bot.backtest.data.StrategyMetadata;
import cz.polymarket.bot.backtest.data.StrategyMetadata.StrategyParameterDescriptor;
import cz.polymarket.bot.domain.Timeframe;
import cz.polymarket.bot.strategy.StrategyRegistry;
import cz.polymarket.bot.strategy.TradingStrategy;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Introspects registered {@link TradingStrategy} implementations and their immutable configuration beans
 * to expose parameter metadata for the UI and construct custom-configured isolated strategy instances
 * without modifying {@code common} hot-path code.
 */
@ApplicationScoped
public class StrategyParameterService {

    private static final Map<String, Set<String>> KEY_PARAMS_BY_STRATEGY = Map.of(
            "OraclePulseLockStrategy", Set.of(
                    "minTauMs", "maxTauMs", "minModelProb", "minLogitEdgePre60", "minLogitEdgeLate",
                    "maxSpread", "maxRiskPerMarketUsd", "kellyBankrollUsd", "maxClipsPerMarket", "takerFeeRate"
            ),
            "PricingModelStrategy", Set.of(
                    "minTauMs", "maxTauMs", "minNetEdgeMc", "minFillNetEdgeMc", "maxSpreadMc",
                    "minOpenPriceMc", "maxOpenPriceMc", "maxOrderShares", "maxInventoryShares", "takerFeeRateBps"
            ),
            "RoundMainstreamStrategy", Set.of(
                    "minTauMs", "maxTauMs", "minCompressionEdgeMc", "minEffectiveSweepShares",
                    "minBidWallShares", "maxSpreadMc", "maxOrderShares", "maxInventorySharesPerSide", "takerFeeRateBps"
            ),
            "UtterParameciumStrategy", Set.of(
                    "minTauMs", "maxTauMs", "minModelProbMc", "minEdgeMc", "maxSpreadMc",
                    "minEntryPriceMc", "maxEntryPriceMc", "maxOrderShares", "maxInventorySharesPerSide", "takerFeeRateBps"
            )
    );

    private static final Set<String> UTTER_VERSION_KEY_PARAMS = Set.of(
            "minTauMs", "maxTauMs", "minModelProbMc", "minEdgeMc", "maxSpreadMc",
            "minEntryPriceMc", "maxEntryPriceMc", "maxOrderShares", "maxInventorySharesPerSide", "takerFeeRateBps"
    );

    private final StrategyRegistry strategyRegistry;
    private final Map<String, StrategyIntrospection> introspectionCache = new ConcurrentHashMap<>();

    /**
     * Constructs the strategy parameter service backed by {@link StrategyRegistry}.
     *
     * @param strategyRegistry unified registry of available trading strategies
     */
    @Inject
    public StrategyParameterService(StrategyRegistry strategyRegistry) {
        if (strategyRegistry == null) {
            Log.fatal("Fatal initialization error in StrategyParameterService: strategyRegistry cannot be null");
            throw new IllegalArgumentException("strategyRegistry cannot be null");
        }
        this.strategyRegistry = strategyRegistry;
    }

    /**
     * Returns metadata descriptors and configurable parameters for every strategy registered in {@link StrategyRegistry},
     * sorted alphabetically by strategy name.
     *
     * @return immutable list of {@link StrategyMetadata}
     */
    public List<StrategyMetadata> listStrategies() {
        List<String> names = new ArrayList<>(strategyRegistry.getAvailableStrategies());
        names.sort(String::compareTo);
        List<StrategyMetadata> result = new ArrayList<>(names.size());
        for (String name : names) {
            result.add(resolveIntrospection(name).metadata());
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Returns metadata for a single strategy by name.
     *
     * @param strategyName registered strategy name
     * @return {@link StrategyMetadata}
     * @throws IllegalArgumentException if strategy is unknown
     */
    public StrategyMetadata getStrategyMetadata(String strategyName) {
        return resolveIntrospection(strategyName).metadata();
    }

    /**
     * Resolves and instantiates a {@link TradingStrategy} configured with optional parameter overrides.
     * Validates all parameter names, value types, and strategy config invariants immediately.
     *
     * @param strategyName strategy name resolved via {@link StrategyRegistry}
     * @param overrides optional map of parameter overrides (may be null or empty)
     * @return configured strategy instance together with complete effective params and applied overrides
     * @throws IllegalArgumentException if the strategy is unknown, an override key is unknown, or any value fails validation
     */
    public ConfiguredStrategy instantiateStrategy(String strategyName, Map<String, Object> overrides) {
        StrategyIntrospection introspection = resolveIntrospection(strategyName);
        Map<String, Object> safeOverrides = (overrides == null) ? Map.of() : overrides;

        if (!introspection.configurable()) {
            if (!safeOverrides.isEmpty()) {
                throw new IllegalArgumentException(
                        "Strategy '" + introspection.metadata().name() + "' does not expose configurable parameters: "
                                + safeOverrides.keySet());
            }
            return new ConfiguredStrategy(introspection.registeredStrategy(), Map.of(), Map.of());
        }

        Map<String, StrategyParameterSlot> slotByName = introspection.slotByName();
        for (String key : safeOverrides.keySet()) {
            if (key == null || !slotByName.containsKey(key)) {
                throw new IllegalArgumentException(
                        "Unknown strategy parameter '" + key + "' for " + introspection.metadata().name());
            }
        }

        List<StrategyParameterSlot> slots = introspection.slots();
        Object[] args = new Object[slots.size()];
        Map<String, Object> effectiveParams = new LinkedHashMap<>(slots.size());
        Map<String, Object> appliedOverrides = new LinkedHashMap<>();

        for (int i = 0; i < slots.size(); i++) {
            StrategyParameterSlot slot = slots.get(i);
            Object defaultVal = slot.descriptor().defaultValue();
            Object effectiveVal;
            if (safeOverrides.containsKey(slot.name())) {
                Object rawOverride = safeOverrides.get(slot.name());
                if (rawOverride == null) {
                    throw new IllegalArgumentException(
                            "Parameter '" + slot.name() + "' for " + introspection.metadata().name() + " cannot be null");
                }
                effectiveVal = convertValue(slot.javaType(), rawOverride, slot.name());
                if (!valuesEqual(defaultVal, effectiveVal, slot.javaType())) {
                    appliedOverrides.put(slot.name(), effectiveVal);
                }
            } else {
                effectiveVal = defaultVal;
            }
            args[i] = effectiveVal;
            effectiveParams.put(slot.name(), effectiveVal);
        }

        try {
            Object configInstance = introspection.configConstructor().newInstance(args);
            TradingStrategy strategyInstance = introspection.strategyConstructor().newInstance(configInstance);
            return new ConfiguredStrategy(
                    strategyInstance,
                    Collections.unmodifiableMap(effectiveParams),
                    Collections.unmodifiableMap(appliedOverrides)
            );
        } catch (InvocationTargetException e) {
            Throwable cause = e.getTargetException();
            String msg = (cause != null && cause.getMessage() != null && !cause.getMessage().isBlank())
                    ? cause.getMessage()
                    : "Invalid configuration for " + introspection.metadata().name();
            throw new IllegalArgumentException(msg, cause != null ? cause : e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed instantiating configured strategy " + introspection.metadata().name() + ": " + e.getMessage(), e);
        }
    }

    private StrategyIntrospection resolveIntrospection(String strategyName) {
        TradingStrategy registered = strategyRegistry.getStrategy(strategyName);
        String canonicalName = registered.getName();
        return introspectionCache.computeIfAbsent(canonicalName, ignored -> inspectStrategy(canonicalName, registered));
    }

    private StrategyIntrospection inspectStrategy(String canonicalName, TradingStrategy registered) {
        TradingStrategy probe = registered.createInstance();
        if (probe == null) {
            probe = registered;
        }
        Class<? extends TradingStrategy> strategyClass = probe.getClass();
        String timeframeCode = probe.getTimeframe() != null ? probe.getTimeframe().getCode() : Timeframe.FIFTEEN_MINUTES.getCode();

        Method getConfigMethod;
        try {
            getConfigMethod = strategyClass.getMethod("getConfig");
        } catch (NoSuchMethodException e) {
            return unconfigurableIntrospection(canonicalName, timeframeCode, registered);
        }

        Class<?> configClass = getConfigMethod.getReturnType();
        Constructor<? extends TradingStrategy> strategyCtor;
        try {
            strategyCtor = strategyClass.getConstructor(configClass);
        } catch (NoSuchMethodException e) {
            return unconfigurableIntrospection(canonicalName, timeframeCode, registered);
        }

        Constructor<?> configCtor = findInjectConstructor(configClass);
        if (configCtor == null) {
            return unconfigurableIntrospection(canonicalName, timeframeCode, registered);
        }

        Object defaultConfigObj;
        try {
            defaultConfigObj = getConfigMethod.invoke(probe);
        } catch (ReflectiveOperationException e) {
            Log.warnf("Could not extract default config from %s: %s", canonicalName, e.getMessage());
            return unconfigurableIntrospection(canonicalName, timeframeCode, registered);
        }

        Set<String> keyParams = resolveKeyParams(canonicalName);
        Parameter[] parameters = configCtor.getParameters();
        List<StrategyParameterSlot> slots = new ArrayList<>(parameters.length);
        Map<String, StrategyParameterSlot> slotMap = new LinkedHashMap<>(parameters.length);
        List<StrategyParameterDescriptor> descriptors = new ArrayList<>(parameters.length);

        for (Parameter param : parameters) {
            String paramName = param.getName();
            ConfigProperty cfgProp = param.getAnnotation(ConfigProperty.class);
            String propertyName = cfgProp != null ? cfgProp.name() : paramName;
            Class<?> javaType = param.getType();
            String typeLabel = normalizeTypeLabel(javaType);
            Object defaultVal = extractDefaultValue(configClass, defaultConfigObj, paramName, javaType, cfgProp);
            boolean isKey = keyParams.contains(paramName);
            String group = classifyGroup(paramName);

            StrategyParameterDescriptor descriptor = new StrategyParameterDescriptor(
                    paramName, propertyName, typeLabel, defaultVal, isKey, group);
            StrategyParameterSlot slot = new StrategyParameterSlot(paramName, javaType, descriptor);
            slots.add(slot);
            slotMap.put(paramName, slot);
            descriptors.add(descriptor);
        }

        StrategyMetadata metadata = new StrategyMetadata(
                canonicalName,
                timeframeCode,
                Collections.unmodifiableList(descriptors)
        );
        return new StrategyIntrospection(
                metadata,
                true,
                registered,
                strategyCtor,
                configCtor,
                Collections.unmodifiableList(slots),
                Collections.unmodifiableMap(slotMap)
        );
    }

    private static StrategyIntrospection unconfigurableIntrospection(
            String canonicalName,
            String timeframeCode,
            TradingStrategy registered) {
        return new StrategyIntrospection(
                new StrategyMetadata(canonicalName, timeframeCode, List.of()),
                false,
                registered,
                null,
                null,
                List.of(),
                Map.of()
        );
    }

    private static Constructor<?> findInjectConstructor(Class<?> configClass) {
        Constructor<?>[] constructors = configClass.getConstructors();
        for (Constructor<?> ctor : constructors) {
            if (ctor.isAnnotationPresent(Inject.class)) {
                return ctor;
            }
        }
        Constructor<?> best = null;
        for (Constructor<?> ctor : constructors) {
            if (best == null || ctor.getParameterCount() > best.getParameterCount()) {
                best = ctor;
            }
        }
        return best;
    }

    private static Object extractDefaultValue(
            Class<?> configClass,
            Object defaultConfigObj,
            String paramName,
            Class<?> javaType,
            ConfigProperty cfgProp) {
        if (defaultConfigObj != null) {
            try {
                Method getter = configClass.getMethod(paramName);
                Object val = getter.invoke(defaultConfigObj);
                if (val instanceof Timeframe tf) {
                    return tf.getCode();
                }
                return val;
            } catch (NoSuchMethodException e) {
                if ("timeframeCode".equals(paramName) || "timeframeLabel".equals(paramName)) {
                    try {
                        Method tfGetter = configClass.getMethod("timeframe");
                        Object tfVal = tfGetter.invoke(defaultConfigObj);
                        if (tfVal instanceof Timeframe tf) {
                            return tf.getCode();
                        }
                    } catch (ReflectiveOperationException ignored) {
                        // Fall through to @ConfigProperty default
                    }
                }
            } catch (ReflectiveOperationException ignored) {
                // Fall through to @ConfigProperty default
            }
        }

        if (cfgProp != null && !ConfigProperty.UNCONFIGURED_VALUE.equals(cfgProp.defaultValue())) {
            return convertValue(javaType, cfgProp.defaultValue(), paramName);
        }
        return defaultForPrimitive(javaType);
    }

    private static Object defaultForPrimitive(Class<?> javaType) {
        if (javaType == int.class || javaType == Integer.class) {
            return 0;
        }
        if (javaType == long.class || javaType == Long.class) {
            return 0L;
        }
        if (javaType == double.class || javaType == Double.class) {
            return 0.0;
        }
        if (javaType == boolean.class || javaType == Boolean.class) {
            return false;
        }
        return "";
    }

    private static Object convertValue(Class<?> targetType, Object raw, String paramName) {
        if (targetType == int.class || targetType == Integer.class) {
            if (raw instanceof Number num) {
                double d = num.doubleValue();
                if (!Double.isFinite(d) || d != Math.rint(d) || d < Integer.MIN_VALUE || d > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Parameter '" + paramName + "' expects an integer, got: " + raw);
                }
                return num.intValue();
            }
            if (raw instanceof String s) {
                try {
                    return Integer.parseInt(s.trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Parameter '" + paramName + "' expects an integer, got: " + raw, e);
                }
            }
            throw new IllegalArgumentException("Parameter '" + paramName + "' expects an integer, got: " + raw);
        }

        if (targetType == long.class || targetType == Long.class) {
            if (raw instanceof Number num) {
                double d = num.doubleValue();
                if (!Double.isFinite(d) || d != Math.rint(d)) {
                    throw new IllegalArgumentException("Parameter '" + paramName + "' expects a long integer, got: " + raw);
                }
                return num.longValue();
            }
            if (raw instanceof String s) {
                try {
                    return Long.parseLong(s.trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Parameter '" + paramName + "' expects a long integer, got: " + raw, e);
                }
            }
            throw new IllegalArgumentException("Parameter '" + paramName + "' expects a long integer, got: " + raw);
        }

        if (targetType == double.class || targetType == Double.class) {
            double val;
            if (raw instanceof Number num) {
                val = num.doubleValue();
            } else if (raw instanceof String s) {
                try {
                    val = Double.parseDouble(s.trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Parameter '" + paramName + "' expects a number, got: " + raw, e);
                }
            } else {
                throw new IllegalArgumentException("Parameter '" + paramName + "' expects a number, got: " + raw);
            }
            if (!Double.isFinite(val)) {
                throw new IllegalArgumentException("Parameter '" + paramName + "' must be finite, got: " + raw);
            }
            return val;
        }

        if (targetType == boolean.class || targetType == Boolean.class) {
            if (raw instanceof Boolean b) {
                return b;
            }
            if (raw instanceof String s) {
                String trimmed = s.trim().toLowerCase(Locale.ROOT);
                if ("true".equals(trimmed)) {
                    return Boolean.TRUE;
                }
                if ("false".equals(trimmed)) {
                    return Boolean.FALSE;
                }
            }
            throw new IllegalArgumentException("Parameter '" + paramName + "' expects a boolean, got: " + raw);
        }

        if (targetType == String.class) {
            return String.valueOf(raw);
        }

        throw new IllegalArgumentException("Unsupported parameter type " + targetType.getSimpleName() + " for '" + paramName + "'");
    }

    private static boolean valuesEqual(Object a, Object b, Class<?> javaType) {
        if (javaType == double.class || javaType == Double.class) {
            if (a instanceof Number na && b instanceof Number nb) {
                return Double.compare(na.doubleValue(), nb.doubleValue()) == 0;
            }
        }
        if (javaType == long.class || javaType == Long.class) {
            if (a instanceof Number na && b instanceof Number nb) {
                return na.longValue() == nb.longValue();
            }
        }
        if (javaType == int.class || javaType == Integer.class) {
            if (a instanceof Number na && b instanceof Number nb) {
                return na.intValue() == nb.intValue();
            }
        }
        return Objects.equals(a, b);
    }

    private static String normalizeTypeLabel(Class<?> javaType) {
        if (javaType == int.class || javaType == Integer.class) {
            return "int";
        }
        if (javaType == long.class || javaType == Long.class) {
            return "long";
        }
        if (javaType == double.class || javaType == Double.class) {
            return "double";
        }
        if (javaType == boolean.class || javaType == Boolean.class) {
            return "boolean";
        }
        return "string";
    }

    private static Set<String> resolveKeyParams(String strategyName) {
        Set<String> explicit = KEY_PARAMS_BY_STRATEGY.get(strategyName);
        if (explicit != null) {
            return explicit;
        }
        if (strategyName.startsWith("UtterParameciumStrategy")) {
            return UTTER_VERSION_KEY_PARAMS;
        }
        return Set.of();
    }

    private static String classifyGroup(String paramName) {
        String lower = paramName.toLowerCase(Locale.ROOT);
        if (lower.contains("tau") || lower.contains("cooldown") || lower.contains("warmup")
                || lower.contains("quoteage") || lower.contains("timeframe") || lower.contains("window")) {
            return "Timing";
        }
        if (lower.contains("edge") || lower.contains("prob") || lower.contains("cushion")
                || lower.contains("impulse") || lower.contains("sweep") || lower.contains("wall")
                || lower.contains("jump") || lower.contains("lead") || lower.contains("drift")
                || lower.contains("ofi") || lower.contains("sync")) {
            return "Signal & Edge";
        }
        if (lower.contains("risk") || lower.contains("kelly") || lower.contains("share")
                || lower.contains("clip") || lower.contains("inventory") || lower.contains("orders")
                || lower.contains("burst") || lower.contains("flip")) {
            return "Risk & Sizing";
        }
        if (lower.contains("sigma") || lower.contains("vol") || lower.contains("range")
                || lower.contains("whipsaw") || lower.contains("parkinson") || lower.contains("bridge")
                || lower.contains("basis") || lower.contains("return")) {
            return "Volatility & Model";
        }
        return "Market & Fees";
    }

    /**
     * Instantiated strategy paired with its complete effective parameter map and overridden subset.
     *
     * @param strategy isolated configured {@link TradingStrategy} instance
     * @param effectiveParams complete map of effective strategy parameter values
     * @param appliedOverrides subset of parameters differing from defaults
     */
    public record ConfiguredStrategy(
            TradingStrategy strategy,
            Map<String, Object> effectiveParams,
            Map<String, Object> appliedOverrides
    ) {
    }

    private record StrategyParameterSlot(
            String name,
            Class<?> javaType,
            StrategyParameterDescriptor descriptor
    ) {
    }

    private record StrategyIntrospection(
            StrategyMetadata metadata,
            boolean configurable,
            TradingStrategy registeredStrategy,
            Constructor<? extends TradingStrategy> strategyConstructor,
            Constructor<?> configConstructor,
            List<StrategyParameterSlot> slots,
            Map<String, StrategyParameterSlot> slotByName
    ) {
    }
}
