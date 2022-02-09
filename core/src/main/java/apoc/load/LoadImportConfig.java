package apoc.load;

import apoc.util.CompressionConfig;
import apoc.util.Util;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.Util.parseCharFromConfig;
import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseSettings.db_temporal_timezone;

public abstract class LoadImportConfig<I> extends CompressionConfig {
    
    public static final LoadImportConfig EMPTY = new LoadImportConfig(null) {
        @Override
        public Object createMapping(Object input) {
            return null;
        }
    };
    
    public static final char DEFAULT_ARRAY_SEP = ';';
    
    public static final String TIMEZONE_KEY = "timezone";
    public static final String IGNORE_KEY = "ignore";
    public static final String NULL_VALUES_KEY = "nullValues";
    public static final String ARRAY_SEP_KEY = "arraySep";
    public static final String ARRAY_KEY = "array";
    public static final String MAPPING_KEY = "mapping";

    protected final List<String> ignore;
    protected final List<String> nullValues;
    protected final Map<String, Map<String, Object>> mapping;
    protected final ZoneId zoneId;
    protected final char arraySep;
    protected final boolean array;
    
    public LoadImportConfig(Map<String, Object> config) {
        this(config, ZoneId.of(apocConfig().getString(db_temporal_timezone.name())));
    }
    
    public LoadImportConfig(Map<String, Object> config, ZoneId zoneId) {
        super(config);
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.zoneId = getTimezoneIfValid(config, zoneId);
        this.ignore = (List<String>) config.getOrDefault(IGNORE_KEY, emptyList());
        this.nullValues = (List<String>) config.getOrDefault(NULL_VALUES_KEY, emptyList());
        this.mapping =  (Map<String, Map<String, Object>>) config.getOrDefault(MAPPING_KEY, new HashMap<String, Map<String, Object>>());
        this.arraySep = parseCharFromConfig(config, ARRAY_SEP_KEY, DEFAULT_ARRAY_SEP);
        this.array = Util.toBoolean(config.get(ARRAY_KEY));
    }

    public static ZoneId getTimezoneIfValid(Map<String, Object> config, ZoneId defaultZone) {
        try {
            return Optional.ofNullable((String) config.get(TIMEZONE_KEY))
                    .map(ZoneId::of)
                    .orElse(defaultZone);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(String.format("The timezone field contains an error: %s", e.getMessage()));
        }
    }

    public ZoneId getZoneId() {
        return this.zoneId;
    }

    public List<String> getIgnore() {
        return ignore;
    }

    public List<String> getNullValues() {
        return nullValues;
    }

    public Map<String, Map<String, Object>> getMapping() {
        return mapping;
    }

    public char getArraySep() {
        return arraySep;
    }

    public boolean isArray() {
        return array;
    }

    public abstract Object createMapping(I input);
}
