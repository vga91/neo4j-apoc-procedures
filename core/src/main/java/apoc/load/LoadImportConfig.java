package apoc.load;

import apoc.util.CompressionConfig;
import apoc.util.Util;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    
    private final List<String> ignore;
    private final List<String> nullValues;
    private final Map<String, Map<String, Object>> mapping;
    protected final String zoneId;

    private char arraySep;
    private boolean array;

    public LoadImportConfig() {
        this(null);
    }
    
    public LoadImportConfig(Map<String, Object> config) {
        this(config, apocConfig().getString(db_temporal_timezone.name()));
    }
    
    public LoadImportConfig(Map<String, Object> config, String zoneId) {
        super(config);
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.zoneId = (String) config.getOrDefault("timezone", zoneId);
        ignore = (List<String>) config.getOrDefault("ignore", emptyList());
        nullValues = (List<String>) config.getOrDefault("nullValues", emptyList());
        mapping =  (Map<String, Map<String, Object>>) config.getOrDefault("mapping", new HashMap<>());
        arraySep = parseCharFromConfig(config, "arraySep", DEFAULT_ARRAY_SEP);
        array = Util.toBoolean(config.get("array"));
    }

    public String getZoneId(){
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
