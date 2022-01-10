package apoc.load;

import apoc.util.CompressionConfig;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static apoc.util.DateParseUtil.DB_TEMPORAL_TIMEZONE;
import static apoc.util.DateParseUtil.getTimezoneIfValid;
import static java.util.Collections.emptyList;

public class CommonLoadImportConfig extends CompressionConfig {
    private final List<String> ignore;
    private final List<String> nullValues;
    private final Map<String, Map<String, Object>> mapping;
    private final ZoneId zoneId;

    public CommonLoadImportConfig(Map<String, Object> config) {
        this(config, DB_TEMPORAL_TIMEZONE);
    }
    
    public CommonLoadImportConfig(Map<String, Object> config, String zoneId) {
        super(config);
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.zoneId = getTimezoneIfValid(config, zoneId);
        
        ignore = (List<String>) config.getOrDefault("ignore", emptyList());
        nullValues = (List<String>) config.getOrDefault("nullValues", emptyList());
        mapping =  (Map<String, Map<String, Object>>) config.getOrDefault("mapping", Collections.emptyMap());
    }

    public ZoneId getZoneId(){
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
}
