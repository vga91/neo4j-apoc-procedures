package apoc.load;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class CommonLoadImportConfig {
    
    private List<String> ignore;
    private List<String> nullValues;
    private Map<String, Map<String, Object>> mapping;
    private ZoneId zoneId = null;

    public CommonLoadImportConfig(Map<String, Object> config) {
        this(config, null);
    }
    
    public CommonLoadImportConfig(Map<String, Object> config, ZoneId zoneId) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.zoneId = getTimezoneIfValid(config, zoneId);
        
        ignore = (List<String>) config.getOrDefault("ignore", emptyList());
        nullValues = (List<String>) config.getOrDefault("nullValues", emptyList());
        mapping =  (Map<String, Map<String, Object>>) config.getOrDefault("mapping", Collections.emptyMap());
    }

    public static ZoneId getTimezoneIfValid(Map<String, Object> config, ZoneId defaultZone) {
        try {
            return config.containsKey("timezone") ?
                    ZoneId.of(config.get("timezone").toString()) : defaultZone;
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(String.format("The timezone field contains an error: %s", e.getMessage()));
        }
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
