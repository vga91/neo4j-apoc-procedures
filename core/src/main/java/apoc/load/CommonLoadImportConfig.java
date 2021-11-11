package apoc.load;

import apoc.util.CompressionConfig;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;

public class CommonLoadImportConfig extends CompressionConfig {
    
    private List<String> ignore;
    private List<String> nullValues;
    private Map<String, Map<String, Object>> mapping;
    private ZoneId zoneId = null;

    public CommonLoadImportConfig(Map<String, Object> config) {
//        this(config, ZoneId.systemDefault().getId());
        this(config, null);
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

    public static ZoneId getTimezoneIfValid(Map<String, Object> config, String defaultZone) {
        try {
            return Optional.ofNullable((String) config.getOrDefault("timezone", defaultZone))
                    .map(ZoneId::of)
                    .orElse(null);
//            return ZoneId.of((String) config.getOrDefault("timezone", defaultZone));
//            return config.containsKey("timezone") ?
//                    ZoneId.of(config.get("timezone").toString()) : defaultZone;
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
