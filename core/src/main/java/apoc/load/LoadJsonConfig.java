package apoc.load;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadJsonConfig extends LoadImportConfig<Map<String, Object>> {
    
    private final boolean failOnError;
    private final List<String> pathOptions;

    public LoadJsonConfig(Map<String, Object> config) {
        super(config);
        if (config == null) {
            config = Collections.emptyMap();
        }
        failOnError = (boolean) config.getOrDefault("failOnError", true);
        pathOptions = (List<String>) config.get("pathOptions");
    }

    @Override
    public Map<String, Object> createMapping(Map<String, Object> mapValue) {
        if (mapValue == null) {
            return null;
        }
        return mapValue.entrySet()
                .stream()
                .collect(HashMap::new,
                        (mapAccumulator, entry) -> {
                            final Map<String, Map<String, Object>> mapping = this.getMapping();
                            final String key = entry.getKey();
                            final Object value = entry.getValue();
                            final BaseMapping jsonMapping = new BaseMapping(key, this);
                            if (!jsonMapping.ignore) {
                                mapAccumulator.put(key,
                                        value instanceof Map && !mapping.containsKey(key) ? createMapping((Map) value)
                                                : jsonMapping.convert(value)
                                );
                            }
                        },
                        HashMap::putAll);
    }
    
    public boolean isFailOnError() {
        return failOnError;
    }

    public List<String> getPathOptions() {
        return pathOptions;
    }
}
