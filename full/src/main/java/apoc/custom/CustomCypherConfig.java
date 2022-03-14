package apoc.custom;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class CustomCypherConfig {
    public static final CustomCypherConfig EMPTY = new CustomCypherConfig(null);
    public static final String WRAP_MAP = "wrapMap";
    
    private final boolean wrapMap;
    
    public CustomCypherConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.wrapMap = Util.toBoolean(config.getOrDefault(WRAP_MAP, true));
    }

    public boolean isWrapMap() {
        return wrapMap;
    }
}
