package apoc.diff;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class DiffConfig {
    private final boolean findById;

    public DiffConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.findById = Util.toBoolean(config.getOrDefault("findById", false));
    }

    public boolean isFindById() {
        return findById;
    }
}