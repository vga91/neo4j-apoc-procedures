package apoc.export.arrow;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class ArrowConfig {

    private final int batchSize;

    public ArrowConfig(Map<String, Object> config) {
        config = config == null ? Collections.emptyMap() : config;
        this.batchSize = Util.toInteger(config.getOrDefault("batchSize", 2000));
    }

    public int getBatchSize() {
        return batchSize;
    }
}