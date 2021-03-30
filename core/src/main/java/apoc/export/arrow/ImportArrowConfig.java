package apoc.export.arrow;

import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class ImportArrowConfig {

    private final int batchSize;

    public ImportArrowConfig(Map<String, Object> config) {
        config = config == null ? Collections.emptyMap() : config;
        this.batchSize = Util.toInteger(config.getOrDefault("batchSize", 2000));
    }

    public int getBatchSize() {
        return batchSize;
    }
}
