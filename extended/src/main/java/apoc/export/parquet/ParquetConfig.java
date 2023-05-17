package apoc.export.parquet;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class ParquetConfig {
    // todo - extends ExportConfig ??

    // -- todo -->  .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)

    private final int batchSize;

    private final Map<String, Object> config;

    public ParquetConfig(Map<String, Object> config) {
        this.config = config == null ? Collections.emptyMap() : config;
        this.batchSize = Util.toInteger(this.config.getOrDefault("batchSize", 2000));
    }

    public int getBatchSize() {
        return batchSize;
    }

    // todo - useful??
    public Map<String, Object> getConfig() {
        return config;
    }
}

