package apoc.export.parquet;

import apoc.util.Util;
import org.apache.parquet.hadoop.ParquetFileWriter;

import java.util.Collections;
import java.util.Map;

public class ParquetConfig {

    private final int batchSize;

    private final Map<String, Object> config;
    private final ParquetFileWriter.Mode mode;
    private final Map<String, Object> mapping;

    public ParquetConfig(Map<String, Object> config) {
        this.config = config == null ? Collections.emptyMap() : config;
        this.batchSize = Util.toInteger(this.config.getOrDefault("batchSize", 20000));
        this.mapping = (Map<String, Object>) this.config.getOrDefault("mapping", Map.of());

        this.mode = ParquetFileWriter.Mode.valueOf((String) this.config.getOrDefault("mode", ParquetFileWriter.Mode.CREATE.name()));
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public Map<String, Object> getMapping() {
        return mapping;
    }
}

