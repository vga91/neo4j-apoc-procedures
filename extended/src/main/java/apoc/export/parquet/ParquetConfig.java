package apoc.export.parquet;

import apoc.util.Util;
import org.apache.parquet.hadoop.ParquetFileWriter;

import java.util.Collections;
import java.util.Map;

public class ParquetConfig {

    private final int batchSize;

    private final Map<String, Object> config;
    private final ParquetFileWriter.Mode mode;

    public ParquetConfig(Map<String, Object> config) {
        this.config = config == null ? Collections.emptyMap() : config;
        this.batchSize = Util.toInteger(this.config.getOrDefault("batchSize", 2000));

        this.mode = ParquetFileWriter.Mode.valueOf((String) this.config.getOrDefault("mode", ParquetFileWriter.Mode.CREATE.name()));
    }

    public int getBatchSize() {
        return batchSize;
    }


    // todo - useful??
    public Map<String, Object> getConfig() {
        return config;
    }
}

