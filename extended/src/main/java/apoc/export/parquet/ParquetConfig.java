package apoc.export.parquet;

import apoc.util.Util;
import org.apache.parquet.hadoop.ParquetFileWriter;

import java.util.Collections;
import java.util.Map;

public class ParquetConfig {
    // todo - extends ExportConfig ??

    // -- todo -->  .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)

    private final int batchSize;
    private final boolean importId;

    private final Map<String, Object> config;
    private final ParquetFileWriter.Mode mode;

    public ParquetConfig(Map<String, Object> config) {
        this.config = config == null ? Collections.emptyMap() : config;
        this.batchSize = Util.toInteger(this.config.getOrDefault("batchSize", 2000));

        this.importId = Util.toBoolean(this.config.get("importId"));

        this.mode = ParquetFileWriter.Mode.valueOf((String) this.config.getOrDefault("mode", ParquetFileWriter.Mode.CREATE.name()));
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isImportId() {
        return importId;
    }

    // todo - useful??
    public Map<String, Object> getConfig() {
        return config;
    }
}

