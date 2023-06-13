package apoc.export.parquet;

import org.neo4j.graphdb.Result;

public interface ExportParquetStrategy<IN, OUT> {

    OUT export(IN data, ParquetConfig config);

    // todo ??
    // Object convertValue(Object data);

    // todo - TerminationGuard


//    Schema schemaFor(List<Map<String, Object>> rows);

    default ParquetExportType getType(IN data) {

        if (data instanceof Result) {
            return new ParquetExportType.ResultType();
        }
        return new ParquetExportType.GraphType();
    }

}
