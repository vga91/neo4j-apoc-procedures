package apoc.export.parquet;


public interface ExportArrowStrategy<IN, OUT> {
    OUT export(IN data, ParquetConfig config);

    // todo ??
    // Object convertValue(Object data);

    // todo - TerminationGuard


}
