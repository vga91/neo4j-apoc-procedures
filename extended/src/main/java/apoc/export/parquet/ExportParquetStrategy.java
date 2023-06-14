package apoc.export.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.neo4j.graphdb.Result;

import java.io.IOException;
import java.util.List;

import static apoc.export.parquet.ParquetUtil.genericData;

public interface ExportParquetStrategy<IN, OUT> {

    OUT export(IN data, ParquetConfig config);

    default <T> void writeRows(List<T> rows, ParquetWriter<GenericRecord> writer, ParquetExportType type, Schema schema) {
        rows.stream().map(i -> type.toRecord(schema, i)).forEach(i -> {
            try {
                writer.write(i);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        rows.clear();
    }

    default ParquetWriter<GenericRecord> getBuild(Schema schema, AvroParquetWriter.Builder<GenericRecord> builder) throws IOException {
        return builder
                .withSchema(schema)
                .withConf(new Configuration())
                .withDataModel(genericData)
                // todo ---> other with

                // todo - configurable?? this generate a .crc file
                .withValidation(false)
                // todo - config...
//                .withCompressionCodec(CompressionCodecName.SNAPPY)
                // todo - config...
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
//                .withDataModel(genericData)
                .build();
    }

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
