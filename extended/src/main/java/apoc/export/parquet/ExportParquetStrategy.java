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
        rows.stream()
                .map(i -> type.toRecord(schema, i))
                .forEach(i -> {
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
                // TODO - check other configs
                .withConf(new Configuration())
                .withDataModel(genericData)
                // TODO - configurable. This generate a .crc file
                .withValidation(false)
                // TODO - config...
//                .withCompressionCodec(CompressionCodecName.SNAPPY)
                // TODO - configurable...
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

}
