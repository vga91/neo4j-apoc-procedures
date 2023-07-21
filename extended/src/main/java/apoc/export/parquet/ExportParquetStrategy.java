package apoc.export.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;


public interface ExportParquetStrategy<IN, OUT> {

    OUT export(IN data, ParquetConfig config);

    default <T> void writeRows(List<T> rows, ParquetWriter<Group> writer, ParquetExportType type, MessageType schema) {
        rows.stream()
                .map(i -> type.toRecord(schema, i))
                .forEach(i -> {
            try {
                writer.write(i);
            } catch (Exception e) {
//            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        rows.clear();
    }

    default ParquetWriter<Group> getBuild(MessageType schema, ExampleParquetWriter.Builder builder)  {
        try {
            return builder
                    .withType(schema)
                    // TODO - check other configs
//                    .withConf(new Configuration())
//                    .withDataModel(genericData)
                    // TODO - configurable. This generate a .crc file
                    .withValidation(false)
                    // TODO - config...
    //                .withCompressionCodec(CompressionCodecName.SNAPPY)
                    // TODO - configurable?
//                    .enableDictionaryEncoding()
//                    .withDictionaryPageSize(2*1024)

                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
