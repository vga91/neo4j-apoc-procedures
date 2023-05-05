package apoc.export.parquet;

import apoc.export.arrow.ArrowConfig;
import apoc.result.ProgressInfo;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.stream.Stream;

// todo - Stream<ProgressInfo> as OUT???
public interface ExportParquetFileStrategy<IN> extends ExportArrowStrategy<IN, Stream<ProgressInfo>> {


    default Stream<ProgressInfo> export(IN data, ArrowConfig config) {
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
