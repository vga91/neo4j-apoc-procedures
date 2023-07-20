//package apoc.export.parquet;
//
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.parquet.column.ParquetProperties;
//import org.apache.parquet.example.data.Group;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.parquet.hadoop.api.WriteSupport;
//import org.apache.parquet.hadoop.example.GroupWriteSupport;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.parquet.io.OutputFile;
//import org.apache.parquet.schema.MessageType;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//
//
//public class ExampleParquetWriterCustom extends ParquetWriter<Group> {
//
//    public static ExampleParquetWriterCustom.Builder builder(Path file) {
//        return new ExampleParquetWriterCustom.Builder(file);
//    }
//
//    public static ExampleParquetWriterCustom.Builder builder(OutputFile file) {
//        return new ExampleParquetWriterCustom.Builder(file);
//    }
//
//    /**
//     * Create a new {@link org.apache.parquet.hadoop.example.ExampleParquetWriter}.
//     *
//     * @param file The file name to write to.
//     * @param writeSupport The schema to write with.
//     * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
//     * @param blockSize the block size threshold.
//     * @param pageSize See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
//     * @param enableDictionary Whether to use a dictionary to compress columns.
//     * @param conf The Configuration to use.
//     * @throws IOException
//     */
//    ExampleParquetWriterCustom(Path file, WriteSupport<Group> writeSupport,
//                         CompressionCodecName compressionCodecName,
//                         int blockSize, int pageSize, boolean enableDictionary,
//                         boolean enableValidation,
//                         ParquetProperties.WriterVersion writerVersion,
//                         Configuration conf)
//            throws IOException {
//        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
//                pageSize, enableDictionary, enableValidation, writerVersion, conf);
//    }
//
//    public static class Builder extends ParquetWriter.Builder<Group, ExampleParquetWriterCustom.Builder> {
//        private MessageType type = null;
//        private Map<String, String> extraMetaData = new HashMap<String, String>();
//
//        private Builder(Path file) {
//            super(file);
//        }
//
//        private Builder(OutputFile file) {
//            super(file);
//        }
//
//        public ExampleParquetWriterCustom.Builder withType(MessageType type) {
//            this.type = type;
//            return this;
//        }
//
//        public ExampleParquetWriterCustom.Builder withExtraMetaData(Map<String, String> extraMetaData) {
//            this.extraMetaData = extraMetaData;
//            return this;
//        }
//
//        @Override
//        protected ExampleParquetWriterCustom.Builder self() {
//            return this;
//        }
//
//        @Override
//        protected WriteSupport<Group> getWriteSupport(Configuration conf) {
//            return new CustomGroupWriteSupport(type, extraMetaData);
//        }
//
//        @Override
//        public ParquetWriter<Group> build() throws IOException {
//            return super.build();
//        }
//    }
//}
