package apoc.export.parquet;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

// todo - not mocked


// todo - Stream<ProgressInfo> as OUT???
public abstract class ExportParquetFileStrategy<IN> implements ExportParquetStrategy<IN, Stream<ProgressInfo>> {

    // todo - ParquetUtil
    public static GenericData genericData;
    static {
        genericData = new GenericData();
        // need to add logicalTime Support
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
//        timeSupport.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
        genericData.addLogicalTypeConversion(new ParquetUtil.DurationValueConversion());
        genericData.addLogicalTypeConversion(CustomTypes.POINT.getConversion());//new ParquetUtil.PointValueConversion());
    }

    private final String fileName;

    // todo - these 4 are common with stream one
    private final GraphDatabaseService db;
    private final Pools pools;

    // todo!!! --> test..
    private final TerminationGuard terminationGuard;


    private final Log logger;

    public ExportParquetFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
        this.fileName = fileName;
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
//        this.exportType = getType(Class<IN>);
    }


    // todo --> https://parquet.apache.org/docs/file-format/configurations/

//    public class CustomParquetWriter extends ParquetWriter<Object> {
//
//        public CustomParquetWriter(
//                Path file,
//                Schema schema,
//                boolean enableDictionary,
//                CompressionCodecName codecName
//        ) throws IOException {
//            super(file, schema);//, codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false);
//        }
//    }

    // todo - here ....
    public Stream<ProgressInfo> export(IN data, ParquetConfig config) {
        // todo - config
        final ParquetExportType exportType = getType(data);

        Schema schema = exportType.schemaFor(db, config, data);

//        exportType.schemaFor(db, )

        // todo --> "getSource(data)"
        ProgressInfo progressInfo = new ProgressInfo(fileName, "getSource(data)", "parquet");
        progressInfo.batchSize = config.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);


        // todo - fileOutputStream instead of Path, which is deprecated
//        Path fileToWrite = new org.apache.hadoop.fs.Path("fileToWrite.parquet");
//        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
//                .<GenericData.Record>builder(fileToWrite)
//                .withSchema(schema)
//                .withConf(new Configuration())
//                // todo - config...
////                .withCompressionCodec(CompressionCodecName.SNAPPY)
//                // todo - config...
//                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
//                .build()) {
//
//            for (Iterator<GenericData.Record> it = toIterator(reporter, data, schema); it.hasNext(); ) {
//                GenericData.Record record = it.next();
//                writer.write(record);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }


        MessageType schema1 = getSchemaForParquetFile();



        Path fileToWrite = new org.apache.hadoop.fs.Path(fileName);
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(fileToWrite)
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
                .build()) {
//        try (ParquetWriter<Object> writer = new CustomParquetWriter(fileToWrite, schema1, true, CompressionCodecName.GZIP)) {

            for (Iterator<GenericRecord> it = toIterator(reporter, data, schema); it.hasNext(); ) {
                GenericRecord record = it.next();
                // todo - try catch...
                try {
                    writer.write(record);
                } catch (Exception e) {
                    System.out.println("e = " + e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // todo - like Arrow???
        return Stream.of(progressInfo);
    }

    private MessageType getSchemaForParquetFile() {
//        Schema test = SchemaBuilder.record("test")
//                .namespace("org.apache.avro.ipc")
//                .fields().endRecord();

        return Types.buildMessage().named("test1");//.union(Types.buildGroup(Type.Repetition.REPEATED).named("test2").getType("naem"));
    }

    // todo - in interface???
    public void toRecord() {

    }

    public abstract String getSource(IN subGraph);

//    public abstract Iterator<Map<String, Object>> toIterator(ProgressReporter reporter, IN data);
    public abstract Iterator<GenericRecord> toIterator(ProgressReporter reporter, IN data, Schema schema);

//    @Override
//    public Schema schemaFor(List<Map<String, Object>> rows) {
//        return null;
//    }

    //    default String getFileName() {
//        return fileName;
//    }

//    String getSource(IN data);
}
