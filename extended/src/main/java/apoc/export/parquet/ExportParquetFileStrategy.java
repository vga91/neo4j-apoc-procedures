package apoc.export.parquet;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import apoc.util.Util;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.parquet.ParquetUtil.genericData;

// todo - not mocked


// todo - Stream<ProgressInfo> as OUT???
public abstract class ExportParquetFileStrategy<TYPE, IN> implements ExportParquetStrategy<IN, Stream<ProgressInfo>> {


    private final String fileName;

    // todo - these 4 are common with stream one
    private final GraphDatabaseService db;
    private final Pools pools;

    // todo!!! --> test..
    private final TerminationGuard terminationGuard;
    private final ParquetExportType exportType;


    private final Log logger;

    public ExportParquetFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        this.fileName = fileName;
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
        this.exportType = exportType;
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
//        final ParquetExportType exportType = getType(data);



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


//        MessageType schema1 = getSchemaForParquetFile();

        Schema schema = exportType.schemaFor(db, config, data);
//        Iterator<GenericRecord> it = toIterator(reporter, data, schema);

//        registerCustomTypes();


        Path fileToWrite = new org.apache.hadoop.fs.Path(fileName);
        final BlockingQueue<ProgressInfo> queue = new ArrayBlockingQueue<>(10);

        Util.inTxFuture(pools.getDefaultExecutorService(), db, tx -> {
            int batchCount = 0;
            List<GenericRecord> rows = new ArrayList<>(config.getBatchSize());
            AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter
                    .builder(fileToWrite);

            try {
                Iterator<TYPE> it = toIterator(reporter, data, schema);
                while (!Util.transactionIsTerminated(terminationGuard) && it.hasNext()) {
                    GenericRecord record = exportType.toRecord(schema, it.next());
                    rows.add(record);

                    if (batchCount > 0 && batchCount % config.getBatchSize() == 0) {
                        writeBatch(exportType, builder, rows, schema);
                    }
                    ++batchCount;
                }
                if (!rows.isEmpty()) {
                    writeBatch(exportType, builder, rows, schema);
                }
                QueueUtil.put(queue, progressInfo, 10);
                return true;
            } catch (Exception e) {
                logger.error("Exception while extracting Parquet data:", e);
            } finally {
                reporter.done();
                QueueUtil.put(queue, ProgressInfo.EMPTY, 10);
            }
            return true;
        });



        QueueBasedSpliterator<ProgressInfo> spliterator = new QueueBasedSpliterator<>(queue, ProgressInfo.EMPTY, terminationGuard, Integer.MAX_VALUE);
        return StreamSupport.stream(spliterator, false);
        // todo - like Arrow???
//        return Stream.of(progressInfo);
    }

    private void writeBatch(ParquetExportType exportType, AvroParquetWriter.Builder<GenericRecord> builder, List<GenericRecord> rows, Schema schema) {
        try (ParquetWriter<GenericRecord> writer = getBuild(schema, builder)) {
            extracted(exportType, rows, schema, writer);
            rows.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    @Override




//    private MessageType getSchemaForParquetFile() {
////        Schema test = SchemaBuilder.record("test")
////                .namespace("org.apache.avro.ipc")
////                .fields().endRecord();
//
//        return Types.buildMessage().named("test1");//.union(Types.buildGroup(Type.Repetition.REPEATED).named("test2").getType("naem"));
//    }

    // todo - in interface???
    public void toRecord() {

    }

    public abstract String getSource(IN subGraph);

//    public abstract Iterator<Map<String, Object>> toIterator(ProgressReporter reporter, IN data);
    public abstract Iterator<TYPE> toIterator(ProgressReporter reporter, IN data, Schema schema);

//    @Override
//    public Schema schemaFor(List<Map<String, Object>> rows) {
//        return null;
//    }

    //    default String getFileName() {
//        return fileName;
//    }

//    String getSource(IN data);
}
