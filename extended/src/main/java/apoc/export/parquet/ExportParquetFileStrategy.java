package apoc.export.parquet;

import apoc.Pools;
import apoc.result.ProgressInfo;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.stream.Stream;

// todo - not mocked


// todo - Stream<ProgressInfo> as OUT???
public abstract class ExportParquetFileStrategy<IN> implements ExportParquetStrategy<IN, Stream<ProgressInfo>> {

    private final String fileName;

    // todo - these 4 are common with stream one
    private final GraphDatabaseService db;
    private final Pools pools;
    private final TerminationGuard terminationGuard;
    private final Log logger;

    public ExportParquetFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
        this.fileName = fileName;
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
    }

    public Stream<ProgressInfo> export(IN data, ParquetConfig config) {
        // todo - config


        // todo --> "getSource(data)"
        ProgressInfo progressInfo = new ProgressInfo(fileName, "getSource(data)", "parquet");


//        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
//                .<GenericData.Record>builder(fileToWrite)
//                .withSchema(SCHEMA)
//                .withConf(new Configuration())
//                // todo - config...
//                .withCompressionCodec(CompressionCodecName.SNAPPY)
//                // todo - config...
//                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
//                .build()) {
//
//            for (GenericData.Record record : recordsToWrite) {
//                writer.write(record);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        return null;
    }

    // todo - in interface???
    public void toRecord() {

    }

//    default String getFileName() {
//        return fileName;
//    }
}
