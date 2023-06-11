package apoc.export.parquet;

import apoc.Pools;
import apoc.result.ByteArrayResult;
import apoc.result.ProgressInfo;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.logging.Log;

import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;

public class ExportParquetService {
    private final GraphDatabaseService db;
    private final Pools pools;
    private final TerminationGuard terminationGuard;
    private final Log logger;

    public ExportParquetService(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
    }

    public Stream<ByteArrayResult> stream(Object data, ParquetConfig config) {
        // TODO
        return null;
//        if (data instanceof Result) {
//            return new ExportParquetResultStreamStrategy(db, pools, terminationGuard, logger).export((Result) data, config);
//        } else {
//            return new ExportParquetGraphStreamStrategy(db, pools, terminationGuard, logger).export((SubGraph) data, config);
//        }
    }

    public Stream<ProgressInfo> file(String fileName, Object data, ParquetConfig config) {
        // todo - substitute with checkWriteAllowed
        // we cannot use apocConfig().checkWriteAllowed(..) because the error is confusing
        //  since it says "... use the `{stream:true}` config", but with arrow procedures the streaming mode is implemented via different procedures
        if (!apocConfig().getBoolean(APOC_EXPORT_FILE_ENABLED)) {
            throw new RuntimeException("todo...");
        }
        if (data instanceof Result) {
            return new ExportParquetResultFileStrategy(fileName, db, pools, terminationGuard, logger).export((Result) data, config);
        } else {
            return new ExportParquetGraphFileStrategy(fileName, db, pools, terminationGuard, logger).export((SubGraph) data, config);
        }
    }


    // TODO !!!
}
