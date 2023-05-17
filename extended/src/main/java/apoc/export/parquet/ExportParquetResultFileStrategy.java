package apoc.export.parquet;

import apoc.Pools;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

public class ExportParquetResultFileStrategy extends ExportParquetFileStrategy {
    public ExportParquetResultFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
        super(fileName, db, pools, terminationGuard, logger);
    }
//    public ExportParquetResultFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
//    }
}
