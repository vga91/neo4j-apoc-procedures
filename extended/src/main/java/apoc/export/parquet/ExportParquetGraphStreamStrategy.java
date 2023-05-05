package apoc.export.parquet;

import apoc.Pools;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

public class ExportParquetGraphStreamStrategy {
    public ExportParquetGraphStreamStrategy(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
    }
}
