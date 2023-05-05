package apoc.export.parquet;

import apoc.Pools;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

public class ExportParquetResultStreamStrategy {
    private final GraphDatabaseService db;
    private final Pools pools;
    private final TerminationGuard terminationGuard;
    private final Log logger;

    public ExportParquetResultStreamStrategy(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
    }
}
