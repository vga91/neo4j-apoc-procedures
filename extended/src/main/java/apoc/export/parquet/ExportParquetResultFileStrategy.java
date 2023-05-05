package apoc.export.parquet;

import apoc.Pools;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

public class ExportParquetResultFileStrategy implements ExportParquetFileStrategy {
    public ExportParquetResultFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
    }
}
