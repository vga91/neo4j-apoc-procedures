package apoc.export.parquet;

import apoc.Pools;
import apoc.result.ProgressInfo;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.stream.Stream;

public class ExportParquetGraphFileStrategy {
    public ExportParquetGraphFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
    }

    public Stream<ProgressInfo> export(SubGraph data, ParquetConfig config) {
        return null;
    }
}
