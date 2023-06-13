package apoc.export.parquet;

import apoc.Pools;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;

import static apoc.export.parquet.ExportParquetResultFileStrategy.mapToRecord;

public class ExportParquetResultStreamStrategy extends ExportParquetStreamStrategy<Result> {
    public ExportParquetResultStreamStrategy(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
        super(db, pools, terminationGuard, logger);
    }

    @Override
    public Iterator<GenericRecord> toIterator(Result data, Schema schema) {
        return data.stream()
                .map(row -> mapToRecord(row, schema))
                .iterator();
    }
}
