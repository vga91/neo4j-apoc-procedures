package apoc.export.parquet;

import apoc.Pools;
import apoc.util.collection.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.stream.Stream;


public class ExportParquetGraphStreamStrategy extends ExportParquetStreamStrategy<Entity, SubGraph> {
    public ExportParquetGraphStreamStrategy(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        super(db, pools, terminationGuard, logger, exportType);
    }

    // todo - create interface???
    @Override
    public Iterator<Entity> toIterator(SubGraph data, Schema schema) {
        return Stream.concat(Iterables.stream(data.getNodes()), Iterables.stream(data.getRelationships()))
//                .map(entity -> entityToRecord(entity, schema))
                .iterator();
    }
}
