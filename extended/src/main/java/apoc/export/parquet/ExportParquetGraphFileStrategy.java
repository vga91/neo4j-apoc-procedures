package apoc.export.parquet;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.stream.Stream;

import static apoc.export.parquet.ParquetUtil.*;

public class ExportParquetGraphFileStrategy extends ExportParquetFileStrategy<Entity, SubGraph>  {
    public ExportParquetGraphFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        super(fileName, db, pools, terminationGuard, logger, exportType);
    }

    @Override
    public Stream<ProgressInfo> export(SubGraph data, ParquetConfig config) {
        return super.export(data, config);
    }

    @Override
    public String getSource(SubGraph subGraph) {
        return String.format("graph: nodes(%d), rels(%d)", Iterables.count(subGraph.getNodes()), Iterables.count(subGraph.getRelationships()));
    }

    @Override
    public Iterator<Entity> toIterator(ProgressReporter reporter, SubGraph data, Schema schema) {
//    public Iterator<GenericData.Record> toIterator(ProgressReporter reporter, SubGraph data, Schema schema) {
//    public Iterator<Map<String, Object>> toIterator(ProgressReporter reporter, SubGraph data) {
        return Stream.concat(Iterables.stream(data.getNodes()), Iterables.stream(data.getRelationships()))
                .map(entity -> {
                    reporter.update(entity instanceof Node ? 1 : 0,
                            entity instanceof Relationship ? 1 : 0, 0);
                    return entity;
//                    return this.entityToRecord(entity, schema);
                })
                .iterator();
    }




////    Object entityToMap(Entity entity, Schema schema) {
//    public static GenericRecord entityToRecord(Entity entity, Schema schema) {
//        // todo - change getId() with getElementId
//
//        GenericRecord flattened = ExportParquetResultFileStrategy.mapToRecord(entity.getAllProperties(), schema);// new GenericData.Record(schema);
//        flattened.put(FIELD_ID, entity.getId());
//        if (entity instanceof Node) {
//            flattened.put(FIELD_LABELS, Util.labelStrings((Node) entity));
//        } else {
//            Relationship rel = (Relationship) entity;
//            flattened.put(FIELD_TYPE, rel.getType().name());
//            flattened.put(FIELD_SOURCE_ID, rel.getStartNodeId());
//            flattened.put(FIELD_TARGET_ID, rel.getEndNodeId());
//        }
////        entity.getAllProperties().forEach(flattened::put);
////        flattened.putAll(entity.getAllProperties());
//
//
//        // todo - to delete
////        Map<String, Object> stringObjectMap = entityToMap(entity);
//
//
//        return flattened;
//    }

//    Map<String, Object> entityToMap(Entity entity) {
//        // todo - change getId() with getElementId
//
//        Map<String, Object> flattened = new HashMap<>();
//        flattened.put(FIELD_ID, entity.getId());
//        if (entity instanceof Node) {
//            flattened.put(FIELD_LABELS, Util.labelStrings((Node) entity));
//        } else {
//            Relationship rel = (Relationship) entity;
//            flattened.put(FIELD_TYPE, rel.getType().name());
//            flattened.put(FIELD_SOURCE_ID, rel.getStartNodeId());
//            flattened.put(FIELD_TARGET_ID, rel.getEndNodeId());
//        }
//        flattened.putAll(entity.getAllProperties());
//        return flattened;
//    }


}
