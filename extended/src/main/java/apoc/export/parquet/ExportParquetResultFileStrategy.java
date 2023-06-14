package apoc.export.parquet;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.meta.Types;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.export.parquet.ParquetExportType.ResultType.fromMetaType;
import static apoc.export.parquet.ParquetUtil.FIELD_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_LABELS;
import static apoc.export.parquet.ParquetUtil.FIELD_SOURCE_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TARGET_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TYPE;
import static apoc.export.parquet.ParquetUtil.getFieldName;


public class ExportParquetResultFileStrategy extends ExportParquetFileStrategy<Map<String,Object>, Result> {
    public ExportParquetResultFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, ParquetExportType exportType) {
        super(fileName, db, pools, terminationGuard, logger, exportType);
    }

    @Override
    public String getSource(Result result) {
        return String.format("statement: cols(%d)", result.columns().size());
    }

    @Override
    public Iterator<Map<String, Object>> toIterator(ProgressReporter reporter, Result data) {

        return data.stream()
                .peek(row -> {
                    row.forEach((key, val) -> {
                        final boolean notNodeNorRelationship = !(val instanceof Node) && !(val instanceof Relationship);
                        reporter.update(val instanceof Node ? 1 : 0,
                                val instanceof Relationship ? 1 : 0,
                                notNodeNorRelationship ? 1 : 0);
                        if (notNodeNorRelationship) {
                            reporter.nextRow();
                        }
                    });
                })
                .iterator();
//        return data.stream()
//                .map(row -> {
//                    row.forEach((key, val) -> {
//                        final boolean notNodeNorRelationship = !(val instanceof Node) && !(val instanceof Relationship);
//                        reporter.update(val instanceof Node ? 1 : 0,
//                                val instanceof Relationship ? 1 : 0,
//                                notNodeNorRelationship ? 1 : 0);
//                        if (notNodeNorRelationship) {
//                            reporter.nextRow();
//                        }
//                    });
//                    return row;
//                })
//                .iterator();
    }

    // todo - util..
//    public static GenericRecord mapToRecord(Map<String, Object> map, Schema schema) {
//        // todo - change getId() with getElementId
//
//        GenericRecord flattened = new GenericData.Record(schema);
//        map.forEach((k,v)-> {
//            try {
//                flattened.put(k, v);
//            } catch (Exception e) {
//                if (!e.getMessage().contains("Not a valid schema field")) {
//                    throw new RuntimeException(e);
//                }
//
//                String s = fromMetaType(Types.of(v));
//                flattened.put(getFieldName(k, s), v);
//            }
//        });
////        map.forEach(flattened::put);
//        return flattened;
//
////        flattened.put(FIELD_ID, entity.getId());
////        if (entity instanceof Node) {
////            flattened.put(FIELD_LABELS, Util.labelStrings((Node) entity));
////        } else {
////            Relationship rel = (Relationship) entity;
////            flattened.put(FIELD_TYPE, rel.getType().name());
////            flattened.put(FIELD_SOURCE_ID, rel.getStartNodeId());
////            flattened.put(FIELD_TARGET_ID, rel.getEndNodeId());
////        }
////        flattened.putAll(entity.getAllProperties());
//
//
//        // todo - to delete
////        Map<String, Object> stringObjectMap = entityToMap(entity);
//
//
//    }

    @Override
    public Stream<ProgressInfo> export(Result data, ParquetConfig config) {
//        schemaFor(List.of(createConfigMap(data, config))); todo maybe nothing, serve lo schema??...
        return super.export(data, config);
    }

//    public ExportParquetResultFileStrategy(String fileName, GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
//    }

//    public final String test() {
//        super.fileName;
//    }


}
