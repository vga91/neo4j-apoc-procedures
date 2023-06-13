package apoc.export.parquet;

import apoc.meta.Types;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static apoc.export.parquet.ParquetUtil.*;

//
//
//// todo - spostare cose qui in caso...


// todo - e se ci mettessi anche getFile?????
public interface ParquetExportType<TYPE, ROW> {
    enum Type {
        RESULT(new ResultType()),
        GRAPH(new GraphType());


        private final ParquetExportType graphType;

        Type(ParquetExportType graphType) {
            this.graphType = graphType;
        }

        public static ParquetExportType from(Object data) {
            Type type = data instanceof Result
                    ? Type.RESULT
                    : Type.GRAPH;

            return type.graphType;
        }
    }

    Schema schemaFor(GraphDatabaseService db, ParquetConfig config, TYPE data);
    GenericRecord toRecord(Schema schema, ROW data);

//    void writeBatch(ParquetWriter writer, Schema schema);

    class GraphType implements ParquetExportType<SubGraph, Entity> {

        private Schema schema;

        @Override
        public Schema schemaFor(GraphDatabaseService db, ParquetConfig config, SubGraph data) {
            if (this.schema != null) {
                return this.schema;
            }
            // todo - this row is equal
            SchemaBuilder.FieldAssembler<Schema> test = SchemaBuilder.record("test")
                    .namespace("org.apache.avro.ipc")
                    .fields();

//            final Function<Map<String, Object>, Stream<? extends Field>> flatMapStream =
//            Consumer<Map> consumer = m -> {
//                String propertyName = (String) m.get("propertyName");
//                List<String> propertyTypes = (List<String>) m.get("propertyTypes");
//                propertyTypes.stream()
//                        .map(propertyType -> toField(propertyName, new HashSet<>(propertyTypes)));
//            };
            final Predicate<Map<String, Object>> filterStream = m -> m.get("propertyName") != null;
            final ResultTransformer<Void> parsePropertiesResult = result -> {
                result.stream()
                        .filter(filterStream)
                        .forEach(m -> {
                            String propertyName = (String) m.get("propertyName");
                            List<String> propertyTypes =  ((List<List<String>>) m.get("types"))
                                    .stream().flatMap(List::stream)
                                    .toList();
//                            propertyTypes.forEach(
                                    /*propertyType -> */toField(propertyName, new HashSet<>(propertyTypes), test);
//                            );
                        });
                return null;
            };
//                    .flatMap(flatMapStream)
//                    .collect(Collectors.toSet());

            final Map<String, Object> cfg = createConfigMap(data, config);
            final Map<String, Object> parameters = Map.of("config", cfg);
//            final Set<Field> allFields = new HashSet<>();
//            Set<Field> nodeFields =

            // group by `propertyName` in order to
            String query = "CALL apoc.meta.%s($config) " +
                           "YIELD propertyName, propertyTypes " +
                           "RETURN propertyName, collect(propertyTypes) as types";

            db.executeTransactionally(String.format(query, "nodeTypeProperties"),
                    parameters, parsePropertiesResult);


            //

            // TODO - everything optional??? --> name(FIELD_LABELS).type().optional()

            // todo - optional or required??
            test.optionalLong(FIELD_ID);
            getItems(FIELD_LABELS, test).stringType();
//            allFields.addAll(nodeFields);

            if (cfg.containsKey("includeRels")) {
//                final Set<Field> relFields =
                db.executeTransactionally(String.format(query, "relTypeProperties"),
                        parameters, parsePropertiesResult);
                test.optionalLong(FIELD_SOURCE_ID);
                test.optionalLong(FIELD_TARGET_ID);
                test.optionalString(FIELD_TYPE);
//                allFields.add(FIELD_SOURCE_ID);
//                allFields.add(FIELD_TARGET_ID);
//                allFields.add(FIELD_TYPE);
//                allFields.addAll(relFields);
            }

            Schema schema = test.endRecord();

            this.schema = schema;
            return this.schema;
        }

        @Override
        public GenericRecord toRecord(Schema schema, Entity entity) {
            GenericRecord flattened = mapToRecord(schema, entity.getAllProperties());
            flattened.put(FIELD_ID, entity.getId());
            if (entity instanceof Node) {
                flattened.put(FIELD_LABELS, Util.labelStrings((Node) entity));
            } else {
                Relationship rel = (Relationship) entity;
                flattened.put(FIELD_TYPE, rel.getType().name());
                flattened.put(FIELD_SOURCE_ID, rel.getStartNodeId());
                flattened.put(FIELD_TARGET_ID, rel.getEndNodeId());
            }

            return flattened;
        }

//        @Override
//        public void writeBatch(ParquetWriter writer, Schema schema) {}

        // todo - util?
        static Map<String, Object> createConfigMap(SubGraph subGraph, ParquetConfig config) {
            final List<String> allLabelsInUse = Iterables.stream(subGraph.getAllLabelsInUse())
                    .map(Label::name)
                    .collect(Collectors.toList());
            final List<String> allRelationshipTypesInUse = Iterables.stream(subGraph.getAllRelationshipTypesInUse())
                    .map(RelationshipType::name)
                    .collect(Collectors.toList());
            Map<String, Object> configMap = new HashMap<>();
            configMap.put("includeLabels", allLabelsInUse);
            if (!allRelationshipTypesInUse.isEmpty()) {
                configMap.put("includeRels", allRelationshipTypesInUse);
            }
            configMap.putAll(config.getConfig());
            return configMap;
        }
    }

    class ResultType implements ParquetExportType<Result, Map<String,Object>> {

        private final List<Map<String, Object>> firstBatch = new ArrayList<>();

        @Override
        public Schema schemaFor(GraphDatabaseService db, ParquetConfig config, Result data) {
            // we re-calculate the schema for each batch

            // todo - this row is equal
            SchemaBuilder.FieldAssembler<Schema> test = SchemaBuilder.record("test")
                    .namespace("org.apache.avro.ipc")
                    .fields();

            int batchSize = config.getBatchSize();
            int batchCount = 0;
            while (batchCount < batchSize && data.hasNext()) {
                firstBatch.add(data.next());
                ++batchCount;
            }

            // todo - first batch
//            this.firstElement = data.next();
            schemaForResult(test, firstBatch);

            return test.endRecord();
        }

        @Override
        public GenericRecord toRecord(Schema schema, Map<String, Object> map) {
            return mapToRecord(schema, map);
        }

        void schemaForResult(SchemaBuilder.FieldAssembler<Schema> test, List<Map<String, Object>> records) {
            Set<Map.Entry<String, Set<String>>> entries = records.stream()
                    .flatMap(m -> m.entrySet().stream())
                    .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), fromMetaType(Types.of(e.getValue()))))
                    .collect(Collectors.groupingBy(e -> e.getKey(), Collectors.mapping(e -> e.getValue(), Collectors.toSet())))
                    .entrySet();
            entries
                    .stream()
                    .forEach(e -> toField(e.getKey(), e.getValue(), test));
//                    .collect(Collectors.toList());
//            return new Schema(fields);
        }
//
//        public Map<String, Object> getFirstBatch() {
//            return firstBatch;
//        }

        public static String fromMetaType(apoc.meta.Types type) {
            switch (type) {
                case INTEGER:
                    return "LONG";
                case FLOAT:
                    return "DOUBLE";
                case LIST:
                    String inner = type.toString().substring("LIST OF ".length()).trim();
                    final apoc.meta.Types innerType = apoc.meta.Types.from(inner);
                    if (innerType == Types.LIST || innerType == Types.MAP ) {
                        return "ANYARRAY";
                    }
                    return fromMetaType(innerType) + "ARRAY";
                default:
                    return type.name().replaceAll("_", "").toUpperCase();
            }
        }

//        @Override
//        public void writeBatch(ParquetWriter writer, Schema schema) {
//            firstBatch.stream().map(item -> toRecord(schema, item)).forEach(i -> {
//                try {
//                    writer.write(i);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            });
////            Map<String, Object> firstElement = ((ParquetExportType.ResultType) exportType).getFirstElement();
////            writer.write(mapToRecord(firstElement, schema));
//        }
    }

}
