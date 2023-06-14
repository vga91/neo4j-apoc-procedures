package apoc.export.parquet;

import apoc.meta.Types;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static apoc.export.parquet.ParquetUtil.*;

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

    Schema schemaFor(GraphDatabaseService db, List<Map<String,Object>> type);
    GenericRecord toRecord(Schema schema, ROW data);
    List<Map<String,Object>> createConfig(List<ROW> row, TYPE data, ParquetConfig config);

    class GraphType implements ParquetExportType<SubGraph, Entity> {

        private Schema schema;
        private List<Map<String,Object>> config;

        @Override
        public Schema schemaFor(GraphDatabaseService db, List<Map<String,Object>> type) {
            if (this.schema != null) {
                return this.schema;
            }
            SchemaBuilder.FieldAssembler<Schema> test = SchemaBuilder.record("apocExport")
                    .namespace("apoc.parquet")
                    .fields();

            final Predicate<Map<String, Object>> filterStream = m -> m.get("propertyName") != null;
            final ResultTransformer<Void> parsePropertiesResult = result -> {
                result.stream()
                        .filter(filterStream)
                        .forEach(m -> {
                            String propertyName = (String) m.get("propertyName");
                            List<String> propertyTypes =  ((List<List<String>>) m.get("types"))
                                    .stream().flatMap(List::stream)
                                    .toList();
                            toField(propertyName, new HashSet<>(propertyTypes), test);
                        });
                return null;
            };

            Map<String, Object> confMap = type.get(0);
            final Map<String, Object> parameters = Map.of("config", confMap);

            // group by `propertyName` in order to
            String query = "CALL apoc.meta.%s($config) " +
                           "YIELD propertyName, propertyTypes " +
                           "RETURN propertyName, collect(propertyTypes) as types";

            db.executeTransactionally(String.format(query, "nodeTypeProperties"),
                    parameters, parsePropertiesResult);

            test.optionalLong(FIELD_ID);
            getItems(FIELD_LABELS, test).stringType();

            if (confMap.containsKey("includeRels")) {
                db.executeTransactionally(String.format(query, "relTypeProperties"),
                        parameters, parsePropertiesResult);
                test.optionalLong(FIELD_SOURCE_ID);
                test.optionalLong(FIELD_TARGET_ID);
                test.optionalString(FIELD_TYPE);
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

        @Override
        public List<Map<String, Object>> createConfig(List<Entity> entity, SubGraph data, ParquetConfig config) {
            if (this.config != null) {
                return this.config;
            }
            final List<String> allLabelsInUse = Iterables.stream(data.getAllLabelsInUse())
                    .map(Label::name)
                    .collect(Collectors.toList());
            final List<String> allRelationshipTypesInUse = Iterables.stream(data.getAllRelationshipTypesInUse())
                    .map(RelationshipType::name)
                    .collect(Collectors.toList());
            Map<String, Object> configMap = new HashMap<>();
            configMap.put("includeLabels", allLabelsInUse);
            if (!allRelationshipTypesInUse.isEmpty()) {
                configMap.put("includeRels", allRelationshipTypesInUse);
            }
            configMap.putAll(config.getConfig());
            this.config = List.of(configMap);
            return this.config;
        }
    }

    class ResultType implements ParquetExportType<List<Map<String, Object>>, Map<String,Object>> {

        @Override
        public Schema schemaFor(GraphDatabaseService db, List<Map<String, Object>> type) {
            // we re-calculate the schema for each batch

            // todo - this row is equal
            SchemaBuilder.FieldAssembler<Schema> test = SchemaBuilder.record("apocExport")
                    .namespace("apoc.parquet")
                    .fields();

            type.stream()
                    .flatMap(m -> m.entrySet().stream())
                    .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), fromMetaType(Types.of(e.getValue()))))
                    .collect(Collectors.groupingBy(e -> e.getKey(), Collectors.mapping(e -> e.getValue(), Collectors.toSet())))
                    .entrySet()
                    .stream()
                    .forEach(e -> toField(e.getKey(), e.getValue(), test));

//            int batchSize = config.getBatchSize();
//            int batchCount = 0;
//            while (batchCount < batchSize && data.hasNext()) {
//                firstBatch.add(data.next());
//                ++batchCount;
//            }

            // todo - first batch
//            this.firstElement = data.next();
//            schemaForResult(test, firstBatch);

            return test.endRecord();
        }

        @Override
        public GenericRecord toRecord(Schema schema, Map<String, Object> map) {
            return mapToRecord(schema, map);
        }

        @Override
        public List<Map<String, Object>> createConfig(List<Map<String, Object>> row, List<Map<String, Object>> data, ParquetConfig config) {
            return row;
        }

//        void schemaForResult(SchemaBuilder.FieldAssembler<Schema> test, List<Map<String, Object>> records) {
//            records.stream()
//                    .flatMap(m -> m.entrySet().stream())
//                    .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), fromMetaType(Types.of(e.getValue()))))
//                    .collect(Collectors.groupingBy(e -> e.getKey(), Collectors.mapping(e -> e.getValue(), Collectors.toSet())))
//                    .entrySet()
//                    .stream()
//                    .forEach(e -> toField(e.getKey(), e.getValue(), test));
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
    }

}
