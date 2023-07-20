package apoc.export.parquet;

import apoc.meta.Types;
import apoc.util.Util;
import apoc.util.collection.Iterables;
//import org.apache.avro.Schema;
//import org.apache.avro.SchemaBuilder;
//import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static apoc.export.parquet.ParquetUtil.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

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

//    default SchemaBuilder.FieldAssembler<MessageType> startFieldAssembler() {
//        return SchemaBuilder.record("apocExport")
//                .namespace("apoc.parquet")
//                .fields();
//    }

    MessageType schemaFor(GraphDatabaseService db, List<Map<String,Object>> type);
    Group toRecord(MessageType schema, ROW data);
    List<Map<String,Object>> createConfig(List<ROW> row, TYPE data, ParquetConfig config);

    class GraphType implements ParquetExportType<SubGraph, Entity> {

        private MessageType schema;
        private List<Map<String,Object>> config;

        @Override
        public MessageType schemaFor(GraphDatabaseService db, List<Map<String,Object>> type) {
//            if (true) {
//                org.apache.parquet.schema.Types.GroupBuilder<MessageType> messageTypeBuilder = org.apache.parquet.schema.Types.buildMessage();
//                getField(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.BINARY, FIELD_LABELS);//.stringType();
//
//                return messageTypeBuilder.named("msg");
//            }



            if (this.schema != null) {
                return this.schema;
            }
//            SchemaBuilder.FieldAssembler<Schema> fieldAssembler = startFieldAssembler();

            org.apache.parquet.schema.Types.GroupBuilder<MessageType> messageTypeBuilder = org.apache.parquet.schema.Types.buildMessage();

            final Predicate<Map<String, Object>> filterStream = m -> m.get("propertyName") != null;
            final ResultTransformer<Void> parsePropertiesResult = result -> {
                result.stream()
                        .filter(filterStream)
                        .forEach(m -> {
                            String propertyName = (String) m.get("propertyName");
                            List<String> propertyTypes =  ((List<List<String>>) m.get("types"))
                                    .stream().flatMap(List::stream)
                                    .collect(Collectors.toList());
                            toField(propertyName, new HashSet<>(propertyTypes), messageTypeBuilder);
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

//            messageTypeBuilder.addField(org.apache.parquet.schema.Types.optional(INT64).named(FIELD_ID));
            getField(messageTypeBuilder, INT64, FIELD_ID);
//            messageTypeBuilder.optionalLong(FIELD_ID);
            getItems(FIELD_LABELS, messageTypeBuilder, PrimitiveType.PrimitiveTypeName.BINARY);//.stringType();
//            getField(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.BINARY, FIELD_LABELS);//.stringType();

            if (confMap.containsKey("includeRels")) {
                db.executeTransactionally(String.format(query, "relTypeProperties"),
                        parameters, parsePropertiesResult);
                getField(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT64, FIELD_SOURCE_ID);
                getField(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.INT64, FIELD_TARGET_ID);
                getField(messageTypeBuilder, PrimitiveType.PrimitiveTypeName.BINARY, FIELD_TYPE);
//                fieldAssembler.optionalLong(FIELD_SOURCE_ID);
//                fieldAssembler.optionalLong(FIELD_TARGET_ID);
//                fieldAssembler.optionalString(FIELD_TYPE);
            }

            this.schema = messageTypeBuilder.named("apocExport");// fieldAssembler.endRecord();
            return this.schema;
        }

        @Override
        public Group toRecord(MessageType schema, Entity entity) {
//            if (true) {
//                GroupFactory factory = new SimpleGroupFactory(schema);
//                return
//            }


            Group flattened = mapToRecord(schema, entity.getAllProperties());
            flattened.append(FIELD_ID, entity.getId());
            if (entity instanceof Node) {
                // todo - mocked toString()
                extracted(flattened, FIELD_LABELS, Util.labelStrings((Node) entity));
//                flattened.add(FIELD_LABELS, Util.labelStrings((Node) entity));
            } else {
                Relationship rel = (Relationship) entity;
                flattened.append(FIELD_TYPE, rel.getType().name());
                flattened.append(FIELD_SOURCE_ID, rel.getStartNodeId());
                flattened.append(FIELD_TARGET_ID, rel.getEndNodeId());
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

    class ResultType implements ParquetExportType<Result, Map<String,Object>> {

        @Override
        public MessageType schemaFor(GraphDatabaseService db, List<Map<String, Object>> type) {
            // todo - implement
            return null;

            // we re-calculate the schema for each batch

//            SchemaBuilder.FieldAssembler<Schema> fieldAssembler = startFieldAssembler();
//
//            type.stream()
//                    .flatMap(m -> m.entrySet().stream())
//                    .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), fromMetaType(Types.of(e.getValue()))))
//                    .collect(Collectors.groupingBy(AbstractMap.SimpleEntry::getKey, Collectors.mapping(AbstractMap.SimpleEntry::getValue, Collectors.toSet())))
//                    .forEach((key, value) -> toField(key, value, fieldAssembler));
//
//            return fieldAssembler.endRecord();
        }

        @Override
        public Group toRecord(MessageType schema, Map<String, Object> map) {
            return mapToRecord(schema, map);
        }

        @Override
        public List<Map<String, Object>> createConfig(List<Map<String, Object>> row, Result data, ParquetConfig config) {
            return row;
        }
    }

}
