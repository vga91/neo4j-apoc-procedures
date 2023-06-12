package apoc.export.parquet;

import apoc.meta.Types;
import apoc.util.collection.Iterables;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static apoc.export.parquet.ParquetUtil.*;
import static org.apache.avro.SchemaBuilder.*;

//
//
//// todo - spostare cose qui in caso...


// todo - e se ci mettessi anche getFile?????
public interface ParquetExportType<T> {
    enum Type {
        RESULT(new ResultType()),
        GRAPH(new GraphType());


        private final ParquetExportType graphType;

        Type(ParquetExportType graphType) {
            this.graphType = graphType;
        }

        ParquetExportType from(Object data) {
            Type type = data instanceof Result
                    ? Type.RESULT
                    : Type.GRAPH;
//            if (data instanceof Result) {
//                return Type.RESULT;
//            }
//            return Type.GRAPH;
            return type.graphType;
        }
    }

    Schema schemaFor(GraphDatabaseService db, ParquetConfig config, T data);

    class GraphType implements ParquetExportType<SubGraph> {

        @Override
        public Schema schemaFor(GraphDatabaseService db, ParquetConfig config, SubGraph data) {
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
                            List<String> propertyTypes = (List<String>) m.get("propertyTypes");
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
            db.executeTransactionally("CALL apoc.meta.nodeTypeProperties($config)",
                    parameters, parsePropertiesResult);


            // TODO ---> try using this: data.neo4j.com/stackoverflow/so-2018-09-58.dump

            // TODO - everything optional??? --> name(FIELD_LABELS).type().optional()

            // todo - optional or required??
            test.optionalLong(FIELD_ID);
            getItems(FIELD_LABELS, test).stringType();
//            allFields.addAll(nodeFields);

            if (cfg.containsKey("includeRels")) {
//                final Set<Field> relFields =
                db.executeTransactionally("CALL apoc.meta.relTypeProperties($config)",
                        parameters, parsePropertiesResult);
                test.optionalLong(FIELD_SOURCE_ID);
                test.optionalLong(FIELD_TARGET_ID);
                test.optionalString(FIELD_TYPE);
//                allFields.add(FIELD_SOURCE_ID);
//                allFields.add(FIELD_TARGET_ID);
//                allFields.add(FIELD_TYPE);
//                allFields.addAll(relFields);
            }

            Schema schema = test
//                    .requiredLong(FIELD_ID)
//                    .name(FIELD_LABELS).type().array().items().stringType().noDefault()
//                    .name("test").type( SchemaBuilder.builder().intType().set)
//                    .name()
                    .endRecord();

            return schema;
        }

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

    class ResultType implements ParquetExportType<Result> {

        private Map<String, Object> firstElement;

        @Override
        public Schema schemaFor(GraphDatabaseService db, ParquetConfig config, Result data) {

            // todo - this row is equal
            SchemaBuilder.FieldAssembler<Schema> test = SchemaBuilder.record("test")
                    .namespace("org.apache.avro.ipc")
                    .fields();

            // todo - first batch
            this.firstElement = data.next();
            schemaForResult(test, List.of(firstElement));

            return test
                    .endRecord();
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

        public Map<String, Object> getFirstElement() {
            return firstElement;
        }

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
                        return "ANY_ARRAY";
                    }
                    return fromMetaType(innerType) + "_ARRAY";
//                case BOOLEAN:
//                    return "Boolean";

                // todo - how to deal with it???
                case MAP:
                    return "MAP";
//                case RELATIONSHIP:
//                    return "Relationship";
//                case NODE:
//                    return "Node";
//                case PATH:
//                    return "Path";
//                case POINT:
//                    return "Point";
//                case DATE:
//                    return "Date";
//                case LOCAL_TIME:
//                case DATE_TIME:
//                case LOCAL_DATE_TIME:
//                    return "DateTime";
//                case TIME:
//                    return "Time";
//                case DURATION:
//                    return "Duration";
                default:
                    return type.name()/*.replaceAll("_", "")*/.toUpperCase();
            }
        }
    }

}
