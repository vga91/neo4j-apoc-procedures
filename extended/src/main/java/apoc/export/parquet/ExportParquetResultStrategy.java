//package apoc.export.parquet;
//
//import apoc.meta.Types;
//import org.apache.avro.SchemaBuilder;
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.apache.avro.Schema;
//
//import java.util.AbstractMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//public interface ExportParquetResultStrategy {
//
//    default Schema schemaFor(GraphDatabaseService db, List<Map<String, Object>> records) {
//        // todo - this row is equal
//        SchemaBuilder.FieldAssembler<Schema> test = SchemaBuilder.record("test").fields();
//
//        return test
//                .endRecord();
//
////        final List<Field> fields = records.stream()
////                .flatMap(m -> m.entrySet().stream())
////                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), fromMetaType(Types.of(e.getValue()))))
////                .collect(Collectors.groupingBy(e -> e.getKey(), Collectors.mapping(e -> e.getValue(), Collectors.toSet())))
////                .entrySet()
////                .stream()
////                .map(e -> toField(e.getKey(), e.getValue()))
////                .collect(Collectors.toList());
////        return new Schema(fields);
//    }
//}
