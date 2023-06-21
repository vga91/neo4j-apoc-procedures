//package apoc.export;
//
//import apoc.Pools;
//import apoc.export.parquet.ParquetConfig;
//import apoc.export.util.BatchTransaction;
//import apoc.export.util.ProgressReporter;
//import apoc.result.ProgressInfo;
//import apoc.util.Util;
//import org.apache.avro.generic.GenericData;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.parquet.example.data.Group;
//import org.apache.parquet.hadoop.ParquetReader;
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.Label;
//import org.neo4j.graphdb.Node;
//import org.neo4j.graphdb.Relationship;
//import org.neo4j.graphdb.RelationshipType;
//import org.neo4j.logging.Log;
//import org.neo4j.procedure.Context;
//import org.neo4j.procedure.Description;
//import org.neo4j.procedure.Mode;
//import org.neo4j.procedure.Name;
//import org.neo4j.procedure.Procedure;
//import org.neo4j.values.storable.LongValue;
//import org.neo4j.values.storable.Value;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Optional;
//import java.util.stream.Stream;
//
//import static apoc.export.parquet.ParquetReadUtil.genericDataLoad;
//import static apoc.export.parquet.ParquetReadUtil.getReaderBuilder;
//import static apoc.export.parquet.ParquetReadUtil.mapFromRecord;
//import static apoc.export.parquet.ParquetUtil.FIELD_ID;
//import static apoc.export.parquet.ParquetUtil.FIELD_LABELS;
//import static apoc.export.parquet.ParquetUtil.FIELD_SOURCE_ID;
//import static apoc.export.parquet.ParquetUtil.FIELD_TARGET_ID;
//import static apoc.export.parquet.ParquetUtil.FIELD_TYPE;
//import static apoc.load.LoadParquet.registerCustomTypes;
//
//public class ImportParquet {
//
//    @Context
//    public GraphDatabaseService db;
//
//    @Context
//    public Pools pools;
//
//    @Context
//    public Log log;
//
//    @Procedure(name = "apoc.import.parquet", mode = Mode.WRITE)
//    @Description("Imports nodes and relationships with the given labels and types from the provided CSV file.")
//    public Stream<ProgressInfo> importParquet(
//            @Name("input") Object input,
//            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
//    ) {
//        ProgressInfo result =
//                Util.inThread(pools, () -> {
//
//                    String file = null;
//                    String sourceInfo = "binary";
//                    if (input instanceof String) {
//                        file =  (String) input;
//                        sourceInfo = "file";
//                    }
//                    final ParquetConfig conf = new ParquetConfig(config);
//
//                    final Map<Long, Long> idMapping = new HashMap<>();
//
//                    try (ParquetReader<Group> reader = getReaderBuilder(input)
////                            .withFilter()
////                            .withDataModel(genericDataLoad)
////                            .withConf(new Configuration())
//                            .build()) {
//
//                        registerCustomTypes();
//
//                        final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, sourceInfo, "parquet"));
//
//                        BatchTransaction btx = new BatchTransaction(db, conf.getBatchSize(), reporter);
//
//                        try {
//                            GenericData.Record record;
//                            while ((record = reader.read()) != null) {
//                                Map<String, Object> recordMap = mapFromRecord(record);
//
//                                String relType = (String) recordMap.remove(FIELD_TYPE);
//                                if (relType == null) {
//                                    // is node
//                                    Object[] stringLabels = (Object[]) recordMap.remove(FIELD_LABELS);
//                                    Label[] labels = Optional.ofNullable(stringLabels)
//                                            .map(l -> Arrays.stream(l).map(Object::toString).map(Label::label).toArray(Label[]::new))
//                                            .orElse(new Label[]{});
//                                    final Node node = btx.getTransaction().createNode(labels);
//
//                                    long remove = (long) recordMap.remove(FIELD_ID);
//                                    idMapping.put(remove, node.getId());
//
//                                    recordMap.forEach((k,v)-> {
//                                            Object value = v instanceof Value ? ((Value) v).asObject() : v;
//                                            node.setProperty(k, value);
//                                    });
//                                    reporter.update(1, 0, recordMap.size());
//                                } else {
//                                    // is relationship
//                                    long remove = (long) recordMap.remove(FIELD_SOURCE_ID);
//                                    Long idSource = idMapping.get(remove);
//                                    final Node source = btx.getTransaction().getNodeById(idSource);
//
//                                    long remove1 = (long) recordMap.remove(FIELD_TARGET_ID);
//                                    Long idTarget = idMapping.get(remove1);
//                                    final Node target = btx.getTransaction().getNodeById(idTarget);
//
//                                    final Relationship rel = source.createRelationshipTo(target, RelationshipType.withName(relType));
//                                    recordMap.forEach((k,v)-> {
//                                        Object value = v instanceof Value ? ((Value) v).asObject() : v;
//                                        rel.setProperty(k, value);
//                                    });
//                                    reporter.update(0, 1, recordMap.size());
//                                }
//
//                                btx.increment();
//                            }
//                            btx.doCommit();
//                        } catch (RuntimeException e) {
//                            btx.rollback();
//                            throw e;
//                        } finally {
//                            btx.close();
//                        }
//
//                        return reporter.getTotal();
//                    }
//                });
//        return Stream.of(result);
//    }
//}
