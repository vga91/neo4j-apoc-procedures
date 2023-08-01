package apoc.export;

import apoc.Pools;
import apoc.export.parquet.ApocParquetReader;
import apoc.export.parquet.ParquetConfig;
import apoc.export.util.BatchTransaction;
import apoc.export.util.ProgressReporter;
import apoc.load.LoadParquet;
import apoc.result.MapResult;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.Value;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

//import static apoc.export.parquet.ParquetReadUtil.getReaderBuilder;
import static apoc.export.parquet.ParquetReadUtil.mapFromRecord;
import static apoc.export.parquet.ParquetUtil.FIELD_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_LABELS;
import static apoc.export.parquet.ParquetUtil.FIELD_SOURCE_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TARGET_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TYPE;

public class ImportParquet {

    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

    @Procedure(name = "apoc.import.parquet", mode = Mode.WRITE)
    @Description("Imports nodes and relationships with the given labels and types from the provided CSV file.")
    public Stream<ProgressInfo> importParquet(
            @Name("input") Object input,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        ProgressInfo result =
                Util.inThread(pools, () -> {

                    String file = null;
                    String sourceInfo = "binary";
                    if (input instanceof String) {
                        file =  (String) input;
                        sourceInfo = "file";
                    }
                    final ParquetConfig conf = new ParquetConfig(config);

                    final Map<Long, Long> idMapping = new HashMap<>();

//                    try (ParquetReader<Group> reader = ApocParquetReader(input, )
//                            .build()) {
                    try (ApocParquetReader reader = getReader(file, conf)
                    ) {

                        final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, sourceInfo, "parquet"));

                        BatchTransaction btx = new BatchTransaction(db, conf.getBatchSize(), reporter);

                        try {
                            Object[] record;
                            while ((record = reader.getRecord()) != null) {
                                Map<String, Object> recordMap = reader.getFinish(record);
//                                Map<String, Object> recordMap = mapFromRecord(finish);

                                String relType = (String) recordMap.remove(FIELD_TYPE);
                                if (relType == null) {
                                    // is node
//                                    List<Object> stringLabels = (List<Object>) recordMap.remove(FIELD_LABELS);
                                    Object[] stringLabels = (Object[]) recordMap.remove(FIELD_LABELS);
                                    Label[] labels = Optional.ofNullable(stringLabels)
                                            .map(l -> Arrays.stream(l).map(Object::toString).map(Label::label).toArray(Label[]::new))
//                                            .map(l -> l.stream().map(Object::toString).map(Label::label).toArray(Label[]::new))
                                            .orElse(new Label[]{});
                                    final Node node = btx.getTransaction().createNode(labels);

                                    long remove = (long) recordMap.remove(FIELD_ID);
                                    idMapping.put(remove, node.getId());

                                    addProps(recordMap, node);
//                                    recordMap.forEach((k,v)-> {
//                                            Object value = v instanceof Value ? ((Value) v).asObject() : ValueUtils.of(value);
////                                            value = value instanceof Collection ? ((Collection) value).toArray() : value;
//                                            node.setProperty(k, ValueUtils.of(value));
//                                    });
                                    reporter.update(1, 0, recordMap.size());
                                } else {
                                    // is relationship
                                    long remove = (long) recordMap.remove(FIELD_SOURCE_ID);
                                    Long idSource = idMapping.get(remove);
                                    final Node source = btx.getTransaction().getNodeById(idSource);

                                    long remove1 = (long) recordMap.remove(FIELD_TARGET_ID);
                                    Long idTarget = idMapping.get(remove1);
                                    final Node target = btx.getTransaction().getNodeById(idTarget);

                                    final Relationship rel = source.createRelationshipTo(target, RelationshipType.withName(relType));
                                    addProps(recordMap, rel);
                                    reporter.update(0, 1, recordMap.size());
                                }

                                btx.increment();
                            }
                            btx.doCommit();
                        } catch (RuntimeException e) {
                            btx.rollback();
                            throw e;
                        } finally {
                            btx.close();
                        }

                        return reporter.getTotal();
                    }
                });
        return Stream.of(result);
    }

    private static void addProps(Map<String, Object> recordMap, Entity rel) {
        recordMap.forEach((k, v)-> {
            Object value = v instanceof Value ? ((Value) v).asObject() : v;
//            Object value = v instanceof Value ? ((Value) v).asObject() : ValueUtils.of(v);
            value = value instanceof Collection ? ((Collection) value).stream().map(Object::toString).toArray(String[]::new) : value;
            rel.setProperty(k, value);
        });
    }

    public static ApocParquetReader getReader(String file, ParquetConfig conf) throws IOException {
        return new ApocParquetReader(HadoopInputFile.fromPath(new Path(file), new Configuration()), Set.of(),
//                null,
//                listOfColumns -> new LoadParquet.PropertiesHydrator2<Map<String, Object>, Object[]>(listOfColumns),
                conf
        );
    }
}
