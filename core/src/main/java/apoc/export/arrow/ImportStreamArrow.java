package apoc.export.arrow;

import apoc.Pools;
import apoc.export.util.BatchTransaction;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static apoc.export.arrow.ArrowUtils.END_FIELD;
import static apoc.export.arrow.ArrowUtils.ID_FIELD;
import static apoc.export.arrow.ArrowUtils.LABELS_FIELD;
import static apoc.export.arrow.ArrowUtils.START_FIELD;
import static apoc.export.arrow.ArrowUtils.STREAM_EDGE_PREFIX;
import static apoc.export.arrow.ArrowUtils.STREAM_NODE_PREFIX;
import static apoc.export.arrow.ArrowUtils.TYPE_FIELD;
import static apoc.export.arrow.ImportArrow.closeVectors;
import static apoc.export.arrow.ImportArrow.setCurrentVector;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static java.util.Arrays.asList;

public class ImportStreamArrow {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

    public ImportStreamArrow() {}

    @Procedure(name = "apoc.import.arrow.stream", mode = Mode.SCHEMA)
    @Description("TODO")
    public Stream<ProgressInfo> importArrow(
            @Name("source") byte[] source,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) throws Exception {
        ImportArrowConfig importConfig = new ImportArrowConfig(config);
        ProgressInfo result =
                Util.inThread(pools, () -> {
                    try (RootAllocator allocator = new RootAllocator()) {
                        Map<Long, Long> cache = new HashMap<>(1024*32);

                        final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo("progress.arrow", "file", "arrow"));
                        final int batchSize = importConfig.getBatchSize();
                        try (ArrowStreamReader streamReader = new ArrowStreamReader(new ByteArrayInputStream(source), allocator)) {

                            VectorSchemaRoot schemaRoot = streamReader.getVectorSchemaRoot();
                            try (BatchTransaction tx = new BatchTransaction(db, batchSize, reporter)) {
                                while (streamReader.loadNextBatch()) {
                                    Map<Long, Dictionary> dictionaryMap = streamReader.getDictionaryVectors();

                                    Map<String, ValueVector> decodedVectorsMap = schemaRoot.getFieldVectors().stream().collect(Collectors.toMap(ValueVector::getName, vector -> {
                                        long idDictionary = vector.getField().getDictionary().getId();
                                        return DictionaryEncoder.decode(vector, dictionaryMap.get(idDictionary));
                                    }));

                                    final UInt8Vector valueVector = (UInt8Vector) decodedVectorsMap.get(ID_FIELD);
                                    final UInt8Vector start = (UInt8Vector) decodedVectorsMap.get(START_FIELD);
                                    final UInt8Vector end = (UInt8Vector) decodedVectorsMap.get(END_FIELD);
                                    final VarCharVector type = (VarCharVector) decodedVectorsMap.get(TYPE_FIELD);

                                    int sizeId = valueVector.getValueCount();

                                    IntStream.range(0, sizeId).forEach(index -> {
                                        try {
                                            valueVector.get(index);
                                            Node node = tx.getTransaction().createNode();
                                            cache.put(valueVector.get(index), node.getId());

                                            VarCharVector labelVector = (VarCharVector) decodedVectorsMap.get(LABELS_FIELD);

                                            asList(new String(labelVector.get(index)).split(":")).forEach(label -> {
                                                try {
                                                    node.addLabel(Label.label(OBJECT_MAPPER.readValue(label, String.class)));
                                                } catch (JsonProcessingException e) {
                                                    e.printStackTrace(); // todo - funzione comune
                                                }
                                            });

                                            // properties
                                            decodedVectorsMap.entrySet().stream()
                                                    .filter(i -> i.getKey().startsWith(STREAM_NODE_PREFIX))
                                                    .forEach(propVector -> setCurrentVector(index, node, propVector, STREAM_NODE_PREFIX));
                                        } catch (IllegalStateException ignored){ }

                                        try {
                                            start.get(index);

                                            Node from = tx.getTransaction().getNodeById(cache.get(start.get(index)));
                                            Node to = tx.getTransaction().getNodeById(cache.get(end.get(index)));

                                            RelationshipType relationshipType = RelationshipType.withName(new String(type.get(index)));
                                            Relationship relationship = from.createRelationshipTo(to, relationshipType);

                                            decodedVectorsMap.entrySet().stream()
                                                    .filter(i -> i.getKey().startsWith(STREAM_EDGE_PREFIX))
                                                    .forEach(propVector -> setCurrentVector(index, relationship, propVector, STREAM_EDGE_PREFIX));

                                        } catch (IllegalStateException ignored) {}
                                    });

                                    closeVectors(schemaRoot, decodedVectorsMap);
                                }
                            }
                        }
                        return reporter.getTotal();
                    }
                });
        return Stream.of(result);
    }
}
