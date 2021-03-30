package apoc.export.arrow;

import apoc.Pools;
import apoc.export.util.BatchTransaction;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static apoc.export.arrow.ArrowConstants.END_FIELD;
import static apoc.export.arrow.ArrowConstants.ID_FIELD;
import static apoc.export.arrow.ArrowConstants.START_FIELD;
import static apoc.export.arrow.ArrowConstants.STREAM_EDGE_PREFIX;
import static apoc.export.arrow.ArrowConstants.STREAM_NODE_PREFIX;
import static apoc.export.arrow.ArrowConstants.TYPE_FIELD;
import static apoc.export.arrow.ImportArrowCommon.closeVectors;
import static apoc.export.arrow.ImportArrowCommon.createNodeFromArrow;
import static apoc.export.arrow.ImportArrowCommon.createRelFromArrow;
import static apoc.export.arrow.ImportArrowCommon.getDecodedVectorMap;

public class ImportStreamArrow {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

    public ImportStreamArrow() {}

    @Procedure(name = "apoc.import.arrow.stream", mode = Mode.WRITE)
    @Description("apoc.import.arrow.stream(source, config) - imports nodes and relationships from the provided byte[] source with given labels and types")
    public Stream<ProgressInfo> importArrow(
            @Name("source") byte[] source,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) throws Exception {
        ImportArrowConfig importConfig = new ImportArrowConfig(config);
        ProgressInfo result =
                Util.inThread(pools, () -> {
                    final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo("progress.arrow", "file", "arrow"));
                    final int batchSize = importConfig.getBatchSize();
                    Map<Long, Long> cache = new HashMap<>(1024*32);

                        try (RootAllocator allocator = new RootAllocator();
                             ArrowStreamReader streamReader = new ArrowStreamReader(new ByteArrayInputStream(source), allocator);
                             VectorSchemaRoot schemaRoot = streamReader.getVectorSchemaRoot();
                             BatchTransaction tx = new BatchTransaction(db, batchSize, reporter)) {

                                while (streamReader.loadNextBatch()) {

                                    Map<String, ValueVector> decodedVectorsMap = getDecodedVectorMap(streamReader, schemaRoot);

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
                                            createNodeFromArrow(node, decodedVectorsMap, index, STREAM_NODE_PREFIX);
                                        } catch (IllegalStateException ignored){ }

                                        try {
                                            start.get(index);
                                            Node from = tx.getTransaction().getNodeById(cache.get(start.get(index)));
                                            Node to = tx.getTransaction().getNodeById(cache.get(end.get(index)));
                                            createRelFromArrow(decodedVectorsMap, from, to, type, index, STREAM_EDGE_PREFIX);
                                        } catch (IllegalStateException ignored) {}
                                    });

                                    closeVectors(schemaRoot, decodedVectorsMap);
                                }

                            return reporter.getTotal();
                        }
                });
        return Stream.of(result);
    }

}
