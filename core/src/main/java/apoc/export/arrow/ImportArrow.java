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
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.FileInputStream;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static apoc.export.arrow.ArrowConstants.END_FIELD;
import static apoc.export.arrow.ArrowConstants.ID_FIELD;
import static apoc.export.arrow.ArrowConstants.START_FIELD;
import static apoc.export.arrow.ArrowConstants.TYPE_FIELD;
import static apoc.export.arrow.ImportArrowCommon.closeVectors;
import static apoc.export.arrow.ImportArrowCommon.createNodeFromArrow;
import static apoc.export.arrow.ImportArrowCommon.createRelFromArrow;
import static apoc.export.arrow.ImportArrowCommon.getDecodedVectorMap;

public class ImportArrow {

    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

    public ImportArrow() {}

    @Procedure(name = "apoc.import.arrow", mode = Mode.WRITE)
    @Description("apoc.import.arrow(fileNodes, fileEdges, config) - imports nodes and relationships from the provided arrow files with given labels and types")
    public Stream<ProgressInfo> importArrow(
            @Name("fileNodes") String fileNodes,
            @Name(value = "fileEdges", defaultValue = "") String fileEdges,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) throws Exception {
        ImportArrowConfig importConfig = new ImportArrowConfig(config);
        ProgressInfo result =
                Util.inThread(pools, () -> {
                    final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo("progress.arrow", "file", "arrow"));
                    final int batchSize = importConfig.getBatchSize();

                    try (RootAllocator allocator = new RootAllocator()) {

                        try (FileInputStream fd = new FileInputStream(fileNodes);
                             ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(fd.getChannel()), allocator);
                             VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();
                             BatchTransaction tx = new BatchTransaction(db, batchSize, reporter)) {

                                while (reader.loadNextBatch()) {

                                    Map<String, ValueVector> decodedVectorsMap = getDecodedVectorMap(reader, schemaRoot);
                                    int sizeId = decodedVectorsMap.get(ID_FIELD).getValueCount();
                                    IntStream.range(0, sizeId).forEach(index -> {
                                        Node node = tx.getTransaction().createNode();
                                        createNodeFromArrow(node, decodedVectorsMap, index, "", reporter);
                                    });
                                    closeVectors(schemaRoot, decodedVectorsMap);
                                }
                        }

                        try (FileInputStream fd = new FileInputStream(fileEdges);
                             ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(fd.getChannel()), allocator);
                             VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();
                             BatchTransaction tx = new BatchTransaction(db, batchSize, reporter)) {

                            while (reader.loadNextBatch()) {
                                Map<String, ValueVector> decodedVectorsMap = getDecodedVectorMap(reader, schemaRoot);

                                final UInt8Vector start = (UInt8Vector) decodedVectorsMap.get(START_FIELD);
                                final UInt8Vector end = (UInt8Vector) decodedVectorsMap.get(END_FIELD);
                                final VarCharVector type = (VarCharVector) decodedVectorsMap.get(TYPE_FIELD);

                                int sizeId = start.getValueCount();

                                IntStream.range(0, sizeId).forEach(index -> {
                                    Node from = tx.getTransaction().getNodeById(start.get(index));
                                    Node to = tx.getTransaction().getNodeById(end.get(index));
                                    createRelFromArrow(decodedVectorsMap, from, to, type, index, "", reporter);
                                });

                                closeVectors(schemaRoot, decodedVectorsMap);
                            }
                        }
                        return reporter.getTotal();
                    }
                });
        return Stream.of(result);
    }

}
