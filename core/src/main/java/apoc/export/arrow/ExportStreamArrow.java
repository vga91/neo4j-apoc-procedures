package apoc.export.arrow;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.cypher.ExportFileManager;
import apoc.export.util.ExportConfig;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ByteArrayResult;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.export.arrow.ExportArrow.implementExportCommon;

public class ExportStreamArrow {

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    @Context
    public TerminationGuard terminationGuard;

    @Procedure("apoc.export.arrow.stream.all")
    @Description("apoc.export.arrow.stream.all")
    public Stream<ByteArrayResult> all(@Name("config") Map<String, Object> config) throws Exception {
        return exportArrow(new DatabaseSubGraph(tx), config);
    }

    @Procedure("apoc.export.arrow.stream.data")
    @Description("apoc.export.arrow.stream.data")
    public Stream<ByteArrayResult> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return exportArrow(new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure("apoc.export.arrow.stream.graph")
    @Description("apoc.export.arrow.stream.graph")
    public Stream<ByteArrayResult> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        return exportArrow(new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure("apoc.export.arrow.stream.query")
    @Description("apoc.export.arrow.stream.query")
    public Stream<ByteArrayResult> query(@Name("query") String query, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query,params);
        return exportArrow(result,config);
    }

    private Stream<ByteArrayResult> exportArrow(Object data, Map<String, Object> config) throws Exception {
        ExportConfig exportConfig = new ExportConfig(config);
        final byte[] dump = dump(data, exportConfig);
        return dump == null ? Stream.empty() : Stream.of(new ByteArrayResult(dump));
    }


    private byte[] dump(Object valueToExport, ExportConfig exportConfig) throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {

            int batchSize = exportConfig.getBatchSize();

            if (valueToExport instanceof SubGraph) {

                return processArrowStreamByteArray(allocator, batchSize, valueToExport, ArrowUtils.FunctionType.STREAM);
//                implementExportCommon(allocator, batchSize, valueToProcess, function, null, fd, false);


//                if (!subGraph.getRelationships().iterator().hasNext()) {
//                    return null;
//                }
//
//                implementExportCommon(reporter, allocator, batchSize, valueToProcess, function, fd, false);

//                File fileNodes = new File(importDir, NODE_FILE_PREFIX + fileName);
//                processArrowStream(reporter, allocator, batchSize, subGraph, fileNodes, ArrowUtils.FunctionType.NODES);
//
//                if (!subGraph.getRelationships().iterator().hasNext()) {
//                    return;
//                }
//
//                File fileEdges = new File(importDir, EDGE_FILE_PREFIX + fileName);
//                processArrowStream(reporter, allocator, batchSize, subGraph, fileEdges, ArrowUtils.FunctionType.EDGES);
            }

            if (valueToExport instanceof Result) {
                return processArrowStreamByteArray(allocator, batchSize, valueToExport, ArrowUtils.FunctionType.RESULT);

//                implementExportCommon(reporter, allocator, batchSize, valueToProcess, function, fd, false);
//                File file = new File(importDir, RESULT_FILE_PREFIX + fileName);
//                processArrowStream(reporter, allocator, batchSize, valueToExport, file, ArrowUtils.FunctionType.RESULT);
            }

            return null;


//            DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
//            AtomicInteger indexNode = new AtomicInteger();
//            Map<String, FieldVector> vectorMap = new TreeMap<>();
//            subGraph.getNodes().forEach(node -> writeNode(null, allocator, vectorMap, indexNode, node, dictProvider, batchSize, out, false));
//            subGraph.getRelationships().forEach(rel -> writeRelationship(null, allocator, vectorMap, indexNode, rel, dictProvider, batchSize, out, false));
//
//            checkBatchStatusAndWriteEventually(dictProvider, out, indexNode, vectorMap, indexNode.get() % batchSize != 0, false);

//            List<FieldVector> encodedVectors = returnEncodedVector(dictProvider, vectorMap, indexNode);

//            // todo - fine robe da mergiare
//            try (VectorSchemaRoot root = new VectorSchemaRoot(getFields(encodedVectors), encodedVectors);
//                    ArrowStreamWriter writer = new ArrowStreamWriter(root, dictProvider, Channels.newChannel(out))) {
//
////                writeWithBatch(batchSize, indexNode, root, writer);
//                writeBatch(writer);
//            }

//            closeFieldVectors(vectorMap, encodedVectors);
//            writer.start();
//            writer.writeBatch();
//            writer.end();
        }
    }

    private static byte[] processArrowStreamByteArray(RootAllocator allocator, int batchSize, Object valueToProcess, ArrowUtils.FunctionType function) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            implementExportCommon(allocator, batchSize, valueToProcess, function, null, out, false);

            return out.toByteArray();

        }
    }

}
