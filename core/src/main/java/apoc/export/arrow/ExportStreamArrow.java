package apoc.export.arrow;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.util.ExportConfig;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.result.ByteArrayResult;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
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
import java.io.FileOutputStream;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static apoc.export.arrow.ExportArrow.checkBatchStatusAndWriteEventually;
import static apoc.export.arrow.ExportArrow.closeFieldVectors;
import static apoc.export.arrow.ExportArrow.getFields;
import static apoc.export.arrow.ExportArrow.returnEncodedVector;
import static apoc.export.arrow.ExportArrow.writeBatch;
import static apoc.export.arrow.ExportArrow.writeNode;
import static apoc.export.arrow.ExportArrow.writeRelationship;

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
    @Description("TODO")
    public Stream<ByteArrayResult> all(@Name("config") Map<String, Object> config) throws Exception {
//        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));

        ByteArrayOutputStream out = new ByteArrayOutputStream();


        // todo - robe da mergiare con l'altro
        ExportConfig exportConfig = new ExportConfig(config);
        int batchSize = exportConfig.getBatchSize();

        try (RootAllocator allocator = new RootAllocator()) {
            DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();

            SubGraph subGraph = new DatabaseSubGraph(tx);

            Map<String, FieldVector> vectorMap = new TreeMap<>();
            AtomicInteger indexNode = new AtomicInteger();
            subGraph.getNodes().forEach(node -> writeNode(null, allocator, vectorMap, indexNode, node, dictProvider, batchSize, out, false));
            subGraph.getRelationships().forEach(rel -> writeRelationship(null, allocator, vectorMap, indexNode, rel, dictProvider, batchSize, out, false));

            checkBatchStatusAndWriteEventually(dictProvider, out, indexNode, vectorMap, indexNode.get() % batchSize != 0, false);

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


        return Stream.of(new ByteArrayResult(out.toByteArray()));
    }

//    @Procedure("apoc.export.arrow.stream.data")
//    @Description("TODO")
//    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
//
//        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
//        return exportArrow(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
//    }
//
//    @Procedure("apoc.export.arrow.stream.graph")
//    @Description("TODO")
//    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
//
//        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
//        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
//        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
//        return exportArrow(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
//    }
//
//    @Procedure("apoc.export.arrow.stream.query")
//    @Description("TODO")
//    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
//        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
//        Result result = tx.execute(query,params);
//        String source = String.format("statement: cols(%d)", result.columns().size());
//        return exportArrow(fileName, source,result,config);
//    }
}
