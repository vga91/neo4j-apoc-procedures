package apoc.export.arrow;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.util.ExportConfig;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.result.ByteArrayResult;
import org.apache.arrow.memory.RootAllocator;
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

import static apoc.export.arrow.ExportArrowUtil.implementArrowExport;

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
    @Description("apoc.export.arrow.stream.all(config) - exports whole database as arrow byte[] result")
    public Stream<ByteArrayResult> all(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return exportArrow(new DatabaseSubGraph(tx), config);
    }

    @Procedure("apoc.export.arrow.stream.data")
    @Description("apoc.export.arrow.stream.data(nodes, rels, config) - exports given nodes and relationships as arrow byte[] result")
    public Stream<ByteArrayResult> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        return exportArrow(new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure("apoc.export.arrow.stream.graph")
    @Description("apoc.export.arrow.stream.graph(graph,config) - exports given graph object as arrow byte[] result")
    public Stream<ByteArrayResult> graph(@Name("graph") Map<String, Object> graph, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        return exportArrow(new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure("apoc.export.arrow.stream.query")
    @Description("apoc.export.arrow.stream.query(query,{config,...,params:{params}}) - exports results from the cypher statement as arrow byte[] result")
    public Stream<ByteArrayResult> query(@Name("query") String query, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        Map<String, Object> params = config == null ? Collections.emptyMap() : (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query, params);
        return exportArrow(result, config);
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
                return processArrowByteArrayOutputStream(allocator, batchSize, valueToExport, ArrowConstants.FunctionType.STREAM);
            }
            if (valueToExport instanceof Result) {
                return processArrowByteArrayOutputStream(allocator, batchSize, valueToExport, ArrowConstants.FunctionType.RESULT);
            }
            return null;
        }
    }

    private static byte[] processArrowByteArrayOutputStream(RootAllocator allocator, int batchSize, Object valueToProcess, ArrowConstants.FunctionType function) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            implementArrowExport(allocator, batchSize, valueToProcess, function, null, out);
            return out.toByteArray();
        }
    }
}