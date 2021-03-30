package apoc.export.arrow;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportUtils;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.StringUtils;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowConstants.EDGE_FILE_PREFIX;
import static apoc.export.arrow.ArrowConstants.NODE_FILE_PREFIX;
import static apoc.export.arrow.ArrowConstants.RESULT_FILE_PREFIX;
import static apoc.export.arrow.ExportArrowCommon.implementExportCommon;
import static apoc.util.Util.labelString;

public class ExportArrow {

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


    @Procedure
    @Description("apoc.export.arrow.all(file, config) - exports whole database as arrow to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name("config") Map<String, Object> config) throws Exception {
        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportArrow(fileName, source, new DatabaseSubGraph(tx), config);
    }

    @Procedure
    @Description("apoc.export.arrow.data(nodes,rels,file,config) - exports given nodes and relationships as csv to the provided file")
    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportArrow(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure
    @Description("apoc.export.arrow.graph(graph,file,config) - exports given graph object as arrow to the provided file")
    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportArrow(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure
    @Description("apoc.export.arrow.query(query,file,{config,...,params:{params}}) - exports results from the cypher statement as arrow to the provided file")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query,params);
        String source = String.format("statement: cols(%d)", result.columns().size());
        return exportArrow(fileName, source,result,config);
    }


    private Stream<ProgressInfo> exportArrow(@Name("file") String fileName, String source, Object data, Map<String, Object> config) throws Exception {

        ExportConfig exportConfig = new ExportConfig(config);
        if (StringUtils.isNotBlank(fileName)) apocConfig.checkWriteAllowed(exportConfig);
        final String format = "arrow";
        ProgressInfo progressInfo = new ProgressInfo(fileName, source, format);
        progressInfo.batchSize = exportConfig.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);

        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName, false);

            if (exportConfig.streamStatements()) {
                return ExportUtils.getProgressInfoStream(db,
                        pools.getDefaultExecutorService(),
                        terminationGuard,
                        format,
                        exportConfig,
                        reporter,
                        cypherFileManager,
                        progressReporter -> {
                            try {
                                dump(data, exportConfig, reporter, fileName);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                );
            } else {
                dump(data, exportConfig, reporter, fileName);
                return reporter.stream();
            }
    }


    private void dump(Object valueToExport, ExportConfig exportConfig, ProgressReporter reporter, String fileName) throws Exception {

        try (RootAllocator allocator = new RootAllocator()) {

            String importDir = apocConfig().getString("dbms.directories.import", "import");

            int batchSize = exportConfig.getBatchSize();

            if (valueToExport instanceof SubGraph) {
                SubGraph subGraph = (SubGraph) valueToExport;

                File fileNodes = new File(importDir, NODE_FILE_PREFIX + fileName);
                processArrowFileOutputStream(reporter, allocator, batchSize, subGraph, fileNodes, ArrowConstants.FunctionType.NODES);

                if (!subGraph.getRelationships().iterator().hasNext()) {
                    return;
                }

                File fileEdges = new File(importDir, EDGE_FILE_PREFIX + fileName);
                processArrowFileOutputStream(reporter, allocator, batchSize, subGraph, fileEdges, ArrowConstants.FunctionType.EDGES);
            }

            if (valueToExport instanceof Result) {

                File file = new File(importDir, RESULT_FILE_PREFIX + fileName);
                processArrowFileOutputStream(reporter, allocator, batchSize, valueToExport, file, ArrowConstants.FunctionType.RESULT);
            }

            reporter.done();
        }
    }

    private static void processArrowFileOutputStream(ProgressReporter reporter, RootAllocator allocator, int batchSize, Object valueToProcess, File file, ArrowConstants.FunctionType function) throws IOException {
        try (FileOutputStream fd = new FileOutputStream(file)) {

            implementExportCommon(allocator, batchSize, valueToProcess, function, reporter, fd, true);

        }
    }

}

// todo -reporter al Result