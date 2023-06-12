package apoc.export.parquet;

import apoc.ApocConfig;
import apoc.Description;
import apoc.Pools;
import apoc.export.util.ExportConfig;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.result.ByteArrayResult;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;

public class ExportParquet {
    // todo - batching
    // todo - copy from arrow...




    // todo --- http://www.hydrogen18.com/blog/writing-parquet-records.html


    // https://blog.contactsunny.com/data-science/how-to-generate-parquet-files-in-java


    // todo - check that finally
    // https://arrow.apache.org/docs/cpp/parquet.html


    // TODO!!!
    // TODO!!!
    // TODO!!!
    // TODO!!!
    // TODO!!!: howto transform in scala?? --> https://github.com/sderosiaux/parquet-custom-reader-writer/blob/master/src/main/scala/custom/CustomWriteSupport.scala


    // todo - maybe preferred
    // https://www.netjstech.com/2018/07/how-to-read-and-write-parquet-file-hadoop.html

    // https://blog.contactsunny.com/data-science/how-to-generate-parquet-files-in-java




//    // --- init parte procedure ---
    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    // todo - termination guard handling...
    @Context
    public TerminationGuard terminationGuard;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    // todo - {stream: true}

    @Procedure("apoc.export.parquet.all")
    @Description("Exports the full database to the provided CSV file.")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
//        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportParquet(fileName, /*source, */new DatabaseSubGraph(tx), new ParquetConfig(config));
    }
//
//    @Procedure("apoc.export.parquet.data")
//    @Description("Exports the given nodes and relationships to the provided CSV file.")
//    public Stream<ProgressInfo> data(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
//        ExportConfig exportConfig = new ExportConfig(config);
//        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
//        return exportCsv(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), exportConfig);
//    }
//    @Procedure("apoc.export.parquet.graph")
//    @Description("Exports the given graph to the provided CSV file.")
//    public Stream<ProgressInfo> graph(@Name("graph") Map<String,Object> graph, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
//        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
//        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
//        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
//        return exportCsv(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config));
//    }
//
    @Procedure("apoc.export.parquet.query")
    @Description("Exports the results from running the given Cypher query to the provided CSV file.")
    public Stream<ProgressInfo> query(@Name("query") String query, @Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ParquetConfig exportConfig = new ParquetConfig(config);
        Map<String,Object> params = config == null ? Collections.emptyMap() : (Map<String,Object>)config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query,params);

//        String source = String.format("statement: cols(%d)", result.columns().size());
        return exportParquet(fileName, result, exportConfig);
//        return exportCsv(fileName, source,result, exportConfig);
    }

    // TODO !!! -- più che stream, conviene chiamarlo export bytes!!!!

    public Stream<ProgressInfo> exportParquet(String fileName, Object data, ParquetConfig config) {
        if (fileName == null) {
            // todo...
            return null;
        }
        if (data instanceof Result) {
            return new ExportParquetResultFileStrategy(fileName, db, pools, terminationGuard, log).export((Result) data, config);
        }
        return new ExportParquetGraphFileStrategy(fileName, db, pools, terminationGuard, log).export((SubGraph) data, config);
        // todo - if data instanceof Result else...
//        return new ExportParquetGraphFileStrategy(fileName, db, pools, terminationGuard, log).export((SubGraph) data, config);
    }


    public Stream<ByteArrayResult> stream(Object data, ParquetConfig config) {
        // TODO
        return null;
//        if (data instanceof Result) {
//            return new ExportParquetResultStreamStrategy(db, pools, terminationGuard, logger).export((Result) data, config);
//        } else {
//            return new ExportParquetGraphStreamStrategy(db, pools, terminationGuard, logger).export((SubGraph) data, config);
//        }
    }

    public Stream<ProgressInfo> file(String fileName, Object data, ParquetConfig config) {
        // todo - substitute with checkWriteAllowed
        // we cannot use apocConfig().checkWriteAllowed(..) because the error is confusing
        //  since it says "... use the `{stream:true}` config", but with arrow procedures the streaming mode is implemented via different procedures
        if (!apocConfig().getBoolean(APOC_EXPORT_FILE_ENABLED)) {
            throw new RuntimeException("todo...");
        }
        if (data instanceof Result) {
            return new ExportParquetResultFileStrategy(fileName, db, pools, terminationGuard, log).export((Result) data, config);
        } else {
            return new ExportParquetGraphFileStrategy(fileName, db, pools, terminationGuard, log).export((SubGraph) data, config);
        }
    }
}

