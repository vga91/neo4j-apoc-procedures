package apoc.export.arrow;

import apoc.export.cypher.ExportFileManager;
import apoc.export.util.ExportConfig;
import apoc.export.util.Format;
import apoc.export.util.Reporter;
import apoc.result.ProgressInfo;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.io.Reader;

public class ArrowFormat implements Format {
    private final GraphDatabaseService db;

    public ArrowFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ProgressInfo load(Reader reader, Reporter reporter, ExportConfig config) throws Exception {
        return null; // todo - ???
    }

    @Override
    public ProgressInfo dump(SubGraph graph, ExportFileManager writer, Reporter reporter, ExportConfig config) throws Exception {
        return null;
    }

    public ProgressInfo dump(Result result, ExportFileManager writer, Reporter reporter, ExportConfig config) {
        return null; // todo - ???
    }
}
