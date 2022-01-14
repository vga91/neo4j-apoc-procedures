package apoc.result;

import apoc.export.util.ExportConfig;
import apoc.export.util.FormatUtils;
import apoc.util.Util;
import org.neo4j.kernel.api.KernelTransaction;

import java.io.StringWriter;
import java.util.Map;

/**
 * @author mh
 * @since 22.05.16
 */
public class ProgressInfo {
    public static final ProgressInfo EMPTY = new ProgressInfo(null, null, null/*, null*/);
    public final String file;
//    public final KernelTransaction ktx;
    public String source;
    public final String format;
    public long nodes;
    public long relationships;
    public long properties;
    public long time;
    public long rows;
    public long batchSize = -1;
    public long batches;
    public boolean done;
    public Object data;

    public ProgressInfo(String file, String source, String format/*, KernelTransaction ktx*/) {
        this.file = file;
        this.source = source;
        this.format = format;
//        this.ktx = ktx;
    }

    public ProgressInfo(ProgressInfo pi) {
        this.file = pi.file;
        this.source = pi.source;
        this.format = pi.format;
        this.nodes = pi.nodes;
        this.relationships = pi.relationships;
        this.properties = pi.properties;
        this.time = pi.time;
        this.rows = pi.rows;
        this.batchSize = pi.batchSize;
        this.batches = pi.batches;
        this.done = pi.done;
//        this.ktx = pi.ktx;
    }

    @Override
    public String toString() {
        return String.format("nodes = %d rels = %d properties = %d", nodes, relationships, properties);
    }

    public ProgressInfo update(long nodes, long relationships, long properties) {
        this.nodes += nodes;
        this.relationships += relationships;
        this.properties += properties;
//        updateStatus();
        return this;
    }

    public ProgressInfo updateTime(long start) {
        this.time = System.currentTimeMillis() - start;
        return this;
    }
    public ProgressInfo done(long start) {
        this.done = true;
        return updateTime(start);
    }

    public void nextRow() {
        this.rows++;
//        updateStatus();
    }

    public ProgressInfo drain(StringWriter writer, ExportConfig config) {
        if (writer != null) {
            this.data = Util.getStringOrCompressedData(writer, config);
        }
        return this;
    }
}
