package apoc.export.arrow;

public class ArrowUtils {
    public enum FunctionType { NODES, EDGES, RESULT, STREAM }

    public static final String LABELS_FIELD = "_labels";
    public static final String ID_FIELD = "_id";
    public static final String START_FIELD = "_start";
    public static final String END_FIELD = "_end";
    public static final String TYPE_FIELD = "_type";
    public static final String DICT_PREFIX = "dict___";
    public static final String STREAM_NODE_PREFIX = "nodeProp___";
    public static final String STREAM_EDGE_PREFIX = "edgeProp___";

    public static final String NODE_FILE_PREFIX = "nodes_";
    public static final String EDGE_FILE_PREFIX = "edges_";
    public static final String RESULT_FILE_PREFIX = "result_";
}
