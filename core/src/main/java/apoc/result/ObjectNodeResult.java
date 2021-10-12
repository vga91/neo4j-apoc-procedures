package apoc.result;

import org.neo4j.graphdb.Node;

import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class ObjectNodeResult extends NodeResult {
    public final Map<String, Object> stats;

    public ObjectNodeResult(Node node, Map<String, Object> stats) {
        super(node);
        this.stats = stats;
    }
}
