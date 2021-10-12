package apoc.result;

import org.neo4j.graphdb.Relationship;

import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class ObjectRelationshipResult extends RelationshipResult {
    public final Map<String, Object> stats;

    public ObjectRelationshipResult(Relationship relationship, Map<String, Object> stats) {
        super(relationship);
        this.stats = stats;
    }
}
