package apoc.result;

import org.neo4j.graphdb.Relationship;

import java.util.List;

public class RelationshipListResult {
    public final List<Relationship> relationships;

    public RelationshipListResult(List<Relationship> value) {
        this.relationships = value;
    }
}
