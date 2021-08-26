package apoc.path;

import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.NestingIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static apoc.path.PathExplorer.COMMA_SEPARATOR;

/**
 * An expander for repeating sequences of relationships. The sequence provided should be a string consisting of
 * relationship type/direction patterns (exactly the same as the `relationshipFilter`), separated by commas.
 * Each comma-separated pattern represents the relationships that will be expanded with each step of expansion, which
 * repeats indefinitely (unless otherwise stopped by `maxLevel`, `limit`, or terminator filtering from the other expander config options).
 * The exception is if `beginSequenceAtStart` is false. This indicates that the sequence should not begin from the start node,
 * but from one node distant. In this case, we may still need a restriction on the relationship used to reach the start node
 * of the sequence, so when `beginSequenceAtStart` is false, then the first relationship step in the sequence given will not
 * actually be used as part of the sequence, but will only be used once to reach the starting node of the sequence.
 * The remaining relationship steps will be used as the repeating relationship sequence.
 */
public class RelationshipSequenceExpander implements PathExpander {
    private final List<List<Triple<RelationshipType, Direction, String>>> relSequences = new ArrayList<>();
    private List<Triple<RelationshipType, Direction, String>> initialRels = null;

    public RelationshipSequenceExpander(String relSequenceString, boolean beginSequenceAtStart, String relPropFilter) {
        int index = 0;

        for (String sequenceStep : relSequenceString.split(COMMA_SEPARATOR)) {
            sequenceStep = sequenceStep.trim();
            Iterable<Triple<RelationshipType, Direction, String>> relDirIterable = RelationshipTypeAndDirections.parseTriple(sequenceStep, relPropFilter);

            List<Triple<RelationshipType, Direction, String>> stepRels = new ArrayList<>();
            for (Triple<RelationshipType, Direction, String> pair : relDirIterable) {
                stepRels.add(pair);
            }

            if (!beginSequenceAtStart && index == 0) {
                initialRels = stepRels;
            } else {
                relSequences.add(stepRels);
            }

            index++;
        }
    }

    public RelationshipSequenceExpander(List<String> relSequenceList, boolean beginSequenceAtStart, String relPropFilter) {
        int index = 0;

        for (String sequenceStep : relSequenceList) {
            sequenceStep = sequenceStep.trim();
            Iterable<Triple<RelationshipType, Direction, String>> relDirIterable = RelationshipTypeAndDirections.parseTriple(sequenceStep, relPropFilter);

            List<Triple<RelationshipType, Direction, String>> stepRels = new ArrayList<>();
            for (Triple<RelationshipType, Direction, String> pair : relDirIterable) {
                stepRels.add(pair);
            }

            if (!beginSequenceAtStart && index == 0) {
                initialRels = stepRels;
            } else {
                relSequences.add(stepRels);
            }

            index++;
        }
    }

    @Override
    public Iterable<Relationship> expand( Path path, BranchState state ) {
        final Node node = path.endNode();
        int depth = path.length();
        List<Triple<RelationshipType, Direction, String>> stepRels;

        if (depth == 0 && initialRels != null) {
            stepRels = initialRels;
        } else {
            stepRels = relSequences.get((initialRels == null ? depth : depth - 1) % relSequences.size());
        }

        return Iterators.asList(
        new NestingIterator<>(
                stepRels.iterator() )
        {
            @Override
            protected Iterator<Relationship> createNestedIterator(
                    Triple<RelationshipType, Direction, String> entry )
            {
                RelationshipType type = entry.getLeft();
                Direction dir = entry.getMiddle();
                String props = entry.getRight();
                final Iterable<Relationship> iterable;
                if (type != null) {
                        iterable = (dir == Direction.BOTH) ? node.getRelationships(type) :
                            node.getRelationships(dir, type);
                } else {
                        iterable = (dir == Direction.BOTH) ? node.getRelationships() :
                         node.getRelationships(dir);
                }
                return Iterables.filter(rel -> PropertyMatcher.matchesProperties(rel, props), iterable).iterator();
            }
        });
    }

    @Override
    public PathExpander reverse() {
        throw new RuntimeException("Not implemented");
    }
}
