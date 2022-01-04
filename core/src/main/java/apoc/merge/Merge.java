package apoc.merge;

import apoc.result.NodeResult;
import apoc.result.RelationshipListResult;
import apoc.result.RelationshipResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static apoc.util.Util.labelString;
import static java.util.Collections.emptyMap;

public class Merge {

    public static final String ERROR_NOT_VIRTUAL_NODE = "All provided nodes must be virtual";
    public static final String ERROR_NOT_VIRTUAL_RELS = "All provided relationships must be virtual";
    
    @Context
    public Transaction tx;

    @UserAggregationFunction("apoc.merge.vNodes")
    @Description("apoc.merge.vNodes(nodes,$config) - merge a virtual node list")
    public MergeVNodes vNodes() {
        return new MergeVNodes();
    }
    
    @UserAggregationFunction("apoc.merge.vRelationships")
    @Description("apoc.merge.vRelationships(nodes,$config) - merge a virtual relationship list")
    public MergeVRels vRelationships() {
        return new MergeVRels();
    }

    private static abstract class MergeCommon<T extends Entity> {
        protected final List<T> result = new ArrayList<>();
        protected final List<Integer> indexes = new ArrayList<>();
        protected MergeConfig conf;

        @UserAggregationResult
        public Object result() {
            if (!conf.getOnMatch().isEmpty() || !conf.getOnCreate().isEmpty()) {
                IntStream.range(0, result.size())
                        .forEach(idx -> {
                            final T entity = result.get(idx);
                            if (indexes.contains(idx)) { 
                                conf.getOnMatch().forEach(entity::setProperty); 
                            } else { 
                                conf.getOnCreate().forEach(entity::setProperty); 
                            }
                });
            }
            return result;
        }
        
        protected boolean haveSameProps(T setItem, T node) {
            return node.getAllProperties().equals(setItem.getAllProperties());
        }

        protected void aggregateResults(T entity, IntPredicate predicate) {
            IntStream.range(0, result.size())
                    .filter(predicate)
                    .findFirst()
                    .ifPresentOrElse(indexes::add, () -> result.add(entity));
        }
    }

    public static class MergeVNodes extends MergeCommon<Node> {
        
        @UserAggregationUpdate
        public void update(@Name("nodes") Node node, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
            conf = new MergeConfig(config);
            final List<String> mergeKeysList = conf.getMergeKeysList();

            if (!(node instanceof VirtualNode)) {
                throw new RuntimeException(ERROR_NOT_VIRTUAL_NODE);
            }

            final Set<String> labelsSet = getLabelsSet(mergeKeysList, node);

            final IntPredicate findEqualsNode = idx -> {
                final Node setItem = result.get(idx);
                return getLabelsSet(mergeKeysList, setItem).equals(labelsSet) && haveSameProps(setItem, node);
            };
            
            aggregateResults(node, findEqualsNode);
        }

        private static Set<String> getLabelsSet(List<String> mergeKeysList, Node item1) {
            final Iterable<String> labelNames = Iterables.map(Label::name, item1.getLabels());
            Iterable<String> labels = mergeKeysList.isEmpty() ? labelNames : Iterables.filter(mergeKeysList::contains, labelNames);
            return Iterables.asSet(labels);
        }

    }

    public static class MergeVRels extends MergeCommon<Relationship> {

        @UserAggregationUpdate
        public void update(@Name("relationships") Relationship rel, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
            conf = new MergeConfig(config);
            
            if (!(rel instanceof VirtualRelationship)) {
                throw new RuntimeException(ERROR_NOT_VIRTUAL_RELS);
            }

            final IntPredicate findEqualsRel = idx -> {
                final Relationship setItem = result.get(idx);
                return setItem.getType().equals(rel.getType()) && haveSameProps(setItem, rel);
            };
            
            aggregateResults(rel, findEqualsRel);
        }
    }
    

    @Procedure(value="apoc.merge.node.eager", mode = Mode.WRITE, eager = true)
    @Description("apoc.merge.node.eager(['Label'], identProps:{key:value, ...}, onCreateProps:{key:value,...}, onMatchProps:{key:value,...}}) - merge nodes eagerly, with dynamic labels, with support for setting properties ON CREATE or ON MATCH")
    public Stream<NodeResult> nodesEager(@Name("label") List<String> labelNames,
                                        @Name("identProps") Map<String, Object> identProps,
                                        @Name(value = "props",defaultValue = "{}") Map<String, Object> props,
                                        @Name(value = "onMatchProps",defaultValue = "{}") Map<String, Object> onMatchProps) {
        return nodes(labelNames, identProps,props,onMatchProps);
    }

    @Procedure(value="apoc.merge.node", mode = Mode.WRITE)
    @Description("\"apoc.merge.node.eager(['Label'], identProps:{key:value, ...}, onCreateProps:{key:value,...}, onMatchProps:{key:value,...}}) - merge nodes with dynamic labels, with support for setting properties ON CREATE or ON MATCH")
    public Stream<NodeResult> nodes(@Name("label") List<String> labelNames,
                                        @Name("identProps") Map<String, Object> identProps,
                                        @Name(value = "props",defaultValue = "{}") Map<String, Object> props,
                                        @Name(value = "onMatchProps",defaultValue = "{}") Map<String, Object> onMatchProps) {
        if (identProps==null || identProps.isEmpty()) {
            throw new IllegalArgumentException("you need to supply at least one identifying property for a merge");
        }

        String labels = labelString(labelNames);

        Map<String, Object> params = Util.map("identProps", identProps, "onCreateProps", props, "onMatchProps", onMatchProps);
        String identPropsString = buildIdentPropsString(identProps);

        final String cypher = "MERGE (n:" + labels + "{" + identPropsString + "}) ON CREATE SET n += $onCreateProps ON MATCH SET n += $onMatchProps RETURN n";
        return tx.execute(cypher, params ).columnAs("n").stream().map(node -> new NodeResult((Node) node));
    }

    @Procedure(value = "apoc.merge.relationship", mode = Mode.WRITE)
    @Description("apoc.merge.relationship(startNode, relType,  identProps:{key:value, ...}, onCreateProps:{key:value, ...}, endNode, onMatchProps:{key:value, ...}) - merge relationship with dynamic type, with support for setting properties ON CREATE or ON MATCH")
    public Stream<RelationshipResult> relationship(@Name("startNode") Node startNode, @Name("relationshipType") String relType,
                                                        @Name("identProps") Map<String, Object> identProps,
                                                        @Name("props") Map<String, Object> onCreateProps,
                                                        @Name("endNode") Node endNode,
                                                        @Name(value = "onMatchProps",defaultValue = "{}") Map<String, Object> onMatchProps) {
        String identPropsString = buildIdentPropsString(identProps);

        Map<String, Object> params = Util.map("identProps", identProps, "onCreateProps", onCreateProps==null ? emptyMap() : onCreateProps,
                "onMatchProps", onMatchProps == null ? emptyMap() : onMatchProps, "startNode", startNode, "endNode", endNode);

        final String cypher =
                "WITH $startNode as startNode, $endNode as endNode " +
                "MERGE (startNode)-[r:"+ Util.quote(relType) +"{"+identPropsString+"}]->(endNode) " +
                "ON CREATE SET r+= $onCreateProps " +
                "ON MATCH SET r+= $onMatchProps " +
                "RETURN r";
        return tx.execute(cypher, params ).columnAs("r").stream().map(rel -> new RelationshipResult((Relationship) rel));
    }
    @Procedure(value = "apoc.merge.relationship.eager", mode = Mode.WRITE, eager = true)
    @Description("apoc.merge.relationship(startNode, relType,  identProps:{key:value, ...}, onCreateProps:{key:value, ...}, endNode, onMatchProps:{key:value, ...}) - merge relationship with dynamic type, with support for setting properties ON CREATE or ON MATCH")
    public Stream<RelationshipResult> relationshipEager(@Name("startNode") Node startNode, @Name("relationshipType") String relType,
                                                        @Name("identProps") Map<String, Object> identProps,
                                                        @Name("props") Map<String, Object> onCreateProps,
                                                        @Name("endNode") Node endNode,
                                                        @Name(value = "onMatchProps",defaultValue = "{}") Map<String, Object> onMatchProps) {
        return relationship(startNode, relType, identProps, onCreateProps, endNode, onMatchProps );
    }


    private String buildIdentPropsString(Map<String, Object> identProps) {
        if (identProps == null) return "";
        return identProps.keySet().stream().map(Util::quote)
                .map(s -> s + ":$identProps." + s)
                .collect(Collectors.joining(","));
    }
}
