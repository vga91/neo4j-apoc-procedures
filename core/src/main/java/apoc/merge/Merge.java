package apoc.merge;

import apoc.result.NodeListResult;
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
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.util.Util.labelString;
import static java.util.Collections.emptyMap;

public class Merge {

    public static final String ERROR_NOT_VIRTUAL_NODE = "All provided nodes must be virtual";
    public static final String ERROR_NOT_VIRTUAL_RELS = "All provided relationships must be virtual";
    
    @Context
    public Transaction tx;
    
    private static final String MERGED = "__is_merged"; 
    
    @Procedure(value="apoc.merge.vNodes", mode = Mode.WRITE, eager = true)
    @Description("apoc.merge.vNodes(nodes, $config) - merge a virtual node list")
    public Stream<NodeListResult> mergeVNodes(@Name("nodes") List<Node> nodes,
//                                         @Name("mergeKeysList") List<String> mergeKeysList,
                                              @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        MergeConfig conf = new MergeConfig(config);


//        for (Iterator<VirtualNode> iterator = nodes.iterator(); 
//             iterator.hasNext(); ) {
//            VirtualNode id = iterator.next();
//            builder.append(quote(id));
//            if (iterator.hasNext()) {
//                builder.append(",");
//            }
//        }
        final List<String> mergeKeysList = conf.getMergeKeysList();
//        List<VirtualNode> set = new HashSet<>(nodes);
        List<Node> set = new ArrayList<>();
        nodes.forEach(item1 -> {
            if (!(item1 instanceof VirtualNode)) {
                throw new RuntimeException(ERROR_NOT_VIRTUAL_NODE);
            }
            
            final Set<String> labelsSet = getLabelsSet(mergeKeysList, item1);
            
            // invece di noneMatch lo trovo e faccio set...
            final Node nodeFound = set.stream().filter(setItem ->
                    getLabelsSet(mergeKeysList, setItem).equals(labelsSet)
                            && item1.getAllProperties().equals(setItem.getAllProperties())).findAny().orElse(null);
            if (
                    nodeFound != null
            ) {
//                conf.getOnMatch()
                // todo - check instance of VirtualNode
                nodeFound.setProperty(MERGED, true);
            } else {
                set.add(item1);
            }
        });

        setOnMatchAndCreate(conf, set);

        return Stream.of(new NodeListResult(set));// set.stream().map(NodeResult::new);

//        Iterator<VirtualNode> i = nodes.iterator();
//        while (i.hasNext()) {
//            VirtualNode current = i.next(); // must be called before you can call i.remove()
//            // Do something
////            nodes.removeIf(item -> item.getId() != )
//            i.forEachRemaining();
////            ListIterator bItr = nodes.listIterator(i.forEachRemaining();)
//            
////            i.remove();
//        }
//        
//        IntStream.range(0,nodes.size()-1).filter(i -> {
//            final VirtualNode curr = nodes.get(i);
//            final VirtualNode next = nodes.get(i + 1);
////            doSomething(list.get(i),list.get(i+1));
//        });

//        for (nodes.iterator().next())


        // todo.. no uso variabile d'appoggio va..

//        nodes.stream().filter(curr -> {
//
//            final Iterable<String> labelNames = Iterables.map(Label::name, curr.getLabels());
//            Iterable<String> labels = mergeKeysList.isEmpty() ? labelNames : Iterables.filter(item -> mergeKeysList.contains(item), labelNames);
//
//
////            final Iterable<String> labelNames2 = Iterables.map(Label::name, other.getLabels());
////            Iterable<String> labels2 = mergeKeysList.isEmpty() ? labelNames2 : Iterables.filter(item -> mergeKeysList.contains(item), labelNames2);
////            if (mergeKeysList.isEmpty()) {
//////                labels = curr.getLabels();
////            } else {
////                labels = Iterables.filter(item -> mergeKeysList.contains(item), labelNames);
////            }
////            if (Iterables.asSet(labels).equals(Iterables.asSet(labels2)) && ) {
////                
////            }
//            nodes.stream().reduce(node -> {
//                if (curr.getId() != node.getId()) {
//                    final Iterable<String> labelNames2 = Iterables.map(Label::name, node.getLabels());
//                    Iterable<String> labels2 = mergeKeysList.isEmpty() ? labelNames2 : Iterables.filter(item -> mergeKeysList.contains(item), labelNames2);
//                    if ()
//                }
//                return false;
//            })
//        })


//        return nodes.stream().filter(node ->
//                nodes.stream().
////                Iterables.asSet(Iterables.map(Label::name, node.getLabels())).equals(Set.copyOf(labelNames))
//                        && isPropertiesMatched(props, node))
//                .findAny()
//                .map(getVirtualNodeVirtualNodeFunction(conf))
//                .orElseGet(getVirtualNodeSupplier(()-> createVirtualNode(labelNames, props), conf));
    }

    private Set<String> getLabelsSet(List<String> mergeKeysList, Node item1) {
        final Iterable<String> labelNames = Iterables.map(Label::name, item1.getLabels());
        Iterable<String> labels = mergeKeysList.isEmpty() ? labelNames : Iterables.filter(mergeKeysList::contains, labelNames);
        return Iterables.asSet(labels);
    }

    @Procedure(value="apoc.merge.vRelationships")
    @Description("apoc.merge.vRelationships(relationships, $config) - merge a virtual relationship list")
    public Stream<RelationshipListResult> mergeVRelationships(@Name("relationships") List<Relationship> relationships,
                                         @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        MergeConfig conf = new MergeConfig(config);

        List<Relationship> set = new ArrayList<>();
        
        relationships.forEach(rel -> {
            if (!(rel instanceof VirtualRelationship)) {
                throw new RuntimeException(ERROR_NOT_VIRTUAL_RELS);
            }
            final Relationship relFound = set.stream().filter(i -> i.getType().equals(rel.getType()) && i.getAllProperties().equals(rel.getAllProperties())).findAny().orElse(null);
            if (relFound != null) {
                relFound.setProperty(MERGED, true);
            } else {
                set.add(rel);
            }
        });

        setOnMatchAndCreate(conf, set);

        return Stream.of(new RelationshipListResult(set));
    }

    private <T extends Entity> void setOnMatchAndCreate(MergeConfig conf, List<T> list) {
        if (!conf.getOnMatch().isEmpty() || !conf.getOnCreate().isEmpty()) {
            list.forEach(entity -> {
                if (entity.hasProperty(MERGED)) {
                    conf.getOnMatch().forEach(entity::setProperty);
                    entity.removeProperty(MERGED);
                } else {
                    conf.getOnCreate().forEach(entity::setProperty);
                }
            });
        }
    }

    private <T extends Entity> Supplier<T> getVirtualNodeSupplier(Supplier<T> supplier, MergeConfig conf) {
        return () -> {
            final T node = supplier.get();
            conf.getOnCreate().forEach(node::setProperty);
            return node;
        };
    }

    private <T extends Entity> Function<T, T> getVirtualNodeVirtualNodeFunction(MergeConfig conf) {
        return node -> {
            conf.getOnMatch().forEach(node::setProperty);
            return node;
        };
    }
    

    private <T extends Entity> boolean isPropertiesMatched(Map<String, Object> props, T entity) {
        return props.entrySet().stream().allMatch(e -> Objects.deepEquals(e.getValue(), entity.getProperty(e.getKey(), null)));
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
