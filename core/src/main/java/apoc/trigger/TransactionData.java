package apoc.trigger;

import apoc.convert.Convert;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.internal.helpers.collection.Iterables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.util.Util.map;

public class TransactionData {
    private final long transactionId;
    private final long commitTime;
    private final List<Node> createdNodes;
    private final List<Relationship> createdRelationships;
    private final List<Node> deletedNodes;
    private final List<Relationship> deletedRelationships;
    private final Map<String, List<Node>> removedLabels;
    private final Map<String, List<PropertyEntryContainer<Node>>> removedNodeProperties;
    private final Map<String, List<PropertyEntryContainer<Relationship>>> removedRelationshipProperties;
    private final Map<String, List<Node>> assignedLabels;
    private final Map<String, List<PropertyEntryContainer<Node>>> assignedNodeProperties;
    private final Map<String, List<PropertyEntryContainer<Relationship>>> assignedRelationshipProperties;
    private final Map<String, Object> metaData;

    private TransactionData(long transactionId, long commitTime,
                            List<Node> createdNodes, List<Relationship> createdRelationships,
                            List<Node> deletedNodes, List<Relationship> deletedRelationships,
                            Map<String, List<Node>> removedLabels,
                            Map<String, List<PropertyEntryContainer<Node>>> removedNodeProperties,
                            Map<String, List<PropertyEntryContainer<Relationship>>> removedRelationshipProperties,
                            Map<String, List<Node>> assignedLabels,
                            Map<String, List<PropertyEntryContainer<Node>>> assignedNodeProperties,
                            Map<String, List<PropertyEntryContainer<Relationship>>> assignedRelationshipProperties,
                            Map<String, Object> metaData) {
        this.transactionId = transactionId;
        this.commitTime = commitTime;
        this.createdNodes = createdNodes;
        this.createdRelationships = createdRelationships;
        this.deletedNodes = deletedNodes;
        this.deletedRelationships = deletedRelationships;
        this.removedLabels = removedLabels;
        this.removedNodeProperties = removedNodeProperties;
        this.removedRelationshipProperties = removedRelationshipProperties;
        this.assignedLabels = assignedLabels;
        this.assignedNodeProperties = assignedNodeProperties;
        this.assignedRelationshipProperties = assignedRelationshipProperties;
        this.metaData = metaData;
    }

    public static TransactionData from(org.neo4j.graphdb.event.TransactionData txData, boolean rebindDeleted) {
        return from(txData, rebindDeleted, false);
    }

    public static TransactionData from(org.neo4j.graphdb.event.TransactionData txData, boolean rebindDeleted, boolean isVirtual) {
        long txId, commitTime;
        try {
            txId = txData.getTransactionId();
        } catch (Exception ignored) {
            txId = -1L;
        }
        try {
            commitTime = txData.getCommitTime();
        } catch (Exception ignored) {
            commitTime = -1L;
        }
        List<Node> createdNodes = todoName(isVirtual, (List<Node>) Convert.convertToList(txData.createdNodes()));
        List<Relationship> createdRelationships = todoName2(isVirtual, Convert.convertToList(txData.createdRelationships()));
        List<Node> deletedNodes = todoName(isVirtual, rebindDeleted ? rebindDeleted(Convert.convertToList(txData.deletedNodes())) : Convert.convertToList(txData.deletedNodes()));
        List<Relationship> deletedRelationships = todoName2(isVirtual, rebindDeleted ? rebindDeleted(Convert.convertToList(txData.deletedRelationships())) : Convert.convertToList(txData.deletedRelationships()));
        Map<String, List<Node>> removedLabels = aggregateLabels(txData.removedLabels(), isVirtual);
        Map<String, List<Node>> assignedLabels = aggregateLabels(txData.assignedLabels(), isVirtual);
        // todo - forse a questi non serve?
        final Map<String, List<PropertyEntryContainer<Node>>> removedNodeProperties = aggregatePropertyKeys(txData.removedNodeProperties(), true, isVirtual);
        final Map<String, List<PropertyEntryContainer<Relationship>>> removedRelationshipProperties = aggregatePropertyKeys(txData.removedRelationshipProperties(), true, isVirtual);
        final Map<String, List<PropertyEntryContainer<Node>>> assignedNodeProperties = aggregatePropertyKeys(txData.assignedNodeProperties(), false, isVirtual);
        final Map<String, List<PropertyEntryContainer<Relationship>>> assignedRelationshipProperties = aggregatePropertyKeys(txData.assignedRelationshipProperties(), false, isVirtual);
        return new TransactionData(txId, commitTime, createdNodes, createdRelationships, deletedNodes, deletedRelationships,
                removedLabels,removedNodeProperties, removedRelationshipProperties, assignedLabels, assignedNodeProperties,
                assignedRelationshipProperties, txData.metaData());
    }
    
    private static List<Node> todoName(boolean isVirtual, List<Node> nodes) {
        if (isVirtual) {
            return nodes.stream().map(node -> new VirtualNode(node, Iterables.asList(node.getPropertyKeys()))).collect(Collectors.toList());
        }
        return nodes;
    }
    
    private static List<Relationship> todoName2(boolean isVirtual, List<Relationship> rels) {
        if (isVirtual) {
            return rels.stream().map(rel -> new VirtualRelationship(rel.getStartNode(), rel.getStartNode(), rel.getType()).withProperties(rel.getAllProperties()))
                    .collect(Collectors.toList());
        }
        return rels;
    }

    private static <T extends Entity> List<T> rebindDeleted(List<T> entities) {
        return (List<T>) entities.stream()
                .map(e -> {
                    if (e instanceof Node) {
                        Node node = (Node) e;
                        Label[] labels = Iterables.asArray(Label.class, node.getLabels());
                        return new VirtualNode(labels, e.getAllProperties());
                    } else {
                        Relationship rel = (Relationship) e;
                        return new VirtualRelationship(rel.getStartNode(), rel.getEndNode(), rel.getType());
                    }
                })
                .collect(Collectors.toList());
    }

    public TransactionData rebind(Transaction tx) {
        final List<Node> createdNodes = Util.rebind(this.createdNodes, tx);
        final List<Relationship> createdRelationships = Util.rebind(this.createdRelationships, tx);
//        final List<Node> deletedNodes = Util.rebind(this.deletedNodes, tx);
//        final List<Relationship> deletedRelationships = Util.rebind(this.deletedRelationships, tx);
        final Map<String, List<Node>> removedLabels = rebindMap(this.removedLabels, tx);
        final Map<String, List<Node>> assignedLabels = rebindMap(this.assignedLabels, tx);
        final Map<String, List<PropertyEntryContainer<Node>>> removedNodeProperties = rebindPropertyEntryContainer(this.removedNodeProperties, tx);
        final Map<String, List<PropertyEntryContainer<Relationship>>> removedRelationshipProperties = rebindPropertyEntryContainer(this.removedRelationshipProperties, tx);
        final Map<String, List<PropertyEntryContainer<Node>>> assignedNodeProperties = rebindPropertyEntryContainer(this.assignedNodeProperties, tx);
        final Map<String, List<PropertyEntryContainer<Relationship>>> assignedRelationshipProperties = rebindPropertyEntryContainer(this.assignedRelationshipProperties, tx);
        return new TransactionData(transactionId, commitTime, createdNodes, createdRelationships, deletedNodes, deletedRelationships,
                removedLabels, removedNodeProperties, removedRelationshipProperties, assignedLabels, assignedNodeProperties,
                assignedRelationshipProperties, metaData);
    }

    private <T extends Entity> Map<String, List<PropertyEntryContainer<T>>> rebindPropertyEntryContainer(Map<String, List<PropertyEntryContainer<T>>> map, Transaction tx) {
        return map.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().stream().map(p -> p.rebind(tx)).collect(Collectors.toList())));
    }

    private <T extends Entity> Map<String, List<T>> rebindMap(Map<String, List<T>> map, Transaction tx) {
        return map.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> Util.rebind(e.getValue(), tx)));
    }

    private <T extends Entity> Map<String, List<Map<String, Object>>> convertMapOfPropertyEntryContainers(Map<String, List<PropertyEntryContainer<T>>> map) {
        return map.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()
                        .stream()
                        .map(PropertyEntryContainer::toMap)
                        .collect(Collectors.toList())));
    }

    public Map<String, Object> toMap() {
        // todo - gestire transactionId
        return map("transactionId", transactionId,
                "commitTime", commitTime,
                "createdNodes", createdNodes,
                "createdRelationships", createdRelationships,
                "deletedNodes", deletedNodes,
                "deletedRelationships", deletedRelationships,
                "removedLabels", removedLabels,
                "removedNodeProperties", convertMapOfPropertyEntryContainers(removedNodeProperties),
                "removedRelationshipProperties", convertMapOfPropertyEntryContainers(removedRelationshipProperties),
                "assignedLabels", assignedLabels,
                "assignedNodeProperties", convertMapOfPropertyEntryContainers(assignedNodeProperties),
                "assignedRelationshipProperties", convertMapOfPropertyEntryContainers(assignedRelationshipProperties),
                "metaData", metaData);
    }

    public Map<String, Object> toMap2() {
        return map(
                "createdNodes", createdNodes,
                "createdRelationships", createdRelationships,
                "deletedNodes", deletedNodes,
                "deletedRelationships", deletedRelationships,
                "removedLabels", removedLabels,
                "assignedLabels", assignedLabels,
                "removedNodeProperties", convertMapOfPropertyEntryContainers(removedNodeProperties),
                "removedRelationshipProperties", convertMapOfPropertyEntryContainers(removedRelationshipProperties),
                "assignedNodeProperties", convertMapOfPropertyEntryContainers(assignedNodeProperties),
                "assignedRelationshipProperties", convertMapOfPropertyEntryContainers(assignedRelationshipProperties),
                "metaData", metaData
        );
    }

    private static Map<String, List<Node>> aggregateLabels(Iterable<LabelEntry> labelEntries, boolean isVirtual) {
        if (!labelEntries.iterator().hasNext()) return Collections.emptyMap();
        Map<String, List<Node>> result = new HashMap<>();
        for (LabelEntry entry : labelEntries) {
            result.compute(entry.label().name(),
                    (k, v) -> {
                        if (v == null) v = new ArrayList<>(100);
                        v.add(isVirtual ? new VirtualNode(entry.node(), Iterables.asList(entry.node().getPropertyKeys())) : entry.node());
                        return v;
                    });
        }
        return result;
    }

    private static class PropertyEntryContainer<T extends Entity> {
        private final String key;
        private final T entity;
        private final Object oldVal;
        private final Object newVal;

        PropertyEntryContainer(String key, T entity, Object oldVal, Object newVal) {
            this.key = key;
            this.entity = entity;
            this.oldVal = oldVal;
            this.newVal = newVal;
        }

        PropertyEntryContainer<T> rebind(Transaction tx) {
            return new PropertyEntryContainer<T>(key, Util.rebind(tx, entity), oldVal, newVal);
        }

        Map<String, Object> toMap() {
            final Map<String, Object> map = map("key", key, entity instanceof Node ? "node" : "relationship", entity, "old", oldVal);
            if (newVal != null) {
                map.put("new", newVal);
            }
            return map;
        }
    }

    private static <T extends Entity> Map<String, List<PropertyEntryContainer<T>>> aggregatePropertyKeys(Iterable<PropertyEntry<T>> entries, boolean removed, boolean isVirtual) {
        if (!entries.iterator().hasNext()) return Collections.emptyMap();
        Map<String,List<PropertyEntryContainer<T>>> result = new HashMap<>();
        for (PropertyEntry<T> entry : entries) {
            result.compute(entry.key(),
                    (k, v) -> {
                        if (v == null) v = new ArrayList<>(100);
                        T entity = entry.entity();
                        if (isVirtual) {
                            if (entity instanceof Node) {
                                entity = (T) new VirtualNode((Node) entity, Iterables.asList(entity.getPropertyKeys()));
                            } else {
                                Relationship rel = (Relationship) entity;
                                entity = (T) new VirtualRelationship(rel.getStartNode(), rel.getEndNode(), rel.getType()).withProperties(rel.getAllProperties());
                            }
                        }
                        v.add(new PropertyEntryContainer<>(k, entity, entry.previouslyCommittedValue(), removed ? null : entry.value()));
                        return v;
                    });
        }
        return result;
    }

}
