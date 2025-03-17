package apoc.graph;

import apoc.Extended;
import apoc.result.GraphResult;
import apoc.result.NodeResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import org.neo4j.storageengine.api.RelationshipDirection;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Extended
public class GraphsExtended {

    public static class HierarchicalRelationshipProcessor {
        public static final int LEVEL_COUNTER = 10;
        private final Iterator<Relationship> iterator;
        private final Node node;
        private final RelationshipType originalType;
        private final Direction direction;
        private final GraphDatabaseService db;
        private int relationshipCounter = 0; // Counts total relationships processed

        public HierarchicalRelationshipProcessor(Node node, RelationshipType type, Direction direction, GraphDatabaseService db) {
            this.iterator = node.getRelationships(direction, type).iterator();
            this.node = node;
            this.originalType = type;
            this.direction = direction;
            this.db = db;
        }

        public void processAll() {
            iterator.forEachRemaining(rel -> {
                try (Transaction tx = db.beginTx()) {
                    Node sourceNode = Util.rebind(tx, node);
                    Node targetNode = rel.getOtherNode(sourceNode);
                    relationshipCounter++;

                    // Determine the level label dynamically
                    String levelLabel = getLevelLabel(relationshipCounter);

                    // Create an intermediate level node
                    Node levelNode = tx.createNode(Label.label(levelLabel));
                    levelNode.setProperty("createdAt", System.currentTimeMillis());

                    // Copy properties from the old relationship
                    for (String key : rel.getPropertyKeys()) {
                        levelNode.setProperty(key, rel.getProperty(key)); // Store properties in LevelX node
                    }

                    // Create new relationships
                    Relationship relAtoL = sourceNode.createRelationshipTo(levelNode, originalType);
                    Relationship relLtoB = levelNode.createRelationshipTo(targetNode, RelationshipType.withName("INTERMEDIATE")); // (LevelX) --[:INTERMEDIATE]--> (B)

                    // Remove old relationship
                    rel.delete();

                    tx.commit();
                }
            });
        }

        // Dynamically generate level labels based on relationship count
        private static String getLevelLabel(int count) {
            if (count <= LEVEL_COUNTER) return "Level1";
            int level = (count - 1) / LEVEL_COUNTER + 1;
            return "Level" + level;// + subIndex;
        }
    }

    @Context
    public GraphDatabaseService db;
    
    @Procedure(name = "apoc.graph.substructure", mode = Mode.WRITE)
    @Description("TODO")
    public Stream<NodeResult> substructure(@Name("node") Node node) {
        // TODO - change this values, put in configs
        RelationshipType type = RelationshipType.withName("test");
        Direction direction = Direction.OUTGOING;
        // -- change the above values

        new HierarchicalRelationshipProcessor(node, type, direction, db).processAll();
        
        return Stream.of(new NodeResult(node));
    }

    @Procedure("apoc.graph.filterProperties")
    @Description(
            "CALL apoc.graph.filterProperties(anyEntityObject, nodePropertiesToRemove, relPropertiesToRemove) YIELD nodes, relationships - returns a set of virtual nodes and relationships without the properties defined in nodePropertiesToRemove and relPropertiesToRemove")
    public Stream<GraphResult> fromData(
            @Name("value") Object value,
            @Name(value = "nodePropertiesToRemove", defaultValue = "{}") Map<String, List<String>> nodePropertiesToRemove,
            @Name(value = "relPropertiesToRemove", defaultValue = "{}") Map<String, List<String>> relPropertiesToRemove) {
        
        VirtualGraphExtractor extractor = new VirtualGraphExtractor(nodePropertiesToRemove, relPropertiesToRemove);
        extractor.extract(value);
        GraphResult result = new GraphResult( extractor.nodes(), extractor.rels() );
        return Stream.of(result);
    }
    
    @UserAggregationFunction("apoc.graph.filterProperties")
    @Description(
            "apoc.graph.filterProperties(anyEntityObject, nodePropertiesToRemove, relPropertiesToRemove) - aggregation function which returns an object {node: [virtual nodes], relationships: [virtual relationships]} without the properties defined in nodePropertiesToRemove and relPropertiesToRemove")
    public GraphFunction filterProperties() {
        return new GraphFunction();
    }

    public static class GraphFunction {
        public static final String NODES = "nodes";
        public static final String RELATIONSHIPS = "relationships";

        private VirtualGraphExtractor virtualGraphExtractor;

        @UserAggregationUpdate
        public void filterProperties(
                @Name("value") Object value,
                @Name(value = "nodePropertiesToRemove", defaultValue = "{}") Map<String, List<String>> nodePropertiesToRemove,
                @Name(value = "relPropertiesToRemove", defaultValue = "{}") Map<String, List<String>> relPropertiesToRemove) {
            
            if (virtualGraphExtractor == null) {
                virtualGraphExtractor = new VirtualGraphExtractor(nodePropertiesToRemove, relPropertiesToRemove);
            }
            virtualGraphExtractor.extract(value);
        }

        @UserAggregationResult
        public Object result() {
            Collection<Node> nodes = virtualGraphExtractor.nodes();
            Collection<Relationship> relationships = virtualGraphExtractor.rels();
            return Map.of(
                    NODES, nodes,
                    RELATIONSHIPS, relationships
            );
        }
    }

    public static class VirtualGraphExtractor {
        private static final String ALL_FILTER = "_all";
        
        private final Map<String, Node> nodes;
        private final Map<String, Relationship> rels;
        private final Map<String, List<String>> nodePropertiesToRemove;
        private final Map<String, List<String>> relPropertiesToRemove;

        public VirtualGraphExtractor(Map<String, List<String>> nodePropertiesToRemove, Map<String, List<String>> relPropertiesToRemove) {
            this.nodes = new HashMap<>();
            this.rels = new HashMap<>();
            this.nodePropertiesToRemove = nodePropertiesToRemove;
            this.relPropertiesToRemove = relPropertiesToRemove;
        }

        public void extract(Object value) {
            if (value == null) {
                return;
            }
            if (value instanceof Node node) {
                addVirtualNode(node);
                
            } else if (value instanceof Relationship rel) {
                addVirtualRel(rel);
                
            } else if (value instanceof Path path) {
                path.nodes().forEach(this::addVirtualNode);
                path.relationships().forEach(this::addVirtualRel);
                
            } else if (value instanceof Iterable) {
                ((Iterable<?>) value).forEach(this::extract);
                
            } else if (value instanceof Map<?,?> map) {
                map.values().forEach(this::extract);
                
            } else if (value instanceof Iterator) {
                ((Iterator<?>) value).forEachRemaining(this::extract);
                
            } else if (value instanceof Object[] array) {
                for (Object i : array) {
                    extract(i);
                }
            }
        }

        /**
         * We can use the elementId as a unique key for virtual nodes/relations, 
         * as it is the same as the analogue for real nodes/relations.
         */
        private void addVirtualRel(Relationship rel) {
            rels.putIfAbsent(rel.getElementId(), createVirtualRel(rel));
        }

        private void addVirtualNode(Node node) {
            nodes.putIfAbsent(node.getElementId(), createVirtualNode(node));
        }

        private Node createVirtualNode(Node startNode) {
            List<String> props = Iterables.asList(startNode.getPropertyKeys());
            nodePropertiesToRemove.forEach((k,v) -> {
                if (k.equals(ALL_FILTER) || startNode.hasLabel(Label.label(k))) {
                    props.removeAll(v);
                }
            });

            return new VirtualNode(startNode, props);
        }

        private Relationship createVirtualRel(Relationship rel) {
            Node startNode = rel.getStartNode();
            startNode = nodes.putIfAbsent(startNode.getElementId(), createVirtualNode(startNode));

            Node endNode = rel.getEndNode();
            endNode = nodes.putIfAbsent(endNode.getElementId(), createVirtualNode(endNode));
            
            Map<String, Object> props = rel.getAllProperties();
            
            relPropertiesToRemove.forEach((k,v) -> {
                if (k.equals(ALL_FILTER) || rel.isType(RelationshipType.withName(k))) {
                    v.forEach(props.keySet()::remove);
                }
            });

            return new VirtualRelationship(startNode, endNode, rel.getType(), props);
        }

        public List<Node> nodes() {
            return List.copyOf(nodes.values());
        }

        public List<Relationship> rels() {
            return List.copyOf(rels.values());
        }
    }
}
