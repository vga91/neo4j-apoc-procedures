package apoc.graph;

import apoc.Extended;
import apoc.result.GraphResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.collection.Iterables;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Extended
public class GraphsExtended {
    
    /*
    `apoc.virtual.graph([list-of-paths, nodes, rels], [properties to remove]) -> {nodes, rels}

to exclude embeddings and large text properties
we already have all the virutal graph methods but they actually just wrap existing nodes and rels
call db.index.fulltext.queryNodes("movieFulltext","Forrest Gump", {limit:1}) yield node as n, score as s1
call db.index.vector.queryNodes("moviePlotsEmbedding",5, n.plotEmbedding) yield node as movie, score
match path = (person:Person)-[rp]->(movie)-[rg:IN_GENRE]->(genre)

with collect(path) as paths
call apoc.graph.fromPaths(paths,"results",{}) yield graph
with graph.nodes as nodes, graph.relationships as rels
with rels, apoc.map.fromPairs([n in nodes | [coalesce(n.tmdbId, n.name), apoc.create.vNode(labels(n),n {.*, plotEmbedding:null, posterEmbedding:null, plot:null, bio:null })]]) as nodes
return nodes, [r in rels | apoc.create.vRelationship(nodes[coalesce(startNode(r).tmdbId,startNode(r).name)], type(r), properties(r), nodes[coalesce(endNode(r).tmdbId,endNode(r).name)])] as rels
I want to replace the whole thing by an aggregation function like this:

call db.index.fulltext.queryNodes("movieFulltext","Forrest Gump", {limit:1}) yield node as n, score as s1
call db.index.vector.queryNodes("moviePlotsEmbedding",5, n.plotEmbedding) yield node as movie, score
match path = (person:Person)-[rp]->(movie)-[rg:IN_GENRE]->(genre)
return apoc.graph.filterProperties(path, ['plotEmbedding', 'posterEmbedding','plot', 'bio'])
so basically we create the same graph object with the extract that we have for the virtual graph
and then go over the nodes and replace the ones that have one of the properties with virtual ones that wrap the original nodes and leave off the properties


     */


    @Procedure("apoc.virtual.graph")
    @Description(
            "CALL () YIELD nodes, relationships - returns a set of ")
    public Stream<GraphResult> fromData(
            @Name("value") Object value, @Name("propertiesToRemove") List<String> propertiesToRemove) {
        VirtualGraphExtractor extractor = new VirtualGraphExtractor(propertiesToRemove);
        extractor.extract(value);
        GraphResult result = new GraphResult( extractor.nodes(), extractor.rels() );
        return Stream.of(result);
    }
    
    @UserAggregationFunction("apoc.graph.filterProperties")
    @Description(
            "apoc.graph.filterProperties")
    public GraphFunction filterProperties() {
        return new GraphFunction();
    }

    public static class GraphFunction {
        public static final String NODES = "nodes";
        public static final String RELATIONSHIPS = "relationships";

        private VirtualGraphExtractor virtualGraphExtractor;

        @UserAggregationUpdate
        public void filterProperties(@Name("value") Object value, @Name("propertiesToRemove") List<String> propertiesToRemove) {
            if (virtualGraphExtractor == null) {
                virtualGraphExtractor = new VirtualGraphExtractor(propertiesToRemove);
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
        private final Map<String, Node> nodes;
        private final Map<String, Relationship> rels;
        private final List<String> propertiesToRemove;

        public VirtualGraphExtractor(List<String> propertiesToRemove) {
            this.nodes = new HashMap<>();
            this.rels = new HashMap<>();
            this.propertiesToRemove = propertiesToRemove;
        }
        
        public VirtualGraphExtractor(Map<String, Node> nodes, Map<String, Relationship> rels, List<String> propertiesToRemove) {
            this.nodes = nodes;
            this.rels = rels;
            this.propertiesToRemove = propertiesToRemove;
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
            props.removeAll(propertiesToRemove);

            return new VirtualNode(startNode, props);
        }

        private Relationship createVirtualRel(Relationship rel) {
            Node startNode = rel.getStartNode();
            startNode = nodes.putIfAbsent(startNode.getElementId(), createVirtualNode(startNode));

            Node endNode = rel.getEndNode();
            endNode = nodes.putIfAbsent(endNode.getElementId(), createVirtualNode(endNode));
            
            Map<String, Object> props = rel.getAllProperties();
            propertiesToRemove.forEach(props.keySet()::remove);

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
