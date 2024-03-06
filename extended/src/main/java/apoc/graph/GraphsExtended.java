package apoc.graph;

import apoc.Extended;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.collection.Iterables;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    
    @UserAggregationFunction("")
    @Description("apoc.graph.filterProperties")
    public GraphFunction filterProperties() {
        return new GraphFunction();
    }

    // todo - forse potrei mettere il VirtualGraphExtractor qui nel costruttore, e fare una volta sola new VirtualGraphExtractor()..
    public static class GraphFunction {
        public static final String NODES = "nodes";
        public static final String RELATIONSHIPS = "relationships";

//        private Map<String, Map<String, Entity>> graph = Map.of(NODES, new HashMap<>(),
//                RELATIONSHIPS, new HashMap<>());
        
//        private Map<String, Node> nodesCache = new HashMap<>();
//        private Map<String, Relationship> relationshipsCache = new HashMap<>();

//        public GraphFunction() {
//            new VirtualGraphExtractor();
//        }

        private VirtualGraphExtractor virtualGraphExtractor;

        @UserAggregationUpdate
        public void filterProperties(@Name("value") Object value, @Name("propertiesToRemove") List<String> propertiesToRemove) {

            if (virtualGraphExtractor == null) {
                virtualGraphExtractor = new VirtualGraphExtractor(propertiesToRemove);
            }
            
            virtualGraphExtractor.extract(value);
            
//            nodesCache = virtualGraphExtractor.getNodes();
//            relationshipsCache = virtualGraphExtractor.getRels();
                    
//            extract(value, nodesCache, relationshipsCache, propertiesToRemove);
        }

        @UserAggregationResult
        public Object result() {
            Collection<Node> nodes = virtualGraphExtractor.getNodes().values();
            Collection<Relationship> relationships = virtualGraphExtractor.getRels().values();
            return Map.of(
                    NODES, nodes,
                    RELATIONSHIPS, relationships
            );
        }
    }
        
    
    
    /*
    @Procedure("apoc.virtual.graph")
    @Description(
            "CALL () YIELD nodes, relationships - returns a set of ")
    public Stream<GraphResult> fromData(
            @Name("value") Object value, @Name("propertiesToRemove") List<String> propertiesToRemove) {
        Set<Node> nodes = new HashSet<>(1000);
        Set<Relationship> rels = new HashSet<>(10000);
        extract(value, nodes, rels, propertiesToRemove);
        return Stream.of(new GraphResult(List.copyOf(nodes), List.copyOf(rels)));
    }

    */
    // todo -->     @UserFunction("apoc.create.virtual.fromNode")


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
//            boolean found = false;
            if (value == null) return;// false;
            if (value instanceof Node node) {


                extracted(node);
//                nodes.put((Node) value);
//                return true;
            } else if (value instanceof Relationship rel) {
                extracted(rel);
//                rels.add((Relationship) value);
//                return true;
                
            } else if (value instanceof Path path) {
                path.nodes().forEach(node -> {
                    extracted(node);
                });
                
                path.relationships().forEach(rel -> {
                    extracted(rel);
                });

//                Iterables.addAll(nodes, ((Path) value).nodes());
//                Iterables.addAll(rels, ((Path) value).relationships());
//                return true;
            } else if (value instanceof Iterable) {
                ((Iterable<?>) value).forEach(i -> extract(i));
                
//                for (Object o : (Iterable) value) found |= extract(o, nodes, rels);
            } else if (value instanceof Map map) {
                map.values().forEach(i -> extract(i));
                
                
//                for (Object o : ((Map) value).values()) found |= extract(o, nodes, rels);
            } else if (value instanceof Iterator) {
                ((Iterator<?>) value).forEachRemaining(i -> extract(i));
                
//                Iterator it = (Iterator) value;
//                while (it.hasNext()) found |= extract(it.next(), nodes, rels);
            } else if (value instanceof Object[] array) {
                for (Object i : array) {
                    extract(i);
                }
                
//                for (Object o : (Object[]) value) found |= extract(o, nodes, rels);
            }
        }

        private void extracted(Relationship rel) {
//            Relationship virtualRel = createVirtualRel(rel);
            rels.putIfAbsent(rel.getElementId(), createVirtualRel(rel));
        }

        private void extracted(Node node) {
//            Node virtualNode = createVirtualNode(node);
            nodes.putIfAbsent(node.getElementId(), createVirtualNode(node));
        }


        private Node createVirtualNode(Node startNode/*, List<String> propertiesToRemove*/) {
//        Map<String, Object> props = startNode.getAllProperties();
            List<String> props = Iterables.asList(startNode.getPropertyKeys());
            props.removeAll(propertiesToRemove);

            return new VirtualNode(startNode, props);
        }

        /*

         */
        private Relationship createVirtualRel(Relationship rel/*, List<String> propertiesToRemove*/) {
            Node startNode = rel.getStartNode();
            startNode = nodes.putIfAbsent(startNode.getElementId(), createVirtualNode(startNode));

            Node endNode = rel.getEndNode();
            endNode = nodes.putIfAbsent(endNode.getElementId(), createVirtualNode(endNode));

            
            Map<String, Object> props = rel.getAllProperties();
//            List<String> props = Iterables.asList(rel.getPropertyKeys());
            props.keySet().removeAll(propertiesToRemove);

            return new VirtualRelationship(startNode, endNode, rel.getType(), props);
        }

        public Map<String, Node> getNodes() {
            return nodes;
        }

        public Map<String, Relationship> getRels() {
            return rels;
        }
    }

    // todo - dire che, a differenza dell'id, nei nodi virtuali l'elementId è univoco in base al nodo
//    
//    public boolean extract(Object value, Map<String, Entity> nodes, Map<String, Entity> rels, List<String> propertiesToRemove) {
//        boolean found = false;
//        if (value == null) return false;
//        if (value instanceof Node) {
//            
//            
//            nodes.add((Node) value);
//            return true;
//        } else if (value instanceof Relationship) {
//            rels.add((Relationship) value);
//            return true;
//        } else if (value instanceof Path path) {
//            path.nodes().forEach(node -> {
//                Node virtualNode = createVirtualNode(node, propertiesToRemove);
//                nodes.put(virtualNode.getElementId(), virtualNode);
//            });
//            
//            Iterables.addAll(nodes, ((Path) value).nodes());
//            Iterables.addAll(rels, ((Path) value).relationships());
//            return true;
//        } else if (value instanceof Iterable) {
//            for (Object o : (Iterable) value) found |= extract(o, nodes, rels);
//        } else if (value instanceof Map) {
//            for (Object o : ((Map) value).values()) found |= extract(o, nodes, rels);
//        } else if (value instanceof Iterator) {
//            Iterator it = (Iterator) value;
//            while (it.hasNext()) found |= extract(it.next(), nodes, rels);
//        } else if (value instanceof Object[]) {
//            for (Object o : (Object[]) value) found |= extract(o, nodes, rels);
//        }
//        return found;
//    }
}
