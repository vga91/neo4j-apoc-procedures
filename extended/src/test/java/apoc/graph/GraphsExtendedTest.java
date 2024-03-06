package apoc.graph;

import apoc.create.Create;
import apoc.map.Maps;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.*;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class GraphsExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static Map<String, Object> propsPerson1 = map("name", "foo", "plotEmbedding", "22", "posterEmbedding", "3", "plot", "4", "bio", "5", "id", 1L);
    private static Map<String, Object> propsPerson2 = map("name", "bar", "plotEmbedding", "22", "posterEmbedding", "3", "plot", "4", "bio", "5", "id", 3L);
    private static Map<String, Object> propsMovie1 = map("title", "1", "tmdbId", "ajeje", "id", 2L, "posterEmbedding", "33");
    private static Map<String, Object> propsMovie2 = map("title", "1", "tmdbId", "brazorf", "id", 4L, "posterEmbedding", "44");
    private static Map<String, Object> propsRel1 = map("id", 1L);
    private static Map<String, Object> propsRel2 = map("id", 2L);
    
    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, GraphsExtended.class, Create.class, Maps.class, Graphs.class);
        
        db.executeTransactionally("""
                CREATE (:Person $propsPerson1)-[:REL {id: 1}]->(:Movie $propsMovie1),
                 (:Person $propsPerson2)-[:REL {id: 2}]->(:Movie $propsMovie2)""",
                map("propsPerson1", propsPerson1,
                        "propsPerson2", propsPerson2,
                        "propsMovie1", propsMovie1,
                        "propsMovie2", propsMovie2,
                        "propsRel1", propsRel1,
                        "propsRel2", propsRel2));
    }
    
    // todo - check that 

    // todo
    @Test
    public void test() {
        testCall(db, """
                match path=(:Person)-[:REL]->(:Movie)
                with collect(path) as paths
                call apoc.graph.fromPaths(paths,"results",{}) yield graph
                with graph.nodes as nodes, graph.relationships as rels
                with rels, apoc.map.fromPairs([n in nodes | [coalesce(n.tmdbId, n.name), apoc.create.vNode(labels(n), apoc.map.removeKeys(properties(n), ['plotEmbedding', 'posterEmbedding', 'plot', 'bio'] ) )]]) as nodes
                return apoc.map.values(nodes, keys(nodes)) AS nodes,
                    [r in rels | apoc.create.vRelationship(nodes[coalesce(startNode(r).tmdbId,startNode(r).name)], type(r), properties(r), nodes[coalesce(endNode(r).tmdbId,endNode(r).name)])] AS relationships""", r -> {
            extracted(r);
        });
        
        testCall(db, """
                MATCH path=(:Person)-[:REL]->(:Movie)
                WITH apoc.graph.filterProperties(path, ['plotEmbedding', 'posterEmbedding', 'plot', 'bio']) as graph
                RETURN graph.nodes AS nodes, graph.relationships AS relationships""", r -> {
            extracted(r);
        });
        
        // todo - check that original nodes haven't changed
        testResult(db, "MATCH path=(n:Person)-[:REL]->(:Movie) RETURN path ORDER BY n.id", r -> {
            Iterator<Path> row = r.columnAs("path");
            Path path = row.next();
            Map<String, Object> propsStart = path.startNode().getAllProperties();
            Map<String, Object> propsEnd = path.endNode().getAllProperties();
            Map<String, Object> propsRel = path.relationships().iterator().next().getAllProperties();
            System.out.println("propsRel = " + propsRel);
            
            assertEquals(propsPerson1, propsStart);
            assertEquals(propsMovie1, propsEnd);
            assertEquals(propsRel1, propsRel);

            path = row.next();
            propsStart = path.startNode().getAllProperties();
            propsEnd = path.endNode().getAllProperties();
            propsRel = path.relationships().iterator().next().getAllProperties();
            assertEquals(propsPerson2, propsStart);
            assertEquals(propsMovie2, propsEnd);
            assertEquals(propsRel2, propsRel);
            
            assertFalse(row.hasNext());
        });
    }

    private static void extracted(Map<String, Object> r) {
        List<Node> nodes = (List<Node>) r.get("nodes");
        nodes.sort(Comparator.comparingLong(i -> (long) i.getProperty("id")));
        assertEquals(4, nodes.size());

        Node node = nodes.get(0);
        assertEquals(List.of(Label.label("Person")), node.getLabels());
        assertEquals(Map.of("name", "foo", "id", 1L), node.getAllProperties());
        node = nodes.get(1);
        assertEquals(List.of(Label.label("Movie")), node.getLabels());
        assertEquals(Map.of("title", "1", "id", 2L, "tmdbId", "ajeje"), node.getAllProperties());
        node = nodes.get(2);
        assertEquals(List.of(Label.label("Person")), node.getLabels());
        assertEquals(Map.of("name", "bar", "id", 3L), node.getAllProperties());
        node = nodes.get(3);
        assertEquals(List.of(Label.label("Movie")), node.getLabels());
        assertEquals(Map.of("title", "1", "id", 4L, "tmdbId", "brazorf"), node.getAllProperties());

        List<Relationship> relationships = (List<Relationship>) r.get("relationships");
        relationships.sort(Comparator.comparingLong(i -> (long) i.getProperty("id")));
        assertEquals(2, relationships.size());

        Relationship rel = relationships.get(0);
        assertEquals(RelationshipType.withName("REL"), rel.getType());
        assertEquals(Map.of("id", 1L), rel.getAllProperties());
        rel = relationships.get(1);
        assertEquals(RelationshipType.withName("REL"), rel.getType());
        assertEquals(Map.of("id", 2L), rel.getAllProperties());
    }
}
