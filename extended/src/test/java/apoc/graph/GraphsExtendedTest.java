package apoc.graph;

import apoc.create.Create;
import apoc.map.Maps;
import apoc.util.TestUtil;
import apoc.util.collection.Iterables;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static apoc.util.TestUtil.*;
import static apoc.util.Util.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class GraphsExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static final Map<String, Object> propsPerson1 = map("name", "foo", "plotEmbedding", "22", "posterEmbedding", "3", "plot", "4", "bio", "5", "idNode", 1L);
    private static final Map<String, Object> propsPerson2 = map("name", "bar", "plotEmbedding", "22", "posterEmbedding", "3", "plot", "4", "bio", "5", "idNode", 3L);
    private static final Map<String, Object> propsMovie1 = map("title", "1", "tmdbId", "ajeje", "idNode", 2L, "posterEmbedding", "33");
    private static final Map<String, Object> propsMovie2 = map("title", "1", "tmdbId", "brazorf", "idNode", 4L, "posterEmbedding", "44");
    private static final Map<String, Object> propsRel1 = map("idRel", 1L);
    private static final Map<String, Object> propsRel2 = map("idRel", 2L);
    
    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, GraphsExtended.class, Create.class, Maps.class, Graphs.class);
        
        db.executeTransactionally("""
                CREATE (:Person $propsPerson1)-[:REL $propsRel1]->(:Movie $propsMovie1),
                 (:Person $propsPerson2)-[:REL $propsRel2]->(:Movie $propsMovie2)""",
                map("propsPerson1", propsPerson1,
                        "propsPerson2", propsPerson2,
                        "propsMovie1", propsMovie1,
                        "propsMovie2", propsMovie2,
                        "propsRel1", propsRel1,
                        "propsRel2", propsRel2));

        db.executeTransactionally(
                """
                        CREATE (a:Foo {idNode: 11, remove: 1})-[r1:MY_REL {idRel: 11, remove: 1}]->(b:Bar {idNode: 22, remove: 1})-[r2:ANOTHER_REL {idRel: 22, remove: 1}]->(c:Baz {idNode: 33, remove: 1})\s
                        WITH b, c\s
                        CREATE (b)-[:REL_TWO {idRel: 33, remove: 1}]->(c), (b)-[:REL_THREE {idRel: 44, remove: 1}]->(c), (b)-[:REL_FOUR {idRel: 55, remove: 1}]->(c)""");

        db.executeTransactionally(
                "CREATE (a:Foo {idNode: 44, remove: 1})-[r1:MY_REL {idRel: 66, remove: 1}]->(b:Bar {idNode: 55, remove: 1})-[r2:ANOTHER_REL {idRel: 77, remove: 1}]->(c:Baz {idNode: 66, remove: 1})");

        db.executeTransactionally(
                "CREATE (a:One {idNode: 77, remove: 1})-[r1:MY_REL {idRel: 88, remove: 1}]->(b:Two {idNode: 88, remove: 1}), " +
                "(:Two {idNode: 100, remove: 1})-[r2:ANOTHER_REL {idRel: 99, remove: 1}]->(c:Three {idNode: 99, remove: 1})");
    }
    
    @Test
    public void testFilterPropertiesConsistentWithManualFilteringAndDoesNotChangeOriginalEntities() {
        // check that the apoc.graph.filterProperties and the query used here: https://github.com/neo4j-contrib/neo4j-apoc-procedures/issues/3937
        // produce the same result
        testCall(db, """
                match path=(:Person)-[:REL]->(:Movie)
                with collect(path) as paths
                call apoc.graph.fromPaths(paths,"results",{}) yield graph
                with graph.nodes as nodes, graph.relationships as rels
                with rels, apoc.map.fromPairs([n in nodes | [coalesce(n.tmdbId, n.name), apoc.create.vNode(labels(n), apoc.map.removeKeys(properties(n), ['plotEmbedding', 'posterEmbedding', 'plot', 'bio'] ) )]]) as nodes
                return apoc.map.values(nodes, keys(nodes)) AS nodes,
                    [r in rels | apoc.create.vRelationship(nodes[coalesce(startNode(r).tmdbId,startNode(r).name)], type(r), properties(r), nodes[coalesce(endNode(r).tmdbId,endNode(r).name)])] AS relationships""",
                this::commonFilterPropertiesAssertions);
        
        testCall(db, """
                MATCH path=(:Person)-[:REL]->(:Movie)
                WITH apoc.graph.filterProperties(path, {_all: ['plotEmbedding', 'posterEmbedding', 'plot', 'bio']}) as graph
                RETURN graph.nodes AS nodes, graph.relationships AS relationships""",
                this::commonFilterPropertiesAssertions);
        
        // check that original nodes haven't changed
        testResult(db, "MATCH path=(n:Person)-[:REL]->(:Movie) RETURN path ORDER BY n.id", r -> {
            Iterator<Path> row = r.columnAs("path");
            Path path = row.next();
            Map<String, Object> propsStart = path.startNode().getAllProperties();
            Map<String, Object> propsEnd = path.endNode().getAllProperties();
            Map<String, Object> propsRel = path.relationships().iterator().next().getAllProperties();
            
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

    @Test
    public void testFilterPropertiesProcedure() {
        
        testCall(db, """
                MATCH path=(:Person)-[:REL]->(:Movie)
                WITH collect(path) AS paths
                CALL apoc.graph.filterProperties(paths, {_all: ['plotEmbedding', 'posterEmbedding', 'plot', 'bio']})
                YIELD nodes, relationships
                RETURN nodes, relationships""",
                this::commonFilterPropertiesAssertions);
        
        testCall(db, """
                MATCH path=(:Person)-[:REL]->(:Movie)
                WITH collect(path) AS paths
                CALL apoc.graph.filterProperties(paths, {Movie: ['posterEmbedding'], Person: ['posterEmbedding', 'plotEmbedding', 'plot', 'bio']})
                YIELD nodes, relationships
                RETURN nodes, relationships""",
                this::commonFilterPropertiesAssertions);
    }

    private void commonFilterPropertiesAssertions(Map<String, Object> r) {
        List<Node> nodes = (List<Node>) r.get("nodes");
        nodes.sort(Comparator.comparingLong(i -> (long) i.getProperty("idNode")));
        assertEquals(4, nodes.size());

        Node node = nodes.get(0);
        assertEquals(List.of(Label.label("Person")), node.getLabels());
        assertEquals(Map.of("name", "foo", "idNode", 1L), node.getAllProperties());
        node = nodes.get(1);
        assertEquals(List.of(Label.label("Movie")), node.getLabels());
        assertEquals(Map.of("title", "1", "idNode", 2L, "tmdbId", "ajeje"), node.getAllProperties());
        node = nodes.get(2);
        assertEquals(List.of(Label.label("Person")), node.getLabels());
        assertEquals(Map.of("name", "bar", "idNode", 3L), node.getAllProperties());
        node = nodes.get(3);
        assertEquals(List.of(Label.label("Movie")), node.getLabels());
        assertEquals(Map.of("title", "1", "idNode", 4L, "tmdbId", "brazorf"), node.getAllProperties());

        List<Relationship> relationships = (List<Relationship>) r.get("relationships");
        relationships.sort(Comparator.comparingLong(i -> (long) i.getProperty("idRel")));
        assertEquals(2, relationships.size());

        Relationship rel = relationships.get(0);
        assertEquals(RelationshipType.withName("REL"), rel.getType());
        assertEquals(Map.of("idRel", 1L), rel.getAllProperties());
        rel = relationships.get(1);
        assertEquals(RelationshipType.withName("REL"), rel.getType());
        assertEquals(Map.of("idRel", 2L), rel.getAllProperties());
    }

    @Test
    public void filterPropertiesWithPathsWithMultipleRels() {
        Set<Object> expectedIdNodes = Set.of(11L, 22L, 33L, 44L, 55L, 66L);
        Set<Object> expectedIdRels = Set.of(11L, 22L, 33L, 44L, 55L, 66L, 77L);
        
        testCall(db, """
                MATCH path=(:Foo)--(:Bar)--(:Baz)
                WITH collect(path) AS paths
                CALL apoc.graph.filterProperties(paths, {_all: ['remove']}, {_all: ['remove']})
                YIELD nodes, relationships
                RETURN nodes, relationships""", 
                r -> assertNodeAndRelIdProps(r, expectedIdNodes, expectedIdRels));
        
        testCall(db, """
                MATCH path=(:Foo)--(:Bar)--(:Baz)
                WITH apoc.graph.filterProperties(path, {_all: ['remove']}, {_all: ['remove']}) as graph
                RETURN graph.nodes AS nodes, graph.relationships AS relationships""",
                r -> assertNodeAndRelIdProps(r, expectedIdNodes, expectedIdRels));
    }

    @Test
    public void testWithCompositeDataTypes() {
        Set<Object> expectedIdNodes = Set.of(100L, 99L, 88L, 77L);
        Set<Object> expectedIdRels = Set.of(99L, 88L);
        
        testCall(db, """
                MATCH p1=(:One)--(:Two), p2=(:Two)--(:Three)
                CALL apoc.graph.filterProperties([p1, p2], {_all: ['remove']}, {_all: ['remove']})
                YIELD nodes, relationships
                RETURN nodes, relationships""",
                r -> assertNodeAndRelIdProps(r, expectedIdNodes, expectedIdRels));
        
        testCall(db, """
                MATCH p1=(:One)--(:Two), p2=(:Two)--(:Three)
                CALL apoc.graph.filterProperties([{key1: p1, key2: [p1, p2]}], {_all: ['remove']}, {_all: ['remove']})
                YIELD nodes, relationships
                RETURN nodes, relationships""",
                r -> assertNodeAndRelIdProps(r, expectedIdNodes, expectedIdRels));
        
        testCall(db, """
                MATCH p1=(:One)--(:Two), p2=(:Two)--(:Three)
                CALL apoc.graph.filterProperties([{key2: {subKey: [p1, p2]}}], {_all: ['remove']}, {_all: ['remove']})
                YIELD nodes, relationships
                RETURN nodes, relationships""",
                r -> assertNodeAndRelIdProps(r, expectedIdNodes, expectedIdRels));
    }

    private void assertNodeAndRelIdProps(Map<String, Object> r, Set<Object> expectedIdNodes, Set<Object> expectedIdRels) {
        Set<Object> actualIdNodes = ((List<Node>) r.get("nodes"))
                .stream()
                .map(i -> i.getProperty("idNode"))
                .collect(Collectors.toSet());
        assertEquals(expectedIdNodes, actualIdNodes);

        Set<Object> actualIdRels = ((List<Relationship>) r.get("relationships"))
                .stream()
                .map(i -> i.getProperty("idRel"))
                .collect(Collectors.toSet());
        assertEquals(expectedIdRels, actualIdRels);
    }

    @Test
    public void testFilterPropertiesWithEmptyNodeAndRelPropertiesToRemove() {
        testCall(db, """
                MATCH path=(:Person)-[:REL]->(:Movie)
                WITH collect(path) AS paths
                CALL apoc.graph.filterProperties(paths)
                YIELD nodes, relationships
                RETURN nodes, relationships""",
                this::assertEmptyFilter);

        testCall(db, """
                MATCH path=(:Person)-[:REL]->(:Movie)
                WITH apoc.graph.filterProperties(path) as graph
                RETURN graph.nodes AS nodes, graph.relationships AS relationships""",
                this::assertEmptyFilter);
    }

    @Test
    public void testSubstructure() {
        try (Transaction tx = db.beginTx()) {
            Node foo = tx.createNode(Label.label("SubstructureNode"));

            IntStream.range(0, 100).boxed()
                    .forEach(i -> {
                        Node bar = tx.createNode(Label.label("Bar"));
                        foo.createRelationshipTo(bar, RelationshipType.withName("TEST"));
                    });
            
            tx.commit();
        }
        
        testCall(db, "MATCH (n:SubstructureNode) WITH n CALL apoc.graph.substructure(n) YIELD node RETURN node",
                r -> {
                    Node node = (Node) r.get("node");
                    String collect = node.getRelationships(Direction.OUTGOING).stream().map(i -> {
                        Node endNode = i.getEndNode();
                        String collected = Iterables.stream(endNode.getRelationships(Direction.OUTGOING)).map(rel -> rel.getEndNode().toString()).collect(Collectors.joining(", "));
                        return "intermediate label: " + endNode.getLabels() + "\n with nodes: " + collected;
                    }).collect(Collectors.joining("\n"));

                    System.out.println(collect);
                    /* The print result is:
                        intermediate label: [Level1]
                         with nodes: Node[55], Node[49], Node[64], Node[73], Node[54], Node[82], Node[87], Node[88], Node[109], Node[89]
                        intermediate label: [Level10]
                         with nodes: Node[111], Node[48], Node[51], Node[85], Node[43], Node[103], Node[63], Node[46], Node[53], Node[92]
                        intermediate label: [Level9]
                         with nodes: Node[67], Node[45], Node[59], Node[75], Node[52], Node[96], Node[99], Node[91], Node[90], Node[80]
                        intermediate label: [Level8]
                         with nodes: Node[50], Node[60], Node[42], Node[44], Node[86], Node[100], Node[47], Node[83], Node[95], Node[35]
                        intermediate label: [Level7]
                         with nodes: Node[34], Node[33], Node[32], Node[31], Node[30], Node[29], Node[28], Node[27], Node[26], Node[25]
                        intermediate label: [Level6]
                         with nodes: Node[24], Node[23], Node[22], Node[21], Node[20], Node[19], Node[18], Node[17], Node[16], Node[15]
                        intermediate label: [Level5]
                         with nodes: Node[79], Node[70], Node[40], Node[65], Node[36], Node[71], Node[57], Node[81], Node[113], Node[37]
                        intermediate label: [Level4]
                         with nodes: Node[76], Node[84], Node[101], Node[66], Node[93], Node[110], Node[106], Node[112], Node[68], Node[72]
                        intermediate label: [Level3]
                         with nodes: Node[78], Node[74], Node[61], Node[39], Node[107], Node[58], Node[97], Node[94], Node[108], Node[114]
                        intermediate label: [Level2]
                         with nodes: Node[41], Node[98], Node[38], Node[62], Node[104], Node[102], Node[105], Node[56], Node[69], Node[77]
                     */
                });
    }

    private void assertEmptyFilter(Map<String, Object> r) {
        List<Node> nodes = (List<Node>) r.get("nodes");
        nodes.sort(Comparator.comparingLong(i -> (long) i.getProperty("idNode")));
        assertEquals(4, nodes.size());

        Node node = nodes.get(0);
        assertEquals(List.of(Label.label("Person")), node.getLabels());
        assertEquals(propsPerson1, node.getAllProperties());
        node = nodes.get(1);
        assertEquals(List.of(Label.label("Movie")), node.getLabels());
        assertEquals(propsMovie1, node.getAllProperties());
        node = nodes.get(2);
        assertEquals(List.of(Label.label("Person")), node.getLabels());
        assertEquals(propsPerson2, node.getAllProperties());
        node = nodes.get(3);
        assertEquals(List.of(Label.label("Movie")), node.getLabels());
        assertEquals(propsMovie2, node.getAllProperties());

        List<Relationship> relationships = (List<Relationship>) r.get("relationships");
        relationships.sort(Comparator.comparingLong(i -> (long) i.getProperty("idRel")));
        assertEquals(2, relationships.size());

        Relationship rel = relationships.get(0);
        assertEquals(RelationshipType.withName("REL"), rel.getType());
        assertEquals(propsRel1, rel.getAllProperties());
        rel = relationships.get(1);
        assertEquals(RelationshipType.withName("REL"), rel.getType());
        assertEquals(propsRel2, rel.getAllProperties());
    }
}
