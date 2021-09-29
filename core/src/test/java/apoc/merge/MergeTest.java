package apoc.merge;

import apoc.create.Create;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import junit.framework.TestCase;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.PointValue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.merge.Merge.ERROR_NOT_VIRTUAL_NODE;
import static apoc.merge.Merge.ERROR_NOT_VIRTUAL_RELS;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;

public class MergeTest {
    private static final PointValue POINT_VALUE_1 = PointValue.parse("point({x: 3, y: 0})");
    private static final PointValue POINT_VALUE_2 = PointValue.parse("point({x: 0, y: 4, z: 1})");
    private static final List<String> LABELS_V_NODES = List.of("labelOne", "labelTwo", "labelThree");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Merge.class, Create.class);
    }

    @Test
    public void testMergeNode() throws Exception {
        testCall(db, "CALL apoc.merge.node(['Person','Bastard'],{ssid:'123'}, {name:'John'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(true, node.hasLabel(Label.label("Bastard")));
                    assertEquals("John", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                });
    }

    @Test
    public void testMergeNodeWithPreExisting() throws Exception {
        db.executeTransactionally("CREATE (p:Person{ssid:'123', name:'Jim'})");
        testCall(db, "CALL apoc.merge.node(['Person'],{ssid:'123'}, {name:'John'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals("Jim", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                });

        testResult(db, "match (p:Person) return count(*) as c", result ->
                assertEquals(1, (long)(Iterators.single(result.columnAs("c"))))
        );
    }

    @Test
    public void testMergeRelationships() throws Exception {
        db.executeTransactionally("create (:Person{name:'Foo'}), (:Person{name:'Bar'})");

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'KNOWS', {rid:123}, {since:'Thu'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'KNOWS', {rid:123}, {since:'Fri'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });
        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'OTHER', null, null, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("OTHER", rel.getType().name());
                    assertTrue(rel.getAllProperties().isEmpty());
                });
    }

    @Test
    public void testMergeWithEmptyIdentityPropertiesShouldFail() {
        for (String idProps: new String[]{"null", "{}"}) {
            try {
                testCall(db, "CALL apoc.merge.node(['Person']," + idProps +", {name:'John'}) YIELD node RETURN node",
                        row -> assertTrue(row.get("node") instanceof Node));
                fail();
            } catch (QueryExecutionException e) {
                assertTrue(e.getMessage().contains("you need to supply at least one identifying property for a merge"));
            }
        }
    }
    
    @Test
    public void testEscapeIdentityPropertiesWithSpecialCharactersShouldWork() {
        for (String key: new String[]{"normal", "i:d", "i-d", "i d"}) {
            Map<String, Object> identProps = MapUtil.map(key, "value");
            Map<String, Object> params = MapUtil.map("identProps", identProps);

            testCall(db, "CALL apoc.merge.node(['Person'], $identProps) YIELD node RETURN node", params,
                        (row) -> {
                            Node node = (Node) row.get("node");
                            assertTrue(node instanceof Node);
                            assertTrue(node.hasProperty(key));
                            assertEquals("value", node.getProperty(key));
                        });
        }
    }
    
    @Test
    public void testLabelsWithSpecialCharactersShouldWork() {
        for (String label: new String[]{"Label with spaces", ":LabelWithColon", "label-with-dash", "LabelWithUmlautsÄÖÜ"}) {
            Map<String, Object> params = MapUtil.map("label", label);
            testCall(db, "CALL apoc.merge.node([$label],{id:1}, {name:'John'}) YIELD node RETURN node", params,
                    row -> assertTrue(row.get("node") instanceof Node));
        }
    }

    @Test
    public void testRelationshipTypesWithSpecialCharactersShouldWork() {
        for (String relType: new String[]{"Reltype with space", ":ReltypeWithCOlon", "rel-type-with-dash"}) {
            Map<String, Object> params = MapUtil.map("relType", relType);
            testCall(db, "CREATE (a), (b) WITH a,b CALL apoc.merge.relationship(a, $relType, null, null, b) YIELD rel RETURN rel", params,
                    row -> assertTrue(row.get("rel") instanceof Relationship));
        }
    }

    @Test
    public void testMergeVirtualNodesAndRelsFailsIfNotVirtual() {
        try {
            testCall(db, "CREATE (n:Real) WITH COLLECT(n) as list CALL apoc.merge.vNodes(list) YIELD nodes RETURN nodes",
                    r -> fail("Should fails because is a 'real' node"));
        } catch (Exception e) {
            final Throwable except = ExceptionUtils.getRootCause(e);
            assertEquals(ERROR_NOT_VIRTUAL_NODE, except.getMessage());
            TestCase.assertTrue(except instanceof RuntimeException);
        }
        try {
            testCall(db, "CREATE ()-[r:REAL]->() WITH COLLECT(r) as list CALL apoc.merge.vRelationships(list) YIELD relationships RETURN relationships",
                    r -> fail("Should fails because is a 'real' rel"));
        } catch (Exception e) {
            final Throwable except = ExceptionUtils.getRootCause(e);
            assertEquals(ERROR_NOT_VIRTUAL_RELS, except.getMessage());
            TestCase.assertTrue(except instanceof RuntimeException);
        }
    }

    @Test
    public void testMergeVirtualNodes() {
        
        testCall(db, "CALL apoc.create.vNode($labels, $propsFirst  ) yield node with node as nodeOne\n" +
                        "CALL apoc.create.vNode($labels, $propsFirst) YIELD node as nodeTwo WITH [nodeOne, nodeTwo] as nodeList\n" +
                        "CALL apoc.merge.vNodes(nodeList, $conf) YIELD nodes RETURN nodes",
                MapUtil.map("labels", LABELS_V_NODES, "propsFirst", MapUtil.map("a", List.of("b", "c"), "p", List.of(POINT_VALUE_1, POINT_VALUE_2)),
                        "conf", MapUtil.map("onMatch", MapUtil.map("merged", true), "onCreate", MapUtil.map("created", true))),
                this::assertionsMergeCommon);
        
        // same value prop, but different keys
        testCall(db, "CALL apoc.create.vNode($labels, $propsFirst  ) yield node with node as nodeOne\n" +
                        "CALL apoc.create.vNode($labels, $propsSecond) YIELD node as nodeTwo WITH [nodeOne, nodeTwo] as nodeList\n" +
                        "CALL apoc.merge.vNodes(nodeList, $conf) YIELD nodes \n" +
                        "RETURN nodes",
                MapUtil.map("labels", LABELS_V_NODES, "propsFirst", MapUtil.map("a", List.of("b", "c"), "p", List.of(POINT_VALUE_1, POINT_VALUE_2)),
                        "propsSecond", MapUtil.map("a", List.of("b", "c"), "p2", List.of(POINT_VALUE_1, POINT_VALUE_2)),
                        "conf", MapUtil.map("onMatch", MapUtil.map("merged", true), "onCreate", MapUtil.map("created", true))),
                r -> {
                    final List<VirtualNode> nodes = (List<VirtualNode>) r.get("nodes");
                    assertEquals(2, nodes.size());
                    nodes.forEach(virtualNode -> {
                        final List<Label> expectedLabels = LABELS_V_NODES.stream().map(Label::label).collect(Collectors.toList());
                        assertEquals(expectedLabels, Iterables.asList(virtualNode.getLabels()));
                        assertionsNotMergedCommon(virtualNode, false);
                    });
                });
    }

    @Test
    public void testMergeVirtualNodesWithMergeKeysList() {
        final List<String> mergeKeysList = List.of("labelOne", "labelTwo");
        testCall(db, "CALL apoc.create.vNode($labels, $propsFirst  ) yield node with node as nodeOne\n" +
                        "CALL apoc.create.vNode($labelsTwo, $propsFirst) YIELD node as nodeTwo WITH [nodeOne, nodeTwo] as nodeList\n" +
                        "CALL apoc.merge.vNodes(nodeList, $conf) YIELD nodes \n" +
                        "RETURN nodes",
                MapUtil.map("labels", LABELS_V_NODES, "labelsTwo", List.of("labelOne", "labelTwo", "another", "another2"),
                        "propsFirst", MapUtil.map("a", List.of("b", "c"), "p", List.of(POINT_VALUE_1, POINT_VALUE_2)),
                        "conf", MapUtil.map("mergeKeysList", mergeKeysList,
                                "onMatch", MapUtil.map("merged", true), "onCreate", MapUtil.map("created", true))),
                this::assertionsMergeCommon);

        testCall(db, "CALL apoc.create.vNode(['labelOne', 'labelTwo', 'labelThree'], $propsFirst  ) yield node with node as nodeOne\n" +
                        "CALL apoc.create.vNode(['labelOne', 'labelTwo'], $propsFirst) YIELD node as nodeTwo WITH [nodeOne, nodeTwo] as nodeList\n" +
                        "CALL apoc.merge.vNodes(nodeList, $conf) YIELD nodes \n" +
                        "RETURN nodes",
                MapUtil.map("propsFirst", MapUtil.map("a", List.of("b", "c"), "p", List.of(POINT_VALUE_1, POINT_VALUE_2)),
                        "conf", MapUtil.map("onMatch", MapUtil.map("merged", true), "onCreate", MapUtil.map("created", true))),
                r -> {
                    final List<VirtualNode> nodes = (List<VirtualNode>) r.get("nodes");
                    assertEquals(2, nodes.size());
                    nodes.forEach(virtualNode -> {
                        final List<Label> expectedLabels = mergeKeysList.stream().map(Label::label).collect(Collectors.toList());
                        assertTrue(Iterables.asList(virtualNode.getLabels()).containsAll(expectedLabels));
                        assertionsNotMergedCommon(virtualNode, false);
                    });
                });
    }

    @Test
    public void testMergeVirtualRels() {
        testCall(db, "CREATE (nodeFrom:MyNode {id:0}), (nodeTo:MyNode {id:1}) with nodeFrom, nodeTo\n" +
                        "CALL apoc.create.vRelationship(nodeFrom,'AAA',$propsFirst, nodeTo) YIELD rel WITH rel as relOne, nodeFrom, nodeTo\n" +
                        "CALL apoc.create.vRelationship(nodeFrom,'AAA', $propsFirst, nodeTo) YIELD rel as relTwo  WITH [relOne, relTwo] as relList\n" +
                        "CALL apoc.merge.vRelationships(relList, $conf) \n" +
                        "YIELD relationships RETURN relationships",
                MapUtil.map("propsFirst", MapUtil.map("a", List.of("b", "c"), "p", List.of(POINT_VALUE_1, POINT_VALUE_2)),
                        "conf", MapUtil.map("onMatch", MapUtil.map("merged", true), "onCreate", MapUtil.map("created", true))),
                r -> {
                    final List<VirtualRelationship> rels = (List<VirtualRelationship>) r.get("relationships");
                    assertEquals(1, rels.size());
                    final VirtualRelationship virtualRel = rels.get(0);
                    assertionsNotMergedCommon(virtualRel, true);
                });

        // same props, but different rel-types
        testCall(db, "CREATE (nodeFrom:MyNode {id:0}), (nodeTo:MyNode {id:1}) with nodeFrom, nodeTo\n" +
                        "CALL apoc.create.vRelationship(nodeFrom,'AAA',$propsFirst, nodeTo) YIELD rel WITH rel as relOne, nodeFrom, nodeTo\n" +
                        "CALL apoc.create.vRelationship(nodeFrom,'CCC', $propsFirst, nodeTo) YIELD rel as relTwo  WITH [relOne, relTwo] as relList\n" +
                        "CALL apoc.merge.vRelationships(relList, $conf) \n" +
                        "YIELD relationships RETURN relationships",
                MapUtil.map("propsFirst", MapUtil.map("a", List.of("b", "c"), "p", List.of(POINT_VALUE_1, POINT_VALUE_2)),
                        "conf", MapUtil.map("onMatch", MapUtil.map("merged", true), "onCreate", MapUtil.map("created", true))),
                r -> {
                    final List<VirtualRelationship> rels = (List<VirtualRelationship>) r.get("relationships");
                    assertEquals(2, rels.size());
                    rels.forEach(virtualRel -> {
                        assertTrue(List.of("AAA", "CCC").contains(virtualRel.getType().name()));
                        assertionsNotMergedCommon(virtualRel, false);
                    });
                });
        
        // same value prop, but different key
        testCall(db, "CREATE (nodeFrom:MyNode {id:0}), (nodeTo:MyNode {id:1}) with nodeFrom, nodeTo\n" +
                        "CALL apoc.create.vRelationship(nodeFrom,'AAA',$propsFirst, nodeTo) YIELD rel WITH rel as relOne, nodeFrom, nodeTo\n" +
                        "CALL apoc.create.vRelationship(nodeFrom,'AAA', $propsSecond, nodeTo) YIELD rel as relTwo  WITH [relOne, relTwo] as relList\n" +
                        "CALL apoc.merge.vRelationships(relList, $conf) YIELD relationships \n" +
                        "RETURN relationships",
                MapUtil.map("propsFirst", MapUtil.map("a", List.of("b", "c"), "p", List.of(POINT_VALUE_1, POINT_VALUE_2)),
                        "propsSecond", MapUtil.map("a", List.of("b", "c"), "p2", List.of(POINT_VALUE_1, POINT_VALUE_2)),
                        "conf", MapUtil.map("onMatch", MapUtil.map("merged", true), "onCreate", MapUtil.map("created", true))),
                r -> {
                    final List<VirtualRelationship> rels = (List<VirtualRelationship>) r.get("relationships");
                    assertEquals(2, rels.size());
                    rels.forEach(virtualRel -> {
                        assertEquals(RelationshipType.withName("AAA"), virtualRel.getType());
                        assertFalse(virtualRel.hasProperty("merged"));
                        assertEquals(true, virtualRel.getProperty("created"));
                        assertEquals(List.of("b", "c"), virtualRel.getProperty("a"));
                        assertEquals(List.of(POINT_VALUE_1, POINT_VALUE_2), virtualRel.getProperty("p", virtualRel.getProperty("p2")));
                    });
                });
    }


    // MERGE EAGER TESTS


    @Test
    public void testMergeEagerNode() throws Exception {
        testCall(db, "CALL apoc.merge.node.eager(['Person','Bastard'],{ssid:'123'}, {name:'John'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(true, node.hasLabel(Label.label("Bastard")));
                    assertEquals("John", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                });
    }

    @Test
    public void testMergeEagerNodeWithOnCreate() throws Exception {
        testCall(db, "CALL apoc.merge.node.eager(['Person','Bastard'],{ssid:'123'}, {name:'John'},{occupation:'juggler'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(true, node.hasLabel(Label.label("Bastard")));
                    assertEquals("John", node.getProperty("name"));
                    assertEquals("123", node.getProperty("ssid"));
                    assertFalse(node.hasProperty("occupation"));
                });
    }

    @Test
    public void testMergeEagerNodeWithOnMatch() throws Exception {
        db.executeTransactionally("CREATE (p:Person:Bastard {ssid:'123'})");
        testCall(db, "CALL apoc.merge.node.eager(['Person','Bastard'],{ssid:'123'}, {name:'John'}, {occupation:'juggler'}) YIELD node RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(true, node.hasLabel(Label.label("Person")));
                    assertEquals(true, node.hasLabel(Label.label("Bastard")));
                    assertEquals("juggler", node.getProperty("occupation"));
                    assertEquals("123", node.getProperty("ssid"));
                    assertFalse(node.hasProperty("name"));
                });
    }

    @Test
    public void testMergeEagerNodesWithOnMatchCanMergeOnMultipleMatches() throws Exception {
        db.executeTransactionally("UNWIND range(1,5) as index MERGE (:Person:`Bastard Man`{ssid:'123', index:index})");

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute("CALL apoc.merge.node.eager(['Person','Bastard Man'],{ssid:'123'}, {name:'John'}, {occupation:'juggler'}) YIELD node RETURN node");

            for (long index = 1; index <= 5; index++) {
                Node node = (Node) result.next().get("node");
                assertEquals(true, node.hasLabel(Label.label("Person")));
                assertEquals(true, node.hasLabel(Label.label("Bastard Man")));
                assertEquals("123", node.getProperty("ssid"));
                assertEquals(index, node.getProperty("index"));
                assertEquals("juggler", node.getProperty("occupation"));
                assertFalse(node.hasProperty("name"));
            }
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testMergeEagerRelationships() throws Exception {
        db.executeTransactionally("create (:Person{name:'Foo'}), (:Person{name:'Bar'})");

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {since:'Thu'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {since:'Fri'}, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                });
        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship(s, 'OTHER', null, null, e) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("OTHER", rel.getType().name());
                    assertTrue(rel.getAllProperties().isEmpty());
                });
    }

    @Test
    public void testMergeEagerRelationshipsWithOnMatch() throws Exception {
        db.executeTransactionally("create (:Person{name:'Foo'}), (:Person{name:'Bar'})");

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {since:'Thu'}, e,{until:'Saturday'}) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Thu", rel.getProperty("since"));
                    assertFalse(rel.hasProperty("until"));
                });

        testCall(db, "MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {}, e,{since:'Fri'}) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("KNOWS", rel.getType().name());
                    assertEquals(123l, rel.getProperty("rid"));
                    assertEquals("Fri", rel.getProperty("since"));
                });
    }

    @Test
    public void testMergeEagerRelationshipsWithOnMatchCanMergeOnMultipleMatches() throws Exception {
        db.executeTransactionally("CREATE (foo:Person{name:'Foo'}), (bar:Person{name:'Bar'}) WITH foo, bar UNWIND range(1,3) as index CREATE (foo)-[:KNOWS {rid:123}]->(bar)");

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute("MERGE (s:Person{name:'Foo'}) MERGE (e:Person{name:'Bar'}) WITH s,e CALL apoc.merge.relationship.eager(s, 'KNOWS', {rid:123}, {}, e, {since:'Fri'}) YIELD rel RETURN rel");

            for (long index = 1; index <= 3; index++) {
                Relationship rel = (Relationship) result.next().get("rel");
                assertEquals("KNOWS", rel.getType().name());
                assertEquals(123l, rel.getProperty("rid"));
                assertEquals("Fri", rel.getProperty("since"));
            }
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testMergeEagerWithEmptyIdentityPropertiesShouldFail() {
        for (String idProps: new String[]{"null", "{}"}) {
            try {
                testCall(db, "CALL apoc.merge.node(['Person']," + idProps +", {name:'John'}) YIELD node RETURN node",
                        row -> assertTrue(row.get("node") instanceof Node));
                fail();
            } catch (QueryExecutionException e) {
                assertTrue(e.getMessage().contains("you need to supply at least one identifying property for a merge"));
            }
        }
    }

    private void assertionsMergeCommon(Map<String, Object> r) {
        final List<VirtualNode> nodes = (List<VirtualNode>) r.get("nodes");
        assertEquals(1, nodes.size());
        final VirtualNode virtualNode = nodes.get(0);
        final List<Label> expectedLabels = List.of("labelOne", "labelTwo", "labelThree").stream().map(Label::label).collect(Collectors.toList());
        assertEquals(expectedLabels, Iterables.asList(virtualNode.getLabels()));
        assertionsNotMergedCommon(virtualNode, true);
    }

    private <T extends Entity> void assertionsNotMergedCommon(T virtualNode, boolean isMerged) {
        if (isMerged) {
            assertFalse(virtualNode.hasProperty("created"));
            assertEquals(true, virtualNode.getProperty("merged"));
        } else {
            assertFalse(virtualNode.hasProperty("merged"));
            assertEquals(true, virtualNode.getProperty("created"));
        }
        assertEquals(List.of("b", "c"), virtualNode.getProperty("a"));
        assertEquals(List.of(POINT_VALUE_1, POINT_VALUE_2), virtualNode.getProperty("p", virtualNode.getProperty("p2")));
    }
}
