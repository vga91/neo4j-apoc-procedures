package apoc.export.arrow;

import apoc.graph.Graphs;
import apoc.load.LoadArrow;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.convert.ConvertJsonTest.assertMaps;
import static apoc.export.arrow.ArrowConstants.ID_FIELD;
import static apoc.export.arrow.ArrowConstants.LABELS_FIELD;
import static apoc.export.arrow.ArrowConstants.STREAM_NODE_PREFIX;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.count;

public class ArrowStreamTest {

    private static File directory = new File("target/import");
    private static String DELETE_ALL = "MATCH (n) DETACH DELETE n";

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, ExportStreamArrow.class, ImportStreamArrow.class, LoadArrow.class, Graphs.class);
    }

    @After
    public void tearDown() {
        db.executeTransactionally(DELETE_ALL);
    }

    @Test
    public void testExportArrowQuery() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place"));

        final String query = "CALL apoc.export.arrow.stream.query('MATCH (n:User) RETURN n') " +
                "YIELD value RETURN value";
        final byte[] result = getBytesFromExportQuery(query);

        db.executeTransactionally(DELETE_ALL);
        assertionGraph(0,0,0,0, Collections.emptySet());

        testResult(db, "CALL apoc.load.arrow.stream($result, null)",
                map("result", result),
                row -> {
                    Map<String, Object> first = row.next();
                    assertEquals(0L, first.get("lineNo"));
                    final List<Object> list = (List<Object>) first.get("list");
                    assertEquals(8, list.size());
                    final Map<String, Object> firstMap = (Map<String, Object>) first.get("map");
                    assertEquals(8, firstMap.size());
                    final String bornPropKey = STREAM_NODE_PREFIX + "born";
                    final String malePropKey = STREAM_NODE_PREFIX + "male";
                    final String agePropKey = STREAM_NODE_PREFIX + "age";
                    final String kidsPropKey = STREAM_NODE_PREFIX + "kids";
                    final String namePropKey = STREAM_NODE_PREFIX + "name";
                    final String placePropKey = STREAM_NODE_PREFIX + "place";
                    assertEquals(Set.of(bornPropKey, namePropKey, ID_FIELD, placePropKey, LABELS_FIELD, agePropKey, malePropKey, kidsPropKey), firstMap.keySet());
                    assertMaps(Map.of("latitude", 33.46789, "longitude", 13.1, "crs", "wgs-84-3d", "height", 100.0), (Map<String, Object>) firstMap.get(placePropKey));
                    assertEquals("2015-07-04T19:32:24", firstMap.get(bornPropKey));
                    assertEquals("Adam", firstMap.get(namePropKey));
                    assertEquals(0L, firstMap.get("_id"));
                    assertEquals("User", firstMap.get("_labels"));
                    assertEquals(42L, firstMap.get(agePropKey));
                    assertEquals(true, firstMap.get(malePropKey));
                    assertEquals(List.of("Sam","Anna","Grace"), firstMap.get(kidsPropKey));

                    Map<String, Object> second = row.next();
                    assertEquals(1L, second.get("lineNo"));
                    final Map<String, Object> secondMap = (Map<String, Object>) second.get("map");
                    final List<Object> secondList = (List<Object>) second.get("list");
                    assertEquals(Set.of("Jim", 1L, "User", 42L), new HashSet<>(secondList));
                    assertMaps(Map.of(namePropKey, "Jim", ID_FIELD, 1L, LABELS_FIELD, "User", agePropKey, 42L), secondMap);

                    Map<String, Object> third = row.next();
                    assertEquals(2L, third.get("lineNo"));
                    final Map<String, Object> thirdMap = (Map<String, Object>) third.get("map");
                    final List<Object> thirdList = (List<Object>) third.get("list");
                    assertEquals(Set.of(2L, "User", 12L), new HashSet<>(thirdList));
                    assertMaps(Map.of(ID_FIELD, 2L, LABELS_FIELD, "User", agePropKey, 12L), thirdMap);

                    assertFalse(row.hasNext());
                });
    }

    @Test
    public void testExportGraphArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place"));

        final String query = "CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.arrow.stream.graph(graph,{quotes: 'none'}) " +
                        "YIELD value RETURN value";
        final byte[] result = getBytesFromExportQuery(query);

        db.executeTransactionally(DELETE_ALL);
        assertionGraph(0,0,0,0, Collections.emptySet());

        testCall(db, "CALL apoc.import.arrow.stream($result, null)",
                map("result", result),
                row -> assertResults(row, "byteArray"));

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place.latitude", "place.longitude", "place.crs"));
    }

    @Test
    public void testExportDataArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:User {foo: 'bar'})");

        assertionGraph(4,1,1,1, Set.of("name", "age", "male", "kids", "born", "place"));

        final String query = "MATCH (n:User) " +
                "MATCH ()-[rels:KNOWS]->() " +
                "WITH collect(n) as node, collect(rels) as rels "+
                "CALL apoc.export.arrow.stream.data(node, rels, null) " +
                "YIELD value RETURN value";
        final byte[] result = getBytesFromExportQuery(query);

        db.executeTransactionally(DELETE_ALL);
        assertionGraph(0,0,0,0, Collections.emptySet());

        testCall(db, "CALL apoc.import.arrow.stream($result, null)",
                map("result", result),
                row -> assertResults(row, "byteArray"));

        assertionGraph(4,1,1,1, Set.of("name", "age", "male", "kids", "born", "place.latitude", "place.longitude", "place.crs"));
    }

    @Test
    public void testExportAllArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place"));

        final String query = "CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                "CALL apoc.export.arrow.stream.all() " +
                "YIELD value RETURN value";
        final byte[] result = getBytesFromExportQuery(query);

        db.executeTransactionally(DELETE_ALL);
        assertionGraph(0,0,0,0, Collections.emptySet());

        testCall(db, "CALL apoc.import.arrow.stream($result, null)",
                map("result", result),
                row -> assertResults(row, "byteArray"));

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place.latitude", "place.longitude", "place.crs"));
    }

    private void assertionGraph(int nodes, int rels, int labels, int types, Set<String> propKeysFirstNode) {
        try (Transaction tx = db.beginTx()) {
            assertEquals(nodes, count(tx.getAllNodes()));
            assertEquals(rels, count(tx.getAllRelationships()));
            assertEquals(labels, count(tx.getAllLabelsInUse()));
            assertEquals(types, count(tx.getAllRelationshipTypesInUse()));
            if (nodes > 0) {
                final Set<String> allProperties = tx.getAllNodes().iterator().next().getAllProperties().keySet();
                assertEquals(propKeysFirstNode, allProperties);
            }
            tx.commit();
        }
    }

    private byte[] getBytesFromExportQuery(String query) {

        try(Transaction tx = db.beginTx()) {
            byte[] result = tx.execute(query).<byte[]>columnAs("value").next();
            tx.commit();
            return result;
        }
    }

    protected static void assertResults(Map<String, Object> r, String source) {
        assertEquals(4L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(12L, r.get("properties"));
        assertEquals(source, r.get("source"));
        assertEquals("arrow", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }
}
