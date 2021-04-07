package apoc.export.arrow;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.count;

public class ArrowStreamTest {

    private static File directory = new File("target/import");
    private static String DELETE_ALL = "MATCH (n) DETACH DELETE n";

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        TestUtil.registerProcedure(db, ExportStreamArrow.class, ImportStreamArrow.class, Graphs.class);
    }

    @Before
    public void setup() {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
    }

    @After
    public void clearDb() {
        db.executeTransactionally(DELETE_ALL);
    }

    @Test
    public void testExportArrowQuery() throws Exception {
        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place"));

        final String query = "CALL apoc.export.arrow.stream.query('MATCH (n:User) RETURN n') " +
                "YIELD value RETURN value";
        final byte[] result = getBytesFromExportQuery(query);

        db.executeTransactionally(DELETE_ALL);
        assertionGraph(0,0,0,0, Collections.emptySet());

        testCall(db, "CALL apoc.import.arrow.stream($result, null)",
                map("result", result),
                row -> assertResults(row, "byteArray", 3L, 0L, 9L));

        assertionGraph(3,0,1,0, Set.of("name", "age", "male", "kids", "born", "place.latitude", "place.longitude", "place.crs", "place.height"));
    }

    @Test
    public void testExportArrowGraph() throws Exception {
        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place"));

        final String query = "CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                "CALL apoc.export.arrow.stream.graph(graph,{quotes: 'none'}) " +
                "YIELD value RETURN value";
        final byte[] result = getBytesFromExportQuery(query);

        db.executeTransactionally(DELETE_ALL);
        assertionGraph(0,0,0,0, Collections.emptySet());

        testCall(db, "CALL apoc.import.arrow.stream($result, null)",
                map("result", result),
                row -> assertResults(row, "byteArray", 4L, 1L, 12L));

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place.latitude", "place.longitude", "place.crs", "place.height"));
    }

    @Test
    public void testExportArrowData() throws Exception {
        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place"));

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
                row -> assertResults(row, "byteArray", 3L, 1L, 11L));

        assertionGraph(3,1,1,1, Set.of("name", "age", "male", "kids", "born", "place.latitude", "place.longitude", "place.crs", "place.height"));
    }

    @Test
    public void testExportArrowAll() throws Exception {
        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place"));

        final String query = "CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                "CALL apoc.export.arrow.stream.all() " +
                "YIELD value RETURN value";
        final byte[] result = getBytesFromExportQuery(query);

        db.executeTransactionally(DELETE_ALL);
        assertionGraph(0,0,0,0, Collections.emptySet());

        testCall(db, "CALL apoc.import.arrow.stream($result, null)",
                map("result", result),
                row -> assertResults(row, "byteArray", 4L, 1L, 12L));

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place.latitude", "place.longitude", "place.crs", "place.height"));
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

    protected static void assertResults(Map<String, Object> r, String source, long nodes, long relationships, long properties) {
        assertEquals(nodes, r.get("nodes"));
        assertEquals(relationships, r.get("relationships"));
        assertEquals(properties, r.get("properties"));
        assertEquals(source, r.get("source"));
        assertEquals("arrow", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }
}