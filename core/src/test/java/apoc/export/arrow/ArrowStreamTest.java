package apoc.export.arrow;

import apoc.ApocSettings;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.PointValue;

import java.io.File;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.firstOrNull;

public class ArrowStreamTest {

    private static File directory = new File("target/import");
    private static File directoryExpected = new File("../docs/asciidoc/modules/ROOT/examples/data/exportJSON");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, ExportStreamArrow.class, ImportStreamArrow.class, Graphs.class);
    }

    @After
    public void tearDown() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testExportArrowQuery() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
        // todo - usare la load stream
        TestUtil.testCall(db, "CALL apoc.export.arrow.stream.query('MATCH p=()-[r]->() RETURN r',$file,{})",
                map("file", "filename"),
                r -> {}
//                (r) -> {
//                    assertResults(filename, r, "database");
//                }
        );
//        assertFileEquals(filename);
    }

    @Test
    public void testExportGraphArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place"));


        byte[] result = db.beginTx().execute("CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.arrow.stream.graph(graph,{quotes: 'none'}) " +
                        "YIELD value RETURN value").<byte[]>columnAs("value").next();



        TestUtil.testCall(db, "CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.arrow.stream.graph(graph,{quotes: 'none'}) " +
                        "YIELD value RETURN value" +
                        "",
                (r) -> {

                });



        // todo - assertion
//        assertEquals(EXPECTED_NONE_QUOTES, readFile(fileName));
    }

    public void assertionGraph(int nodes, int rels, int labels, int types, Set<String> propKeysFirstNode) {
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

    @Test
    public void testExportDataArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");


        TestUtil.testCall(db, "MATCH (nod:User) " +
                        "MATCH ()-[reels:KNOWS]->() " +
                        "WITH collect(nod) as node, collect(reels) as rels "+
                        "CALL apoc.export.arrow.stream.data(node, rels, $file, null) " +
                        "YIELD value WITH value CALL apoc.import.arrow.stream(value) YIELD file RETURN file",
                (r) -> {
                    assertEquals("s3Url", r.get("file"));
//                    assertEquals("json", r.get("format"));
                });

//        verifyUpload(s3Url, filename);
    }

    @Test
    public void testExportAllArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place"));

        byte[] result = db.beginTx().execute("CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                "CALL apoc.export.arrow.stream.graph(graph,{quotes: 'none'}) " +
                "YIELD value RETURN value").<byte[]>columnAs("value").next();

        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        assertionGraph(0,0,0,0, Collections.emptySet());

        TestUtil.testCall(db, "CALL apoc.import.arrow.stream($result, null)",
                map("result", result),
                this::assertResults);

        assertionGraph(4,1,2,1, Set.of("name", "age", "male", "kids", "born", "place.latitude", "place.longitude", "place.crs"));
    }


    private void assertResults(Map<String, Object> r) {
        assertEquals(4L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(12L, r.get("properties"));
        assertEquals("file", r.get("source"));
        assertEquals("arrow", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }
}
