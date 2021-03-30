package apoc.export.arrow;

import apoc.ApocSettings;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Map;

import static apoc.export.arrow.ArrowConstants.EDGE_FILE_PREFIX;
import static apoc.export.arrow.ArrowConstants.NODE_FILE_PREFIX;
import static apoc.export.arrow.ArrowConstants.RESULT_FILE_PREFIX;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExportArrowTest {

    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_export_file_enabled, true);

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, ExportArrow.class, Graphs.class);
    }

    @After
    public void tearDown() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testExportArrowQuery() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
        String fileName = "query.arrow";
        TestUtil.testCall(db, "CALL apoc.export.arrow.query('MATCH p=()-[r]->() RETURN r',$file,{})",
                map("file", fileName),
                r -> {
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals(2L, r.get("properties"));
                    assertEquals("statement: cols(1)", r.get("source"));
                }
        );

        assertFileExists(RESULT_FILE_PREFIX + fileName);
    }

    @Test
    public void testExportGraphArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
        String fileName = "graph.arrow";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.arrow.graph(graph, $file,{quotes: 'none'}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName),
                (r) -> assertResults(fileName, r, "graph")
        );

        assertFileExists(NODE_FILE_PREFIX + fileName);
        assertFileExists(EDGE_FILE_PREFIX + fileName);
    }

    @Test
    public void testExportDataArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
        String fileName = "data.arrow";

        TestUtil.testCall(db, "MATCH (n:User) " +
                        "MATCH ()-[rels:KNOWS]->() " +
                        "WITH collect(n) as node, collect(rels) as rels "+
                        "CALL apoc.export.arrow.data(node, rels, $file, null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("file", fileName),
                (r) ->  {
                    assertEquals(3L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(11L, r.get("properties"));
                }
        );

        assertFileExists(NODE_FILE_PREFIX + fileName);
        assertFileExists(EDGE_FILE_PREFIX + fileName);
    }

    @Test
    public void testExportAllArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789, height: 100})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
        String filename = "all.arrow";
        TestUtil.testCall(db, "CALL apoc.export.arrow.all($file,null)",
                map("file", filename),
                r -> assertResults(filename, r, "database")
        );

        assertFileExists(NODE_FILE_PREFIX + filename);
        assertFileExists(EDGE_FILE_PREFIX + filename);
    }


    @Test
    public void testExportAllArrowBatch() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
        String filename = "stream.arrow";
        TestUtil.testCall(db, "CALL apoc.export.arrow.all($file,{batchSize: 21, streamStatements: true})",
                map("file", filename),
                r -> assertResults(filename, r, "database")
        );

        assertFileExists(NODE_FILE_PREFIX + filename);
        assertFileExists(EDGE_FILE_PREFIX + filename);
    }


    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertEquals(4L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(12L, r.get("properties"));
        assertEquals(source + ": nodes(4), rels(1)", r.get("source"));
        assertEquals(fileName, r.get("file"));
        assertEquals("arrow", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    private void assertFileExists(String fileName) {
        assertTrue(new File(directory, fileName).exists());
    }

}
