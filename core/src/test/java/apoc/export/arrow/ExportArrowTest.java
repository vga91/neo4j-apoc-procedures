package apoc.export.arrow;

import apoc.ApocSettings;
import apoc.export.json.ExportJson;
import apoc.graph.Graphs;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExportArrowTest {

    private static File directory = new File("target/import");
    private static File directoryExpected = new File("../docs/asciidoc/modules/ROOT/examples/data/exportJSON");

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
        String filename = "withQuery.arrow";
        TestUtil.testCall(db, "CALL apoc.export.arrow.query('MATCH p=()-[r]->() RETURN r',$file,{})",
                map("file", filename),
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
        String fileName = "graphUNO.arrow";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.arrow.graph(graph, $file,{quotes: 'none'}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName),
                (r) -> {});
        // todo - assertion
//        assertEquals(EXPECTED_NONE_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportDataArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
        String filename = "dataUNO.arrow";
//        String s3Url = getS3Url(filename);

        TestUtil.testCall(db, "MATCH (nod:User) " +
                        "MATCH ()-[reels:KNOWS]->() " +
                        "WITH collect(nod) as node, collect(reels) as rels "+
                        "CALL apoc.export.arrow.data(node, rels, $file, null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("file", filename),
                (r) -> {
//                    assertEquals(s3Url, r.get("file"));
//                    assertEquals("json", r.get("format"));
                });

        // TODO TODO TODO TODO TODO - FARE UNA COSA DEL GENERE COME EXPORTJSONS3TEST
//        verifyUpload(s3Url, filename);
    }

    @Test
    public void testExportAllArrow() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
        String filename = "ugone.arrow";
        TestUtil.testCall(db, "CALL apoc.export.arrow.all($file,null)",
                map("file", filename),
                r -> assertResults(filename, r, "database")
//                (r) -> {
//                    assertResults(filename, r, "database");
//                }
        );
//        assertFileEquals(filename);
    }

    @Test
    public void testExportAllArrowWithoutRels() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})");
        String filename = "withAllMaybeNOREL.arrow";
        TestUtil.testCall(db, "CALL apoc.export.arrow.all($file,null)",
                map("file", filename),
                r -> {}
//                (r) -> {
//                    assertResults(filename, r, "database");
//                }
        );

        // todo - asserire che non ci sia il file edges_
//        assertFileEquals(filename);
    }


    // todo - NON FUNGE....
    @Test
    public void testExportAllArrowBatch() throws Exception {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015185T19:32:24'), place:point({latitude: 13.1, longitude: 33.46789})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42}),(c:User {age:12}),(d:Another {foo: 'bar'})");
        String filename = "withAllMaybeASDASDBatch.arrow";
        TestUtil.testCall(db, "CALL apoc.export.arrow.all($file,{batchSize: 21, streamStatements: true})",
                map("file", filename),
                r -> assertResults(filename, r, "database")
//                (r) -> {
//                    assertResults(filename, r, "database");
//                }
        );
//        assertFileEquals(filename);
    }


    // todo - nella documentazione dico che devono essere passati tutti e due
    private void assertResults(String filename, Map<String, Object> r, final String source) {
        assertEquals(4L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(12L, r.get("properties"));
        assertEquals(source + ": nodes(4), rels(1)", r.get("source"));
        assertEquals(filename, r.get("file"));
        assertEquals("arrow", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    private void assertFileEquals(String fileName) {
        String actualText = TestUtil.readFileToString(new File(directory, fileName));
        assertStreamEquals(fileName, actualText);
    }

    private void assertStreamEquals(String fileName, String actualText) {
        // TODO - necessario?
//        String expectedText = TestUtil.readFileToString(new File(directoryExpected, fileName));
//        String[] actualArray = actualText.split("\n");
//        String[] expectArray = expectedText.split("\n");
//        assertEquals(expectArray.length, actualArray.length);
//        for (int i = 0; i < actualArray.length; i++) {
//            assertEquals(JsonUtil.parse(expectArray[i],null, Object.class), JsonUtil.parse(actualArray[i],null, Object.class));
//        }
    }
}
