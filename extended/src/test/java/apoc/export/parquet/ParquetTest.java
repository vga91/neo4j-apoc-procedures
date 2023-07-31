package apoc.export.parquet;

import apoc.convert.ConvertUtils;
import apoc.export.ImportParquet;
import apoc.graph.Graphs;
import apoc.load.LoadParquet;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.VirtualValues;

import java.io.File;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.LOAD_FROM_FILE_ERROR;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.parquet.ExportParquet.EXPORT_TO_FILE_PARQUET_ERROR;
import static apoc.export.parquet.ParquetUtil.FIELD_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_LABELS;
import static apoc.export.parquet.ParquetUtil.FIELD_SOURCE_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TARGET_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TYPE;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class ParquetTest {

    public static final Map<String, Map<String, String>> MAPPING_ALL = Map.of("mapping",
            Map.of("bffSince", "Duration", "place", "Point",
                    "listDate", "DateArray", "listInt", "LongArray")
    );
    public static final Map<String, Map<String, String>> MAPPING_QUERY = Map.of("mapping",
            Map.of("n", "Node", "r", "Relationship", "o", "Node")
    );
    private static File directory = new File("target/parquet import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    private static void assertFirstUserNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "User");
        assertFirstUserNodeProps(map);
    }

    private static void assertSecondUserNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "User");
        assertSecondUserNodeProps(map);
    }

    private static void assertFirstUserNodeProps(Map<String, Object> props) {
        assertEquals("Adam", props.get("name"));
        assertEquals(42L, props.get("age"));
        assertEquals( true, props.get("male"));
        assertArrayEquals(new String[] { "Sam", "Anna", "Grace" }, (String[]) props.get("kids"));
        Map<String, Double> latitude = Map.of("latitude", 13.1D, "longitude", 33.46789D, "height", 100.0D);
        assertEquals(PointValue.fromMap(VirtualValues.map(latitude.keySet().toArray(new String[0]), latitude.values().stream().map(ValueUtils::of).toArray(AnyValue[]::new))),
                props.get("place"));
        assertEquals(LocalDateTimeValue.parse("2015-05-18T19:32:24.000").asObject(), props.get("born"));
    }

    private static void assertSecondUserNodeProps(Map<String, Object> props) {
        assertEquals( "Jim", props.get("name"));
        assertEquals(42L, props.get("age"));
    }

    private static void assertFirstAnotherNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "Another");
        assertFirstAnotherNodeProps(map);
    }

    private static void assertFirstAnotherNodeProps(Map<String, Object> map) {
        assertEquals(1L, map.get("foo"));
        List<LocalDate> listDate = ConvertUtils.convertToList(map.get("listDate"));
        assertEquals(2, listDate.size());
        assertEquals(LocalDate.of(1999, 1, 1), listDate.get(0));
        assertEquals(LocalDate.of(2000, 1, 1), listDate.get(1));
        assertArrayEquals(new long[] {1L, 2L}, (long[]) map.get("listInt"));
    }

    private static void assertSecondAnotherNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "Another");
        assertSecondAnotherNodeProps(map);
    }

    private static void assertSecondAnotherNodeProps(Map<String, Object> map) {
        assertEquals("Sam", map.get("bar"));
    }

    private static void assertRelationship(Map<String, Object> map) {
        assertTrue(map.get(FIELD_ID) instanceof Long);
        assertTrue(map.get(FIELD_SOURCE_ID) instanceof Long);
        assertTrue(map.get(FIELD_TARGET_ID) instanceof Long);
        assertRelationshipProps(map);
    }

    private static void assertRelationshipProps(Map<String, Object> props) {
        assertEquals(DurationValue.parse("P5M1DT12H"), props.get("bffSince"));
        assertEquals(1993L, props.get("since"));
    }

    private static final Map<String, Object> E_5_PROPS = Map.of(
            "bffSince", DurationValue.parse("P5M1DT12H"),
            "since", 1993L
    );

    @BeforeClass
    public static void beforeClass() {
        TestUtil.registerProcedure(db, ExportParquet.class, LoadParquet.class, ImportParquet.class, Graphs.class, Meta.class);
    }

    @Before
    public void before() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})");
        db.executeTransactionally("CREATE (:Another {foo:1, listDate: [date('1999'), date('2000')], listInt: [1,2]}), (:Another {bar:'Sam'})");

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    private static byte[] extractByteArray(Result result) {
        ResourceIterator<byte[]> value = result.columnAs("value");
        return value.next();
    }

    public static String extractFileName(Result result) {
        return Iterators.single(result.columnAs("file"));
    }

    @Test
    public void testStreamRoundtripParquetQueryAnothertype() {
        List<Object> values = List.of(1L, "", 7.0, DateValue.parse("1999"), LocalDateTimeValue.parse("2023-06-14T08:38:28.193000000"));

        final byte[] byteArray = db.executeTransactionally(
                "CALL apoc.export.parquet.query.stream('UNWIND $values AS item RETURN item', {params: {values: $values}})",
                Map.of("values", values),
                ParquetTest::extractByteArray);

        // then
        final String query = "CALL apoc.load.parquet($byteArray, $config) YIELD value " +
                             "RETURN value";
        testResult(db, query, Map.of("byteArray", byteArray, "config", MAPPING_ALL), result -> {
            List<Map<String, Object>> value = Iterators.asList(result.columnAs("value"));
            Set<Object> actual = value.stream()
                    .flatMap(i -> i.values().stream())
                    .collect(Collectors.toSet());
            System.out.println("actual = " + actual);
        });
    }

    @Test
    public void testFileRoundtripParquetGraph() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.parquet.graph(graph, 'graph_test.parquet') YIELD file " +
                        "RETURN file",
                Map.of(),
                ParquetTest::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                "RETURN value";
        testResult(db, query, Map.of("file", file, "config", MAPPING_ALL),
                ParquetTest::roundtripLoadAllAssertion);
    }

    @Test
    public void testStreamRoundtripParquetAllWithImportExportConfsDisabled() {
        // disable both export and import configs
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, false);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, false);

        // should work regardless of the previous config
        testStreamRoundtripAllCommon();
    }

    @Test
    public void testExportFileWithConfigDisabled() {
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, false);

        assertFails("CALL apoc.export.parquet.all('ignore.parquet')", EXPORT_TO_FILE_PARQUET_ERROR);
    }

    @Test
    public void testLoadImportFiletWithConfigDisabled() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, false);

        assertFails("CALL apoc.load.parquet('ignore.parquet')", LOAD_FROM_FILE_ERROR);
        assertFails("CALL apoc.import.parquet('ignore.parquet')", LOAD_FROM_FILE_ERROR);
    }

    private static void assertFails(String call, String expectedErrMsg) {
        try {
            testCall(db, call, r -> fail("Should fail due to " + expectedErrMsg));
        } catch (Exception e) {
            String actualErrMsg = e.getMessage();
            assertTrue("Actual err. message is: " + actualErrMsg, actualErrMsg.contains(expectedErrMsg));
        }
    }

    @Test
    public void testStreamRoundtripParquetAll() {
        testStreamRoundtripAllCommon();
    }

    private static void testStreamRoundtripAllCommon() {
        // given - when
        final byte[] bytes = db.executeTransactionally("CALL apoc.export.parquet.all.stream()",
                Map.of(),
                ParquetTest::extractByteArray);

        // then
        final String query = "CALL apoc.load.parquet($bytes, $config) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("bytes", bytes, "config", MAPPING_ALL),
                ParquetTest::roundtripLoadAllAssertion);
    }

    @Test
    public void testStreamRoundtripWithAnotherpleBatches() {
        final List<byte[]> bytes = db.executeTransactionally("CALL apoc.export.parquet.all.stream({batchSize:1})",
                Map.of(),
                r -> Iterators.asList(r.columnAs("value")));

        // then
        final String query = "UNWIND $bytes AS byte CALL apoc.load.parquet(byte, $config) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("bytes", bytes, "config", MAPPING_ALL),
                ParquetTest::roundtripLoadAllAssertion);
    }

    @Test
    public void testRoundtripWithMultipleBatches() {
        final String fileName = db.executeTransactionally("CALL apoc.export.parquet.all('test.parquet', {batchSize:1})",
                Map.of(),
                ParquetTest::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("file", fileName, "config", MAPPING_ALL),
                ParquetTest::roundtripLoadAllAssertion);
    }

    public static void roundtripLoadAllAssertion(Result result) {
        ResourceIterator<Map<String, Object>> value = result.columnAs("value");
        Map<String, Object> actual = value.next();
        assertFirstUserNode(actual);
        actual = value.next();
        assertSecondUserNode(actual);
        actual = value.next();
        assertFirstAnotherNode(actual);
        actual = value.next();
        assertSecondAnotherNode(actual);
        actual = value.next();
        assertRelationship(actual);
        assertFalse(value.hasNext());
    }

    @Test
    public void testFileRoundtripImportParquetAll() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.export.parquet.all('test_all.parquet') YIELD file",
                Map.of(),
                ParquetTest::extractFileName);

        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // then
        final String query = "CALL apoc.import.parquet($file, $config)";

        testCall(db, query, Map.of("file", file, "config", MAPPING_ALL),
                r -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                });

        testCall(db, "MATCH (start:User)-[rel:KNOWS]->(end:User) RETURN start, rel, end", r -> {
            Node start = (Node) r.get("start");
            assertFirstUserNodeProps(start.getAllProperties());
            Node end = (Node) r.get("end");
            assertSecondUserNodeProps(end.getAllProperties());
            Relationship rel = (Relationship) r.get("rel");
            assertRelationshipProps(rel.getAllProperties());
        });

        testResult(db, "MATCH (m:Another) RETURN m", r -> {
            ResourceIterator<Node> m = r.columnAs("m");
            Node node = m.next();
            assertFirstAnotherNodeProps(node.getAllProperties());
            node = m.next();
            assertSecondAnotherNodeProps(node.getAllProperties());
            assertFalse(r.hasNext());
        });
    }

    @Test
    public void testFileRoundtripParquetAll() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.export.parquet.all('test_all.parquet') YIELD file",
                Map.of(),
                ParquetTest::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                "RETURN value";

        testResult(db, query, Map.of("file", file,  "config", MAPPING_ALL),
                ParquetTest::roundtripLoadAllAssertion);
    }

    @Test
    public void testReturnNodeAndRelStream() {
        testReturnNodeAndRelCommon(() -> db.executeTransactionally("CALL apoc.export.parquet.query.stream('MATCH (n:ParquetNode)-[r:BAR]->(o:Other) RETURN n,r,o ORDER BY n.idStart') ",
                Map.of(),
                ParquetTest::extractByteArray));
    }

    @Test
    public void testReturnNodeAndRel() {
        testReturnNodeAndRelCommon(() -> db.executeTransactionally(
                "CALL apoc.export.parquet.query('MATCH (n:ParquetNode)-[r:BAR]->(o:Other) RETURN n,r,o ORDER BY n.idStart', " +
                "'volume_test.parquet', $config) YIELD file ",
                Map.of("config", MAPPING_QUERY),
                ParquetTest::extractFileName));
    }

    public static void testReturnNodeAndRelCommon(Supplier<Object> supplier) {
        db.executeTransactionally("CREATE (:ParquetNode{idStart:1})-[:BAR {idRel: 'one'}]->(:Other {idOther: datetime('2020')})");
        db.executeTransactionally("CREATE (:ParquetNode{idStart:2})-[:BAR {idRel: 'two'}]->(:Other {idOther: datetime('1999')})");

        Object fileOrBinary = supplier.get();

        // then
        final String query = "CALL apoc.load.parquet($file, $config)";

        testResult(db, query, Map.of("file", fileOrBinary, "config", MAPPING_QUERY),
                res -> {
                    ResourceIterator<Map<String, Object>> value = res.columnAs("value");
                    Map<String, Object> row = value.next();
                    Map<String, Object> relTwo = (Map<String, Object>) row.get("r");
                    assertBarRel("one", relTwo);

                    Map<String, Object> startTwo = (Map<String, Object>) row.get("n");
                    assertNodeAndLabel(startTwo, "ParquetNode");
                    assertEquals(1L, startTwo.get("idStart"));

                    Map<String, Object> endTwo = (Map<String, Object>) row.get("o");
                    assertNodeAndLabel(endTwo, "Other");
                    assertEquals("2020-01-01T00:00Z", endTwo.get("idOther"));

                    row = value.next();
                    Map<String, Object> rel = (Map<String, Object>) row.get("r");
                    assertBarRel("two", rel);

                    Map<String, Object> start = (Map<String, Object>) row.get("n");
                    assertNodeAndLabel(start, "ParquetNode");
                    assertEquals(2L, start.get("idStart"));

                    Map<String, Object> end = (Map<String, Object>) row.get("o");
                    assertNodeAndLabel(end, "Other");
                    assertEquals("1999-01-01T00:00Z", end.get("idOther"));

                    assertFalse(res.hasNext());
                });

        db.executeTransactionally("MATCH (n:ParquetNode), (o:Other) DETACH DELETE n, o");
    }

    private static void assertNodeAndLabel(Map<String, Object> startTwo, String label) {
        assertTrue(startTwo.get(FIELD_ID) instanceof Long);
        assertEquals(ValueUtils.of(List.of(label)), ValueUtils.of(startTwo.get(FIELD_LABELS)));
    }

    private static void assertBarRel(String one, Map<String, Object> relTwo) {
        assertEquals(one, relTwo.get("idRel"));
        assertEquals("BAR", relTwo.get(FIELD_TYPE));
        assertTrue(relTwo.get(FIELD_ID) instanceof Long);
        assertTrue(relTwo.get(FIELD_SOURCE_ID) instanceof Long);
        assertTrue(relTwo.get(FIELD_TARGET_ID) instanceof Long);
    }

    @Test
    public void testFileVolumeParquetAll() {
        // given - when
        db.executeTransactionally("UNWIND range(0, 10000 - 1) AS id CREATE (:ParquetNode{id:id})");

        String file = db.executeTransactionally("CALL apoc.export.parquet.query('MATCH (n:ParquetNode) RETURN n.id AS id', 'volume_test.parquet') YIELD file ",
                Map.of(),
                ParquetTest::extractFileName);

        final List<Long> expected = LongStream.range(0, 10000)
                .boxed()
                .collect(Collectors.toList());

        // then
        final String query = "CALL apoc.load.parquet($file, $config) YIELD value " +
                "WITH value.id AS id ORDER BY id RETURN collect(id) as ids";

        testCall(db, query, Map.of("file", file, "config", MAPPING_ALL),
                r -> assertEquals(expected, r.get("ids")));

        db.executeTransactionally("MATCH (n:ParquetNode) DELETE n");
    }


}