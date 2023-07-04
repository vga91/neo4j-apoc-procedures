package apoc.export.parquet;

//import apoc.export.ImportParquet;
import apoc.graph.Graphs;
import apoc.load.LoadParquet;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import blue.strategic.parquet.Dehydrator;
import blue.strategic.parquet.Hydrator;
import blue.strategic.parquet.HydratorSupplier;
import blue.strategic.parquet.ParquetReader;
import blue.strategic.parquet.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
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
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.parquet.ParquetUtil.FIELD_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_LABELS;
import static apoc.export.parquet.ParquetUtil.FIELD_SOURCE_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TARGET_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TYPE;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

public class ParquetTest {

    private static File directory = new File("target/parquet import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());



    @Test
    public void writes_and_reads_parquet() throws IOException {
        File parquet = new File("foo.parquet");

        MessageType schema = new MessageType("foo",
                Types.required(INT64).named("id"),
                Types.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("email"),
                Types.requiredList().requiredElement(BINARY).as(LogicalTypeAnnotation.stringType()).named("list")
        );

        Dehydrator<Object[]> dehydrator = (record, valueWriter) -> {
            valueWriter.write("id", record[0]);
            valueWriter.write("email", record[1]);
            valueWriter.write("list", record[2].toString());
        };

        Hydrator<Map<String, Object>, Map<String, Object>> hydrator = new Hydrator<>() {
            @Override
            public Map<String, Object> start() {
                return new HashMap<>();
            }

            @Override
            public HashMap<String, Object> add(Map<String, Object> target, String heading, Object value) {
                HashMap<String, Object> r = new HashMap<>(target);
                r.put(heading, value);
                return r;
            }

            @Override
            public Map<String, Object> finish(Map<String, Object> target) {
                return target;
            }
        };

        try(ParquetWriter<Object[]> writer = ParquetWriter.writeFile(schema, parquet, dehydrator)) {
            writer.write(new Object[]{1L, "hello1", List.of("1", "2")});
            writer.write(new Object[]{2L, "hello2", List.of("1", "23")});
        }

        try (Stream<Map<String, Object>> s = ParquetReader.streamContent(parquet, HydratorSupplier.constantly(hydrator))) {
            List<Map<String, Object>> result = s.collect(Collectors.toList());

            //noinspection unchecked
            assertThat(result, hasItems(
                    Map.of("id", 1L, "email", "hello1"),
                    Map.of("id", 2L, "email", "hello2")));
        }

        try (Stream<Map<String, Object>> s = ParquetReader.streamContent(parquet, HydratorSupplier.constantly(hydrator), Collections.singleton("id"))) {
            List<Map<String, Object>> result = s.collect(Collectors.toList());

            //noinspection unchecked
            assertThat(result, hasItems(
                    Map.of("id", 1L),
                    Map.of("id", 2L)));
        }
    }


    private void assertFirstUserNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "User");
        assertFirstUserNodeProps(map);
    }

    private void assertSecondUserNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "User");
        assertSecondUserNodeProps(map);
    }

    private void assertFirstUserNodeProps(Map<String, Object> props) {
        assertEquals("Adam", props.get("name"));
        assertEquals(42L, props.get("age"));
        assertEquals( true, props.get("male"));
        assertArrayEquals(new String[] { "Sam", "Anna", "Grace" }, (String[]) props.get("kids"));
        Map<String, Double> latitude = Map.of("latitude", 13.1D, "longitude", 33.46789D, "height", 100.0D);
        assertEquals(PointValue.fromMap(VirtualValues.map(latitude.keySet().toArray(new String[0]), latitude.values().stream().map(ValueUtils::of).toArray(AnyValue[]::new))),
                props.get("place"));
        assertEquals(LocalDateTimeValue.parse("2015-05-18T19:32:24.000").asObject(), props.get("born"));
    }

    private void assertSecondUserNodeProps(Map<String, Object> props) {
        assertEquals( "Jim", props.get("name"));
        assertEquals(42L, props.get("age"));
    }

    private void assertFirstMultiNodeProps(Map<String, Object> map) {
        assertNodeAndLabel(map, "Multi");
        assertEquals(1L, map.get("name"));
    }

    private void assertSecondMultiNodeProps(Map<String, Object> map) {
        assertNodeAndLabel(map, "Multi");
        assertEquals("Sam", map.get("name"));
    }

    private void assertRelationship(Map<String, Object> map) {
        assertTrue(map.get(FIELD_ID) instanceof Long);
        assertTrue(map.get(FIELD_SOURCE_ID) instanceof Long);
        assertTrue(map.get(FIELD_TARGET_ID) instanceof Long);
        assertRelationshipProps(map);
    }

    private void assertRelationshipProps(Map<String, Object> props) {
        assertEquals(DurationValue.parse("P5M1DT12H"), props.get("bffSince"));
        assertEquals(1993L, props.get("since"));
    }

    private static final Map<String, Object> E_5_PROPS = Map.of(
            "bffSince", DurationValue.parse("P5M1DT12H"),
            "since", 1993L
    );

    @BeforeClass
    public static void beforeClass() {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})");
        db.executeTransactionally("CREATE (:Multi {name:1}), (:Multi {name:'Sam'})");
        TestUtil.registerProcedure(db, /*ExportParquet.class, */LoadParquet.class,/* ImportParquet.class, */Graphs.class, Meta.class);
    }

    @Before
    public void before() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    private byte[] extractByteArray(Result result) {
        ResourceIterator<byte[]> value = result.columnAs("value");
        return value.next();
    }

    private String extractFileName(Result result) {
        return Iterators.single(result.columnAs("file"));
    }

    @Test
    public void testStreamRoundtripParquetQueryMultitype() {
        List<Object> values = List.of(1L, "", 7.0, DateValue.parse("1999"), LocalDateTimeValue.parse("2023-06-14T08:38:28.193000000"));

        final byte[] byteArray = db.executeTransactionally(
                "CALL apoc.export.parquet.query.stream('UNWIND $values AS item RETURN item', {params: {values: $values}})",
                Map.of("values", values),
                this::extractByteArray);

        // then
        final String query = "CALL apoc.load.parquet($byteArray) YIELD value " +
                             "RETURN value";
        testResult(db, query, Map.of("byteArray", byteArray), result -> {
            List<Map<String, Object>> value = Iterators.asList(result.columnAs("value"));
            Set<Object> actual = value.stream()
                    .flatMap(i -> i.values().stream())
                    .collect(Collectors.toSet());
            System.out.println("actual = " + actual);
        });
    }

    private List<Map<String, Object>> getActual(Result result) {
        return result.stream()
                .map(m -> (Map<String, Object>) m.get("value"))
                .collect(Collectors.toList());
    }

    @Test
    public void testFileRoundtripParquetGraph() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.parquet.graph(graph, 'graph_test.parquet') YIELD file " +
                        "RETURN file",
                Map.of(),
                this::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file) YIELD value " +
                "RETURN value";
        testResult(db, query, Map.of("file", file),
                this::roundtripLoadAllAssertion);
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
    public void testStreamRoundtripParquetAll() {
        testStreamRoundtripAllCommon();
    }

    private void testStreamRoundtripAllCommon() {
        // given - when
        final byte[] bytes = db.executeTransactionally("CALL apoc.export.parquet.all.stream()",
                Map.of(),
                this::extractByteArray);

        // then
        final String query = "CALL apoc.load.parquet($bytes) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("bytes", bytes),
                this::roundtripLoadAllAssertion);
    }

    @Test
    public void testStreamRoundtripWithMultipleBatches() {
        final List<byte[]> bytes = db.executeTransactionally("CALL apoc.export.parquet.all.stream({batchSize:1})",
                Map.of(),
                r -> Iterators.asList(r.columnAs("value")));

        // then
        final String query = "UNWIND $bytes AS byte CALL apoc.load.parquet(byte) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("bytes", bytes),
                this::roundtripLoadAllAssertion);
    }

    @Test
    public void testRoundtripWithMultipleBatches() {
//        final String fileName = "all_test.parquet";
        final String fileName = "yellow_tripdata_2023-03.parquet";
//        db.executeTransactionally("CALL apoc.export.parquet.all('test.parquet', {batchSize:1})",
//                Map.of(),
//                this::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("file", fileName),
                this::roundtripLoadAllAssertion);
    }

    @Test
    public void testRoundtripWithMultipleBatches2() {
//        final String fileName = "all_test.parquet";
        final String fileName = "graph_test-avro.parquet";
//        db.executeTransactionally("CALL apoc.export.parquet.all('test.parquet', {batchSize:1})",
//                Map.of(),
//                this::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("file", fileName),
                this::roundtripLoadAllAssertion);
    }

    private void roundtripLoadAllAssertion(Result result) {
        ResourceIterator<Map<String, Object>> value = result.columnAs("value");
        Map<String, Object> actual = value.next();
        assertFirstUserNode(actual);
        actual = value.next();
        assertSecondUserNode(actual);
        actual = value.next();
        assertFirstMultiNodeProps(actual);
        actual = value.next();
        assertSecondMultiNodeProps(actual);
        actual = value.next();
        assertRelationship(actual);
        assertFalse(value.hasNext());
    }

    @Test
    public void testFileRoundtripImportParquetAll() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.export.parquet.all('test_all.parquet') YIELD file",
                Map.of(),
                this::extractFileName);

        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // then
        final String query = "CALL apoc.import.parquet($file)";

        testCall(db, query, Map.of("file", file),
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

        testResult(db, "MATCH (m:Multi) RETURN m", r -> {
            ResourceIterator<Node> m = r.columnAs("m");
            Node node = m.next();
            assertEquals(Map.of("name", 1L), node.getAllProperties());
            node = m.next();
            assertEquals(Map.of("name", "Sam"), node.getAllProperties());
            assertFalse(r.hasNext());
        });
    }

    @Test
    public void testFileRoundtripParquetAll() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.export.parquet.all('test_all.parquet') YIELD file",
                Map.of(),
                this::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file) YIELD value " +
                "RETURN value";

        testResult(db, query, Map.of("file", file),
                this::roundtripLoadAllAssertion);
    }

    @Test
    public void testReturnNodeAndRelStream() {
        testReturnNodeAndRelCommon(() -> db.executeTransactionally("CALL apoc.export.parquet.query.stream('MATCH (n:ParquetNode)-[r:BAR]->(o:Other) RETURN n,r,o ORDER BY n.idStart') ",
                Map.of(),
                this::extractByteArray));
    }

    @Test
    public void testReturnNodeAndRel() {
        testReturnNodeAndRelCommon(() -> db.executeTransactionally("CALL apoc.export.parquet.query('MATCH (n:ParquetNode)-[r:BAR]->(o:Other) RETURN n,r,o ORDER BY n.idStart', 'volume_test.parquet') YIELD file ",
                Map.of(),
                this::extractFileName));
    }

    private void testReturnNodeAndRelCommon(Supplier<Object> supplier) {
        db.executeTransactionally("CREATE (:ParquetNode{idStart:1})-[:BAR {idRel: 'one'}]->(:Other {idOther: datetime('2020')})");
        db.executeTransactionally("CREATE (:ParquetNode{idStart:2})-[:BAR {idRel: 'two'}]->(:Other {idOther: datetime('1999')})");

        Object fileOrBinary = supplier.get();

        // then
        final String query = "CALL apoc.load.parquet($file)";

        testResult(db, query, Map.of("file", fileOrBinary),
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

    private static void assertNodeAndLabel(Map<String, Object> startTwo, String ParquetNode) {
        assertTrue(startTwo.get(FIELD_ID) instanceof Long);
        assertArrayEquals(new String[]{ParquetNode}, (String[]) startTwo.get(FIELD_LABELS));
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
                this::extractFileName);

        final List<Long> expected = LongStream.range(0, 10000)
                .boxed()
                .collect(Collectors.toList());

        // then
        final String query = "CALL apoc.load.parquet($file) YIELD value " +
                "WITH value.id AS id ORDER BY id RETURN collect(id) as ids";

        testCall(db, query, Map.of("file", file),
                r -> assertEquals(expected, r.get("ids")));

        db.executeTransactionally("MATCH (n:ParquetNode) DELETE n");
    }


}