package apoc.export.parquet;

import apoc.graph.Graphs;
import apoc.load.LoadParquet;
import apoc.meta.Meta;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.AnyValue;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


// TODO
// TODO
// TODO
// TODO
// TODO

public class ParquetTest {

    // todo - exportFullSecurityTest, like export.xls

    // todo - add export.parquet.data(...) tests

    // todo - ApocConfig.checkWriteAllowed(...)


    // todo - MA è VERAMENTE NECESSARIO STREAM: TRUE???? --> DOVREBBE BASTARE NULL COME NOME FILE... --> ah però mette in multi-row...


    // todo - if urlOrBinary instanceof String --> url altrimenti stream



    private static File directory = new File("target/parquet import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    private static final HashMap<String, Object> MT_1 = new HashMap<>() {{
        put(FIELD_ID, 2L);
        put("name", 1L);
        put(FIELD_LABELS, List.of("Multi"));
    }};

    private static final HashMap<String, Object> MT_2 = new HashMap<>() {{
        put(FIELD_ID, 3L);
        put("name", "Jim");
        put(FIELD_LABELS, List.of("Multi"));
    }};

    private static final HashMap<String, Object> E_1 = new HashMap<>() {{
        put("name", "Adam");
        put("bffSince", null);
        put(FIELD_SOURCE_ID, null);
        put(FIELD_ID, 0L);
        put("age", 42L);
        put(FIELD_LABELS, List.of("User"));
        put("male", true);
        put(FIELD_TYPE, null);
        put("kids", List.of("Sam", "Anna", "Grace"));
        Map<String, Double> latitude = Map.of("latitude", 13.1D, "longitude", 33.46789D, "height", 100.0D);
        put("place", PointValue.fromMap(VirtualValues.map(latitude.keySet().toArray(new String[0]), latitude.values().stream().map(ValueUtils::of).toArray(AnyValue[]::new))));
        put(FIELD_TARGET_ID, null);
        put("since", null);
        put("born", LocalDateTimeValue.parse("2015-05-18T19:32:24.000").asObject());//.atOffset(ZoneOffset.UTC).toZonedDateTime());
    }};
    private static final HashMap<String, Object> E_2 = new HashMap<>() {{
        put("name", "Jim");
        put("bffSince", null);
        put(FIELD_SOURCE_ID, null);
        put(FIELD_ID, 1L);
        put("age", 42L);
        put(FIELD_LABELS, List.of("User"));
        put("male", null);
        put(FIELD_TYPE, null);
        put("kids", null);
        put("place", null);
        put(FIELD_TARGET_ID, null);
        put("since", null);
        put("born", null);
    }};
    private static final HashMap<String, Object> E_3 = new HashMap<>() {{
        put("name", null);
        put("bffSince", DurationValue.parse("P5M1DT12H"));
        put(FIELD_SOURCE_ID, 0L);
        put(FIELD_ID, 0L);
        put("age", null);
        put(FIELD_LABELS, null);
        put("male", null);
        put(FIELD_TYPE, "KNOWS");
        put("kids", null);
        put("place", null);
        put(FIELD_TARGET_ID, 1L);
        put("since", 1993L);
        put("born", null);
    }};

    public static final List<Map<String, Object>> EXPECTED = List.of(
            E_1,
            E_2,
            E_3
    );

    @BeforeClass
    public static void beforeClass() {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})");
        db.executeTransactionally("CREATE (:Multi {name:1}), (:Multi {name:'Jim'})");
        TestUtil.registerProcedure(db, ExportParquet.class, LoadParquet.class, Graphs.class, Meta.class);
    }

    @Before
    public void before() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    private byte[] extractByteArray(Result result) {
        return result.<byte[]>columnAs("byteArray").next();
    }

    private String extractFileName(Result result) {
        return result.<String>columnAs("file").next();
    }

    // todo - ??
    private <T> T readValue(String json, Class<T> clazz) {
        if (json == null) return null;
        try {
            return JsonUtil.OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    // todo - test with this: unwind [1, "", 7.0, date()] as u return u


    @Test
    public void testStreamRoundtripParquetQuery() {
        // given - when
        final String returnQuery = "RETURN 1 AS intData," +
                "'a' AS stringData," +
                "true AS boolData," +
                "[1, 2, 3] AS intArray," +
                "[1.1, 2.2, 3.3] AS doubleArray," +
                "[true, false, true] AS boolArray," +
                "[1, '2', true, null] AS mixedArray," +
                "{foo: 'bar'} AS mapData," +
                "localdatetime('2015-05-18T19:32:24') as dateData," +
                "[[0]] AS arrayArray," +
                "1.1 AS doubleData";
        final byte[] byteArray = db.executeTransactionally("CALL apoc.export.parquet.query($query, null, {stream: true}) YIELD value AS byteArray",
                Map.of("query", returnQuery),
                this::extractByteArray);

        // then
        final String query = "CALL apoc.load.parquet($byteArray, null, {stream: true}) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query, Map.of("byteArray", byteArray), result -> {
            final Map<String, Object> row = (Map<String, Object>) result.next().get("value");
            assertEquals(1L, row.get("intData"));
            assertEquals("a", row.get("stringData"));
            assertEquals(Arrays.asList(1L, 2L, 3L), row.get("intArray"));
            assertEquals(Arrays.asList(1.1D, 2.2D, 3.3), row.get("doubleArray"));
            assertEquals(Arrays.asList(true, false, true), row.get("boolArray"));
            assertEquals(Arrays.asList("1", "2", "true", null), row.get("mixedArray"));
            assertEquals("{\"foo\":\"bar\"}", row.get("mapData"));
            assertEquals(LocalDateTime.parse("2015-05-18T19:32:24.000")
                    .atOffset(ZoneOffset.UTC)
                    .toZonedDateTime(), row.get("dateData"));
            assertEquals(Arrays.asList("[0]"), row.get("arrayArray"));
            assertEquals(1.1D, row.get("doubleData"));
            return true;
        });
    }

    @Test
    public void testFileRoundtripParquetQuery() {
        // given - when
        final String returnQuery = "RETURN 1 AS intData," +
                "'a' AS stringData," +
                "true AS boolData," +
                "[1, 2, 3] AS intArray," +
                "[1.1, 2.2, 3.3] AS doubleArray," +
                "[true, false, true] AS boolArray," +
                "[1, '2', true, null] AS mixedArray," +
                "{foo: 'bar'} AS mapData," +
                "localdatetime('2015-05-18T19:32:24') as dateData," +
                "[[0]] AS arrayArray," +
                "1.1 AS doubleData";
        String file = db.executeTransactionally("CALL apoc.export.parquet.query($query, 'query_test.parquet') YIELD file",
                Map.of("query", returnQuery),
                this::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query,
                Map.of("file", file),
                result -> {
                    final Map<String, Object> row = (Map<String, Object>) result.next().get("value");
                    assertEquals(1L, row.get("intData"));
                    assertEquals("a", row.get("stringData"));
                    assertEquals(Arrays.asList(1L, 2L, 3L), row.get("intArray"));
                    assertEquals(Arrays.asList(1.1D, 2.2D, 3.3), row.get("doubleArray"));
                    assertEquals(Arrays.asList(true, false, true), row.get("boolArray"));
                    assertEquals(Arrays.asList("1", "2", "true", null), row.get("mixedArray"));
                    assertEquals("{\"foo\":\"bar\"}", row.get("mapData"));
                    assertEquals(LocalDateTime.parse("2015-05-18T19:32:24.000")
                            .atOffset(ZoneOffset.UTC)
                            .toZonedDateTime(), row.get("dateData"));
                    assertEquals(Arrays.asList("[0]"), row.get("arrayArray"));
                    assertEquals(1.1D, row.get("doubleData"));
                    return true;
                });
    }

    @Test
    public void testStreamRoundtripParquetGraph() {
        // given - when
        final byte[] byteArray = db.executeTransactionally("CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.parquet.graph(graph, null, {stream: true}) YIELD value AS byteArray " +
                        "RETURN byteArray",
                Map.of(),
                this::extractByteArray);

        // then
        final String query = "CALL apoc.load.parquet($byteArray, null, {stream: true}) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query, Map.of("byteArray", byteArray), result -> {
            final List<Map<String, Object>> actual = getActual(result);
            assertEquals(EXPECTED, actual);
            return null;
        });
    }

    private List<Map<String, Object>> getActual(Result result) {
        return result.stream()
                .map(m -> (Map<String, Object>) m.get("value"))
//                .map(m -> {
//                    final Map<String, Object> newMap = new HashMap(m);
//                    newMap.put("place", readValue((String) m.get("place"), Map.class));
//                    return newMap;
//                })
                .collect(Collectors.toList());
    }

    @Test
    public void testFileRoundtripParquetGraph() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.parquet.graph('graph_test.parquet', graph) YIELD file " +
                        "RETURN file",
                Map.of(),
                this::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query, Map.of("file", file), result -> {
            final List<Map<String, Object>> actual = getActual(result);
            assertEquals(EXPECTED, actual);
            return null;
        });
    }

    @Test
    public void testStreamRoundtripParquetAll() {
        testStreamRoundtripAllCommon();
    }

    @Test
    public void testStreamRoundtripParquetAllWithImportExportConfsDisabled() {
        // disable both export and import configs
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, false);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, false);

        // should work regardless of the previous config
        testStreamRoundtripAllCommon();
    }

    private void testStreamRoundtripAllCommon() {
        // given - when
        final byte[] byteArray = db.executeTransactionally("CALL apoc.export.parquet.all(null, {stream: true}) YIELD value AS byteArray ",
                Map.of(),
                this::extractByteArray);

        // then
        final String query = "CALL apoc.load.parquet($byteArray, null, {stream: true}) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query, Map.of("byteArray", byteArray), result -> {
            final List<Map<String, Object>> actual = getActual(result);
            assertEquals(EXPECTED, actual);
            return null;
        });
    }

    @Test
    public void testRoundtripMultiType() {
        // todo - transform in export data and REMOVE detach delete
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // given - when
        String file = db.executeTransactionally("CALL apoc.export.parquet.all('test_all.parquet') YIELD file",
                Map.of(),
                this::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file) YIELD value " +
                             "RETURN value";

        testResult(db, query, Map.of("file", file), result -> {
            ResourceIterator<Map<String, Object>> value = result.columnAs("value");
            Map<String, Object> actual = value.next();
            assertEquals(MT_1, actual);
            actual = value.next();
            assertEquals(MT_2, actual);
            assertFalse(value.hasNext());
        });

        db.executeTransactionally("MATCH (n:Multi) DETACH DELETE n");
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

        testResult(db, query, Map.of("file", file), result -> {
            ResourceIterator<Map<String, Object>> value = result.columnAs("value");
            Map<String, Object> actual = value.next();
            assertEquals(E_1, actual);
            actual = value.next();
            assertEquals(E_2, actual);
            actual = value.next();
            assertEquals(E_3, actual);
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testStreamVolumeParquetAll() {
        // given - when
        db.executeTransactionally("UNWIND range(0, 10000 - 1) AS id CREATE (n:ParquetNode{id:id})");

        final List<byte[]> list = db.executeTransactionally("CALL apoc.export.parquet.query('MATCH (n:ParquetNode) RETURN n.id AS id', null, {stream: true}) YIELD value AS byteArray ",
                Map.of(),
                result -> result.<byte[]>columnAs("byteArray").stream().collect(Collectors.toList()));

        final List<Long> expected = LongStream.range(0, 10000)
                .mapToObj(l -> l)
                .collect(Collectors.toList());

        // then
        final String query = "UNWIND $list AS byteArray " +
                "CALL apoc.load.parquet(byteArray) YIELD value " +
                "RETURN value.id AS id";
        db.executeTransactionally(query, Map.of("list", list), result -> {
            final List<Long> actual = result.stream()
                    .map(m -> (Long) m.get("id"))
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(expected, actual);
            return null;
        });

        db.executeTransactionally("MATCH (n:ParquetNode) DELETE n");
    }

    @Test
    public void testReturnNodeAndRel() {
        db.executeTransactionally("CREATE (:ParquetNode{idStart:1})-[:REL {idRel: 'one'}]->(:Other {idOther: datetime('2020')})");
        db.executeTransactionally("CREATE (:ParquetNode{idStart:2})-[:REL {idRel: 'two'}]->(:Other {idOther: datetime('1999')})");

        String file = db.executeTransactionally("CALL apoc.export.parquet.query('MATCH (n:ParquetNode)-[r:REL]->(o:Other) RETURN n,r,o ORDER BY n.idStart', 'volume_test.parquet') YIELD file ",
                Map.of(),
                this::extractFileName);

        // then
        final String query = "CALL apoc.load.parquet($file)";

        testResult(db, query, Map.of("file", file),
                res -> {
                    ResourceIterator<Map<String, Object>> value = res.columnAs("value");
                    Map<String, Object> row = value.next();
                    Map<String, Object> relTwo = (Map<String, Object>) row.get("r");
                    assertEquals("one", relTwo.get("idRel"));
                    assertEquals("REL", relTwo.get(FIELD_TYPE));
                    assertTrue(relTwo.get(FIELD_ID) instanceof Long);
                    assertTrue(relTwo.get(FIELD_SOURCE_ID) instanceof Long);
                    assertTrue(relTwo.get(FIELD_TARGET_ID) instanceof Long);

                    Map<String, Object> startTwo = (Map<String, Object>) row.get("n");
                    assertTrue(startTwo.get(FIELD_ID) instanceof Long);
                    assertEquals(1L, startTwo.get("idStart"));
                    assertEquals(List.of("ParquetNode"), startTwo.get(FIELD_LABELS));

                    Map<String, Object> endTwo = (Map<String, Object>) row.get("o");
                    assertTrue(endTwo.get(FIELD_ID) instanceof Long);
                    assertEquals("2020-01-01T00:00Z", endTwo.get("idOther"));
                    assertEquals(List.of("Other"), endTwo.get(FIELD_LABELS));

                    row = value.next();
                    Map<String, Object> rel = (Map<String, Object>) row.get("r");
                    assertEquals("two", rel.get("idRel"));
                    assertEquals("REL", rel.get(FIELD_TYPE));
                    assertTrue(rel.get(FIELD_ID) instanceof Long);
                    assertTrue(rel.get(FIELD_SOURCE_ID) instanceof Long);
                    assertTrue(rel.get(FIELD_TARGET_ID) instanceof Long);

                    Map<String, Object> start = (Map<String, Object>) row.get("n");
                    assertTrue(start.get(FIELD_ID) instanceof Long);
                    assertEquals(2L, start.get("idStart"));
                    assertEquals(List.of("ParquetNode"), start.get(FIELD_LABELS));

                    Map<String, Object> end = (Map<String, Object>) row.get("o");
                    assertTrue(end.get(FIELD_ID) instanceof Long);
                    assertEquals("1999-01-01T00:00Z", end.get("idOther"));
                    assertEquals(List.of("Other"), end.get(FIELD_LABELS));

                    assertFalse(res.hasNext());
                });

        db.executeTransactionally("MATCH (n:ParquetNode), (o:Other) DETACH DELETE n, o");
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

    @Test
    public void testValidNonStorableQuery() {
        final List<byte[]> list = db.executeTransactionally("CALL apoc.export.parquet.query($query, null, {stream: true}) YIELD value AS byteArray ",
                Map.of("query", "RETURN [1, true, 2.3, null, { name: 'Dave' }] AS array"),
                result -> result.<byte[]>columnAs("byteArray").stream().collect(Collectors.toList()));

        final List<String> expected = Arrays.asList("1", "true", "2.3", null, "{\"name\":\"Dave\"}");

        // then
        final String query = "UNWIND $list AS byteArray " +
                "CALL apoc.load.parquet(byteArray) YIELD value " +
                "RETURN value.array AS array";
        db.executeTransactionally(query, Map.of("list", list), result -> {
            List<String> actual = result.<List<String>>columnAs("array").next();
            assertEquals(expected, actual);
            return null;
        });

    }


}