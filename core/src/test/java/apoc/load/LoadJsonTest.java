package apoc.load;

import apoc.util.CompressionAlgo;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.*;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.*;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.convert.ConvertJsonTest.EXPECTED_AS_PATH_LIST;
import static apoc.convert.ConvertJsonTest.EXPECTED_PATH;
import static apoc.convert.ConvertJsonTest.EXPECTED_PATH_WITH_NULLS;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class LoadJsonTest {

    private static ClientAndServer mockServer;

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();
//            .withSetting(ApocSettings.apoc_import_file_enabled, true)
//            .withSetting(ApocSettings.apoc_import_file_use__neo4j__config, false);

	@Before public void setUp() throws Exception {
	    apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
	    apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
	    apocConfig().setProperty("apoc.json.zip.url", "https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.zip?raw=true!person.json");
	    apocConfig().setProperty("apoc.json.simpleJson.url", ClassLoader.getSystemResource("map.json").toString());
        TestUtil.registerProcedure(db, LoadJson.class);
    }

    @Test 
    public void testLoadJsonWithMultiTypeMapping() {
        URL url = ClassLoader.getSystemResource("point.json");
        
        final Map<String, Object> mapping = map("pointKey", map("type", "point"), 
                "myLocalDate", map("type", "date", "dateParse", List.of("dd MM yyyy")),
                "myTimeDate", map("type", "datetime", "dateParse", List.of("dd MM yyyy - HH:mm XXX")));
        
        final Map<String, Object> config = map("ignore", List.of("unused"),
                "nullValues", List.of("asNull"),
                "mapping", mapping);
        testCall(db, "CALL apoc.load.json($url, '', $config)",
                map("url", url.toString(), "config", config),
                (row) -> {
                    final Object actual = row.get("value");
                    final Map<String, Object> foo = map("baz", 1L,
                            "myLocalDate", LocalDate.of(1991, 12, 12),
                            "myTimeDate", ZonedDateTime.of(1991, 12, 12, 15, 52, 0, 0, ZoneOffset.of("+01:00")),
                            "asNull", null,
                            "pointKey", List.of(Values.pointValue(CoordinateReferenceSystem.WGS84, 13.1, 33.46789)));
                    assertEquals(map("foo", foo), actual);
                });
    }
    
    @Test public void testLoadJson() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
        testCall(db, "CALL apoc.load.json($url)",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }
    
    @Test 
    public void testLoadJsonWithMapping() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.json($url, '', $config)",
                map("url",url.toString(), "config", map("mapping", map("foo", map("type", "float")))),
                (row) -> {
                    assertEquals(map("foo",asList(1D,2D,3D)), row.get("value"));
                });
    }
    
    @Test 
    public void testLoadJsonWithMappingAndPath() {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.json($url, '$.foo', $config)",
                map("url",url.toString(), "config", map("mapping", map("result", map("type", "string")))),
                (row) -> assertEquals(map("result",asList("1", "2", "3")), row.get("value")));
    }
    
    @Test
    public void testLoadJsonWithArraySepInMapping() {
		String url = ClassLoader.getSystemResource("mapMultiType.json").toString();
		
		// with array: true specified in config, generic
        final Map<String, Object> mapping = map("list", map("array", false, "type", "string"),
                "arrayOne", map("arraySep", "!"),
                "arrayTwo", map("arraySep", "-", "type", "int"));
        
        final Map<String, Object> config1 = map("array", true, "arraySep", "_", "mapping", mapping);
        
        final List<String> listOfThree = asList("alpha", "beta", "gamma");
        testCall(db, "CALL apoc.load.json($url, '', $config)", map("url", url, "config", config1), 
                (row) -> {
                    final Map<String, Object> expected = map("list", asList("1", "2", "3"),
                            "arrayOne", listOfThree,
                            "arrayTwo", asList(1L, 2L, 3L),
                            "arrayThree", listOfThree,
                            "arrayInList", asList(listOfThree, asList("delta", "epsilon")));
                    assertEquals(expected, row.get("value"));
                });

        // with array: true specified in mapping, specific
        final Map<String, Object> mapping2 = map("list", map( "type", "string"),
                "arrayOne", map("array", true, "arraySep", "!"),
                "arrayTwo", map("array", true, "arraySep", "-", "type", "int"));
        
        final Map<String, Object> config = map("arraySep", "_", "mapping", mapping2);

        testCall(db, "CALL apoc.load.json($url, '', $config)", map("url", url, "config", config),
                (row) -> {
                    final Map<String, Object> expected = map("list", asList("1", "2", "3"),
                            "arrayOne", listOfThree,
                            "arrayTwo", List.of(1L, 2L, 3L),
                            "arrayThree", "alpha_beta_gamma",
                            "arrayInList", List.of("alpha_beta_gamma", "delta_epsilon"));
                    assertEquals(expected, row.get("value"));
                });
    }

    @Test 
    public void testLoadMultiJsonWithBinary() {
        testResult(db, "CALL apoc.load.jsonParams($url, null, null, null, $config)",
                map("url", fileToBinary(new File(ClassLoader.getSystemResource("multi.json").getPath()), CompressionAlgo.FRAMED_SNAPPY.name()), 
                        "config", map(COMPRESSION, CompressionAlgo.FRAMED_SNAPPY.name())),
                this::commonAssertionsLoadJsonMulti);
    }

    private void commonAssertionsLoadJsonMulti(Result result) {
        Map<String, Object> row = result.next();
        assertEquals(map("foo", asList(1L, 2L, 3L)), row.get("value"));
        row = result.next();
        assertEquals(map("bar", asList(4L, 5L, 6L)), row.get("value"));
        assertFalse(result.hasNext());
    }

    @Test public void testLoadMultiJson() throws Exception {
		URL url = ClassLoader.getSystemResource("multi.json");
		testResult(db, "CALL apoc.load.json($url)",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                this::commonAssertionsLoadJsonMulti);
    }
    @Test public void testLoadMultiJsonPaths() throws Exception {
		URL url = ClassLoader.getSystemResource("multi.json");
		testResult(db, "CALL apoc.load.json($url,'$')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                this::commonAssertionsLoadJsonMulti);
    }
    @Test public void testLoadJsonPath() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.json($url,'$.foo')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("result",asList(1L,2L,3L)), row.get("value"));
                });
    }
    @Test public void testLoadJsonPathRoot() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.json($url,'$')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }
    
    @Test
    public void testLoadJsonWithPathOptions() throws Exception {
        URL url = ClassLoader.getSystemResource("columns.json");

        // -- load.json
        testResult(db, "CALL apoc.load.json($url, '$..columns')", map("url", url.toString()),
                (res) -> assertEquals(EXPECTED_PATH_WITH_NULLS, Iterators.asList(res.columnAs("value"))));

        testResult(db, "CALL apoc.load.json($url, '$..columns', $config)", 
                map("url", url.toString(), "config", map("pathOptions", Collections.emptyList())),
                (res) -> assertEquals(EXPECTED_PATH, Iterators.asList(res.columnAs("value"))));

        testResult(db, "CALL apoc.load.json($url, '$..columns', $config)", 
                map("url", url.toString(), "config", map("pathOptions", List.of("AS_PATH_LIST"))),
                (res) -> assertEquals(List.of(Map.of("result", EXPECTED_AS_PATH_LIST)), Iterators.asList(res.columnAs("value"))));

        // -- load.jsonArray
        testResult(db, "CALL apoc.load.jsonArray($url, '$..columns')", map("url", url.toString()),
                (res) -> assertEquals(EXPECTED_PATH_WITH_NULLS, Iterators.asList(res.columnAs("value"))));
        
        testResult(db, "CALL apoc.load.jsonArray($url, '$..columns', $config)",
                map("url", url.toString(), "config", map("pathOptions", Collections.emptyList())),
                (res) -> assertEquals(EXPECTED_PATH, Iterators.asList(res.columnAs("value"))));
        
        testResult(db, "CALL apoc.load.jsonArray($url, '$..columns', $config)",
                map("url", url.toString(), "config", map("pathOptions", List.of("AS_PATH_LIST"))),
                (res) -> assertEquals(List.of(EXPECTED_AS_PATH_LIST), Iterators.asList(res.columnAs("value"))));
    }
    
    @Test public void testLoadJsonArrayPath() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.jsonArray($url,'$.foo')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(asList(1L,2L,3L), row.get("value"));
                });
    }
    @Test public void testLoadJsonArrayPathRoot() throws Exception {
		URL url = ClassLoader.getSystemResource("map.json");
		testCall(db, "CALL apoc.load.jsonArray($url,'$')",map("url",url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }
    @Test @Ignore public void testLoadJsonGraphCommons() throws Exception {
		String url = "https://graphcommons.com/graphs/8da5327d-7829-4dfe-b60b-4c0bda956b2a.json";
		testCall(db, "CALL apoc.load.json($url)",map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    Map value = (Map)row.get("value");
                    assertEquals(true, value.containsKey("users"));
                    assertEquals(true, value.containsKey("nodes"));
                });
    }

    @Test public void testLoadJsonStackOverflow() throws Exception {
        String url = "https://api.stackexchange.com/2.2/questions?pagesize=10&order=desc&sort=creation&tagged=neo4j&site=stackoverflow&filter=!5-i6Zw8Y)4W7vpy91PMYsKM-k9yzEsSC1_Uxlf";
        testCall(db, "CALL apoc.load.json($url)",map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertFalse(value.isEmpty());
                    List<Map<String, Object>> items = (List<Map<String, Object>>) value.get("items");
                    assertEquals(10, items.size());
                });
    }


    @Test public void testLoadJsonNoFailOnError() throws Exception {
        String url = "file.json";
        testResult(db, "CALL apoc.load.json($url,null, {failOnError:false})",map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertFalse(row.hasNext());
                });
    }

    @Test public void testLoadJsonZip() throws Exception {
        URL url = ClassLoader.getSystemResource("testload.zip");
        testCall(db, "CALL apoc.load.json($url)",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTar() throws Exception {
        URL url = ClassLoader.getSystemResource("testload.tar");
        testCall(db, "CALL apoc.load.json($url)",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTarGz() throws Exception {
        URL url = ClassLoader.getSystemResource("testload.tar.gz");
        testCall(db, "CALL apoc.load.json($url)",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTgz() throws Exception {
        URL url = ClassLoader.getSystemResource("testload.tgz");
        testCall(db, "CALL apoc.load.json($url)",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonZipByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.zip?raw=true");
        testCall(db, "CALL apoc.load.json($url)",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTarByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.tar?raw=true");
        testCall(db, "CALL apoc.load.json($url)",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTarGzByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.tar.gz?raw=true");
        testCall(db, "CALL apoc.load.json($url)",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonTgzByUrl() throws Exception {
        URL url = new URL("https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.tgz?raw=true");
        testCall(db, "CALL apoc.load.json($url)",map("url",url.toString()+"!person.json"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonZipByUrlInConfigFile() throws Exception {
        testCall(db, "CALL apoc.load.json($key)",map("key","zip"),
                (row) -> {
                    Map<String,Object> r = (Map<String, Object>) row.get("value");
                    assertEquals("Michael", r.get("name"));
                    assertEquals(41L, r.get("age"));
                    assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
                });
    }

    @Test public void testLoadJsonByUrlInConfigFile() throws Exception {

        testCall(db, "CALL apoc.load.json($key)",map("key","simpleJson"), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }

    @Test(expected = QueryExecutionException.class)
    public void testLoadJsonByUrlInConfigFileWrongKey() throws Exception {

        try {
            testResult(db, "CALL apoc.load.json($key)",map("key","foo"), (r) -> r.hasNext());
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof IOException);
            final String message = except.getMessage();
            assertTrue(message.startsWith("Cannot open file "));
            assertTrue(message.endsWith("foo for reading."));
            throw e;
        }
    }

    @Test
    public void testLoadJsonWithAuth() throws Exception {
        String userPass = "user:password";
        String token = Util.encodeUserColonPassToBase64(userPass);
        Map<String, String> responseBody = Map.of("result", "message");

        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withPath("/docs/search")
                                .withHeader("Authorization", "Basic " + token),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeaders(
                                        new Header("Cache-Control", "private, max-age=1000"))
                                .withBody(JsonUtil.OBJECT_MAPPER.writeValueAsString(responseBody))
                                .withDelay(TimeUnit.SECONDS, 1)
                );

        testCall(db, "call apoc.load.json($url)",
                    map( "url", "http://" + userPass + "@localhost:1080/docs/search"),
                    (row) -> assertEquals(responseBody, row.get("value"))
                );
    }

    @Test
    public void testLoadJsonParamsWithAuth() throws Exception {
	    String userPass = "user:password";
        String token = Util.encodeUserColonPassToBase64(userPass);
        Map<String, String> responseBody = Map.of("result", "message");

        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/docs/search")
                                .withHeader("Authorization", "Basic " + token)
                                .withHeader("Content-type", "application/json")
                                .withBody("{\"query\":\"pagecache\",\"version\":\"3.5\"}"),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeaders(
                                        new Header("Content-Type", "application/json"),
                                        new Header("Cache-Control", "public, max-age=86400"))
                                .withBody(JsonUtil.OBJECT_MAPPER.writeValueAsString(responseBody))
                                .withDelay(TimeUnit.SECONDS, 1)
                );

        testCall(db, "call apoc.load.jsonParams($url, $config, $payload)",
                    map("payload", "{\"query\":\"pagecache\",\"version\":\"3.5\"}",
                        "url", "http://" + userPass + "@localhost:1080/docs/search",
                        "config", map("method", "POST", "Content-Type", "application/json")),
                    (row) -> assertEquals(responseBody, row.get("value"))
                );
    }

    @Test
    public void testLoadJsonParams() throws Exception {
        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/docs/search")
                                .withHeader("Content-type", "application/json"),
                        exactly(1))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeaders(
                                        new Header("Content-Type", "application/json"),
                                        new Header("Cache-Control", "public, max-age=86400"))
                                .withBody("{ result: 'message' }")
                                .withDelay(TimeUnit.SECONDS,1)
                );


        testCall(db, "call apoc.load.jsonParams($url, $config, $json)",
                map("json", "{\"query\":\"pagecache\",\"version\":\"3.5\"}",
                        "url", "http://localhost:1080/docs/search",
                        "config", map("method", "POST", "Content-Type", "application/json")),
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertFalse("value should be not empty", value.isEmpty());
                });
    }
}
