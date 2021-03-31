package apoc.load;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.convert.ConvertJsonTest.assertMaps;
import static apoc.export.arrow.ArrowConstants.ID_FIELD;
import static apoc.export.arrow.ArrowConstants.LABELS_FIELD;
import static apoc.export.arrow.ArrowConstants.NODE_FILE_PREFIX;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LoadArrowTest {

    private static final String URL_IMPORT = NODE_FILE_PREFIX + "all.arrow";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, Paths.get(getUrlFileName(URL_IMPORT).toURI()).getParent());

    public LoadArrowTest() throws URISyntaxException {}

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        apocConfig().setProperty("apoc.json.zip.url", "https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.zip?raw=true!person.json");
        TestUtil.registerProcedure(db, LoadArrow.class);
    }

    @Test
    public void testLoadArrow() throws Exception {
        URL url = ClassLoader.getSystemResource(NODE_FILE_PREFIX + "all.arrow");
        testResult(db, "CALL apoc.load.arrow($url)",map("url", url.getPath()),
                (row) -> {
                    Map<String, Object> first = row.next();
                    assertEquals(0L, first.get("lineNo"));
                    final List<Object> list = (List<Object>) first.get("list");
                    assertEquals(8, list.size());
                    final Map<String, Object> firstMap = (Map<String, Object>) first.get("map");
                    assertEquals(8, firstMap.size());
                    assertEquals(Set.of("born", "name", ID_FIELD, "place", LABELS_FIELD, "age", "male", "kids"), firstMap.keySet());
                    assertMaps(Map.of("latitude", 33.46789, "longitude", 13.1, "crs", "wgs-84-3d", "height", 100.0), (Map<String, Object>) firstMap.get("place"));
                    assertEquals("2015-07-04T19:32:24", firstMap.get("born"));
                    assertEquals("Adam", firstMap.get("name"));
                    assertEquals(0L, firstMap.get("_id"));
                    assertEquals("User", firstMap.get("_labels"));
                    assertEquals(42L, firstMap.get("age"));
                    assertEquals(true, firstMap.get("male"));
                    assertEquals(List.of("Sam","Anna","Grace"), firstMap.get("kids"));

                    Map<String, Object> second = row.next();
                    assertEquals(1L, second.get("lineNo"));
                    final Map<String, Object> secondMap = (Map<String, Object>) second.get("map");
                    final List<Object> secondList = (List<Object>) second.get("list");
                    assertEquals(Set.of("Jim", 1L, "User", 42L), new HashSet<>(secondList));
                    assertMaps(Map.of("name", "Jim", ID_FIELD, 1L, LABELS_FIELD, "User", "age", 42L), secondMap);

                    Map<String, Object> third = row.next();
                    assertEquals(2L, third.get("lineNo"));
                    final Map<String, Object> thirdMap = (Map<String, Object>) third.get("map");
                    final List<Object> thirdList = (List<Object>) third.get("list");
                    assertEquals(Set.of(2L, "User", 12L), new HashSet<>(thirdList));
                    assertMaps(Map.of(ID_FIELD, 2L, LABELS_FIELD, "User", "age", 12L), thirdMap);

                    Map<String, Object> fourth = row.next();
                    assertEquals(3L, fourth.get("lineNo"));
                    final Map<String, Object> fourthMap = (Map<String, Object>) fourth.get("map");
                    final List<Object> fourthList = (List<Object>) fourth.get("list");
                    assertEquals(Set.of("bar", 3L, "Another"), new HashSet<>(fourthList));
                    assertMaps(Map.of("foo", "bar", ID_FIELD, 3L, LABELS_FIELD, "Another"), fourthMap);

                    assertFalse(row.hasNext());
                });
    }

    @Test
    public void testLoadArrowWithoutFileProtocol() throws Exception {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
        testResult(db, "CALL apoc.load.arrow($url, {skip: 1})",map("url", URL_IMPORT),
                (row) -> {
                    Map<String, Object> first = row.next();
                    assertEquals(1L, first.get("lineNo"));
                    final Map<String, Object> secondMap = (Map<String, Object>) first.get("map");
                    final List<Object> secondList = (List<Object>) first.get("list");
                    assertEquals(Set.of("Jim", 1L, "User", 42L), new HashSet<>(secondList));
                    assertMaps(Map.of("name", "Jim", ID_FIELD, 1L, LABELS_FIELD, "User", "age", 42L), secondMap);

                    Map<String, Object> second = row.next();
                    assertEquals(2L, second.get("lineNo"));
                    final Map<String, Object> thirdMap = (Map<String, Object>) second.get("map");
                    final List<Object> thirdList = (List<Object>) second.get("list");
                    assertEquals(Set.of(2L, "User", 12L), new HashSet<>(thirdList));
                    assertMaps(Map.of(ID_FIELD, 2L, LABELS_FIELD, "User", "age", 12L), thirdMap);

                    Map<String, Object> third = row.next();
                    assertEquals(3L, third.get("lineNo"));
                    final Map<String, Object> fourthMap = (Map<String, Object>) third.get("map");
                    final List<Object> fourthList = (List<Object>) third.get("list");
                    assertEquals(Set.of("bar", 3L, "Another"), new HashSet<>(fourthList));
                    assertMaps(Map.of("foo", "bar", ID_FIELD, 3L, LABELS_FIELD, "Another"), fourthMap);

                    assertFalse(row.hasNext());
                });
    }

    @Test
    public void testLoadArrowWithSkip() throws Exception {
        URL url = ClassLoader.getSystemResource(NODE_FILE_PREFIX + "all.arrow");
        testResult(db, "CALL apoc.load.arrow($url, {skip: 1})",map("url", url.getPath()),
                (row) -> {
                    Map<String, Object> first = row.next();
                    assertEquals(1L, first.get("lineNo"));
                    final Map<String, Object> secondMap = (Map<String, Object>) first.get("map");
                    final List<Object> secondList = (List<Object>) first.get("list");
                    assertEquals(Set.of("Jim", 1L, "User", 42L), new HashSet<>(secondList));
                    assertMaps(Map.of("name", "Jim", ID_FIELD, 1L, LABELS_FIELD, "User", "age", 42L), secondMap);

                    Map<String, Object> second = row.next();
                    assertEquals(2L, second.get("lineNo"));
                    final Map<String, Object> thirdMap = (Map<String, Object>) second.get("map");
                    final List<Object> thirdList = (List<Object>) second.get("list");
                    assertEquals(Set.of(2L, "User", 12L), new HashSet<>(thirdList));
                    assertMaps(Map.of(ID_FIELD, 2L, LABELS_FIELD, "User", "age", 12L), thirdMap);

                    Map<String, Object> third = row.next();
                    assertEquals(3L, third.get("lineNo"));
                    final Map<String, Object> fourthMap = (Map<String, Object>) third.get("map");
                    final List<Object> fourthList = (List<Object>) third.get("list");
                    assertEquals(Set.of("bar", 3L, "Another"), new HashSet<>(fourthList));
                    assertMaps(Map.of("foo", "bar", ID_FIELD, 3L, LABELS_FIELD, "Another"), fourthMap);

                    assertFalse(row.hasNext());
                });
    }

    @Test
    public void testLoadArrowWithLimit() throws Exception {
        URL url = ClassLoader.getSystemResource(NODE_FILE_PREFIX + "all.arrow");
        testResult(db, "CALL apoc.load.arrow($url, {skip: 1, limit: 1})",map("url", url.getPath()),
                (row) -> {
                    Map<String, Object> result = row.next();
                    assertEquals(1L, result.get("lineNo"));
                    final Map<String, Object> secondMap = (Map<String, Object>) result.get("map");
                    final List<Object> secondList = (List<Object>) result.get("list");
                    assertEquals(Set.of("Jim", 1L, "User", 42L), new HashSet<>(secondList));
                    assertMaps(Map.of("name", "Jim", ID_FIELD, 1L, LABELS_FIELD, "User", "age", 42L), secondMap);

                    assertFalse(row.hasNext());
                });
    }

    @Test
    public void testLoadArrowWithResult() throws Exception {
        URL url = ClassLoader.getSystemResource(NODE_FILE_PREFIX + "all.arrow");
        testResult(db, "CALL apoc.load.arrow($url, {skip: 1, results: ['list']})",map("url", url.getPath()),
                (row) -> {
                    Map<String, Object> first = row.next();
                    assertEquals(1L, first.get("lineNo"));
                    assertTrue(((Map<String, Object>) first.get("map")).isEmpty());
                    final List<Object> secondList = (List<Object>) first.get("list");
                    assertEquals(Set.of("Jim", 1L, "User", 42L), new HashSet<>(secondList));

                    Map<String, Object> second = row.next();
                    assertEquals(2L, second.get("lineNo"));
                    assertTrue(((Map<String, Object>) second.get("map")).isEmpty());
                    final List<Object> thirdList = (List<Object>) second.get("list");
                    assertEquals(Set.of(2L, "User", 12L), new HashSet<>(thirdList));

                    Map<String, Object> third = row.next();
                    assertEquals(3L, third.get("lineNo"));
                    assertTrue(((Map<String, Object>) third.get("map")).isEmpty());
                    final List<Object> fourthList = (List<Object>) third.get("list");
                    assertEquals(Set.of("bar", 3L, "Another"), new HashSet<>(fourthList));

                    assertFalse(row.hasNext());
                });
    }

}
