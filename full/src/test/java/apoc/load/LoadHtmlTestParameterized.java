package apoc.load;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static apoc.load.LoadHtmlConfig.FailSilently.WITH_LIST;
import static apoc.load.LoadHtmlConfig.FailSilently.WITH_LOG;
import static apoc.load.LoadHtmlTest.RESULT_QUERY_H2;
import static apoc.load.LoadHtmlTest.RESULT_QUERY_METADATA;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class LoadHtmlTestParameterized {    
    // Tests taken from LoadHtmlTest.java.
    // To check that `browser` configuration preserve the result.

    private static final String INVALID_PATH = new File("src/test/resources/wikipedia1.html").getName();
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, new File("src/test").toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, LoadHtml.class);
    }


    @Parameters
    public static Collection<Object> data() {
        return List.of("notSet", "NONE", "CHROME", "FIREFOX");
    }

    @Parameter
    public String browser;


    @Test
    public void testQueryAll() {
        Map<String, Object> query = map("metadata", "meta", "h2", "h2");

        Map<String, Object> config = browserSet() ? Map.of("browser", browser) : emptyMap();
        testResult(db, "CALL apoc.load.html($url,$query, $config)",
                map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", config),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");

                    List<Map<String, Object>> metadata = (List<Map<String, Object>>) value.get("metadata");
                    List<Map<String, Object>> h2 = (List<Map<String, Object>>) value.get("h2");

                    assertEquals(asList(RESULT_QUERY_METADATA).toString().trim(), metadata.toString().trim());
                    assertEquals(asList(RESULT_QUERY_H2).toString().trim(), h2.toString().trim());
                });
    }

    @Test
    public void testQueryAllRelativeUrl() {
        Map<String, Object> query = map("metadata", "meta", "h2", "h2");

        Map<String, Object> config = browserSet() ? Map.of("browser", browser) : emptyMap();
        testResult(db, "CALL apoc.load.html($url,$query, $config)",
                map("url", "resources/wikipedia.html", "query", query, "config", config),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");

                    List<Map<String, Object>> metadata = (List<Map<String, Object>>) value.get("metadata");
                    List<Map<String, Object>> h2 = (List<Map<String, Object>>) value.get("h2");

                    assertEquals(asList(RESULT_QUERY_METADATA).toString().trim(), metadata.toString().trim());
                    assertEquals(asList(RESULT_QUERY_H2).toString().trim(), h2.toString().trim());
                });
    }

    @Test
    public void testQueryH2WithConfig() {
        Map<String, Object> query = map("h2", "h2");
        Map<String, Object> config = map("charset", "UTF-8", "baseUri", "");
        putBrowserIfSet(config);

        testResult(db, "CALL apoc.load.html($url, $query, $config)",
                map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", config),
                result -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("h2",asList(RESULT_QUERY_H2)).toString().trim(), row.get("value").toString().trim());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testQueryWithChildren() {
        Map<String, Object> query = map("toc", ".toc ul");
        Map<String, Object> config = map("children", true);
        putBrowserIfSet(config);

        testResult(db, "CALL apoc.load.html($url, $query, $config)",
                map("url",new File("src/test/resources/wikipedia.html").toURI().toString(), "query", query, "config", config),
                result -> {
                    Map<String, Object> row = result.next();
                    Map<String, Object> value = (Map<String, Object>) row.get("value");

                    List<Map<String, Object>> toc = (List) value.get("toc");
                    Map<String, Object> first = toc.get(0);

                    // Should be <ul>
                    assertEquals("ul", first.get("tagName"));

                    // Should have four children
                    assertEquals(4, ((List) first.get("children")).size());

                    Map<String, Object> firstChild = (Map)((List) first.get("children")).get(0);

                    assertEquals("li", firstChild.get("tagName"));
                    assertEquals(1, ((List) firstChild.get("children")).size());
                });
    }


    @Test(expected = QueryExecutionException.class)
    public void testQueryWithExceptionIfIncorrectUrl() {
        Map<String, Object> config = browserSet() ? Map.of("browser", browser) : emptyMap();
        testIncorrectUrl("CALL apoc.load.html('" + INVALID_PATH + "',{a:'a'}, $config)", config);
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithFailsSilentlyWithLogWithExceptionIfIncorrectUrl() {
        Map<String, Object> config = map("failSilently", WITH_LOG.name());
        putBrowserIfSet(config);
        
        testIncorrectUrl("CALL apoc.load.html('" + INVALID_PATH + "',{a:'a'}, $config)", config);
    }

    @Test(expected = QueryExecutionException.class)
    public void testQueryWithFailsSilentlyWithListWithExceptionIfIncorrectUrl() {
        Map<String, Object> config = map("failSilently", WITH_LIST.name());
        putBrowserIfSet(config);
        
        testIncorrectUrl("CALL apoc.load.html('" + INVALID_PATH + "',{a:'a'}, $config)", config);
    }

    private void testIncorrectUrl(String query, Map<String, Object> config) {
        try {
            testCall(db, query, map("config", config), (r) -> fail());
        } catch (Exception e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            final String message = except.getMessage();
            assertTrue(message.startsWith("Cannot open file "));
            assertTrue(message.endsWith(INVALID_PATH + " for reading."));
            throw e;
        }
    }

    private void putBrowserIfSet(Map<String, Object> config) {
        if (browserSet()) {
            config.put("browser", browser);
        }
    }

    private boolean browserSet() {
        return !browser.equals("notSet");
    }
}
