package apoc.load;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

public class LoadArrowTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        apocConfig().setProperty("apoc.json.zip.url", "https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.4/src/test/resources/testload.zip?raw=true!person.json");
        TestUtil.registerProcedure(db, LoadArrow.class);
    }

    @Test
    public void testLoadArrow() throws Exception {
        URL url = ClassLoader.getSystemResource("result_withQuery.arrow");
        testCall(db, "CALL apoc.load.arrow($url)",map("url", url.getPath()),
                (row) -> {
                    assertEquals(new ArrayList<>(List.of(1L, 0L, "KNOWS", "P5M1DT12H", 1993L)), row.get("list"));
                    assertEquals(List.of(1, 0, "KNOWS", "P5M1DT12H", 1993), row.get("map"));
                    // todo - assertion
                });
    }

}
