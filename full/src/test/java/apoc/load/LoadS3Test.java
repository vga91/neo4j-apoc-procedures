package apoc.load;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.xml.XmlTestUtils;
import org.junit.*;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadS3Test {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_use__neo4j__config, false)
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    private MinioSetUp minio;

    @BeforeClass
    public static void init() {
        // In test environment we skip the MD5 validation that can cause issues
        System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
    }

    @AfterClass
    public static void destroy() {
        System.clearProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation");
    }

    @Before public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, LoadJson.class, Xml.class);
        minio = new MinioSetUp("dddbucketddd");
    }

    @After public void tearDown() throws Exception {
        // The line below is quite flaky, but we don't want it to fail the build
        try {
            minio.deleteAll();
        } catch(Exception ignored) {

        }
    }

    @Test
    public void testLoadCsvS3() throws Exception {
        String url = minio.putFile("src/test/resources/test.csv");
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url", url), this::assertionsCsv);
    }
    
    @Test
    public void testLoadCsvS3WithEnvVariables() throws Exception {
        Assume.assumeNotNull(System.getenv(SECRET_KEY_ENV_VAR), System.getenv(ACCESS_KEY_ENV_VAR));
        String url = minio.putFile("src/test/resources/test.csv", false);
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url", url), this::assertionsCsv);
    }

    private void assertionsCsv(Result r) {
        assertRow(r, "Selma", "8", 0L);
        assertRow(r, "Rana", "11", 1L);
        assertRow(r, "Selina", "18", 2L);
        assertFalse(r.hasNext());
    }

    @Test public void testLoadJsonS3() throws Exception {
        String url = minio.putFile("src/test/resources/map.json");

        testCall(db, "CALL apoc.load.json($url,'')",map("url", url),
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }

    @Test public void testLoadXmlS3() throws Exception {
        String url = minio.putFile("src/test/resources/xml/books.xml");

        testCall(db, "CALL apoc.load.xml($url,'/catalog/book[title=\"Maeve Ascendant\"]/.',{failOnError:false}) yield value as result", Util.map("url", url), (r) -> {
            Object value = Iterables.single(r.values());
            Assert.assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
        });
    }


}
