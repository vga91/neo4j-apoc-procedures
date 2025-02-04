package apoc.s3;

import apoc.export.csv.ExportCSV;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.load.LoadCsv;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.*;
import static apoc.util.MapUtil.map;


import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

public class LoadS3MinioTest {

    static final String ACCESS_KEY = "testAccessKey";
    static final String SECRET_KEY = "testSecretKey";
    static final String BUCKET_NAME = "test";
    static GenericContainer<?> minioContainer;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void init() throws Throwable {
        // to make Minio work
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");

        TestUtil.registerProcedure(db, 
                ExportCSV.class, ExportGraphML.class, ExportJson.class, 
                LoadCsv.class, LoadJson.class, Xml.class);

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);

        db.executeTransactionally(
                "CREATE (f:User1:User {name:'foo'})-[:KNOWS]->(b:User {name:'bar'})");



        minioContainer = new GenericContainer<>("bitnami/minio:2025.1.20")
            .withExposedPorts(9000, 9001)
            .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .withEnv("MINIO_DEFAULT_BUCKETS", BUCKET_NAME)
                .waitingFor(Wait.forHttp("/").forStatusCode(200));

       // TODO - NON FUNZIONA
        minioContainer.setWaitStrategy(
                Wait.forLogMessage(".*Bucket created successfully.*\\n", 1)
                        .withStartupTimeout(Duration.ofSeconds(30))
        );
            //   minioContainer.withCommand("server", "/data");

        minioContainer.start();

      //  assertEventually(() -> minioContainer.getLogs().contains("Bucket created successfully"),
      //          val -> val, 20L, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void destroy() {
        minioContainer.close();
    }


    private static String getUrl(String fileName) {
        return String.format(
                "s3://%s:%s/%s/%s?accessKey=%s&secretKey=%s",
                minioContainer.getHost(),
                minioContainer.getMappedPort(9000),
                BUCKET_NAME,
                fileName,
                ACCESS_KEY,
                SECRET_KEY
        );
    }

    @Test
    public void testLoadCsvS3() throws Exception {

//        Thread.sleep(5000);

        String url = getUrl("test.csv");

        String url12 = db.executeTransactionally("CALL apoc.export.csv.all($url,{failOnError:true})",
                map("url", url),
                org.neo4j.graphdb.Result::resultAsString);
        System.out.println("url12 = " + url12);

        String url1 = db.executeTransactionally("CALL apoc.load.csv($url,{failOnError:true})",
                map("url", url),
                org.neo4j.graphdb.Result::resultAsString);
        System.out.println("url1 = " + url1);
        System.out.println("LoadS3MinioTest.testLoadCsvS3");
        // --> Failed to invoke procedure `apoc.load.csv`: Caused by: javax.net.ssl.SSLException: Unsupported or unrecognized SSL message

//        String url = minio.putFile("../extended/src/test/resources/test.csv");
//        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url", url), (r) -> {
//            assertRow(r, "Selma", "8", 0L);
//            assertRow(r, "Rana", "11", 1L);
//            assertRow(r, "Selina", "18", 2L);
//            assertEquals(false, r.hasNext());
//        });
    }

    @Test
    public void testLoadJsonS3() throws Exception {
        //String url = minio.putFile("../extended/src/test/resources/map.json");

        String url = getUrl("test.json");

        String url12 = db.executeTransactionally("CALL apoc.export.json.all($url,{failOnError:true})",
                map("url", url),
                org.neo4j.graphdb.Result::resultAsString);
        System.out.println("url12 = " + url12);

        String url1 = db.executeTransactionally("CALL apoc.load.json($url,'')",
                map("url", url),
                org.neo4j.graphdb.Result::resultAsString);
        System.out.println("url1 = " + url1);
        System.out.println("LoadS3MinioTest.testLoadCsvS3");

    /*    testCall(db, "CALL apoc.load.json($url,'')",
                map("url", url),
                (row) -> {
                    System.out.println("row = " + row);
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });

     */
    }

    @Test
    public void testLoadXmlS3() throws Exception {
        //String url = minio.putFile("../extended/src/test/resources/xml/books.xml");
        String url = getUrl("test.xml");

        String url12 = db.executeTransactionally("CALL apoc.export.graphml.all($url,{failOnError:true})",
                map("url", url),
                org.neo4j.graphdb.Result::resultAsString);
        System.out.println("url12 = " + url12);

        String url1 = db.executeTransactionally("CALL apoc.load.xml($url,'')",
                map("url", url),
                org.neo4j.graphdb.Result::resultAsString);
        System.out.println("url1 = " + url1);
        System.out.println("LoadS3MinioTest.testLoadCsvS3");
    }


}