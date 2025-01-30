//package apoc.s3;
//
//import apoc.load.LoadCsv;
//import apoc.load.LoadJson;
//import apoc.load.Xml;
//import apoc.util.TestUtil;
//import apoc.util.Util;
//import apoc.xml.XmlTestUtils;
//import io.minio.MinioClient;
//import io.minio.Result;
//import io.minio.messages.Item;
//import org.junit.*;
//import org.neo4j.driver.internal.util.Iterables;
//import org.neo4j.test.rule.DbmsRule;
//import org.neo4j.test.rule.ImpermanentDbmsRule;
//import org.testcontainers.containers.GenericContainer;
//
//import java.net.URI;
//
//import static apoc.ApocConfig.*;
//import static apoc.util.MapUtil.map;
//import static apoc.util.TestUtil.testCall;
//import static java.util.Arrays.asList;
//import static org.junit.Assert.assertEquals;
//
//public class LoadS3MinioContainerTest {
//
//    @Rule
//    public DbmsRule db = new ImpermanentDbmsRule();
//
//    static GenericContainer<?> minioContainer;
//
//    @BeforeClass
//    public static void init() {
//        minioContainer = new GenericContainer<>("minio/minio:latest")
//                .withExposedPorts(9000)
//                .withEnv("MINIO_ROOT_USER", "testAccessKey") // Access Key
//                .withEnv("MINIO_ROOT_PASSWORD", "testSecretKey") // Secret Key
//                .withCommand("server /data"); // Command to start MinIO
//
//        minioContainer.start();
//
//        // Build S3 client to connect to MinIO
//        URI endpoint = URI.create("http://" + minioHost + ":" + minioPort);
//        S3Client s3Client = S3Client.builder()
//                .endpointOverride(endpoint)
//                .credentialsProvider(() -> AwsBasicCredentials.create("testAccessKey", "testSecretKey"))
//                .build();
//
//        // Create an S3 bucket
//        String bucketName = "test";
//        try {
//            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
//            System.out.println("Bucket '" + bucketName + "' created successfully!");
//        } catch (S3Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @AfterClass
//    public static void destroy() {
//        minioContainer.close();
//    }
//
//    @Before
//    public void setUp() throws Exception {
//        TestUtil.registerProcedure(db, LoadCsv.class, LoadJson.class, Xml.class);
//
//        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
//        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
//
//        // minio = new MinioSetUp("dddbucketddd");
//    }
//
//    @After
//    public void tearDown() throws Exception {
//
//    }
//
//    @Test
//    public void testLoadCsvS3() throws Exception {
//        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");
//
//        String minioHost = minioContainer.getHost();
//        Integer minioPort = minioContainer.getMappedPort(9000);
//        String url1 = db.executeTransactionally("CALL apoc.load.csv($url,{failOnError:true})",
//                map("url", "s3://127.0.0.1:9000/test/test.csv?accessKey=oXO3ay76oNoPfLuI2arS&secretKey=dhXtgn1IiVMDXl1FIChpQADjLGiU6Q3JOuvGJXGn"),
//                org.neo4j.graphdb.Result::resultAsString);
//        System.out.println("url1 = " + url1);
//        System.out.println("LoadS3MinioTest.testLoadCsvS3");
//        // --> Failed to invoke procedure `apoc.load.csv`: Caused by: javax.net.ssl.SSLException: Unsupported or unrecognized SSL message
//
////        String url = minio.putFile("../extended/src/test/resources/test.csv");
////        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url", url), (r) -> {
////            assertRow(r, "Selma", "8", 0L);
////            assertRow(r, "Rana", "11", 1L);
////            assertRow(r, "Selina", "18", 2L);
////            assertEquals(false, r.hasNext());
////        });
//    }
//
//
//
//}