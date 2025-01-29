package apoc.s3;

import apoc.load.LoadCsv;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.xml.XmlTestUtils;
import org.junit.*;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.*;
import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;

import java.io.FileInputStream;

public class LoadS3MinioTest {


    public static class MinioSetUp {

        private static final String S3_PROTOCOL = "s3://";
        private static final String ACCESS_KEY = "Q3AM3UQ867SPQQA43P2F";
        private static final String SECRET_KEY = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG";
        private static final String ENDPOINT = "play.minio.io:9000";
        private static final String REGION = "us-east-1";

        private final MinioClient minioClient;
        private final String bucketName;

        public MinioSetUp(String bucketName) throws Exception{
            minioClient = new MinioClient("https://" + ENDPOINT, ACCESS_KEY, SECRET_KEY, REGION);
            this.bucketName = bucketName;
        }

        public String putFile(String filePath) throws Exception{
            String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
            if(!minioClient.bucketExists(bucketName)) {
                minioClient.makeBucket(bucketName);
            }
            minioClient.putObject(bucketName,fileName, filePath);

            return S3_PROTOCOL + ENDPOINT + "/" + bucketName +  "/" + fileName + "?accessKey=" + ACCESS_KEY + "&secretKey=" + SECRET_KEY;
        }

        public void deleteAll() throws Exception{
            Iterable<Result<Item>> results = minioClient.listObjects(bucketName);
            for (Result<Item> result : results) {
                minioClient.removeObject(bucketName, result.get().objectName());
            }
            if (minioClient.bucketExists(bucketName)) {
                minioClient.removeBucket(bucketName);
            }
        }
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    private MinioSetUp minio;

    @BeforeClass
    public static void init() {
        // In test environment we skip the MD5 validation that can cause issues
        //System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
    }

    @AfterClass
    public static void destroy() {
        //System.clearProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation");
    }

    @Before public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, LoadJson.class, Xml.class);

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);

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
        try {
            // Initialize MinIO client
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://localhost:9000") // Replace with your MinIO server URL
                            .credentials("minioadmin", "minioadmin") // Replace with your Access and Secret Keys
                            .build();

            // Create a bucket if it doesn't exist
            String bucketName = "my-bucket";
            boolean isBucketExists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!isBucketExists) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                System.out.println("Bucket created successfully: " + bucketName);
            } else {
                System.out.println("Bucket already exists: " + bucketName);
            }

            // Upload an object to the bucket
            String objectName = "example.txt";
            String filePath = "/path/to/example.txt"; // Replace with your file path
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .stream(new FileInputStream(new File(filePath)), new File(filePath).length(), -1)
                            .build()
            );
            System.out.println("File uploaded successfully: " + objectName);

            // Download the object
            String downloadPath = "/path/to/downloaded_example.txt"; // Replace with your desired download path
            minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .build(),
                    new File(downloadPath)
            );
            System.out.println("File downloaded successfully: " + downloadPath);

        } catch (Exception e) {
            e.printStackTrace();
        }


        String url = minio.putFile("../extended/src/test/resources/test.csv");
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url", url), (r) -> {
            assertRow(r, "Selma", "8", 0L);
            assertRow(r, "Rana", "11", 1L);
            assertRow(r, "Selina", "18", 2L);
            assertEquals(false, r.hasNext());
        });
    }

    @Test public void testLoadJsonS3() throws Exception {
        String url = minio.putFile("../extended/src/test/resources/map.json");

        testCall(db, "CALL apoc.load.json($url,'')",map("url", url),
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }

    @Test public void testLoadXmlS3() throws Exception {
        String url = minio.putFile("../extended/src/test/resources/xml/books.xml");

        testCall(db, "CALL apoc.load.xml($url,'/catalog/book[title=\"Maeve Ascendant\"]/.',{failOnError:false}) yield value as result", Util.map("url", url), (r) -> {
            Object value = Iterables.single(r.values());
            Assert.assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
        });
    }


}