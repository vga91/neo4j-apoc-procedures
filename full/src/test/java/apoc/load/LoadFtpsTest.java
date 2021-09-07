package apoc.load;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import org.apache.commons.net.ftp.FTPSClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

import java.time.Duration;

import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class LoadFtpsTest {


    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule().withSetting(ApocSettings.apoc_import_file_enabled, true);
    

    private static GenericContainer ftpsServer;

    @BeforeClass
    public static void setUp() throws Exception {
//        assumeFalse(isRunningInCI()); todo - valutare...

        TestUtil.registerProcedure(db, LoadCsv.class);
        
//        TestUtil.ignoreException(() -> {
//            ftpsServer = new GenericContainer("mikatux/ftps-server")
//                    .withEnv("USER", "username")
//                    .withEnv("PASSWORD", "password");
////                    .withExposedPorts(6475)
////                    .withNetworkAliases("mongo-" + Base58.randomString(6))
////                    .withExposedPorts(MONGO_DEFAULT_PORT)
////                    .waitingFor(new HttpWaitStrategy()
////                            .forPort(6475)
////                            .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
////                            .withStartupTimeout(Duration.ofMinutes(2))
////                    );
//            ftpsServer.start();
//
//        }, Exception.class);


        /*
           public FTPSClient() {
        this(DEFAULT_PROTOCOL, false);  todo - questo config metterlo "configurabile"
    }
         */
        
        FTPSClient client = new FTPSClient();
        client.connect("prova");
        System.out.println("LoadFtpsTest.setUp");
//        client.connect();
    }
    
    /*
    //FTPClient client1 = new FTPClient(SSLContext.getDefault());
FTPClient client1 = new FTPClient();
//client1.setEndpointCheckingEnabled(true);
//client1.addProtocolCommandListener();
client1.connect("localhost", 4567);
client1.login("username", "password");
ByteArrayInputStream arrayOutputStream = new ByteArrayInputStream("aaa".getBytes())
//arrayOutputStream.
client1.storeFile("/prova.csv", arrayOutputStream);
        
//client1.login("username", "password");





//client1.isConnected()
     */


    @AfterClass
    public static void tearDown() {
        if (ftpsServer != null) {
            ftpsServer.stop();
        }
    }

    @Test
    public void testLoadCsvFromFTPS() throws Exception {
        testResult(db, "CALL apoc.load.csv($url, {results:['map','list','stringMap','strings']})",
                map("url", String.format("ftps://%s:%s@localhost:%s/%s", "username", "password", 6475, "dir/sample.csv")),
                // -- home/dir/sample.txt parametrizzarlo
                (r) -> {
                    assertRow(r,0L,"name","Selina");
                    assertEquals(false, r.hasNext());
                });
    }
}

