package apoc.load;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

public class LoadFtpTest {
    private static final String USERNAME = "myUser";
    private static final String PASSWORD = "myPassword";
    
    private static final String HOME_DIR = "/";
    private static final String FILE = "dir/sample.csv";
    private static final String CONTENTS = "name\nSelina";
    
    private final static int PORT = 6453;
    
    // todo - provare un urlConnection senza user o password se possibile...
    // todo - provare un urlConnection senza porta se possibile...
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule().withSetting(ApocSettings.apoc_import_file_enabled, true);

    // todo - rule?
    private static FakeFtpServer ftpServer;
    
    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class);
        
        ftpServer = new FakeFtpServer();
        ftpServer.setServerControlPort(PORT);

        FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new FileEntry(HOME_DIR + FILE, CONTENTS));
        ftpServer.setFileSystem(fileSystem);

        UserAccount userAccount = new UserAccount(USERNAME, PASSWORD, HOME_DIR);
        ftpServer.addUserAccount(userAccount);

        ftpServer.start();

//        remoteFile = new RemoteFile();
//        remoteFile.setServer("localhost");
//        remoteFile.setPort(port);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ftpServer.stop();
    }

    @Test
    public void testLoadCsvFromFTP() throws Exception {
        testResult(db, "CALL apoc.load.csv($url, {results:['map','list','stringMap','strings']})", 
                map("url", String.format("ftp://%s:%s@localhost:%s/%s", USERNAME, PASSWORD, PORT, FILE)), 
                // -- home/dir/sample.txt parametrizzarlo
                (r) -> {
                    assertRow(r,0L,"name","Selina");
                    assertEquals(false, r.hasNext());
                });
    }

//    @Test
//    public void testRemoteUrl() throws Exception {
//        // todo - e se facessi un assume? che se l'url non esiste fermo il test?
//        
//        testResult(db, "CALL apoc.load.csv($url, {results:['map','list','stringMap','strings']})", 
//                map("url", "ftp://ftp.funet.fi/pub/doc/rfc/rfc1738.txt"), 
//                // -- home/dir/sample.txt parametrizzarlo
//                (r) -> {
//                    assertRow(r,0L,"name","Selina");
//                    assertEquals(false, r.hasNext());
//                });
//    }
}
