package apoc.load;

import apoc.ApocSettings;
import apoc.util.TestUtil;
import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;
import org.junit.Before;
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

import java.io.IOException;

import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class SftpTest {
    private static final String USERNAME = "myUser";
    private static final String PASSWORD = "myPassword";

    private static final String DIRECTORY_FILE_CSV = "/directory/file.csv";
    
    @ClassRule
    public static final FakeSftpServerRule sftpServer = new FakeSftpServerRule();
    
    // -- https://github.com/stefanbirkner/fake-sftp-server-rule


    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule().withSetting(ApocSettings.apoc_import_file_enabled, true);



    @BeforeClass
    public static void setUp() throws Exception {


        TestUtil.registerProcedure(db, LoadCsv.class);

        sftpServer.setPort(4758);
        sftpServer.addUser(USERNAME, PASSWORD);

        sftpServer.putFile(DIRECTORY_FILE_CSV, "name\nSelina", UTF_8);
        
//        ftpServer = new FakeFtpServer();
//        ftpServer.setServerControlPort(6453);  // use any free port

//        FileSystem fileSystem = new UnixFakeFileSystem();
//        fileSystem.add(new FileEntry(FILE, CONTENTS));
//        ftpServer.setFileSystem(fileSystem);

//        UserAccount userAccount = new UserAccount(USERNAME, PASSWORD, HOME_DIR);
//        ftpServer.addUserAccount(userAccount);

//        sftpServer.c
//        port = ftpServer.getServerControlPort();

//        remoteFile = new RemoteFile();
//        remoteFile.setServer("localhost");
//        remoteFile.setPort(port);
    }
    
    @Before
    public void before() throws IOException {
        sftpServer.putFile(DIRECTORY_FILE_CSV, "name\nSelina", UTF_8);
    }

    @Test
    public void testTextFile() throws IOException {
        //code that uploads the file

        String fileContent = sftpServer.getFileContent(DIRECTORY_FILE_CSV, UTF_8);

        System.out.println("FtpTest.testTextFile");
        //verify content
    }



    @Test
    public void testLoadCsvFromFTP() throws Exception {
        testResult(db, "CALL apoc.load.csv($url, {results:['map','list','stringMap','strings']})",
                map("url", String.format("sftp://%s:%s@localhost:%s/%s", USERNAME, PASSWORD, 4758, "dir/sample.csv")),
                // -- home/dir/sample.txt parametrizzarlo
                (r) -> {
                    assertRow(r,0L,"name","Selina");
                    assertEquals(false, r.hasNext());
                });
    }
}
