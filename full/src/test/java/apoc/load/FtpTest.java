package apoc.load;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import java.io.IOException;

import static org.assertj.core.api.Fail.fail;
import static org.junit.Assert.assertEquals;

public class FtpTest {

    private static final String HOME_DIR = "/dir";
    private static final String FILE = "/dir/sample.txt";
    private static final String CONTENTS = "abcdef 1234567890";

    private static RemoteFile remoteFile;
    private static FakeFtpServer fakeFtpServer;

    @BeforeClass
    public static void setUp() throws Exception {
        fakeFtpServer = new FakeFtpServer();
        fakeFtpServer.setServerControlPort(0);  // use any free port

        FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new FileEntry(FILE, CONTENTS));
        fakeFtpServer.setFileSystem(fileSystem);

        UserAccount userAccount = new UserAccount(RemoteFile.USERNAME, RemoteFile.PASSWORD, HOME_DIR);
        fakeFtpServer.addUserAccount(userAccount);

        fakeFtpServer.start();
        int port = fakeFtpServer.getServerControlPort();

        remoteFile = new RemoteFile();
        remoteFile.setServer("localhost");
        remoteFile.setPort(port);
    }

    // todo - farli static
    @AfterClass
    public static void tearDown() throws Exception {
        fakeFtpServer.stop();
    }


    @Test
    public void testReadFileThrowsException() {
        try {
            remoteFile.readFile("NoSuchFile.txt");
            fail("Expected IOException");
        }
        catch (IOException expected) {
            // Expected this
        }
    }
    
    @Test
    public void testReadFile() throws Exception {
        String contents = remoteFile.readFile("sample.txt");
        assertEquals("contents", CONTENTS, contents);
    }
}