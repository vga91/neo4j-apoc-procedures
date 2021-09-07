package apoc.load;

import org.apache.commons.net.ftp.FTPClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RemoteFile {
    // mentre hdfs è --> hdfs://localhost:61848/user/GiuseppeVillani/test.csv
// qui ci sono username e password che passo come stringa ftp://user:password@host:port/path
    public static final String USERNAME = "user";
    public static final String PASSWORD = "password";

    private String server;
    private int port;

    public String readFile(String filename) throws IOException {

        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(server, port);
        ftpClient.login(USERNAME, PASSWORD);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        boolean success = ftpClient.retrieveFile(filename, outputStream);
        ftpClient.disconnect();

        if (!success) {
            throw new IOException("Retrieve file failed: " + filename);
        }
        return outputStream.toString();
    }

    public void setServer(String server) {
        this.server = server;
    }

    public void setPort(int port) {
        this.port = port;
    }

    // Other methods ...
}