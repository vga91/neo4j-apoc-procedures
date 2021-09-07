package apoc.util.ftp;

import apoc.export.util.CountingReader;
import apoc.util.StreamConnection;
import apoc.util.hdfs.HDFSUtils;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.net.ftp.FTPClient;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.regex.Pattern;

public class FtpUtils {
    public static final Pattern HDFS_PATTERN = Pattern.compile("^(hdfs:\\/\\/)(?:[^@\\/\\n]+@)?([^\\/\\n]+)");
    
    // todo - mettere con un altro nome / altro file
//    public static CountingReader readFtps(URI uri) {
//        
//    }
    
    public static CountingReader readFtp(URI uri) {
        try {
            StreamConnection streamConnection = readFile(uri);
            Reader reader = new BufferedReader(new InputStreamReader(streamConnection.getInputStream(), streamConnection.getEncoding()));
            return new CountingReader(reader, streamConnection.getLength());
            /*
            todo : in Util.openInputStream fa così
            
        sc = getStreamConnection(urlAddress, headers, payload);
        stream = getInputStream(sc, urlAddress);

        return new CountingInputStream(stream, sc.getLength()); 
             */
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }
    
    public static StreamConnection readFile(URI uri) throws IOException {

        FTPClient ftpClient = new FTPClient();
        final int port = uri.getPort();
        if (port == -1) {
            ftpClient.connect(uri.getHost());
        } else {
            ftpClient.connect(uri.getHost(), port);
        }

        final String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] user = userInfo.split(":");
            ftpClient.login(user[0], user[1]); // todo - e se non c'è la password?
        }

        
//        ftpClient.login(USERNAME, PASSWORD);

//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        ftpClient.retrieveFileStream(uri.getPath());
        
        
//        boolean success = ftpClient.retrieveFile(filename, outputStream);
        

//        if (!success) {
////            throw new IOException("Retrieve file failed: " + filename);
//        }
        
//        return outputStream.toString();

        final InputStream inputStream = ftpClient.retrieveFileStream(uri.getPath());
        final String controlEncoding = ftpClient.getControlEncoding();
        final int bufferSize = ftpClient.getBufferSize();

        final StreamConnection streamConnection = new StreamConnection() {
            @Override
            public InputStream getInputStream() {
                // todo - se non c'è il path da un criptico NullPointer exception
                return inputStream;
            }

            @Override
            public String getEncoding() {
                return controlEncoding; // todo - questo credo vada bene
            }

            @Override
            public long getLength() {
                return bufferSize;
                // todo - andrebbe bene questo? boh..
            }
        };
        
        ftpClient.disconnect(); // todo - try-res - qua non va bene, lo chiude prima
        
        return streamConnection;
    }

    public static CountingReader readSFTP(URI uri) {
        try {
            StreamConnection streamConnection = readSFTPFile(uri);
            Reader reader = new BufferedReader(new InputStreamReader(streamConnection.getInputStream(), streamConnection.getEncoding()));
            return new CountingReader(reader, streamConnection.getLength());
            /*
            todo : in Util.openInputStream fa così
            
        sc = getStreamConnection(urlAddress, headers, payload);
        stream = getInputStream(sc, urlAddress);

        return new CountingInputStream(stream, sc.getLength()); 
             */
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static StreamConnection readSFTPFile(URI uri) throws JSchException {
        JSch jsch = new JSch();
        jsch.setKnownHosts(uri.getPath());
        final String userInfo = uri.getUserInfo();
        Session session = null;
//        if (userInfo != null) {
            String[] user = userInfo.split(":");
            session = jsch.getSession(user[0], uri.getHost());
            session.setPassword(user[1]);
            session.connect();
//        }
        
        
//        Session session = jsch.getSession(uri.getUserInfo())
        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
        System.out.println(channelSftp);
        return null;
    }
}
