package apoc.load;

import apoc.Extended;
import apoc.result.ObjectResult;
import apoc.result.StringResult;
import apoc.util.*;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
//import com.google.cloud.storage.*;
//import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.s3.*;
//import software.amazon.awssdk.services.s3.model.*;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
//import com.github.junrar.Archive;
//import com.github.junrar.rarfile.FileHeader;


@Extended
public class LoadPartial {
    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    // TODO - REUSE Util.getStreamConnection
        // potrei usarlo per tutti, tramite lo skip, tranne per uncompressed local file
        // ⚠️ Skipping with FileInputStream is less efficient than RandomAccessFile
    
        // --> if (uncompressed and local) { RandomAccessFile } else { Util.getStreamConnection(...) }
    // TODO - REUSE Util.openUrlConnection
    // Util.
    
    // TODO -funcion convert to json

    @Procedure("apoc.load.jsonPartial")
    @Description("TODO 2")
    public Stream<ObjectResult> json(@Name("urlOrBinary") Object urlOrBinary,
                                    @Name("offset") Long offset,
                                    @Name(value = "limit") Long limit,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        String value = getStringResultStream(urlOrBinary, offset, limit);

        String jsonPath = (String) config.getOrDefault("jsonPath", "");
        List<String> pathOptions = (List<String>) config.getOrDefault("pathOptions", "");
        return Stream.of(
                new ObjectResult( JsonUtil.parse(value, jsonPath, Object.class, pathOptions) )
        );
        //return csvParams(urlOrBinary, null, null,configMap);
    }

    @Procedure("apoc.load.stringPartial")
    @Description("TODO")
    public Stream<StringResult> offset(@Name("urlOrBinary") Object urlOrBinary,
                                    @Name("offset") Long offset,
                                    @Name(value = "limit") Long limit,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        String value = getStringResultStream(urlOrBinary, offset, limit);
        return Stream.of(new StringResult(value));
        
        
        
        //return csvParams(urlOrBinary, null, null,configMap);
    }

    private String getStringResultStream(Object urlOrBinary, Long offset, Long limit) throws IOException, URISyntaxException, URLAccessValidationError {
        int intExact = Math.toIntExact(limit);
        if (urlOrBinary instanceof String filePath) {
            final ArchiveType archiveType = ArchiveType.from(filePath);
            if (archiveType.isArchive()) {
                String[] tokens = filePath.split("!");
                
                return readFromArchive(archiveType, tokens[0], tokens[1], offset, intExact);
            } else {
                return readFromFile(filePath, offset, limit);
            }
            
//            String[] tokens = filePath.split("!");
//            if (tokens.length == 1) {
////                return Stream.of(
////                        new StringResult(
//                return readFromFile(filePath, offset, limit);
////                        )
////                );
//            } else {
////                return Stream.of(
////                        new StringResult(
//                return readFromArchive(tokens[0], tokens[1], offset, intExact);
////                        )
////                );        
//            }
        } else if (urlOrBinary instanceof byte[] bytes) {
//            return Stream.of(
//                    new StringResult(
            return readFromByteArray(bytes, offset, limit);
//                    )
//            );
        } else {
            throw new RuntimeException("TODO");
        }
    }

//    public static void main(String[] args) throws Exception {
//        // Example usage with different sources
//        String filePath = "s3://your-bucket-name/path/to/file.csv"; // Change to your source
//        long offset = 100;
//        int limit = 500;
//
//        // Example usage for byte array
//        byte[] byteArray = "Hello, this is a test byte array containing file data.".getBytes(StandardCharsets.UTF_8);
//        String dataFromBytes = readFromByteArray(byteArray, offset, limit);
//        System.out.println("Read from Byte Array:\n" + dataFromBytes);
//
//        // Example usage for other sources
//        String data = readFromFile(filePath, offset, limit);
//        System.out.println("Read from File:\n" + data);
//    }


    public String readFromFile(String path, Long offset, Long limit) throws IOException, URISyntaxException, URLAccessValidationError {
        SupportedProtocols from = FileUtils.from(path);
        if (!from.equals(SupportedProtocols.file)) {
            return getString(path, offset, limit, from);
        }

//        if (path.startsWith("http://") || path.startsWith("https://")) {
//            return readFromHttpUrl(path, offset, limit);
//        } else if (path.startsWith("gs://")) {
//            return readFromGcs(path, offset, limit);
//        } else if (path.startsWith("s3://")) {
//            return readFromS3(path, offset, limit);
//        } else {
            return readFromLocalFile(path, offset, limit);
//        }
    }

    private String getString(String path, Long offset, Long limit, SupportedProtocols from) throws IOException, URISyntaxException, URLAccessValidationError {
        // TODO - if archive...

        Map<String, Object> headers = new HashMap<>();
        headers.putIfAbsent("Range", "bytes=" + offset + "-" + (offset + limit - 1));
        // todo - ADDITIONAL CONFIG
        
        StreamConnection streamConnection = Util.getStreamConnection(path, headers, null, urlAccessChecker);

        System.out.println("from = " + from);
        try (InputStream inputStream = streamConnection.getInputStream()) {
            // TODO - evaluate 1000
            byte[] buffer = new byte[1000];
            // byte[] buffer = new byte[limit];

            // TODO
            if (from.equals(SupportedProtocols.s3) || from.equals(SupportedProtocols.hdfs)) {
                inputStream.skip(offset);
            }
            // TODO - Math.toIntExact(limit) before readFromHttpUrl() method
            int bytesRead = inputStream.read(buffer, 0, Math.toIntExact(limit));
            return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
        }
    }

    private static String readFromLocalFile(String filePath, Long offset, Long limit) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(offset);
            // if (limit )
            // TODO - evaluate 1000
            return getString(limit, raf);
        }
    }

    private static String getString(Long limit, RandomAccessFile raf) throws IOException {
        byte[] buffer = new byte[1000];
        // byte[] buffer = new byte[limit];
        int bytesRead = limit == null
                ? raf.read(buffer)
                : raf.read(buffer, 0, Math.toIntExact(limit));
        return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
    }

/*    private static String readFromHttpUrl(String fileUrl, long offset, Long limit) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(fileUrl).openConnection();
        connection.setRequestProperty("Range", "bytes=" + offset + "-" + (offset + limit - 1));

        try (InputStream inputStream = connection.getInputStream()) {
            inputStream.skip(offset);
            // TODO - evaluate 1000
            byte[] buffer = new byte[1000];
            // byte[] buffer = new byte[limit];
            
            // TODO - Math.toIntExact(limit) before readFromHttpUrl() method
            int bytesRead = inputStream.read(buffer, 0, Math.toIntExact(limit));
            return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
        }
    }*/
    

    // ---> String[] tokens = urlAddress.split("!");

    // TODO - implement and test it
//    public static class FileReaderWithOffsetLimit {
//        public static void main(String[] args) throws Exception {
//            // Example usage
//            String filePath = "data.tar.gz"; // Change to data.zip, data.7z, or data.rar
//            String csvFileName = "data.csv"; // CSV inside the archive
//            long offset = 100;
//            int limit = 500;
//
//            String data = readFromArchive(filePath, csvFileName, offset, limit);
//            System.out.println("Read from archive:\n" + data);
//        }

        public static String readFromArchive(ArchiveType type, String archivePath, String csvFileName, long offset, int limit) throws IOException {
//            if (archivePath.endsWith(".zip")) {
                return readFromZip(type, archivePath, csvFileName, offset, limit);
//            } else if (archivePath.endsWith(".tar.gz")) {
//                return readFromTarGz(archivePath, csvFileName, offset, limit);
//            } else if (archivePath.endsWith(".7z")) {
//                return readFrom7z(archivePath, csvFileName, offset, limit);
//            } else if (archivePath.endsWith(".rar")) {
//                return readFromRar(archivePath, csvFileName, offset, limit);
//            }
//            throw new IllegalArgumentException("Unsupported archive format: " + archivePath);
        }

    public static String readCsvFromRemoteZip(ArchiveType archiveType, String zipUrl, String fileName, long offset, int limit) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(zipUrl).openConnection();
        // TODO - configurable
        connection.setRequestProperty("Range", "bytes=0-1048576"); // Fetch first 1MB to locate ZIP entries

        try (InputStream inputStream = connection.getInputStream();
             ArchiveInputStream is = archiveType.getInputStream(inputStream);
//             ZipInputStream zipStream = new ZipInputStream(inputStream)
        ) {
            return getString(fileName, offset, limit, is);

//            ZipEntry entry;
//            while ((entry = archiveTypeInputStream.getNextEntry()) != null) {
//                if (entry.getName().equals(csvFileName)) {
//                    return readOffsetFromStream(zipStream, offset, limit);
//                }
//            }
        }
//        throw new FileNotFoundException("CSV file not found in ZIP: " + csvFileName);
    }

    private static String readOffsetFromStream(InputStream stream, long offset, int limit) throws IOException {
        stream.skip(offset);
        byte[] buffer = new byte[limit];
        int bytesRead = stream.read(buffer, 0, limit);
        return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
    }

        public static String readFromZip(ArchiveType archiveType, String zipFilePath, String fileName, long offset, int limit) throws IOException {
            if (zipFilePath.startsWith("http://") || zipFilePath.startsWith("https://")) {
                return readCsvFromRemoteZip(archiveType, zipFilePath, fileName, offset, limit);
            }

            try (ArchiveInputStream is = archiveType.getInputStream( new FileInputStream(zipFilePath) )) {
                ArchiveEntry archiveEntry;

                return getString(fileName, offset, limit, is);
            }
            
        
//            try (ZipFile zipFile = new ZipFile(zipFilePath)) {
//                ZipEntry entry = zipFile.getEntry(csvFileName);
//                if (entry == null) {
//                    throw new FileNotFoundException("CSV not found in ZIP: " + csvFileName);
//                }

//                try ( InputStream is = type.getInputStream(new FileInputStream(zipFilePath)) ) {
////                try (InputStream is = ArchiveType.ZIP.getInputStream() zipFile.getInputStream(entry)) {
////                try (InputStream is = zipFile.getInputStream(entry)) {
//                    is.skip(offset);
//                    byte[] buffer = new byte[limit];
//                    int bytesRead = is.read(buffer, 0, limit);
//                    return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
//                }
//            }
        }

    private static @NotNull String getString(String fileName, long offset, int limit, ArchiveInputStream is) throws IOException {
        ArchiveEntry archiveEntry;
        while ((archiveEntry = is.getNextEntry()) != null) {
            if (!archiveEntry.isDirectory() && archiveEntry.getName().equals(fileName)) {
//                        return new ByteArrayInputStream(IOUtils.toByteArray(archive));
                                    is.skip(offset);
            byte[] buffer = new byte[limit];
            int bytesRead = is.read(buffer, 0, limit);
                return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
            }
        }

        throw new FileNotFoundException("File not found in archive: " + fileName);
    }

    
    
    public static String readFromTarGz(String tarGzFilePath, String csvFileName, long offset, int limit) throws IOException {
            try (FileInputStream fis = new FileInputStream(tarGzFilePath);
                 GzipCompressorInputStream gzis = new GzipCompressorInputStream(fis);
                 TarArchiveInputStream tais = new TarArchiveInputStream(gzis)) {

                TarArchiveEntry entry;
                while ((entry = tais.getNextTarEntry()) != null) {
                    if (entry.getName().equals(csvFileName)) {
                        tais.skip(offset);
                        byte[] buffer = new byte[limit];
                        int bytesRead = tais.read(buffer, 0, limit);
                        return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
                    }
                }
            }
            throw new FileNotFoundException("CSV not found in TAR.GZ: " + csvFileName);
        }

//        public static String readFrom7z(String sevenZFilePath, String csvFileName, long offset, int limit) throws IOException {
//            try (SevenZFile sevenZFile = new SevenZFile(new File(sevenZFilePath))) {
//                SevenZArchiveEntry entry;
//                while ((entry = sevenZFile.getNextEntry()) != null) {
//                    if (!entry.isDirectory() && entry.getName().equals(csvFileName)) {
//                        sevenZFile.seek(offset);
//                        byte[] buffer = new byte[limit];
//                        int bytesRead = sevenZFile.read(buffer, 0, limit);
//                        return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
//                    }
//                }
//            }
//            throw new FileNotFoundException("CSV not found in 7z: " + csvFileName);
//        }

//        public static String readFromRar(String rarFilePath, String csvFileName, long offset, int limit) throws IOException {
//            try (Archive archive = new Archive(new File(rarFilePath))) {
//                FileHeader fileHeader;
//                while ((fileHeader = archive.nextFileHeader()) != null) {
//                    if (!fileHeader.isDirectory() && fileHeader.getFileNameString().equals(csvFileName)) {
//                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//                        archive.extractFile(fileHeader, baos);
//                        byte[] data = baos.toByteArray();
//
//                        if (offset >= data.length) {
//                            return "";
//                        }
//
//                        int end = (int) Math.min(offset + limit, data.length);
//                        return new String(data, (int) offset, end - (int) offset, StandardCharsets.UTF_8);
//                    }
//                }
//            }
//            throw new FileNotFoundException("CSV not found in RAR: " + csvFileName);
//        }
//    }


    // TODO --> use getFileStreamIntoCompressedFile to read compressed files. like zip or tar.gz
    
    

// TODO
//    private static String readFromGcs(String gcsPath, long offset, int limit) throws IOException {
//        String[] parts = gcsPath.replace("gs://", "").split("/", 2);
//        String bucketName = parts[0];
//        String objectName = parts[1];
//
//        Storage storage = StorageOptions.getDefaultInstance().getService();
//        Blob blob = storage.get(bucketName, objectName);
//
//        if (blob == null) {
//            throw new FileNotFoundException("GCS file not found: " + gcsPath);
//        }
//
//        try (ReadChannel reader = blob.reader()) {
//            reader.seek(offset);
//            byte[] buffer = new byte[limit];
//            int bytesRead = reader.read(ByteBuffer.wrap(buffer));
//            return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
//        }
//    }

// TODO
//    private static String readFromS3(String s3Path, long offset, int limit) {
//        String[] parts = s3Path.replace("s3://", "").split("/", 2);
//        String bucketName = parts[0];
//        String objectKey = parts[1];
//
//        S3Client s3 = S3Client.builder()
//                .region(Regions.US_EAST_1)
//                .credentialsProvider(ProfileCredentialsProvider.create())
//                .build();
//
//        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
//                .bucket(bucketName)
//                .key(objectKey)
//                .range("bytes=" + offset + "-" + (offset + limit - 1))
//                .build();
//
//        try (InputStream inputStream = s3.getObject(getObjectRequest)) {
//            byte[] buffer = new byte[limit];
//            int bytesRead = inputStream.read(buffer, 0, limit);
//            return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
//        }
//    }

    private static String readFromByteArray(byte[] data, long offset, Long limit) {
        if (offset >= data.length) {
            return "";
        }

        // TODO - commonize
        int end = (int) Math.min(offset + limit, data.length);
        return new String(data, (int) offset, end - (int) offset, StandardCharsets.UTF_8);
    }
    
}
