package apoc.load.partial;

import apoc.Extended;
import apoc.result.ObjectResult;
import apoc.result.StringResult;
import apoc.util.*;
import apoc.util.s3.S3Aws;
import apoc.util.s3.S3Params;
import apoc.util.s3.S3ParamsExtractor;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
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
import java.util.Objects;
import java.util.stream.Stream;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.CompressionConfig.COMPRESSION;


@Extended
public class LoadPartial {
    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;


    
    // --> if (uncompressed and local) { RandomAccessFile } else { Util.getStreamConnection(...) }
    // TODO - REUSE Util.openUrlConnection
    // Util.
    
    // TODO -funcion convert to json

    @Procedure("apoc.load.jsonPartial")
    @Description("TODO 2")
    public Stream<ObjectResult> json(@Name("urlOrBinary") Object urlOrBinary,
                                    @Name("offset") long offset,
                                    @Name(value = "limit") Long limit,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        String value = getStringResultStream(urlOrBinary, offset, limit, config);

        String jsonPath = (String) config.getOrDefault("jsonPath", "");
        List<String> pathOptions = (List<String>) config.getOrDefault("pathOptions", "");
        return Stream.of(
                new ObjectResult( JsonUtil.parse(value, jsonPath, Object.class, pathOptions) )
        );
    }

    @Procedure("apoc.load.stringPartial")
    @Description("TODO")
    public Stream<StringResult> offset(@Name("urlOrBinary") Object urlOrBinary,
                                    @Name("offset") long offset,
                                    @Name(value = "limit") Long limit,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        String value = getStringResultStream(urlOrBinary, offset, limit, config);
        return Stream.of(new StringResult(value));
    }

    private String getStringResultStream(Object urlOrBinary, long offset, Long limit, Map<String, Object> config) throws Exception {
        Map<String, Object> headers = (Map) config.getOrDefault("headers", Map.of());
        String payload = (String) config.get("payload");
        Integer archiveLimit = Util.toInteger(config.get("archiveLimit"));
        Integer bufferLimit = Util.toInteger(config.get("bufferLimit"));
        
        if (urlOrBinary instanceof String filePath) {
            apocConfig().checkReadAllowed(filePath, urlAccessChecker);
            final ArchiveType archiveType = ArchiveType.from(filePath);
            if (archiveType.isArchive()) {
                String[] tokens = filePath.split("!");
                
                return readFromArchive(archiveType, tokens[0], tokens[1], offset, limit);
            } else {
                return readFromFile(filePath, offset, limit);
            }

        } 
        if (urlOrBinary instanceof byte[] bytes) {
            return readFromByteArray(bytes, (int) offset, limit, config);
        }
        
        throw new RuntimeException("The first parameter must be a String URL or a byte[]");
        
    }


    public String readFromFile(String path, Long offset, Long limit) throws IOException, URISyntaxException, URLAccessValidationError {
        SupportedProtocols from = FileUtils.from(path);
        if (!from.equals(SupportedProtocols.file)) {
            return getPartialString(path, offset, limit, from);
        }

        return readFromLocalFile(path, offset, limit);
    }

    private String getPartialString(String path, Long offset, Long limit, SupportedProtocols from) throws IOException, URISyntaxException, URLAccessValidationError {
        // TODO - if archive...

        Map<String, Object> headers = new HashMap<>();
        headers.putIfAbsent("Range", "bytes=" + offset + "-" + (offset + limit - 1));
        // todo - ADDITIONAL CONFIG

         boolean S3Protocol = from.equals(SupportedProtocols.s3);

        try (InputStream inputStream = getInputStream(path, offset, limit, from, headers, S3Protocol)) {
            // byte[] buffer = new byte[limit];

            // TODO
            if (S3Protocol || from.equals(SupportedProtocols.hdfs)) {
                inputStream.skip(offset);
            }

            return getPartialString(limit, inputStream);
        }
    }

    private static String getPartialString(Long limit, InputStream inputStream) throws IOException {
        byte[] buffer = new byte[limit == null ? 1000 : Math.toIntExact(limit)];
        int bytesRead = limit == null ? inputStream.read(buffer) : inputStream.read(buffer, 0, Math.toIntExact(limit));
        return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
    }

    private InputStream getInputStream(String path, Long offset, Long limit, SupportedProtocols from, Map<String, Object> headers, boolean equals) throws IOException, URISyntaxException, URLAccessValidationError {
        //boolean equals = from.equals(SupportedProtocols.s3);
        InputStream inputStream1;
        StreamConnection streamConnection = Util.getStreamConnection(path, headers, null, urlAccessChecker);

        System.out.println("from = " + from);
        if (equals) {
            S3Params s3Params = S3ParamsExtractor.extract(path);
            String region = Objects.nonNull(s3Params.getRegion()) ? s3Params.getRegion() : Regions.US_EAST_1.getName();
            S3Aws s3Aws = new S3Aws(s3Params, region);

            GetObjectRequest request = new GetObjectRequest(s3Params.getBucket(), s3Params.getKey())
                    .withRange(offset, offset + limit - 1);

            S3Object object = s3Aws.getClient().getObject(request);
            inputStream1 = object.getObjectContent();
        }

        inputStream1 = streamConnection.getInputStream();
        return inputStream1;
    }

    private static String readFromLocalFile(String filePath, Long offset, Long limit) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(offset);
            // if (limit )
            // TODO - evaluate 1000
            return getPartialString(limit, raf);
        }
    }

    private static String getPartialString(Long limit, RandomAccessFile raf) throws IOException {
        byte[] buffer = new byte[1000];
        // byte[] buffer = new byte[limit];
        int bytesRead = limit == null
                ? raf.read(buffer)
                : raf.read(buffer, 0, Math.toIntExact(limit));
        return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
    }
    
    public String readFromArchive(ArchiveType type, String archivePath, String csvFileName, long offset, Long limit) throws IOException, URISyntaxException, URLAccessValidationError {

        SupportedProtocols from = FileUtils.from(archivePath);
        boolean S3Protocol = from.equals(SupportedProtocols.s3);

        Map<String, Object> headers = new HashMap<>();
        // TODO - configurable
        headers.putIfAbsent("Range", "bytes=0-1048576"); // Fetch first 1MB to locate ZIP entries
        
        try (InputStream inputStream = getInputStream(archivePath, offset, (long) limit, from, headers, S3Protocol);
             ArchiveInputStream is = type.getInputStream(inputStream)) {

            return getPartialString(csvFileName, offset, limit, is);
        }
    }

    private static String getPartialString(String fileName, long offset, Long limit, ArchiveInputStream is) throws IOException {
        ArchiveEntry archiveEntry;
        while ((archiveEntry = is.getNextEntry()) != null) {
            if (!archiveEntry.isDirectory() && archiveEntry.getName().equals(fileName)) {
                                    is.skip(offset);

                return getPartialString(limit, is);
            }
        }

        throw new FileNotFoundException("File not found in archive: " + fileName);
    }

    private static String readFromByteArray(byte[] data, int offset, Long limit, Map<String, Object> config) throws Exception {
        if (offset >= data.length) {
            return "";
        }

        String compressionAlgo = (String) config.getOrDefault(COMPRESSION, CompressionAlgo.NONE.toString());
        CompressionAlgo algo = CompressionAlgo.valueOf(compressionAlgo);
        try (ByteArrayInputStream stream = new ByteArrayInputStream(data);
//        try (ByteArrayInputStream stream = new ByteArrayInputStream(data, offset, (int) Math.min(limit, data.length - offset));
             InputStream inputStream = algo.getInputStream(stream)) {

            inputStream.skip(offset);
            return getPartialString(limit, inputStream);
        }
        
        // ---> if offset in new ByteArrayInputStream(data, offset, (int) Math.min(limit, data.length - offset)
        //      ---> java.io.IOException: Stream is not in the BZip2 format

        // TODO - commonize
//        int end = (int) Math.min(offset + limit, data.length);
//        return new String(data, (int) offset, end - (int) offset, StandardCharsets.UTF_8);
    }
    
}
