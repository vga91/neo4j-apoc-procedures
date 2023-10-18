package apoc.ml.bedrock;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;


public class AmazonRequestSignatureV4Utils {

    /**
     * Generates signing headers for HTTP request in accordance with Amazon AWS API Signature version 4 process.
     * <p>
     * Following steps outlined here: <a href="https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html">docs.aws.amazon.com</a>
     * 
     * This method takes many arguments as read-only, but adds necessary headers to @{code headers} argument, which is a map.
     * The caller should make sure those parameters are copied to the actual request object.
     * <p>
     * The ISO8601 date parameter can be created by making a call to:<br>
     * - {@code java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(ZonedDateTime.now(ZoneOffset.UTC))}<br>
     * or, if you prefer joda:<br>
     * - {@code org.joda.time.format.ISODateTimeFormat.basicDateTimeNoMillis().print(DateTime.now().withZone(DateTimeZone.UTC))}
     *
     * @param method - HTTP request method, (GET|POST|DELETE|PUT|...), e.g., {@link java.net.HttpURLConnection#getRequestMethod()}
     * @param headers - HTTP request header map. This map is going to have entries added to it by this method. Initially populated with
     *     headers to be included in the signature. Like often compulsory 'Host' header. e.g., {@link java.net.HttpURLConnection#getRequestProperties()}.
     * @param body - The binary request body, for requests like POST.
     * @param awsIdentity - AWS Identity, e.g., "AKIAJTOUYS27JPVRDUYQ"
     * @param awsSecret - AWS Secret Key, e.g., "I8Q2hY819e+7KzBnkXj66n1GI9piV+0p3dHglAzQ"
     * @param awsRegion - AWS Region, e.g., "us-east-1"
     * @param awsService - AWS Service, e.g., "route53"
     */
    public static Map<String, Object> calculateAuthorizationHeaders(
            String method,
            URL url, // String path, String query,
            Map<String, Object> headers,
            byte[] body,
            String awsIdentity, String awsSecret, String awsRegion, String awsService
    ) {
        headers = new HashMap<>(headers);
        String isoDateTime = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(ZonedDateTime.now(ZoneOffset.UTC));
        
        String host = url.getHost();
        String path = url.getPath();
        String query = url.getQuery();
        
        
//        try {
        String bodySha256 = hex(sha256(body));
        String isoJustDate = isoDateTime.substring(0, 8); // Cut the date portion of a string like '20150830T123600Z';

        headers.put("Host", host);
//            headers.put("X-Amz-Content-Sha256", bodySha256);
        headers.put("X-Amz-Date", isoDateTime);

        Pair<String, String> pairSignedHeaderAndCanonicalHash = createCanonicalRequest(method, headers, path, query, bodySha256);

        Pair<String, String> pairCredentialAndStringSign = createStringToSign(awsRegion, awsService, isoDateTime, isoJustDate, pairSignedHeaderAndCanonicalHash);

        String signature = calculateSignature(awsSecret, awsRegion, awsService, isoJustDate, pairCredentialAndStringSign.getRight());

        String authParameter = "AWS4-HMAC-SHA256 Credential=" + awsIdentity + "/" + pairCredentialAndStringSign.getLeft() + ", SignedHeaders=" + pairSignedHeaderAndCanonicalHash.getLeft() + ", Signature=" + signature;
        headers.put("Authorization", authParameter);

        return headers;
    }

    /**
     * Based on <a href="https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html">sigv4-create-string-to-sign</a>
     */
    private static Pair<String, String> createStringToSign(String awsRegion, String awsService, String isoDateTime, String isoJustDate, Pair<String, String> pairSignedHeaderCanonicalHash) {
        List<String> stringToSignLines = new ArrayList<>();
        stringToSignLines.add("AWS4-HMAC-SHA256");
        stringToSignLines.add(isoDateTime);
        String credentialScope = isoJustDate + "/" + awsRegion + "/" + awsService + "/aws4_request";
        stringToSignLines.add(credentialScope);
        stringToSignLines.add(pairSignedHeaderCanonicalHash.getRight());
        String stringToSign = String.join("\n", stringToSignLines);
        return Pair.of(credentialScope, stringToSign);
    }

    /**
     * Based on <a href="https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html">sigv4-create-canonical-request</a>
     */
    private static Pair<String, String> createCanonicalRequest(String method, Map<String, Object> headers, String path, String query, String bodySha256) {
        List<String> canonicalRequestLines = new ArrayList<>();
        canonicalRequestLines.add(method);
        canonicalRequestLines.add(path);
        canonicalRequestLines.add(query);
        List<String> hashedHeaders = new ArrayList<>();
        List<String> headerKeysSorted = headers.keySet().stream().sorted(Comparator.comparing(e -> e.toLowerCase(Locale.US))).toList();
        for (String key : headerKeysSorted) {
            hashedHeaders.add(key.toLowerCase(Locale.US));
            canonicalRequestLines.add(key.toLowerCase(Locale.US) + ":" + normalizeSpaces((String) headers.get(key)));
        }
        canonicalRequestLines.add(null); // new line required after headers
        String signedHeaders = String.join(";", hashedHeaders);
        canonicalRequestLines.add(signedHeaders);
        canonicalRequestLines.add(bodySha256);
        String canonicalRequestBody = canonicalRequestLines.stream().map(line -> line == null ? "" : line).collect(Collectors.joining("\n"));
        String canonicalRequestHash = hex(sha256(canonicalRequestBody.getBytes(StandardCharsets.UTF_8)));
        return Pair.of(signedHeaders, canonicalRequestHash);
    }

    /**
     * Based on <a href="https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html">sigv4-calculate-signature</a>
     */
    private static String calculateSignature(String awsSecret, String awsRegion, String awsService, String isoJustDate, String stringToSign) {
        byte[] kDate = hmac(("AWS4" + awsSecret).getBytes(StandardCharsets.UTF_8), isoJustDate);
        byte[] kRegion = hmac(kDate, awsRegion);
        byte[] kService = hmac(kRegion, awsService);
        byte[] kSigning = hmac(kService, "aws4_request");
        return hex(hmac(kSigning, stringToSign));
    }

    private static String normalizeSpaces(String value) {
        return value.replaceAll("\\s+", " ").trim();
    }

    public static String hex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for(byte b: a) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes);
            return digest.digest();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] hmac(byte[] key, String msg) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(msg.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
