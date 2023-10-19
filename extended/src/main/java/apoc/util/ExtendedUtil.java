package apoc.util;

import static apoc.export.cypher.formatter.CypherFormatterUtils.formatProperties;
import static apoc.export.cypher.formatter.CypherFormatterUtils.formatToString;
import static apoc.util.JsonUtil.streamObjetsFromIStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.neo4j.graphdb.Entity;

public class ExtendedUtil
{

    /**
     * Get the {@link HttpRequestBase} from the method name
     * Similar to <a href="https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/http/apache/request/impl/ApacheHttpRequestFactory.java#L118">aws implementation</a>
     */
    public static HttpRequestBase fromMethodName(String method, String uri) {
        return switch (method) {
            case HttpHead.METHOD_NAME -> new HttpHead(uri);
            case HttpGet.METHOD_NAME -> new HttpGet(uri);
            case HttpDelete.METHOD_NAME -> new HttpDelete(uri);
            case HttpOptions.METHOD_NAME -> new HttpOptions(uri);
            case HttpPatch.METHOD_NAME -> new HttpPatch(uri);
            case HttpPost.METHOD_NAME -> new HttpPost(uri);
            case HttpPut.METHOD_NAME -> new HttpPut(uri);
            default -> throw new RuntimeException("Unknown HTTP method name: " + method);
        };
    }

    /**
     * Similar to JsonUtil.loadJson(..) but works e.g. with GET method as well,
     * for which it would return a FileNotFoundException
     */
    public static Stream<Object> getModelItemResultStream(String method, HttpClient httpClient, String payloadString, Map<String, Object> headers, String endpoint, String path, List<String> of
                                                  /*Function<Stream<Object>, Stream<Object>> function*/) {

        try {
            HttpRequestBase request = fromMethodName(method, endpoint);

            headers.forEach((k, v) -> request.setHeader(k, v.toString()));

            if (request instanceof HttpEntityEnclosingRequestBase entityRequest) {
                try {
                    entityRequest.setEntity(new StringEntity(payloadString));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
//        try (
//                HttpClient httpClient = HttpClientBuilder.create().build();//) {

//            DefaultHttpClient httpClient = new DefaultHttpClient();
            HttpResponse response = httpClient.execute(request);

            InputStream stream = response.getEntity().getContent();

            return streamObjetsFromIStream(stream, path, of);
//            Stream<Object> objStream = streamObjetsFromIStream(stream, path, of);
//
//            return function.apply(objStream);
//                    .onClose(() -> {
//                try {
//                    httpClient.close();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            return objectStream
//                    .flatMap(i -> ((List<Map<String, Object>>) i).stream())
//                    .map(ModelItemResult::new);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    
    public static String dateFormat( TemporalAccessor value, String format){
        return Util.getFormat(format).format(value);
    }

    public static double doubleValue( Entity pc, String prop, Number defaultValue) {
        return Util.toDouble(pc.getProperty(prop, defaultValue));
    }

    public static Duration durationParse(String value) {
        return Duration.parse(value);
    }

    public static boolean isSumOutOfRange(long... numbers) {
        try {
            sumLongs(numbers).longValueExact();
            return false;
        } catch (ArithmeticException ae) {
            return true;
        }
    }

    private static BigInteger sumLongs(long... numbers) {
        return LongStream.of(numbers)
                .mapToObj(BigInteger::valueOf)
                .reduce(BigInteger.ZERO, (x, y) -> x.add(y));
    }

    public static String toCypherMap( Map<String, Object> map) {
        final StringBuilder builder = formatProperties(map);
        return "{" + formatToString(builder) + "}";
    }
}
