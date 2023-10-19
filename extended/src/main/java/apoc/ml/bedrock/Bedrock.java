package apoc.ml.bedrock;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import apoc.Description;
import apoc.result.ObjectResult;
import apoc.util.ExtendedUtil;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static apoc.ml.bedrock.AwsRequestSignatureV4Converter.calculateAuthorizationHeaders;
import static apoc.ml.bedrock.BedrockInvokeConfig.MODEL_ID;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static apoc.util.JsonUtil.streamObjetsFromIStream;
import static apoc.ml.bedrock.BedrockInvokeResult.*;


public class Bedrock {
    public static final String ALL = "*/*";
    public static final String JSON = "application/json";
    
    @Procedure("apoc.ml.bedrock.list")
    public Stream<ModelItemResult> list(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {

        BedrockConfig conf = new BedrockModelsConfig(config);
        Map<String, Object> headers = Map.of("Content-Type", JSON);

        headers = calculateAuthorizationHeaders("GET", conf, headers, "".getBytes());


        String path = "modelSummaries[*]";

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        return getModelItemResultStream(conf, httpClient,null, headers, path,
                objectStream -> objectStream
                        .flatMap(i -> ((List<Map<String, Object>>) i).stream())
                        .map(ModelItemResult::new)
                        .onClose(() -> Util.close(httpClient))
        );
    }

    public static <T> Stream<T> getModelItemResultStream(BedrockConfig conf, HttpClient client, String payload, Map<String, Object> headers, String path,
                                                         Function<Stream<Object>, Stream<T>> function) {
        return ExtendedUtil.getModelItemResultStream(conf.getMethod(), client, payload, headers, conf.getEndpoint(), path, List.of(), function);
//        
//        HttpRequestBase request = ExtendedUtil.fromMethodName(conf.getMethod(), endpoint);
//
//        headers.forEach((k, v) -> request.addHeader(k, v.toString()));
//
//        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
//            HttpResponse response = httpClient.execute(request);
//
//            InputStream stream = response.getEntity().getContent();
//
//            Stream<Object> objtream = streamObjetsFromIStream(stream, path, of);
//
//            return function.apply(objtream);
////            return objectStream
////                    .flatMap(i -> ((List<Map<String, Object>>) i).stream())
////                    .map(ModelItemResult::new);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }


//    public <T> T streamWithHttpClient(Function<Transaction, T> action) {
//        try (Transaction tx = db.beginTx()) {
//            T result = action.apply(tx);
//            return result;
//        }
//    }


    @Procedure
    @Description("To create a customizabled bedrock call")
    public Stream<ObjectResult> custom(@Name(value = "body") Object body,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        
        return executeInvokeRequest(body, config, null)
                .map(ObjectResult::new);
    }
    
    @Procedure("apoc.ml.bedrock.jurassic")
    public Stream<AnthropicClaude> jurassic2(@Name(value = "body") Object body,
                                                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        config.putIfAbsent(MODEL_ID, "ai21.j2-ultra-v1");

        return executeInvokeRequest(body, config, null)
                .map(AnthropicClaude::from);
    }
    
    @Procedure("apoc.ml.bedrock.anthropic.claude")
    public Stream<AnthropicClaude> anthropicClaude(@Name(value = "body") Object body,
                                                                     @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        config.putIfAbsent(MODEL_ID, "anthropic.claude-v1");

        return executeInvokeRequest(body, config, null)
                .map(AnthropicClaude::from);
    }
    
    @Procedure("apoc.ml.bedrock.titan.embedding")
    public Stream<TitanEmbedding> titanEmbedding(@Name(value = "body") Object body,
                                                                            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        config.putIfAbsent(MODEL_ID, "amazon.titan-embed-text-v1");

        return executeInvokeRequest(body, config, null)
                .map(TitanEmbedding::from);
    }

    @Procedure("apoc.ml.bedrock.stability")
    public Stream<StabilityAi> stability(@Name(value = "body") Object body,
                                               @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
        config.putIfAbsent(MODEL_ID, "stability.stable-diffusion-xl-v0");
        
        return executeInvokeRequest(body, config, "$.artifacts[0]")
                .map(StabilityAi::from);
    }

    private Stream<Object> executeInvokeRequest(Object payload, Map<String, Object> config, String path) throws IOException {
        String payloadString = payload instanceof String
                ? (String) payload
                : OBJECT_MAPPER.writeValueAsString(payload);
        
        BedrockConfig conf = new BedrockInvokeConfig(config);

        Map<String, Object> headers = new HashMap<>(conf.getHeaders());
        headers.putIfAbsent("Content-Type", JSON);
        headers.putIfAbsent("accept", ALL);
        
        headers = calculateAuthorizationHeaders("POST", conf, headers, payloadString.getBytes());

//        Stream<Object> objectStream = JsonUtil.loadJson(conf.getEndpoint(), headers, payloadString, path);
//        return objectStream;

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
//        List<Object> objects = getModelItemResultStream(conf, httpClient, payloadString, headers, path, objStream -> objStream)
//                .toList();
        return getModelItemResultStream(conf, httpClient, payloadString, headers, path, objStream -> objStream)
//                .stream();
                .onClose(() -> Util.close(httpClient));
    }

    // basic_date
    // basic_date_time_no_millis




}
