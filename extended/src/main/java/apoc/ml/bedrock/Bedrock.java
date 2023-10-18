package apoc.ml.bedrock;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import apoc.result.ObjectResult;
import apoc.util.JsonUtil;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_AWS_SECRET_KEY;
import static apoc.ml.bedrock.AmazonRequestSignatureV4Utils.calculateAuthorizationHeaders;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static apoc.util.JsonUtil.streamObjetsFromIStream;

/*
TODO: 
https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/bedrock/BedrockClient.html#createProvisionedModelThroughput(software.amazon.awssdk.services.bedrock.model.CreateProvisionedModelThroughputRequest)
 
 */
public class Bedrock {
    public static final String ALL = "*/*";
    public static final String JSON = "application/json";
    public static final String PNG = "image/png";
    //    @Context
//    public ApocConfig apocConfig;
    
    
    // todo - forse basta creare delle classi che estendono interfaccia e basta...
    //      farei tipo new CustomModel(...)
    // todo - implement
    enum ModelId {
        JURASSIC_2_MID("ai21.j2-mid-v1", ALL, null),
        JURASSIC_2_ULTRA("ai21.j2-ultra-v1", ALL, null),
        
        TITAN_EMBEDDING_G1("amazon.titan-embed-text-v1", ALL, null),
        TITAN_TEXT_G1_EXPRESS("amazon.titan-text-express-v1", ALL, null),
        
        CLAUDE_V1("anthropic.claude-v1", JSON, null),
        CLAUDE_V2("anthropic.claude-v2", JSON, null),
        CLAUDE_INSTANT("anthropic.claude-instant-v1", JSON, null),
        
        STABLE_DIFFUSION_XL("stability.stable-diffusion-xl-v0", PNG, "$.artifacts[0]");
//        CUSTOM("idName", ALL, null);
        
        private final String id;
        private final String acceptValue;
        private final String jsonPath;

        ModelId(String id, String acceptValue, String jsonPath) {
            this.id = id;
            this.acceptValue = acceptValue;
            this.jsonPath = jsonPath;
        }

        public String getId() {
            return id;
        }

        public String getAcceptValue() {
            return acceptValue;
        }

        public String getJsonPath() {
            return jsonPath;
        }
        
        // todo - forse inutile..
        public static ModelId from(String id) {
            for (ModelId modelId: ModelId.values()) {
                if (modelId.getId().equals(id)) {
                    return modelId;
                }
            }
            return ModelId.TITAN_EMBEDDING_G1;
        }
    }
    
    enum GetModel {
        CUSTOM("custom-models"),
        FOUNDATION("foundation-models");
        
        private final String path;

        GetModel(String path) {
            this.path = path;
        }

        public String getPath() {
            return path;
        }
    }
    
    
    

    


    // todo - generic function??


    public record ModelItemResult(String modelId, String modelArn,String modelName, String providerName, Boolean responseStreamingSupported,
                           List<String> customizationsSupported, List<String> inferenceTypesSupported,  List<String> inputModalities,
                           List<String> outputModalities) {
        
        public ModelItemResult(Map<String, Object> map) {
            this((String) map.get("modelId"),
                    (String) map.get("modelArn"),
                    (String) map.get("modelName"),
                    (String) map.get("providerName"),
                    (Boolean) map.get("responseStreamingSupported"),
                    (List<String>) map.get("customizationsSupported"),
                    (List<String>) map.get("inferenceTypesSupported"),
                    (List<String>) map.get("inputModalities"),
                    (List<String>) map.get("outputModalities")
                    );
        }
    }
    
    // TODO - list models??
    @Procedure("apoc.ml.bedrock.list")
    public Stream<ModelItemResult> list(@Name(value = "type") String type,
                                     @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        GetModel getModel;
        try {
            getModel = GetModel.valueOf(type);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("The type config can be one of the following: " + Arrays.toString(GetModel.values()));
        }
        
        URL url = new URL("https://bedrock.us-east-1.amazonaws.com/" + getModel.getPath());
        // todo - maybe remove "method", "GET"
        Map<String, Object> headers = Map.of("Content-Type", "application/json", 
                "method", "GET");
        BedrockConfig conf = new BedrockConfig(config, url.toString());
        
        String payload = "";
        headers = calculateAuthorizationHeaders("GET", url, headers, payload.getBytes(),
                conf.getKeyId(), conf.getSecretKey(), "us-east-1", "bedrock");
        

        HttpGet request = new HttpGet(url.toString());
        headers.forEach((k,v) -> request.addHeader(k, v.toString()));

        try (DefaultHttpClient httpClient = new DefaultHttpClient()) {
            HttpResponse response = httpClient.execute(request);

            InputStream stream = response.getEntity().getContent();

            Stream<Object> objectStream = streamObjetsFromIStream(stream, "modelSummaries[*]", List.of());
//        Stream<Object> objectStream = JsonUtil.loadJson(url.toString(), headers, null, path, true, List.of());

            return objectStream
                    .flatMap(i -> ((List<Map<String, Object>>) i).stream())
                    .map(ModelItemResult::new);
        }

    }


    

    // --> TODO: remove final String accessKey, final String secretKey


//    @Procedure
//    public Stream<BedrockResult.StabilityAiResult> stability(@Name(value = "modelId") String modelId,
//                                                           @Name(value = "payload") Object payload,
//                                                           @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
//
//    }

    @Procedure("apoc.ml.bedrock")
    public Stream<ObjectResult> bedrock(@Name(value = "modelId") String modelId, // todo - remove modelId from generic proc...
                                        @Name(value = "payload") Object payload,
                                        @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        
        
        // todo - validation: modelId dev'essere non nullo??
        
        // todo - validation: config deve avere o session/key oppure nell'header
        
        ModelId modelId1 = ModelId.from(modelId);

        Stream<Object> objectStream = getObjectStream(payload, config, modelId1);

//        List<Object> objects = objectStream.toList();
//        System.out.println("objects = " + objects);
        
        return objectStream.map(ObjectResult::new);
    }

    private Stream<Object> getObjectStream(Object payload, Map<String, Object> config, ModelId modelId1) throws IOException {
        BedrockConfig conf = new BedrockConfig(config);

        // todo - endpoint customizable, document it


        Map<String, Object> headers = new HashMap<>(conf.getHeaders());
        
        headers.putIfAbsent("Content-Type", "application/json");
        headers.putIfAbsent("accept", ALL);
//        Map<String, Object> headers = Map.of(
//                "Content-Type", "application/json",
////                "Authorization", "AWS4-HMAC-SHA256 Credential=AKIASSO3M7CCVJ26AETR/20231017/us-east-1/bedrock/aws4_request, SignedHeaders=content-length;content-type;host;x-amz-date, Signature=013e29432a839f68f9de4934c76d7b478fec597e2980bbbea6faaa1ad6af0320",
//                "accept", ALL
////                "x-amz-date", "20231017T135438Z"
////                "Content-Length", "21"
////                "authorization", "AWS4-HMAC-SHA256 Credential=AKIASSO3M7CCVJ26AETR/20231017/us-east-1/bedrock/aws4_request,SignedHeaders=content-length;content-type;host;x-amz-date,Signature=56f22872fe9a68d676bd6e2232a4ee8a22c507bfc2bbb79f789e6f9cdc88b67b"
//        );
        String path = null;


        URL url = new URL(conf.getEndpoint());

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        System.out.println(connection.getRequestMethod() + " " + url);

        // todo - get region - or customizable
        String replace = url.getHost().replace(".amazonaws.com", "");
        String region = replace.substring(replace.lastIndexOf(".") + 1);

        String payloadString = payload instanceof String
                ? (String) payload
                : OBJECT_MAPPER.writeValueAsString(payload);// "{\"inputText\": \"Provona\"}";// JsonUtil.writeValueAsString("{\"inputText\": \"Provona\"}");
        System.out.println("payloadString = " + payloadString);

        // todo - directly BedrockConfig?
        headers = calculateAuthorizationHeaders("POST", url, headers, payloadString.getBytes(),

                conf.getKeyId(), conf.getSecretKey(), region, "bedrock");
//
//        // todo - path customizable, document it
//        headers = aWSV4Auth.getHeaders();
        System.out.println("headers = " + headers.entrySet().stream().map(Object::toString).collect(Collectors.joining("\n")));

//        new HttpPost(url.toString())
//                .addHeader(new Header());

        Stream<Object> objectStream = JsonUtil.loadJson(url.toString(), headers, payloadString, path);
        return objectStream;
    }

    // basic_date
    // basic_date_time_no_millis


}
