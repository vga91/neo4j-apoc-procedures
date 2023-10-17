package apoc.ml;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import apoc.util.DateFormatUtil;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.json.JSONObject;

import org.neo4j.cypher.internal.expressions.functions.Head;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import static apoc.ml.AmazonRequestSignatureV4Utils.calculateAuthorizationHeaders;

/*
TODO: 
https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/bedrock/BedrockClient.html#createProvisionedModelThroughput(software.amazon.awssdk.services.bedrock.model.CreateProvisionedModelThroughputRequest)
 
 */
public class Bedrock {
    
    // todo - implement
    enum ModelId {
        JURASSIC_2_MID("idName", "accept", ""),
        JURASSIC_2_ULTRA("idName", "accept", ""),
        TITAN_TEXT_G1_LITE("idName", "accept", ""),
        TITAN_EMBEDDING_G1("amazon.titan-embed-text-v1", "*/*", null),
        TITAN_TEXT_G1_EXPRESS("idName", "accept", ""),
        TITAN_TEXT_G1_AGILE("idName", "accept", ""),
        CLAUDE_V1("idName", "accept", ""),
        CLAUDE_V2("idName", "accept", ""),
        CLAUDE_INSTANT("idName", "accept", ""),
        COMMAND("idName", "accept", ""),
        STABLE_DIFFUSION_XL("idName", "accept", ""),
        CUSTOM("idName", "accept", "");
        
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
        
        public static ModelId from(String id) {
            for (ModelId modelId: ModelId.values()) {
                if (modelId.getId().equals(id)) {
                    return modelId;
                }
            }
            return ModelId.CUSTOM;
        }
    }


    @Procedure
    public void bedrock(@Name(value = "modelId") String modelId, 
                        @Name(value = "payload") Object payload,
                        @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) throws Exception {
        // todo - validation: modelId dev'essere non nullo??
        
        // todo - validation: conf deve avere o session/key oppure nell'header
        
        ModelId modelId1 = ModelId.from(modelId);

        // todo - endpoint customizable, document it
        String urlString = String.format("https://bedrock-runtime.us-east-1.amazonaws.com/model/%s/invoke", 
                modelId1.getId());
//        String url = "https://bedrock-runtime.us-east-1.amazonaws.com/model/stability.stable-diffusion-xl-v0/invoke";
        Map<String, Object> headers = Map.of(
                "Content-Type", "application/json",
//                "Authorization", "AWS4-HMAC-SHA256 Credential=AKIASSO3M7CCVJ26AETR/20231017/us-east-1/bedrock/aws4_request, SignedHeaders=content-length;content-type;host;x-amz-date, Signature=013e29432a839f68f9de4934c76d7b478fec597e2980bbbea6faaa1ad6af0320",
                "accept", "*/*"
//                "x-amz-date", "20231017T135438Z"
//                "Content-Length", "21"
//                "authorization", "AWS4-HMAC-SHA256 Credential=AKIASSO3M7CCVJ26AETR/20231017/us-east-1/bedrock/aws4_request,SignedHeaders=content-length;content-type;host;x-amz-date,Signature=56f22872fe9a68d676bd6e2232a4ee8a22c507bfc2bbb79f789e6f9cdc88b67b"
        );
//        String payload = "";
        String path = null;


        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        System.out.println(connection.getRequestMethod() + " " + url);
        
        String payloadString = payload instanceof String
                ? (String) payload
                : JsonUtil.writeValueAsString(payload);// "{\"inputText\": \"Provona\"}";// JsonUtil.writeValueAsString("{\"inputText\": \"Provona\"}");
        System.out.println("payloadString = " + payloadString);

//        String format = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'").format(new Date());
//        String basicDateTimeNoMillis = DateFormatUtil.getOrCreate("basic_date_time_no_millis").toFormat().format(date);
        headers = calculateAuthorizationHeaders("POST", url.getHost(), url.getPath(), url.getQuery(), new HashMap<>(headers), payloadString.getBytes(),
//                format,
                KEY_ID, SECRET, "us-east-1", "bedrock");
        
//        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(KEY_ID, SECRET)
//                // todo - customize it? or retrieve from URL(..)?
//                .regionName("us-east-1")
//                // todo - remove. alwasys bedrock
//                .serviceName("bedrock") // es - elastic search. use your service name
//                // todo - remove?  alwasys POST i guess...
//                .httpMethodName("POST") //GET, PUT, POST, DELETE, etc...
//                // todo - remove?
//                .canonicalURI(url)//"https://bedrock-runtime.us-east-1.amazonaws.com/model/stability.stable-diffusion-xl-v0/invoke") //end point
//
//                // todo - remove?
////                .queryParametes(queryParametes) //query parameters if any
//                .awsHeaders(headers) //aws header parameters
//                .payload(payloadString) // payload if any
//                .build();
//
//        // todo - path customizable, document it
//        headers = aWSV4Auth.getHeaders();
        System.out.println("headers = " + headers.entrySet().stream().map(Object::toString).collect(Collectors.joining("\n")));
        
//        new HttpPost(url.toString())
//                .addHeader(new Header());
        
        Stream<Object> objectStream = JsonUtil.loadJson(url.toString(), headers, payloadString, path, true, List.of());

        List<Object> objects = objectStream.toList();
        System.out.println("objects = " + objects);
    }
    
    // basic_date
    // basic_date_time_no_millis


}
