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
//    enum ModelId {
//        JURASSIC_2_MID("idName", "accept", ""),
//        JURASSIC_2_ULTRA(),
//        TITAN_TEXT_G1_LITE(),
//        TITAN_EMBEDDING_G1(),
//        TITAN_TEXT_G1_EXPRESS(),
//        TITAN_TEXT_G1_AGILE(),
//        CLAUDE_V1(),
//        CLAUDE_V2(),
//        CLAUDE_INSTANT(),
//        COMMAND(),
//        STABLE_DIFFUSION_XL(),
//        CUSTOM();
//    }

    // --> final String accessKey, final String secretKey
    public static final String KEY_ID = "AKIASSO3M7CCVJ26AETR";
    public static final String SECRET = "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";

    @Procedure
    public void bedrock(@Name(value = "payload", defaultValue = "{}") Map<String, Object> payload,
                        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        // todo - validation: modelId dev'essere non nullo??
        
        
//        InvokeBedrock.invoke();
        
        /*
        POST https://bedrock.us-east-1.amazonaws.com/model/stability.stable-diffusion-xl-v0/invoke

        -H accept: image/png
        -H content-type: application/json
        
        Payload : `{"inputText": "Picture of a bird"}`
         */

        String s = Util.encodeUserColonPassToBase64(KEY_ID + ":" + SECRET);
        System.out.println("s = " + s);


        String access_key = new String("AKIAIOSFODNN7EXAMPLE".getBytes(), StandardCharsets.UTF_8);//.encode("UTF-8")
        String secret_key = new String("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".getBytes(), StandardCharsets.UTF_8);//.encode("UTF-8")

//        String string_to_sign = new String("GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/awsexamplebucket1/photos/puppy.jpg".getBytes(), StandardCharsets.UTF_8)// ;.encode("UTF-8")
//        signature = base64.encodestring(
//                hmac.new(
//                secret_key, string_to_sign, sha1
//                                         ).digest()
//                                ).strip()
//
//
//        print(f"AWS {access_key.decode()}:{signature.decode()}");
        

        
//        String url = "https://bedrock-runtime.us-east-1.amazonaws.com";


        Date date = new Date();

        // todo - endpoint customizable, document it
        String urlString = "https://bedrock-runtime.us-east-1.amazonaws.com/model/amazon.titan-embed-text-v1/invoke";
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
        
        String payloadString = "{\"inputText\": \"Provona\"}";// JsonUtil.writeValueAsString("{\"inputText\": \"Provona\"}");

        String format = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'").format(new Date());
//        String basicDateTimeNoMillis = DateFormatUtil.getOrCreate("basic_date_time_no_millis").toFormat().format(date);
        headers = calculateAuthorizationHeaders("POST", url.getHost(), url.getPath(), url.getQuery(), new HashMap<>(headers), payloadString.getBytes(),
                format,
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

    public static class InvokeBedrock {
        // Todo - StaticCredentialsProvider?? diverso dall'analogo  AWSStaticCredentialsProvider di S3Aws??? wtf...
        // todo - maybe because of --> https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/migration-whats-different.html
        //  ma allora sdk 1.x non contiene bedrock.. giusto?
        
        public static void invoke() {
            AwsBasicCredentials basicAWSCredentials = AwsBasicCredentials.create(KEY_ID, SECRET);
            StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(basicAWSCredentials);
            AwsCredentials awsCredentials = credentialsProvider.resolveCredentials();
            try (BedrockRuntimeClient client = BedrockRuntimeClient.builder()
                    
//            try (BedrockClient client = BedrockClient.builder()
                    .region(Region.US_EAST_1)
                    // todo - mocked credentials... try with S3Aws
                    .credentialsProvider(credentialsProvider)
                    .build()) {

                String prompt = "Hello Claude, how are you?";

                JSONObject jsonBody = new JSONObject()
                        .put("prompt", "Human: " + prompt + " Assistant:")
                        .put("temperature", 0.8)
                        .put("max_tokens_to_sample", 1024);

                SdkBytes body = SdkBytes.fromUtf8String(
                        jsonBody.toString()
                );

                SdkBytes test = SdkBytes.fromString("test", StandardCharsets.UTF_8);
                InvokeModelRequest request = InvokeModelRequest.builder()
                        .modelId("amazon.titan-embed-text-v1")
//                        .modelId("anthropic.claude-v2")
                        .body(test)
                        .build();

//                InvokeModelRequest build = InvokeModelRequest.builder()
//                        .contentType("application/json")
//                        .accept("*/*")
//                        .modelId("amazon.titan-embed-text-v1")
//                        .build();

                InvokeModelRequest build = InvokeModelRequest.builder()
                        .contentType("application/json")
                        .accept("*/*")
                        .modelId("amazon.titan-embed-text-v1")
                        .body(SdkBytes.fromUtf8String("{\"inputText\": \"ajeje\"}"))
                        .build();

//                GetCustomModelRequest.Builder consBuilder = GetCustomModelRequest.builder();
//                BedrockServiceClientConfiguration bedrockServiceClientConfiguration = runtime.serviceClientConfiguration();

//                System.out.println("bedrockServiceClientConfiguration = " + bedrockServiceClientConfiguration);
                InvokeModelResponse response = client.invokeModel(build);
                
                

                JSONObject jsonObject = new JSONObject(
                        response.body().asString(StandardCharsets.UTF_8)
                );

//                String completion = jsonObject.getString("completion");

                System.out.println();
                System.out.println(jsonObject);
                System.out.println();
            }
            
        }
//        public static void main(String[] args) {
//        }
    }
}
