package apoc.ml.sagemaker;

import apoc.Description;
import apoc.ml.VertexAI;
import apoc.ml.bedrock.AwsSignatureV4Generator;
import apoc.ml.bedrock.AWSConfig;
import apoc.ml.bedrock.BedrockInvokeConfig;
import apoc.ml.bedrock.SageMakerConfig;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static apoc.ml.bedrock.AWSConfig.ENDPOINT_KEY;
import static apoc.ml.bedrock.AWSConfig.JSON_PATH;
import static apoc.ml.bedrock.SageMakerConfig.ENDPOINT_NAME_KEY;
import static apoc.util.JsonUtil.OBJECT_MAPPER;

public class SageMaker {

    public record EmbeddingResult(long index, String text, List<Double> embedding) {}


    // todo - NO DEFAULT ENDPOINT!!
    
    // TODO --> https://aws.amazon.com/marketplace/ai/configuration?productId=3deb2647-5287-405a-88a9-5947a08436b9
    //   https://us-east-1.console.aws.amazon.com/sagemaker/home?region=us-east-1#/marketplace-search-model-packages!mpSearch/search?text=text+generation&filter%3AFULFILLMENT_OPTION_TYPE=SAGEMAKER_MODEL
    
    @Procedure("apoc.ml.sagemaker.custom")
    @Description("apoc.ml.sagemaker.chat(body, $conf) - To create a customizable SageMaker call")
    public Stream<MapResult> custom(@Name(value = "body") Object body,
                                    @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        AWSConfig conf = new SageMakerConfig(configuration);

        return executeRequestReturningMap(body, conf)
                .map(MapResult::new);
    }

    // todo - default:
    
    
    // todo: https://studio-d-l9bumpjij1kt.studio.us-east-1.sagemaker.aws/inference-experience/models/deploy?jumpstart_model_id=meta-textgeneration-llama-2-70b-f&jumpstart_model_version=3.0.0&jumpstart_hub=SageMakerJumpStart&base_model_relative_path=/jumpstart/meta/meta-textgeneration-llama-2-70b-f&deployment_event_id=20231130-095657
    
    
    
    // todo todo todo --> https://aws.amazon.com/marketplace/ai/configuration?productId=5b256918-be81-4533-810c-7d7f76f4f863&ref_=aws-mp-console-subscription-card-action
    
    // --> https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/events?stackId=arn%3Aaws%3Acloudformation%3Aus-east-1%3A177090656389%3Astack%2FStack-VARCO-LLM-KO-1-3B-IST-1%2F8db0e980-8f19-11ee-99c0-0e3554150dfb&filteringText=&filteringStatus=active&viewNested=true
    
    @Procedure("apoc.ml.sagemaker.chat")
    @Description("apoc.ml.sagemaker.chat(messages, $conf) - Prompts the chat completion API")
    public Stream<MapResult> chatCompletion(
            @Name("messages") List<Map<String, String>> messages,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {

        AWSConfig conf = new SageMakerConfig(configuration);

        return messages
                .stream()
                .flatMap(message -> executeRequestReturningMap(message, conf)
                        .map(MapResult::new)
                );
    }

    // todo - default ?
    @Procedure("apoc.ml.sagemaker.completion")
    @Description("apoc.ml.sagemaker.completion(prompt, $conf) - Prompts the completion API")
    public Stream<MapResult> completion(@Name("prompt") String prompt,
                                        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {

        AWSConfig conf = new SageMakerConfig(configuration);

        return executeRequestReturningMap(prompt, conf)
                .map(MapResult::new);
    }

    // todo - default: https://runtime.sagemaker.eu-central-1.amazonaws.com/endpoints/Endpoint-Jina-Embeddings-v2-Base-en-1/invocations
    @Procedure("apoc.ml.sagemaker.embedding")
    @Description("apoc.ml.sagemaker.embedding([texts], $configuration) - Returns the embeddings for a given text")
    public Stream<EmbeddingResult> embedding(@Name(value = "texts") List<String> texts,
                                                           @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        var config = new HashMap<>(configuration);
        config.putIfAbsent(ENDPOINT_NAME_KEY, "Endpoint-Jina-Embeddings-v2-Base-en-1");
        config.putIfAbsent(JSON_PATH, "data[*]");
        AWSConfig conf = new SageMakerConfig(config);

        List<Map<String, String>> inputs = texts.stream().map(text -> Map.of("text", text)).toList();
        Object data = Map.of("data", inputs);

        AtomicInteger idx = new AtomicInteger();
        return executeRequestCommon(data, conf)
                .flatMap(v -> ((List<Map<String, Object>>) v).stream())
                .map(i -> {
                    int index = idx.getAndIncrement();
                    return new EmbeddingResult(index, texts.get(index), (List<Double>) i.get("embedding"));
                });
    }

    private Stream<Map<String, Object>> executeRequestReturningMap(Object body, AWSConfig config) {
        return executeRequestCommon(body, config)
                .map(i -> (Map<String, Object>) i);
    }
    
    private Stream<Object> executeRequestCommon(Object body, AWSConfig conf) {
        try {
            String bodyString = body instanceof String string
                    ? string
                    : OBJECT_MAPPER.writeValueAsString(body);
            
            Map<String, Object> headers = conf.getHeaders();
            headers.putIfAbsent("Content-Type", "application/json");
            headers.putIfAbsent("accept", "*/*");

            if (!headers.containsKey("Authorization")) {
                AwsSignatureV4Generator.calculateAuthorizationHeaders(conf, bodyString, "sagemaker");
            }

            return JsonUtil.loadJson(conf.getEndpoint(), conf.getHeaders(), bodyString, conf.getJsonPath(), true, List.of());
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static final String X_AMZ_DATE = "X-Amz-Date";
    
    
}
