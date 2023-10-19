package apoc.ml.bedrock;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import apoc.Description;
import apoc.result.ObjectResult;
import apoc.util.ExtendedUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.http.impl.client.HttpClientBuilder;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static apoc.ml.bedrock.AwsSignatureV4Generator.calculateAuthorizationHeaders;
import static apoc.ml.bedrock.BedrockInvokeConfig.MODEL_ID;
import static apoc.util.JsonUtil.OBJECT_MAPPER;
import static apoc.ml.bedrock.BedrockInvokeResult.*;
import static apoc.ml.bedrock.BedrockUtil.ModelId.*;
import static apoc.ml.bedrock.BedrockUtil.ALL;
import static apoc.ml.bedrock.BedrockUtil.JSON;


public class Bedrock {
    
    @Procedure
    public Stream<ModelItemResult> list(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        BedrockConfig conf = new BedrockGetModelsConfig(config);
        
        return executeRequestCommon(null, "modelSummaries[*]", conf)
                .flatMap(i -> ((List<Map<String, Object>>) i).stream())
                .map(ModelItemResult::new);
    }
    
    @Procedure
    @Description("To create a customizable bedrock call")
    public Stream<ObjectResult> custom(@Name(value = "body") Object body,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        
        return executeCustomRequest(body, config, null)
                .map(ObjectResult::new);
    }
    
    @Procedure
    public Stream<Jurassic> jurassic(@Name(value = "body") Object body,
                                     @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        
        config.putIfAbsent(MODEL_ID, JURASSIC_2_ULTRA.id());

        return executeRequestReturningMap(body, config, null)
                .map(Jurassic::from);
    }
    
    @Procedure("apoc.ml.bedrock.anthropic.claude")
    public Stream<AnthropicClaude> anthropicClaude(@Name(value = "body") Object body,
                                                                     @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        config.putIfAbsent(MODEL_ID, CLAUDE_V2.id());

        return executeRequestReturningMap(body, config, null)
                .map(AnthropicClaude::from);
    }
    
    @Procedure("apoc.ml.bedrock.titan.embedding")
    public Stream<TitanEmbedding> titanEmbedding(@Name(value = "body") Object body,
                                                                            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        config.putIfAbsent(MODEL_ID, TITAN_EMBEDDING_G1.id());

        return executeRequestReturningMap(body, config, null)
                .map(TitanEmbedding::from);
    }

    @Procedure("apoc.ml.bedrock.stability")
    public Stream<StabilityAi> stability(@Name(value = "body") Object body,
                                               @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        config.putIfAbsent(MODEL_ID, STABLE_DIFFUSION_XL.id());
        
        return executeRequestReturningMap(body, config, "$.artifacts[0]")
                .map(StabilityAi::from);
    }
    

    private Stream<Map<String, Object>> executeRequestReturningMap(Object body, Map<String, Object> config, String path) {
        return executeCustomRequest(body, config, path)
                .map(i -> (Map<String, Object>) i);
    }

    private Stream<Object> executeCustomRequest(Object body, Map<String, Object> config, String path) {
        BedrockConfig conf = new BedrockInvokeConfig(config);

        return executeRequestCommon(body, path, conf);
    }

    private Stream<Object> executeRequestCommon(Object body, String path, BedrockConfig conf) {
        try {
            String bodyString = getBodyAsString(body);
            Map<String, Object> headers = new HashMap<>(conf.getHeaders());
            headers.putIfAbsent("Content-Type", JSON);
            headers.putIfAbsent("accept", ALL);

            headers = calculateAuthorizationHeaders(conf, headers, bodyString.getBytes());

            CloseableHttpClient httpClient = HttpClientBuilder.create().build();

            return ExtendedUtil.getModelItemResultStream(conf.getMethod(), httpClient, bodyString, headers, conf.getEndpoint(), path, List.of())
                    .onClose(() -> Util.close(httpClient));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getBodyAsString(Object body) throws JsonProcessingException {
        if (body == null) {
            return "";
        }
        if (body instanceof String bodyString) {
            return bodyString;
        }
        return OBJECT_MAPPER.writeValueAsString(body);
    }

}
