package apoc.ml;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.ml.bedrock.AwsSignatureV4Generator;
import apoc.ml.bedrock.BedrockConfig;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ExtendedApocConfig.APOC_ML_OPENAI_URL;
import static apoc.ExtendedApocConfig.APOC_OPENAI_KEY;
import static apoc.ExtendedApocConfig.APOC_WATSON_KEY;
import static apoc.ExtendedApocConfig.APOC_WATSON_PROJECT_ID;
import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;
import static apoc.util.JsonUtil.OBJECT_MAPPER;

@Extended
public class WatsonAI {
    @Context
    public ApocConfig apocConfig;

    @Context
    public URLAccessChecker urlAccessChecker;

    public static final String APOC_ML_WATSON_URL = "apoc.ml.watson.url";

    @Procedure("apoc.ml.watson.completion")
    @Description("apoc.ml.watson.completion(prompt, accessToken, $configuration) - prompts the completion API")
    public Stream<MapResult> completion(@Name("prompt") String prompt, @Name("accessToken") String accessToken, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        return getMapResultStream(accessToken, configuration, prompt);
    }

    @Procedure("apoc.ml.watson.chat")
    @Description("apoc.ml.watson.chat(messages, accessToken, $configuration) - prompts the completion API")
    public Stream<MapResult> chatCompletion(@Name("messages") List<Map<String, Object>> messages, @Name("accessToken") String accessToken, @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) throws Exception {
        String prompt = messages.stream()
                // todo - check che devono avere entrambi...
                .map(i -> {
                    Object role = i.get("role");
                    Object content = i.get("content");
                    if (role == null || content == null) {
                        throw new RuntimeException("TODO");
                    }
                    return role + ": " + content;
                })
                .collect(Collectors.joining("\n\n"));
        
        return getMapResultStream(accessToken, configuration, prompt);
    }
    
    
    private Stream<MapResult> getMapResultStream(String accessToken, Map<String, Object> configuration, String prompt) {
        Stream<Object> resultStream = executeRequest(accessToken, configuration, "input", prompt, "$.results[0]"/*, apocConfig, urlAccessChecker*/);

        return resultStream.map(v -> (Map<String, Object>) v).map(MapResult::new);
    }

    private Stream<Object> executeRequest(String accessToken, Map<String, Object> configuration, String key, Object inputs, String jsonPath /*, WatsonConfig conf*/) {
        try {

            accessToken = (String) configuration.getOrDefault(APOC_ML_WATSON_URL, apocConfig.getString(APOC_WATSON_KEY, accessToken));
            
            
            
            String projectId = (String) configuration.getOrDefault("project_id", apocConfig.getString(APOC_WATSON_PROJECT_ID, accessToken));

            // the body request has to contain space_id or project_id or wml_instance_crn,
            // in case is missing we put the project_id from apoc.conf, otherwise we throw an exception
            if (!configuration.containsKey("project_id") && !configuration.containsKey("space_id") && !configuration.containsKey("wml_instance_crn")) {
                String apocConfProjectId = apocConfig.getString(APOC_WATSON_PROJECT_ID, null);
                if (apocConfProjectId == null) {
                    throw new RuntimeException("eccezioen TODO");
                }
                configuration.put("project_id", apocConfProjectId);
            }
            
//          
//            
//            Object projectId = configuration.putIfAbsent("project_id", apocConfig.getString(APOC_WATSON_PROJECT_ID, null));
            // todo - necessario?
            // vedere cosa succede senza..
//            if (projectId == null) {
//                
//            }

            String bodyString = null;
//            if (body != null) {
//                // to be used e.g to add body entries to `apoc.ml.bedrock.completion` 
//                body.putAll(conf.getBody());
//                bodyString = OBJECT_MAPPER.writeValueAsString(body);
//            }

            
            // "ibm/granite-13b-instruct-v2"
            String endpoint = getEndpoint(configuration);

            var config = new HashMap<>(configuration);
            Stream.of(ENDPOINT_CONF_KEY).forEach(config::remove);
            config.putIfAbsent("model_id", "ibm/granite-13b-instruct-v2");
            // todo - putIfAbsent?
            config.put(key, inputs);
            
            
            // TODO - Map.of()
            Map<String, Object> headers = new HashMap<>();
            headers.put("Content-Type", "application/json");
            headers.put("accept", "application/json");
            headers.put("Authorization", "Bearer " + accessToken);

            // conf.getJsonPath()??


            String payload = JsonUtil.OBJECT_MAPPER.writeValueAsString(config);

            return JsonUtil.loadJson(endpoint, headers, payload, jsonPath, true, List.of(), urlAccessChecker);

            // return JsonUtil.loadJson(url, headers, payload, jsonPath, true, List.of(), urlAccessChecker);
            
//            return JsonUtil.loadJson(conf.getEndpoint(), conf.getHeaders(), bodyString, conf.getJsonPath(), true, List.of(), urlAccessChecker);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public String getEndpoint(Map<String, Object> config/*, ApocConfig apocConfig*/) {
        return (String) config.getOrDefault(ENDPOINT_CONF_KEY,
                apocConfig.getString(APOC_ML_WATSON_URL, "https://eu-de.ml.cloud.ibm.com/ml/v1-beta/generation/text?version=2023-05-29")
        );
    }
    
    // todo - dire che non ci sono modelli embedding
    //  tutti restituiscono stringhe
    //      https://www.ibm.com/docs/en/cloud-paks/cp-data/4.8.x?topic=models-supported-foundation
    
    
    
    // TODO --> https://eu-de.ml.cloud.ibm.com/ml/v1-beta/generation/text?version=2023-05-29
    //  COSA CAMBIA, region, version, ..
    
    // TODO - tramite apoc.conf anche.. --> APOC_ML_WATSON_URL

//    public static class WatsonConfig {
//        private final String endpoint;
//        private final Map<String, Object> parameters;
//        
//        
//    }
    
    
    /*
    curl "https://eu-de.ml.cloud.ibm.com/ml/v1-beta/generation/text?version=2023-05-29" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'Authorization: Bearer YOUR_ACCESS_TOKEN' \
  -d '{
  "model_id": "ibm/granite-13b-instruct-v2",
  "input": "Generate a 5 sentence marketing message for a company with the given characteristics.\n\nDetails Characteristics:\n\nCompany - Golden Bank\n\nOffer includes - no fees, 2% interest rate, no minimum balance\n\nTone - informative\n\nResponse requested - click the link\n\nEnd date - July 15\n\nEmail ",
  "parameters": {
    "decoding_method": "sample",
    "max_new_tokens": 200,
    "min_new_tokens": 50,
    "random_seed": 111,
    "stop_sequences": [],
    "temperature": 0.8,
    "top_k": 50,
    "top_p": 1,
    "repetition_penalty": 2
  },
  "project_id": "17b41fa1-b4c4-4687-b78c-f43957453168"
}'
     */

}
