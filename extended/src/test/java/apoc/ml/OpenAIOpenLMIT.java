package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;
import static apoc.ml.OpenAI.MODEL_CONF_KEY;
import static apoc.ml.OpenAI.PATH_CONF_KEY;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OpenLM allows
 * 
 * It works only for `\completion` API
 */
public class OpenAIOpenLMIT {
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {

        TestUtil.registerProcedure(db, OpenAI.class);
    }

//    @Test
//    public void getEmbedding() {
//        // https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2
//        
//        // TODO?? -- JsonUtil.loadJson(url, headers, payload, "$", true, List.of(), urlAccessChecker)??
//        testCall(db, "CALL apoc.ml.openai.embedding(['Some Text'], $apiKey, $conf)",
//                getParams("thenlper/gte-large"),
//                OpenAITestResultUtils::assertEmbeddings);
//    }

    @Test
    public void completionWithHuggingFace() {
        String huggingFaceApiKey = System.getenv("HF_API_TOKEN");
        Assume.assumeNotNull("No HF_API_TOKEN environment configured", huggingFaceApiKey);
        
        String modelId = "gpt2";
        Map<String, String> conf = Map.of(ENDPOINT_CONF_KEY, "https://api-inference.huggingface.co/models/" + modelId,
                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.HUGGINGFACE.name()
                ,
                "model", modelId
        );
        testCall(db, "CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', $apiKey, $conf)",
                Map.of("conf", conf, "apiKey", huggingFaceApiKey),
//                getParams("Meta-Llama/Llama-Guard-7b"),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    String generatedText = (String) result.get("generated_text");
                    assertTrue(generatedText.toLowerCase().contains("blue"),
                            "Actual generatedText is " + generatedText);
                });
    }

    @Test
    public void completionWithCohere() {
        String cohereApiKey = System.getenv("COHERE_API_TOKEN");
        Assume.assumeNotNull("No COHERE_API_TOKEN environment configured", cohereApiKey);
        
        String modelId = "command";
        Map<String, String> conf = Map.of(ENDPOINT_CONF_KEY, "https://api.cohere.ai/v1/generate",
                PATH_CONF_KEY, "",
//                API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.COHERE.name(),
                // API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.COHERE.name()
                MODEL_CONF_KEY, modelId
        );
        
        testCall(db, "CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', $apiKey, $conf)",
                Map.of("conf", conf, "apiKey", cohereApiKey),
                (row) -> {
                    var result = (Map<String,Object>) row.get("value");
                    Map meta = (Map) result.get("meta");
                    assertEquals(Set.of("warnings", "billed_units", "api_version"), meta.keySet());

                    List<Map> generations = (List<Map>) result.get("generations");
                    assertEquals(1, generations.size());
                    assertEquals(Set.of("finish_reason", "id", "text"), generations.get(0).keySet());
                    
                    assertTrue(result.get("id") instanceof String);
                    assertTrue(result.get("prompt") instanceof String);
                });
    }

//    @Test
//    public void chatCompletion() {
//        testCall(db, """
//                        CALL apoc.ml.openai.chat([
//                                    {
//                                    	inputs: "non so"
//                                    }
//                        ],  $apiKey, $conf)
//                        """, 
//                getParams("meta-llama/Llama-2-70b-chat-hf"),
//                (row) -> assertChatCompletion(row, "gpt2"));
//    }

    private Map<String, Object> getParams(String model) {
        return Map.of(//"apiKey", openaiKey,
                "conf", Map.of(//ENDPOINT_CONF_KEY, "https://api-inference.huggingface.co/models/gpt2",
                        API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.HUGGINGFACE.name(),
                        "model", model
                )
        );
    }
}