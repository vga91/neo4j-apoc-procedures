package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.assertion.Assert;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.apocConfig;
import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.ml.OpenAITestResultUtils.assertCompletion;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
To start the test, follow the instructions in this video: https://www.youtube.com/watch?v=V_baaAZMY44

The APIs, especially the `/completions` one, are extremely unstable (i.e. we could get many SocketTimeoutExceptions),
even via e.g. Insomnia,
so it's better to change the `nTokPredict` value, placed in `llmatic.config.json`, to a low value, like `128`

 */
public class OpenAILMMaticIT {
    public static final String MODEL_ID = "meta-llama/Llama-2-70b-chat-hf";

    private String localAIUrl;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty("apoc.http.timeout.connect", 30_000);
        apocConfig().setProperty("apoc.http.timeout.read", 30_000);
        
        localAIUrl = System.getenv("LLM_MATIC_URL");
        Assume.assumeNotNull("No LOCAL_AI_URL environment configured", localAIUrl);
//        openaiKey = System.getenv("OPENAI_KEY");
//        Assume.assumeNotNull("No OPENAI_KEY environment configured", openaiKey);
        TestUtil.registerProcedure(db, OpenAI.class);
        
    }

    @Test
    public void getEmbedding() {
        assertEventually(() -> db.executeTransactionally("CALL apoc.ml.openai.embedding(['Some Text'], $apiKey, $conf)",
                getParams("thenlper/gte-large"),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(0L, row.get("index"));
                    assertEquals("Some Text", row.get("text"));
                    var embedding = (List<Double>) row.get("embedding");
                    assertEquals(5120, embedding.size());
                    assertFalse(r.hasNext());
                    return true;
                }
        ));
    }

    @Test
    public void completion() {
        assertEventually(() -> db.executeTransactionally("CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', $apiKey, $conf)",
                getParams(MODEL_ID),
                (r) -> {
                    Map<String, Object> row = r.next();
                    var result = (Map<String,Object>) row.get("value");
                    assertTrue(result.get("created") instanceof Number);
                    assertTrue(result.containsKey("choices"));
                    var finishReason = (String)((List<Map>) result.get("choices")).get(0).get("finish_reason");
                    assertTrue(finishReason.matches("stop|length"));
                    assertEquals(MODEL_ID, result.get("model"));
                    
                    assertFalse(r.hasNext());
                    return true;
                }
        ));
    }

    @Test
    public void chatCompletion() {
        assertEventually(() -> db.executeTransactionally("""
            CALL apoc.ml.openai.chat([
            {role:"system", content:"Only answer with a single word"},
            {role:"user", content:"What planet do humans live on?"}
            ],  $apiKey, $conf)
            """, 
                getParams(MODEL_ID),
                (r) -> {
                    Map<String, Object> row = r.next();
                    var result = (Map<String,Object>) row.get("value");
                    assertTrue(result.get("created") instanceof Number);
                    assertTrue(result.containsKey("choices"));

                    Map message = ((List<Map<String,Map>>) result.get("choices")).get(0).get("message");
                    assertEquals("assistant", message.get("role"));
                    String text = (String) message.get("content");
                    assertTrue(text != null && !text.isBlank());
                    assertTrue(result.get("model").toString().startsWith(MODEL_ID));
                    
                    assertFalse(r.hasNext());
                    return true;
                }
        ));
    }


    private void assertEventually(Callable<Boolean> booleanCallable) {
        Assert.assertEventually(() -> {
            try {
                return booleanCallable.call();
            } catch (RuntimeException e) {
                System.out.println("e = " + e);
                return false;
            }
        }, val -> val, 60, TimeUnit.SECONDS);
    }

    private Map<String, Object> getParams(String model) {
        return Map.of("apiKey", "openaiKey",
                "conf", Map.of(
                        ENDPOINT_CONF_KEY, localAIUrl,
//                        ENDPOINT_CONF_KEY, "https://api.endpoints.anyscale.com/v1",
                        "model", model
                )
        );
    }
}