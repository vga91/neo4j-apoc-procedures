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

import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.ml.OpenAITestResultUtils.assertCompletion;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OpenAILocalAIIT {

//    private String openaiKey;
    
    
    /*
    Follow the instructions provided here: https://localai.io/basics/build/
    Plus download the embedding model, as explained here: https://localai.io/models/#embeddings-bert 
    
    Finally, set the env var `LOCAL_AI_URL=http://localhost:<portNumber>/v1` 
     */
    /* http://localhost:8080/v1 */
    private String localAIUrl;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {
        localAIUrl = System.getenv("LOCAL_AI_URL");
        Assume.assumeNotNull("No LOCAL_AI_URL environment configured", localAIUrl);
//        openaiKey = System.getenv("OPENAI_KEY");
//        Assume.assumeNotNull("No OPENAI_KEY environment configured", openaiKey);
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void getEmbedding() {
        testCall(db, "CALL apoc.ml.openai.embedding(['Some Text'], null, $conf)",
                getParams("text-embedding-ada-002"),
                row -> {
                    assertEquals(0L, row.get("index"));
                    assertEquals("Some Text", row.get("text"));
                    var embedding = (List<Double>) row.get("embedding");
                    assertEquals(384, embedding.size());
                });
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', null, $conf)",
                getParams("ggml-gpt4all-j"),
                (row) -> assertCompletion(row, "ggml-gpt4all-j"));
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
            CALL apoc.ml.openai.chat([
            {role:"system", content:"Only answer with a single word"},
            {role:"user", content:"What planet do humans live on?"}
            ],  null, $conf)
            """, 
                getParams("ggml-gpt4all-j"),
                (row) -> assertChatCompletion(row, "ggml-gpt4all-j"));
    }

    private Map<String, Object> getParams(String model) {
        // todo - openai key?
        return Map.of(// "apiKey", "openaiKey",
                "conf", Map.of(//API_TYPE_CONF_KEY, OpenAIRequestHandler.Type.ANY_SCALE.name(),
                        ENDPOINT_CONF_KEY, localAIUrl,
                        "model", model
                )
        );
    }
}