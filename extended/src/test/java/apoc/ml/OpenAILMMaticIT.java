package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ml.OpenAI.API_TYPE_CONF_KEY;
import static apoc.ml.OpenAI.ENDPOINT_CONF_KEY;
import static apoc.ml.OpenAITestResultUtils.assertChatCompletion;
import static apoc.ml.OpenAITestResultUtils.assertCompletion;
import static apoc.util.TestUtil.testCall;

public class OpenAILMMaticIT {

    /*
    Follow the instructions TODO
    */
    
    private String openaiKey;

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
        testCall(db, "CALL apoc.ml.openai.embedding(['Some Text'], $apiKey, $conf)",
                getParams("thenlper/gte-large"),
                OpenAITestResultUtils::assertEmbeddings);
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', $apiKey, $conf)",
                getParams("Meta-Llama/Llama-Guard-7b"),
                (row) -> assertCompletion(row, "text-davinci-003"));
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
            CALL apoc.ml.openai.chat([
            {role:"system", content:"Only answer with a single word"},
            {role:"user", content:"What planet do humans live on?"}
            ],  $apiKey, $conf)
            """, 
                getParams("meta-llama/Llama-2-70b-chat-hf"),
                (row) -> assertChatCompletion(row, "gpt-3.5-turbo"));
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