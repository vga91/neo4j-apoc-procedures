package apoc.ml;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assume.assumeNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class OpenAIAzureIT {
    // In Azure, the endpoint can be different 
    private static String OPENAI_EMBEDDING_URL;
    private static String OPENAI_CHAT_URL;
    private static String OPENAI_COMPLETION_URL;
    
    private static String OPENAI_KEY;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        OPENAI_KEY = System.getenv("OPENAI_KEY");
        OPENAI_EMBEDDING_URL = System.getenv("OPENAI_EMBEDDING_URL");
        OPENAI_CHAT_URL = System.getenv("OPENAI_CHAT_URL");
        OPENAI_COMPLETION_URL = System.getenv("OPENAI_COMPLETION_URL");
        
        assumeNotNull("No OPENAI_KEY environment configured", OPENAI_KEY);
        assumeNotNull("No OPENAI_EMBEDDING_URL environment configured", OPENAI_EMBEDDING_URL);
        assumeNotNull("No OPENAI_CHAT_URL environment configured", OPENAI_CHAT_URL);
        assumeNotNull("No OPENAI_COMPLETION_URL environment configured", OPENAI_COMPLETION_URL);
        
        System.setProperty("OPENAI_KEY", OPENAI_KEY);
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void getEmbedding() {
        testCall(db, "CALL apoc.ml.openai.embedding(['Some Text'], $apiKey, $conf)",
                getParams(OPENAI_EMBEDDING_URL),
                (row) -> {
            assertEquals(0L, row.get("index"));
            assertEquals("Some Text", row.get("text"));
            var embedding = (List<Double>) row.get("embedding");
            assertEquals(1536, embedding.size());
        });
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', $apiKey, $conf)",
                getParams(OPENAI_COMPLETION_URL), (row) -> {
            var result = (Map<String,Object>)row.get("value");
            assertTrue(result.get("created") instanceof Number);
            assertTrue(result.containsKey("choices"));
            var finishReason = (String)((List<Map>) result.get("choices")).get(0).get("finish_reason");
            assertTrue(finishReason.matches("stop|length"));
            String text = (String) ((List<Map>) result.get("choices")).get(0).get("text");
            assertTrue(text != null && !text.isBlank());
            assertTrue(text.toLowerCase().contains("blue"));
            assertTrue(result.containsKey("usage"));
            assertTrue(((Map) result.get("usage")).get("prompt_tokens") instanceof Number);
            assertEquals("text-davinci-003", result.get("model"));
            assertEquals("text_completion", result.get("object"));
        });
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
            CALL apoc.ml.openai.chat([
            {role:"system", content:"Only answer with a single word"},
            {role:"user", content:"What planet do humans live on?"}
            ],  $apiKey, $conf)
            """, getParams(OPENAI_CHAT_URL), (row) -> {
            var result = (Map<String,Object>)row.get("value");
            assertTrue(result.get("created") instanceof Number);
            assertTrue(result.containsKey("choices"));

            Map message = ((List<Map<String,Map>>) result.get("choices")).get(0).get("message");
            assertEquals("assistant", message.get("role"));
            String text = (String) message.get("content");
            assertTrue(text != null && !text.isBlank());

            assertTrue(result.containsKey("usage"));
            assertTrue(((Map) result.get("usage")).get("prompt_tokens") instanceof Number);
            assertTrue(result.get("model").toString().startsWith("gpt-35-turbo"));
            assertEquals("chat.completion", result.get("object"));
        });
    }

    private static Map<String, Object> getParams(String endpoint) {
        return Map.of("apiKey", OPENAI_KEY,
                "conf", Map.of("endpoint", endpoint,
                        "authType", OpenAI.AuthType.API_KEY.name())
        );
    }
}