package apoc.ml;

import apoc.ApocConfig;
import apoc.util.TestUtil;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ml.OpenAI.APOC_ML_OPENAI_URL;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * TODO: WORK IN PROGRESS
 */
public class OpenAIAzureIT {

    private static String OPENAI_KEY;
    private static String OPENAI_URL;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    public OpenAIAzureIT() {
    }

    @BeforeClass
    public static void setUp() throws Exception {
        OPENAI_KEY = System.getenv("OPENAI_KEY");
        OPENAI_URL = System.getenv("OPENAI_URL");
        
        // TODO - HOW IN REAL INSTANCE?
        // insert Azure OpenAI key
        Assume.assumeNotNull("No OPENAI_KEY environment configured", OPENAI_KEY);
        Assume.assumeNotNull("No OPENAI_URL environment configured", OPENAI_KEY);
        
        System.setProperty("OPENAI_KEY", OPENAI_KEY);
        ApocConfig.apocConfig().setProperty(APOC_ML_OPENAI_URL, OPENAI_URL); 
        TestUtil.registerProcedure(db, OpenAI.class);
    }

    @Test
    public void getEmbedding() {
        testCall(db, "CALL apoc.ml.openai.embedding(['Some Text'], $apiKey)", Map.of("apiKey", OPENAI_KEY),(row) -> {
            System.out.println("row = " + row);
            assertEquals(0L, row.get("index"));
            assertEquals("Some Text", row.get("text"));
            var embedding = (List<Double>) row.get("embedding");
            assertEquals(1536, embedding.size());
            assertEquals(true, embedding.stream().allMatch(d -> d instanceof Double));
        });
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.openai.completion('What color is the sky? Answer in one word: ', $apiKey)",
                Map.of("apiKey", OPENAI_KEY),(row) -> {
                    System.out.println("row = " + row);
            var result = (Map<String,Object>)row.get("value");
            assertEquals(true, result.get("created") instanceof Number);
            assertEquals(true, result.containsKey("choices"));
            var finishReason = (String)((List<Map>) result.get("choices")).get(0).get("finish_reason");
                assertEquals(true, finishReason.matches("stop|length"));
            String text = (String) ((List<Map>) result.get("choices")).get(0).get("text");
            assertEquals(true, text != null && !text.isBlank());
            assertEquals(true, text.toLowerCase().contains("blue"));
            assertEquals(true, result.containsKey("usage"));
            assertEquals(true, ((Map)result.get("usage")).get("prompt_tokens") instanceof Number);
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
],  $apiKey)
""", Map.of("apiKey", OPENAI_KEY), (row) -> {
            System.out.println("row = " + row);
            var result = (Map<String,Object>)row.get("value");
            assertEquals(true, result.get("created") instanceof Number);
            assertEquals(true, result.containsKey("choices"));

            Map message = ((List<Map<String,Map>>) result.get("choices")).get(0).get("message");
            assertEquals("assistant", message.get("role"));
            // assertEquals("stop", message.get("finish_reason"));
            String text = (String) message.get("content");
            assertEquals(true, text != null && !text.isBlank());


            assertEquals(true, result.containsKey("usage"));
            assertEquals(true, ((Map)result.get("usage")).get("prompt_tokens") instanceof Number);
            assertTrue(result.get("model").toString().startsWith("gpt-3.5-turbo"));
            assertEquals("chat.completion", result.get("object"));
        });
    }
}