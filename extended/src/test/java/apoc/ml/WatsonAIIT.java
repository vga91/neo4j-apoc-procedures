package apoc.ml;

import apoc.ApocConfig;
import apoc.ml.bedrock.Bedrock;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ExtendedApocConfig.APOC_WATSON_PROJECT_ID;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assume.assumeNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Generate accessToken via:
 * ```
 * curl -X POST 'https://iam.cloud.ibm.com/identity/token' -H 'Content-Type: application/x-www-form-urlencoded' -d 'grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=<API_KEY>'
 * ```
 */
public class WatsonAIIT {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static String accessToken;

    @BeforeClass
    public static void setUp() throws Exception {
        String keyIdEnv = "WATSON_ACCESS_TOKEN";
        String projectIdEnv = "WATSON_PROJECT_ID";

        accessToken = System.getenv(keyIdEnv);
        // project_id is mandatory
        String projectId = System.getenv(projectIdEnv);
        
        assumeNotNull(keyIdEnv + "environment not configured", accessToken);
        assumeNotNull(projectIdEnv + "environment not configured", projectId);

        ApocConfig.apocConfig().setProperty(APOC_WATSON_PROJECT_ID, projectId);

        TestUtil.registerProcedure(db, WatsonAI.class);
    }

    @Test
    public void completion() {
        testCall(db, "CALL apoc.ml.watson.completion('What color is the sky? Answer in one word: ', $accessToken)",
                Map.of("accessToken", accessToken),(row) -> {
                    var result = (Map<String,Object>)row.get("value");
                    String generatedText = (String) result.get("generated_text");
                    assertTrue(generatedText.toLowerCase().contains("blue"));
                    assertEquals(12L, result.get("input_token_count"));
                    assertEquals(2L, result.get("generated_token_count"));
                    assertEquals("eos_token", result.get("stop_reason"));
                });
    }

    @Test
    public void chatCompletion() {
        testCall(db, """
                    CALL apoc.ml.watson.chat([
                    {role:"system", content:"Only answer with a single word"},
                    {role:"user", content:"What planet do humans live on?"}
                    ],  $apiKey)""",
                Map.of("apiKey",accessToken), (row) -> {
            var result = (Map<String,Object>)row.get("value");
            String generatedText = (String) result.get("generated_text");
            assertTrue(generatedText.toLowerCase().contains("earth"));
            assertEquals(19L, result.get("input_token_count"));
            assertEquals(2L, result.get("generated_token_count"));
            assertEquals("eos_token", result.get("stop_reason"));
        });
    }
}
