/**
 * WIP: improve assertions
 */
package apoc.ml.bedrock;

import apoc.util.TestUtil;
import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_AWS_SECRET_KEY;
import static apoc.ml.bedrock.BedrockConfig.KEY_ID;
import static apoc.ml.bedrock.BedrockConfig.METHOD_KEY;
import static apoc.ml.bedrock.BedrockConfig.SECRET_KEY;
import static apoc.ml.bedrock.BedrockUtil.ModelId.*;
import static apoc.ml.bedrock.BedrockInvokeConfig.MODEL_ID;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;


public class BedrockIT {

    public static final Map<String, Object> STABILITY_AI_BODY = Map.of(
            "text_prompts", List.of(Map.of("text", "picture of a bird", "weight", 1.0)),
            "cfg_scale", 5,
            "seed", 123,
            "steps", 70,
            "style_preset", "photographic"
    );
    public static final Map<String, Object> JURASSIC_BODY = Map.of(
            "prompt", "Review: Extremely old cabinets, phone was half broken and full of dust. Bathroom door was broken, bathroom floor was dirty and yellow. Bathroom tiles were falling off. Asked to change my room and the next room was in the same conditions. The most out of date and least maintained hotel i ever been on. Extracted sentiment:",
            "maxTokens", 50,
            "temperature", 0,
            "topP", 1.0
    );
    public static final Map<String, Object> ANTHROPIC_CLAUDE_BODY = Map.of(
            "prompt", "\n\nHuman: Hello world\n\nAssistant:",
            "max_tokens_to_sample", 300,
            "temperature", 0.5,
            "top_k", 250,
            "top_p", 1,
            "stop_sequences", List.of("\\n\\nHuman:"),
            "anthropic_version", "bedrock-2023-05-31"
    );
    public static final Map<String, Object> TITAN_BODY = Map.of("inputText", "Test");

    
    public static final String BEDROCK_CUSTOM_PROC = "call apoc.ml.bedrock.custom($body, $conf)";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static String keyId;
    private static String secretKey;
    
    @BeforeClass
    public static void setUp() throws Exception {
        String keyIdEnv = "AWS_KEY_ID";
        String secretKeyEnv = "AWS_SECRET_KEY";
        
        keyId = System.getenv(keyIdEnv);
        secretKey = System.getenv(secretKeyEnv);
        assumeNotNull(keyIdEnv + "environment not configured", keyId);
        assumeNotNull(secretKeyEnv + " environment configured", secretKey);
        
        TestUtil.registerProcedure(db, Bedrock.class);
    }
    
    @Before
    public void before() throws Exception {
        apocConfig().setProperty(APOC_AWS_KEY_ID, keyId);
        apocConfig().setProperty(APOC_AWS_SECRET_KEY, secretKey);
    }
    
    @Test
    public void testCustomWithTitanEmbedding() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", TITAN_BODY,
                        "conf", Map.of(MODEL_ID, TITAN_EMBEDDING_G1.id())
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    assertionsTitanEmbed(value);
                });
    }

    @Test
    public void testAuthViaConfigMap() {
        apocConfig().getConfig().clearProperty(APOC_AWS_KEY_ID);
        apocConfig().getConfig().clearProperty(APOC_AWS_SECRET_KEY);
        
        // check apocConfig correctly cleared, i.e. auth error
        try {
            testCall(db, BEDROCK_CUSTOM_PROC,
                    Map.of("body", TITAN_BODY,
                            "conf", Map.of(MODEL_ID, TITAN_EMBEDDING_G1.id())
                    ),
                    r -> {
                        Map value = (Map) r.get("value");
                        assertionsTitanEmbed(value);
                    });
        } catch (Exception e) {
            String msg = e.getMessage();
            assertTrue("Actual error message is: " + msg, 
                    msg.contains("The security token included in the request is invalid"));
        }
        
        // check that with auth as a conf map it should work
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", TITAN_BODY,
                        "conf", Map.of(MODEL_ID, TITAN_EMBEDDING_G1.id(),
                                KEY_ID, keyId,
                                SECRET_KEY, secretKey)
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    assertionsTitanEmbed(value);
                });
        
    }

    @Test
    public void testCustomWithStringBody() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", "{\"inputText\": \"Test\" }",
                        "conf", Map.of(MODEL_ID, TITAN_EMBEDDING_G1.id())
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    assertionsTitanEmbed(value);
                });
    }
    
    @Test
    public void testCustomWithJurassicUltra() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", JURASSIC_BODY,
                        "conf", Map.of(MODEL_ID, JURASSIC_2_ULTRA.id())
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    assertNotNull(value.get("completions"));
                });
    }

    @Test
    public void testCustomWithAnthropicClaude() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", ANTHROPIC_CLAUDE_BODY,
                        "conf", Map.of(MODEL_ID, CLAUDE_V1.id())
                ),
        r -> {
            Map value = (Map) r.get("value");
            assertNotNull(value.get("completion"));
            assertNotNull(value.get("stop_reason"));
        });
    }
    
    @Test
    public void testCustomWithJurassicMid() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", JURASSIC_BODY,
                        "conf", Map.of(MODEL_ID, JURASSIC_2_MID.id())
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    assertNotNull(value.get("completions"));
                });
    }
    
    @Test
    public void testCustomWithStability() {
        testCall(db, BEDROCK_CUSTOM_PROC,
                Map.of("body", STABILITY_AI_BODY,
                        "conf", Map.of(MODEL_ID, STABLE_DIFFUSION_XL.id())
                ),
                r -> {
                    Map value = (Map) r.get("value");
                    List<Map> artifacts = (List<Map>) value.get("artifacts");
                    String base64Image = (String) artifacts.get(0).get("base64");
                    assertTrue(Base64.isBase64(base64Image));
                });
    }


    @Test
    public void testGetModelInvocationWithNullBody() {
        Map<String, String> conf = Map.of(
                "endpoint", "https://bedrock.us-east-1.amazonaws.com/logging/modelinvocations",
                METHOD_KEY, "GET");

        testCall(db, "call apoc.ml.bedrock.custom(null, $conf)",
                Map.of("conf", conf),
                r -> {
                    Map value = (Map) r.get("value");
                    assertTrue(value.containsKey("loggingConfig"));
                });
    }

    @Test
    public void testWrongMethod() {
        try {
            Map<String, String> conf = Map.of(
                    "endpoint", "https://bedrock.us-east-1.amazonaws.com/logging/modelinvocations",
                    METHOD_KEY, "POST");

            testCall(db, "call apoc.ml.bedrock.custom(null, $conf)",
                    Map.of( "conf", conf),
                    r -> fail());
        } catch (Exception e) {
            String message = e.getMessage();
            assertTrue("Actual message is: "+ message, message.contains("Unexpected character "));
        }
    }

    @Test
    public void testStability() {
        testCall(db, "call apoc.ml.bedrock.stability($body)",
                Map.of("body", STABILITY_AI_BODY),
                r -> {
                    String base64Image = (String) r.get("base64Image");
                    assertTrue(Base64.isBase64(base64Image));
                });
    }

    @Test
    public void testJurassic() {
        testCall(db, "call apoc.ml.bedrock.jurassic($body)",
                Map.of("body", JURASSIC_BODY),
                r -> {
                    assertNotNull(r.get("promptTokens"));
                });
    }

    @Test
    public void testJurassicWithModelMid() {
        testCall(db, "call apoc.ml.bedrock.jurassic($body)",
                Map.of("body", JURASSIC_BODY,
                        "conf", Map.of(MODEL_ID, JURASSIC_2_MID.id())),
                r -> {
                    assertNotNull(r.get("promptTokens"));
                });
    }

    @Test
    public void testAnthropicClaude() {
        testCall(db, "call apoc.ml.bedrock.anthropic.claude($body)",
                Map.of("body", ANTHROPIC_CLAUDE_BODY),
                r -> {
            assertNotNull(r.get("completion"));
            assertNotNull(r.get("stopReason"));
                });
    }

    @Test
    public void testAnthropicClaudeV1() {
        testCall(db, "call apoc.ml.bedrock.anthropic.claude($body, $conf)",
                Map.of("body", ANTHROPIC_CLAUDE_BODY,
                        "conf", Map.of(MODEL_ID, CLAUDE_V1.id())),
                r -> {
            assertNotNull(r.get("completion"));
            assertNotNull(r.get("stopReason"));
                });
    }

    @Test
    public void testAnthropicClaudeInstant() {
        testCall(db, "call apoc.ml.bedrock.anthropic.claude($body, $conf)",
                Map.of("body", ANTHROPIC_CLAUDE_BODY,
                        "conf", Map.of(MODEL_ID, CLAUDE_INSTANT.id())),
                r -> {
            assertNotNull(r.get("completion"));
            assertNotNull(r.get("stopReason"));
                });
    }

    @Test
    public void testTitanEmbedding() {
        testCall(db, "call apoc.ml.bedrock.titan.embed($body)",
                Map.of("body", TITAN_BODY),
                r -> {
                    assertionsTitanEmbed(r);
                });
    }
    
    @Test
    public void testGetModel() {
        for (BedrockGetModelsConfig.TypeGet model: BedrockGetModelsConfig.TypeGet.values()) {
            testResult(db, "call apoc.ml.bedrock.list({typeGet: $type})",
                Map.of("type", model.name()),
                r -> {
                    r.forEachRemaining(row -> {
                        String modelArn = (String) row.get("modelArn");
                        assertTrue(modelArn.contains("arn:aws:bedrock"));
                    });
                });
        }
    }

    private static void assertionsTitanEmbed(Map value) {
        assertNotNull(value.get("inputTextTokenCount"));
        assertNotNull(value.get("embedding"));
    }
}
