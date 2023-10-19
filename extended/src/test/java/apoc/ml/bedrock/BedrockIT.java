package apoc.ml.bedrock;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_AWS_SECRET_KEY;
import static apoc.ml.bedrock.BedrockConfig.METHOD_KEY;
import static apoc.ml.bedrock.BedrockIT.ModelId.*;
import static apoc.ml.bedrock.BedrockInvokeConfig.MODEL_ID;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

/**
 * Todo: extractRegionFromEndpoint()
 * TODO: test with wrong method; e.g. DELETE
 */
public class BedrockIT {

    public static final Map<String, Object> STABILITY_AI_BODY = Map.of(
            "text_prompts", List.of(Map.of("text", "picture of a bird", "weight", 1.0)),
            "cfg_scale", 5,
            "seed", 123,
            "steps", 70,
            "style_preset", "photographic"
    );
    public static final Map<String, Object> JURASSIC_PAYLOAD = Map.of(
            "prompt", "Review: Extremely old cabinets, phone was half broken and full of dust. Bathroom door was broken, bathroom floor was dirty and yellow. Bathroom tiles were falling off. Asked to change my room and the next room was in the same conditions. The most out of date and least maintained hotel i ever been on. Extracted sentiment:",
            "maxTokens", 50,
            "temperature", 0,
            "topP", 1.0
    );
    public static final Map<String, Object> ANTHROPIC_CLAUDE = Map.of(
            "prompt", "\n\nHuman: Hello world\n\nAssistant:",
            "max_tokens_to_sample", 300,
            "temperature", 0.5,
            "top_k", 250,
            "top_p", 1,
            "stop_sequences", List.of("\\n\\nHuman:"),
            "anthropic_version", "bedrock-2023-05-31"
    );
    public static final Map<String, String> TITAN_PAYLOAD = Map.of("inputText", "Test");

    enum ModelId {
        JURASSIC_2_MID("ai21.j2-mid-v1"),
        JURASSIC_2_ULTRA("ai21.j2-ultra-v1"),

        TITAN_EMBEDDING_G1("amazon.titan-embed-text-v1"),
        TITAN_TEXT_G1_EXPRESS("amazon.titan-text-express-v1"),

        CLAUDE_V1("anthropic.claude-v1"),
        CLAUDE_V2("anthropic.claude-v2"),
        CLAUDE_INSTANT("anthropic.claude-instant-v1"),

        STABLE_DIFFUSION_XL("stability.stable-diffusion-xl-v0");

        private final String id;

        ModelId(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }
    }
    
    
    private static final String BEDROCK_PROC = "call apoc.ml.bedrock.custom($payload, $conf)";
    
    private static String keyId;
    private static String secretKey;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    @BeforeClass
    public static void setUp() throws Exception {
        String awsKeyId = "AWS_KEY_ID";
        String awsSecretKey = "AWS_SECRET_KEY";
        keyId = System.getenv(awsKeyId);
        secretKey = System.getenv(awsSecretKey);
        assumeNotNull(awsKeyId + "environment not configured", keyId);
        assumeNotNull(awsSecretKey + " environment configured", secretKey);
        
        apocConfig().setProperty(APOC_AWS_KEY_ID, keyId);
        apocConfig().setProperty(APOC_AWS_SECRET_KEY, secretKey);
        
        TestUtil.registerProcedure(db, Bedrock.class);
    }
    
    @Test
    public void testCustomWithTitanEmbedding() {
        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("payload", TITAN_PAYLOAD,
                        "conf", Map.of(MODEL_ID, TITAN_EMBEDDING_G1.id())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }


    @Test
    public void testStringPayload() {
        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("payload", "{\"inputText\": \"Prova\" }",
                        "conf", Map.of(MODEL_ID, TITAN_EMBEDDING_G1.id())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }

    // TODO - to delete... maybe... just to see the output and create custom procs in case, like OpenAI
    @Test
    public void testCustomWithJurassic() {
//        objectObjectHashMap.put(JURASSIC_2_MID, payload);

        // TODO - prompt and completions for jurassic
        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("payload", JURASSIC_PAYLOAD,
                        "conf", Map.of(MODEL_ID, JURASSIC_2_MID.id())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }

    @Test
    public void testAlls23() {
        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("payload", ANTHROPIC_CLAUDE,
                        "conf", Map.of(MODEL_ID, CLAUDE_V1.id())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }
    
    
    @Test
    public void testAlls2() {
        Map<String, Object> payload = JURASSIC_PAYLOAD;

        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("payload", payload,
                        "conf", Map.of(MODEL_ID, JURASSIC_2_MID.id())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }

    // todo - try another model id
    @Test
    public void testImage() {
        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("payload", STABILITY_AI_BODY,
                        "conf", Map.of(MODEL_ID, STABLE_DIFFUSION_XL.id())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }

    @Test
    public void testStability() {
        testCall(db, "call apoc.ml.bedrock.stability($payload)",
                Map.of("payload", STABILITY_AI_BODY),
                r -> {
                    Object base64Image = r.get("base64Image");
                    System.out.println("base64Image = " + base64Image);
                    assertNotNull(base64Image);
                });
//        String s = db.executeTransactionally("call apoc.ml.bedrock.stability($payload)",
//                Map.of("payload", STABILITY_AI_BODY),
//                Result::resultAsString);
//        System.out.println("s = " + s);
    }

    @Test
    public void testJurassic() {
        String s = db.executeTransactionally("call apoc.ml.bedrock.jurassic($payload)",
                Map.of("payload", JURASSIC_PAYLOAD),
                Result::resultAsString);
        System.out.println("s = " + s);
    }

    @Test
    public void testAnthropicClaude() {
        String s = db.executeTransactionally("call apoc.ml.bedrock.anthropic.claude($payload)",
                Map.of("payload", ANTHROPIC_CLAUDE),
                Result::resultAsString);
        System.out.println("s = " + s);
    }

    @Test
    public void testTitanEmbedding() {
        String s = db.executeTransactionally("call apoc.ml.bedrock.titan.embedding($payload)",
                Map.of("payload", TITAN_PAYLOAD),
                Result::resultAsString);
        System.out.println("s = " + s);
    }
    
    // TODO: provare questo: https://docs.aws.amazon.com/bedrock/latest/APIReference/API_GetModelInvocationLoggingConfiguration.html
    @Test
    public void testGetModelInvocation() {
        Map<String, String> conf = Map.of("endpoint", "https://bedrock.us-east-1.amazonaws.com//logging/modelinvocations",
            METHOD_KEY, "GET");
        String s = db.executeTransactionally("call apoc.ml.bedrock.custom('', $conf)",
                Map.of("conf", conf),
                Result::resultAsString);
        System.out.println("s = " + s);
    }


    //  todo - try another endpoind via custom... e.g. https://docs.aws.amazon.com/bedrock/latest/APIReference/API_DeleteCustomModel.html
        // or - https://docs.aws.amazon.com/bedrock/latest/APIReference/API_GetModelInvocationLoggingConfiguration.html
        // todo - try with null value
    
    
    //  todo - try create

    // todo - payload can be map or string??



    // todo - try with Authorization


    // todo - try with jsonPath?


    // todo - try with custom url in config...


    // todo - forse questo tipo di test va bene anche in BedrockTest
        // todo - try with apocConfig() and wrong confMap --> should work
        
        // todo - try with wrong apocConfig() and right confMap --> should NOT work
        
        // todo - try deactivating apocConfig() and put in confMap
    
    
    
    // todo - procedura con get model...

    // https://bedrock.us-east-1.amazonaws.com/foundation-models
    // https://bedrock.us-east-1.amazonaws.com/custom-models
    @Test
    public void testGetModel() {
        for (BedrockModelsConfig.TypeGet model: BedrockModelsConfig.TypeGet.values()) {
            testResult(db, "call apoc.ml.bedrock.list({typeGet: $type})",
                Map.of("type", model.name()),
                r -> {
                    r.forEachRemaining(row -> {
                        System.out.println("row = " + row);
                        String modelArn = (String) row.get("modelArn");
                        assertTrue(modelArn.contains("arn:aws:bedrock"));
                    });
                });
        }
    }
}
