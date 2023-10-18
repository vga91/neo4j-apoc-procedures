package apoc.ml.bedrock;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_AWS_SECRET_KEY;
import static apoc.ml.bedrock.Bedrock.ModelId.CLAUDE_V1;
import static apoc.ml.bedrock.Bedrock.ModelId.JURASSIC_2_MID;
import static apoc.ml.bedrock.Bedrock.ModelId.STABLE_DIFFUSION_XL;
import static apoc.ml.bedrock.Bedrock.ModelId.TITAN_EMBEDDING_G1;
import static apoc.ml.bedrock.BedrockConfig.MODEL_ID;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

public class BedrockIT {
    private static final String BEDROCK_PROC = "call apoc.ml.bedrock($id, $payload, $conf)";
    
    private static String keyId;
    private static String secretKey;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

//    public BedrockIT() {
//        this.keyId = keyId;
//        this.secretKey = secretKey;
//    }


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
    public void testTitanEmbedding() {
        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("id", TITAN_EMBEDDING_G1.getId(),
                        "payload", Map.of("inputText", "Test"),
                        "conf", Map.of(MODEL_ID, TITAN_EMBEDDING_G1.getId())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }
    // todo - test payload as a string..


    @Test
    public void testStringPayload() {
        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("id", TITAN_EMBEDDING_G1.getId(),
                        "payload", "{\"inputText\": \"Prova\" }",
                        "conf", Map.of(MODEL_ID, TITAN_EMBEDDING_G1.getId())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }

    // TODO - to delete... maybe... just to see the output and create custom procs in case, like OpenAI
    @Test
    public void testAlls() {
        HashMap<Bedrock.ModelId, Map> objectObjectHashMap = new HashMap<>();

        Map<String, Object> payload = Map.of(
                "prompt",  "Review: Extremely old cabinets, phone was half broken and full of dust. Bathroom door was broken, bathroom floor was dirty and yellow. Bathroom tiles were falling off. Asked to change my room and the next room was in the same conditions. The most out of date and least maintained hotel i ever been on. Extracted sentiment:",
                "maxTokens", 50,
                "temperature", 0,
                "topP", 1.0
        );
//        objectObjectHashMap.put(JURASSIC_2_MID, payload);

        // TODO - prompt and completions for jurassic
        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("id", JURASSIC_2_MID.getId(),
                        "payload", payload,
                        "conf", Map.of(MODEL_ID, JURASSIC_2_MID.getId())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }
    

    @Test
    public void testAlls23() {
        Map<String, Object> payload = Map.of(
                "prompt", "\n\nHuman: Hello world\n\nAssistant:", 
                "max_tokens_to_sample", 300, 
                "temperature", 0.5, 
                "top_k", 250, 
                "top_p", 1, 
                "stop_sequences", List.of("\\n\\nHuman:"),
                "anthropic_version", "bedrock-2023-05-31"
        );

        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("id", CLAUDE_V1.getId(),
                        "payload", payload,
                        "conf", Map.of(MODEL_ID, CLAUDE_V1.getId())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }
    
    
    @Test
    public void testAlls2() {
        Map<String, Object> payload = Map.of(
                "prompt",  "Review: Extremely old cabinets, phone was half broken and full of dust. Bathroom door was broken, bathroom floor was dirty and yellow. Bathroom tiles were falling off. Asked to change my room and the next room was in the same conditions. The most out of date and least maintained hotel i ever been on. Extracted sentiment:",
                "maxTokens", 50,
                "temperature", 0,
                "topP", 1.0
        );

        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("id", JURASSIC_2_MID.getId(),
                        "payload", payload,
                        "conf", Map.of(MODEL_ID, JURASSIC_2_MID.getId())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }

    // todo - try another model id
    @Test
    public void testImage() {
        Map<String, Object> payload = Map.of(
                "text_prompts", List.of(Map.of("text", "picture of a bird", "weight", 1.0)), 
                "cfg_scale", 5,
                "seed", 123,
                "steps", 70,
                "style_preset", "photographic"
        );
        String s = db.executeTransactionally(BEDROCK_PROC,
                Map.of("id", STABLE_DIFFUSION_XL.getId(),
                        "payload", payload,
                        "conf", Map.of(MODEL_ID, STABLE_DIFFUSION_XL.getId())
                ),
                Result::resultAsString);
        System.out.println("s = " + s);
    }
    
    
    // todo -  cohere.command-text-v14


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
        String s = db.executeTransactionally("call apoc.ml.bedrock.list($type)",
                Map.of("type", Bedrock.GetModel.FOUNDATION.name(),
                        "payload", "{\"inputText\": \"Prova\" }"),
                Result::resultAsString);
        
        testResult(db, "call apoc.ml.bedrock.list($type)",
                Map.of("type", Bedrock.GetModel.FOUNDATION.name(),
                        "payload", "{\"inputText\": \"Prova\" }"),
        r -> {
            r.forEachRemaining(row -> {
                String modelArn = (String) row.get("modelArn");
                assertTrue(modelArn.contains("arn:aws:bedrock"));
            });
        });
    }
}
