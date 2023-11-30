package apoc.ml.sagemaker;

import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.apocConfig;
import static apoc.ExtendedApocConfig.APOC_AWS_KEY_ID;
import static apoc.ExtendedApocConfig.APOC_AWS_SECRET_KEY;
import static apoc.ml.bedrock.AWSConfig.HEADERS_KEY;
import static apoc.ml.bedrock.AWSConfig.REGION_KEY;
import static apoc.ml.bedrock.SageMakerConfig.ENDPOINT_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class SageMakerIT {
    public static final String BODY = "As far as I am concerned, I will";
    
    // TODO - change with env var..
    public static final String ENDPOINT_GPT_2 = "Endpoint-GPT-2-1-0";
//    public static final Map<String, Object> TITAN_BODY = Map.of("inputText", "Test");

    public static final String SAGEMAKER_CUSTOM_PROC = "CALL apoc.ml.sagemaker.custom($body, $conf)";
    
    private static final Map<String, Object> params;

    private static final Map<String, Object> config;

    static {
        config = Map.of(ENDPOINT_NAME_KEY, ENDPOINT_GPT_2,
                HEADERS_KEY, Map.of("Content-Type", "application/x-text"),
                REGION_KEY, "eu-central-1"
        );
        params = Map.of("body", BODY,
                // todo - region with env var
                "conf", config
        );
    }

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

        TestUtil.registerProcedure(db, SageMaker.class);
    }

    @Before
    public void before() throws Exception {
        apocConfig().setProperty(APOC_AWS_KEY_ID, keyId);
        apocConfig().setProperty(APOC_AWS_SECRET_KEY, secretKey);
        apocConfig().setProperty("apoc.http.timeout.connect", 5_000);
        apocConfig().setProperty("apoc.http.timeout.read", 5_000);
    }
    
    @Test
    public void testCustom() {
        assertEventually(() -> {
            try {
                return db.executeTransactionally(SAGEMAKER_CUSTOM_PROC, params, r -> {
                    Map value = Iterators.single(r.columnAs("value"));
                    String generatedText = (String) value.get("generated_text");
                    assertThat(generatedText).contains(BODY);
                    return true;
                });
            } catch (Exception e) {
                System.out.println("e = " + e);
                return false;
            }
        }, val -> val, 30, TimeUnit.SECONDS);
    }
    
    // todo - find another provider?..
    @Test
    public void testChat() {
        assertEventually(() -> {
            try {
                return db.executeTransactionally("CALL apoc.ml.sagemaker.chat($messages, $conf)", 
                        Map.of("messages", List.of(), "conf", config), r -> {
                    Map value = Iterators.single(r.columnAs("value"));
                    String generatedText = (String) value.get("generated_text");
                    System.out.println("generatedText = " + generatedText);
                    assertThat(generatedText).contains(BODY);
                    return true;
                });
            } catch (Exception e) {
                System.out.println("e = " + e);
                return false;
            }
        }, val -> val, 30, TimeUnit.SECONDS);
    }
    
    @Test
    public void testChatCompletion() {
        assertEventually(() -> {
            try {
                return db.executeTransactionally("CALL apoc.ml.sagemaker.completion($prompt, $conf)",
                        Map.of("prompt", BODY, "conf", config), r -> {
                    Map value = Iterators.single(r.columnAs("value"));
                    String generatedText = (String) value.get("generated_text");
                    assertThat(generatedText).contains(BODY);
                    return true;
                });
            } catch (Exception e) {
                System.out.println("e = " + e);
                return false;
            }
        }, val -> val, 30, TimeUnit.SECONDS);
    }
    
    @Test
    public void testEmbedding() {
        String text1 = "How is the weather today?";
        String text2 = "What's the color of an orange?";
        String text3 = "Are you open on weekends?";
        List<String> text = List.of(text1, text2, text3);
        
        assertEventually(() -> {
            try {
                return db.executeTransactionally("CALL apoc.ml.sagemaker.embedding($texts, $conf)", 
                        Map.of("texts", text, "conf", Map.of(REGION_KEY, "eu-central-1") ), r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(text1, row.get("text"));
                    assertEquals(0L, row.get("index"));
                    assertEquals(768, ((List) row.get("embedding")).size());
                    
                    row = r.next();
                    assertEquals(text2, row.get("text"));
                    assertEquals(1L, row.get("index"));
                    assertEquals(768, ((List) row.get("embedding")).size());
                    
                    row = r.next();
                    assertEquals(text3, row.get("text"));
                    assertEquals(2L, row.get("index"));
                    assertEquals(768, ((List) row.get("embedding")).size());
                    return true;
                });
            } catch (Exception e) {
                System.out.println("e = " + e);
                return false;
            }
        }, val -> val, 30, TimeUnit.SECONDS);
    }
}
