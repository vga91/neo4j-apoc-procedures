package apoc.ml.sagemaker;

import apoc.ml.bedrock.Bedrock;
import apoc.ml.bedrock.BedrockTest;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.ml.bedrock.AWSConfig.ENDPOINT_KEY;
import static apoc.ml.bedrock.AWSConfig.HEADERS_KEY;
import static apoc.ml.bedrock.BedrockInvokeConfig.MODEL;
import static apoc.ml.bedrock.BedrockTestUtil.BEDROCK_CUSTOM_PROC;
import static apoc.ml.bedrock.BedrockTestUtil.STABILITY_AI_BODY;
import static apoc.ml.bedrock.BedrockUtil.STABILITY_STABLE_DIFFUSION_XL;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * TODO: WORK IN PROGRESS
 */
public class SageMakerTest {

    private static final String LIST_MODELS_JSON = "/list-models.json";
    private static final String STABLE_DIFFUSION_JSON = "/stable-diffusion.json";
    private static final String TITAN_EMBED_JSON = "/titan-embed.json";
    private static final Pair<String, String> AUTH_HEADER = Pair.of("Authorization", "AWS V4 mocked");

    private static ClientAndServer mockServer;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void startServer() {
        TestUtil.registerProcedure(db, Bedrock.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);

        mockServer = startClientAndServer(1080);

        Stream.of(
                LIST_MODELS_JSON,
                STABLE_DIFFUSION_JSON,
                TITAN_EMBED_JSON
        ).forEach(BedrockTest::setRequestResponse);
    }

    private static void setRequestResponse(String path) {
        try {
            File urlFileName = new File(getUrlFileName("bedrock" + path).getFile());
            String body = FileUtils.readFileToString(urlFileName, UTF_8);

            mockServer.when(
                            request()
                                    .withPath(path)
                                    .withHeader(AUTH_HEADER.getKey(), AUTH_HEADER.getValue())
                    )
                    .respond(
                            response()
                                    .withStatusCode(200)
                                    .withHeaders(
                                            new Header("Cache-Control", "private, max-age=1000"))
                                    .withBody(body)
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @Test
    public void testStability() {
        testCall(db, "call apoc.ml.bedrock.image($body, $conf)",
                getParams(STABILITY_AI_BODY, STABLE_DIFFUSION_JSON),
                r -> {
                    String base64Image = (String) r.get("base64Image");
                    assertEquals("myBase64image", base64Image);
                });
    }

    private static Map<String, Object> getParams(Map<String, Object> body, String path) {
        Map authHeader = Map.of(AUTH_HEADER.getKey(), AUTH_HEADER.getValue());
        return Map.of("body", body,
                "conf", Map.of(ENDPOINT_KEY, "http://localhost:1080" + path,
                        HEADERS_KEY, authHeader)
        );
    }
}
