package apoc.es;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.util.*;

import static apoc.es.ElasticSearchConfig.VERSION_KEY;

/**
 * @author mh
 * @since 21.05.16
 */
public class ElasticSearchTestToDelete {

    /**
     * TODO: fare un BaseTest dove ci metto i test con gli header
     * poi fare un test con 7 con i test senza header
     * 
     * ed uno con 8, forse senza niente da cambiare??? oppure con altri test con _create invece di _doc???
     * 
     */


    private static final String URL_CONF = "apoc.es.url";
    private static String HTTP_HOST_ADDRESS;
    private static String HTTP_URL_ADDRESS;
    
    public static ElasticsearchContainer elastic;

    private final static String ES_INDEX = "test-index";

//    private final static String ES_TYPE = "test-type";
    private final static String ES_TYPE = "_doc";

    private final static String ES_ID = UUID.randomUUID().toString();


    private static final String DOCUMENT = "{\"name\":\"Neo4j\",\"company\":\"Neo Technology\",\"description\":\"Awesome stuff with a graph database\"}";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static Map<String, Object> defaultParams = Util.map("index", ES_INDEX, "type", ES_TYPE, "id", ES_ID);
    private static Map<String, Object> paramsWithBasicAuth;
    // todo - change to private?
    public static Map<String, Object> basicAuthHeader;
    


    @BeforeClass
    public static void setUp() throws Exception {
        final String password = "myPassword";
        String tag = "8.12.1";
//        String tag = "7.9.2";
        Map<String, String> envMap = Map.of("xpack.security.transport.ssl.enabled", "false",
                "xpack.security.http.ssl.enabled", "false");
        
        elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + tag)
                .withPassword(password)
                .withEnv(envMap);
        elastic.start();

        String httpHostAddress = elastic.getHttpHostAddress();
        HTTP_HOST_ADDRESS = String.format("elastic:%s@%s", 
                password,
                httpHostAddress);
        
        HTTP_URL_ADDRESS = "http://" + HTTP_HOST_ADDRESS;

        defaultParams.put("host", HTTP_HOST_ADDRESS);
        defaultParams.put("url", HTTP_URL_ADDRESS);
        
        // We can authenticate to elastic using the url `<elastic>:<password>@<hostAddress>`
        // or via Basic authentication, i.e. using the url `<hostAddress>` together with the header `Authorization: Basic <token>`
        // where <token> is Base64(<username>:<password>)
        String token = Base64.getEncoder().encodeToString(("elastic:"+ password).getBytes());
        basicAuthHeader = Map.of("Authorization", "Basic " + token);
        
        paramsWithBasicAuth = new HashMap<>(defaultParams);
        paramsWithBasicAuth.put("host", elastic.getHttpHostAddress());

        Map<String, Object> config = Map.of("headers", basicAuthHeader, VERSION_KEY, ElasticSearchHandler.Version.EIGHT.name());
        paramsWithBasicAuth.put("config", config);
//        paramsWithBasicAuth.put("headers", basicAuthHeader);
        
        // todo- only in 8
//        paramsWithBasicAuth.put(VERSION_KEY, ElasticSearchHandler.Version.EIGHT.name());

        TestUtil.registerProcedure(db, ElasticSearch.class);
//        insertDocuments();
    }

    @AfterClass
    public static void tearDown() {
        elastic.stop();
        db.shutdown();
    }

    /**
     * Default params (host, index, type, id) + payload
     *
     * @param payload
     * @return
     */
    private static Map<String, Object> createDefaultProcedureParametersWithPayloadAndId(String payload, String id) {
        try {
            Map mapPayload = JsonUtil.OBJECT_MAPPER.readValue(payload, Map.class);
            return addPayloadAndIdToParams(paramsWithBasicAuth, mapPayload, id);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static Map<String, Object> addPayloadAndIdToParams(Map<String, Object> params, Object payload, String id) {
            return Util.merge(params, Util.map("payload", payload, "id", id));
    }
    
//    private static void insertDocuments() throws JsonProcessingException {
//        Map<String, Object> params = createDefaultProcedureParametersWithPayloadAndId("{\"procedurePackage\":\"es\",\"procedureName\":\"get\",\"procedureDescription\":\"perform a GET operation to ElasticSearch\"}", UUID.randomUUID().toString());
////        TestUtil.testCall(db, "CALL apoc.es.put($host,$index,null,$id,'refresh=true',$payload) yield value", params, r -> {
//        TestUtil.testCall(db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload, $config) yield value", params, r -> {
//            Object created = extractValueFromResponse(r, "$.result");
//            assertEquals("created", created);
//        });
//
//        params = createDefaultProcedureParametersWithPayloadAndId("{\"procedurePackage\":\"es\",\"procedureName\":\"post\",\"procedureDescription\":\"perform a POST operation to ElasticSearch\"}", UUID.randomUUID().toString());
//        TestUtil.testCall(db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload, $config) yield value", params, r -> {
//            Object created = extractValueFromResponse(r, "$.result");
//            assertEquals("created", created);
//        });
//
//        params = createDefaultProcedureParametersWithPayloadAndId(DOCUMENT, ES_ID);
//        TestUtil.testCall(db, "CALL apoc.es.put($host,$index,$type,$id,'refresh=true',$payload, $config) yield value", params, r -> {
//            Object created = extractValueFromResponse(r, "$.result");
//            assertEquals("created", created);
//        });
//    }
//
//    private static Object extractValueFromResponse(Map response, String jsonPath) {
//        Object jsonResponse = response.get("value");
//        assertNotNull(jsonResponse);
//
//        String json = JsonPath.parse(jsonResponse).jsonString();
//        Object value = JsonPath.parse(json, JSON_PATH_CONFIG).read(jsonPath);
//
//        return value;
//    }
//
//
//
//
//
//    private static Consumer<Map<String, Object>> commonEsGetConsumer() {
//        return r -> {
//            Object name = extractValueFromResponse(r, "$._source.name");
//            assertEquals("Neo4j", name);
//        };
//    }
//
//    private static Consumer<Map<String, Object>> commonEsStatsConsumer() {
//        return r -> {
//            assertNotNull(r.get("value"));
//
//            Object numOfDocs = extractValueFromResponse(r, "$._all.total.docs.count");
//            assertEquals(3, numOfDocs);
//        };
//    }
}
