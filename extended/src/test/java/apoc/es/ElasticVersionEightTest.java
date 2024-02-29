package apoc.es;

import apoc.util.Util;
import org.junit.BeforeClass;

import java.util.Map;

import static apoc.es.ElasticSearchConfig.VERSION_KEY;

public class ElasticVersionEightTest extends ElasticSearchTest {
    public static final String ES_TYPE = "_doc";

    @BeforeClass
    public static void setUp() throws Exception {
        Map<String, Object> config = Map.of("headers", basicAuthHeader, VERSION_KEY, ElasticSearchHandler.Version.DEFAULT.name());
        Map<String, Object> params = Util.map("index", ES_INDEX,
                "id", ES_ID, "type", ES_TYPE, "config", config);
        
        String tag = "8.12.1";
        Map<String, String> envMap = Map.of("xpack.security.http.ssl.enabled", "false");

        getElasticContainer(tag, envMap, params);
    }

    @Override
    String getEsType() {
        return ES_TYPE;
    }
}
