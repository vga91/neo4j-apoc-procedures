package apoc.es;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.load.ConnectionConfig.KEYSTORE_PWD_KEY;
import static apoc.load.ConnectionConfig.KEYSTORE_URL_KEY;
import static org.junit.Assert.assertTrue;

public class ElasticSearchTestCertificate {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, ElasticSearch.class);
    }
    
    // TODO - Create a docker test, if feasible
    @Ignore
    @Test
    public void statsTest() {
        // change credentials, host and keystore configs 
        String userPass = "elastic:PleaseChangeMe";
        final String url = "https://" + userPass + "@localhost:9200";
        TestUtil.testCall(db, "CALL apoc.es.stats($host, $conf)",
                Map.of("host", url, 
                        "conf", 
                        Map.of(KEYSTORE_URL_KEY, "my_keystore.jks", 
                                KEYSTORE_PWD_KEY, "password")), r -> {
                    final Object value = r.get("value");
                    System.out.println("value = " + value);
                    assertTrue(value instanceof Map);
                });
    }
}
