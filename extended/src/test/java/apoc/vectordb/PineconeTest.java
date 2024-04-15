package apoc.vectordb;

import apoc.ExtendedApocConfig;
import apoc.util.TestUtil;
import apoc.vectordb.Pinecone;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static java.util.Collections.emptyMap;

public class PineconeTest {
    private String apiKey;
    private String host;
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        apiKey = System.getenv("PINECONE_KEY");
        Assume.assumeNotNull("No PINECONE_KEY environment configured", apiKey);
        
        host = System.getenv("PINECONE_HOST");
        Assume.assumeNotNull("No PINECONE_HOST environment configured", host);
        
        apocConfig().setProperty(ExtendedApocConfig.APOC_PINECONE_KEY, apiKey); 
        TestUtil.registerProcedure(db, Pinecone.class);
    }

    @Test
    public void getEmbedding() {
        String filter = System.getenv("PINECONE_FILTER");
        Assume.assumeNotNull("No PINECONE_FILTER environment configured", host);
        
        testCall(db, "CALL apoc.vectordb.pinecone.query($host, $filter, $conf)", 
                Map.of("host", host, "filter", filter, "conf", emptyMap()),
                r -> {
                    System.out.println("r = " + r);
                });
    }
    
    // todo - auto creation
}
