package apoc.ml.bedrock;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;




/**
 * NB: WORK IN PROGRESS. Mock Test, with local endpoint
 * TODO:
 * //  like OpenAITest, via fake url
 * //      try with apocConfig() and wrong confMap --> should work
 * //      try with wrong apocConfig() and right confMap --> should NOT work
 * //      try deactivating apocConfig() and put in confMap
 */

/**
 * Mock tests, with local endpoint
 */
public class BedrockTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        // openaiKey = System.getenv("OPENAI_KEY");
        // Assume.assumeNotNull("No OPENAI_KEY environment configured", openaiKey);
//        var path = Paths.get(getUrlFileName("embeddings").toURI()).getParent().toUri();
//        System.setProperty(OpenAI.APOC_ML_OPENAI_URL, path.toString());
//        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        
        TestUtil.registerProcedure(db, Bedrock.class);
    }
    
    @Test
    public void test() {
        // TODO
    }
}
