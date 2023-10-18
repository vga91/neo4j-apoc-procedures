package apoc.ml.bedrock;

import apoc.ml.bedrock.Bedrock;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;


// TODO - test with fake api like OpenAITest??
//      tramite fake url che ha dei file mockati..
//      oppure provare tipo come LoadJsonTest con il private static ClientAndServer mockServer;


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


        // TODO - document this
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        
        TestUtil.registerProcedure(db, Bedrock.class);
    }
    
    // TODO - MapResult??? 
    //      or as OpenAI, with custom result (i.e. EmbeddingResult??)
    

}
