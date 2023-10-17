package apoc.ml;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;

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
    
    @Test
    public void test() {
        String s = db.executeTransactionally("call apoc.ml.bedrock()", Map.of(), Result::resultAsString);
        System.out.println("s = " + s);
    }


    @Test
    public void test2() {
        // todo - try another model id
    }
}
