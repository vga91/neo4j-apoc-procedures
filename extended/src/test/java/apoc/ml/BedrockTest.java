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

import static apoc.ml.Bedrock.ModelId.*;

public class BedrockTest {
    private static final String BEDROCK_PROC = "call apoc.ml.bedrock($id, $payload, $conf)"; 

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
    
    @Test
    public void test() {
        String s = db.executeTransactionally(BEDROCK_PROC,
                "call apoc.ml.bedrock('amazon.titan-embed-text-v1', {inputText: 'Prova' } )", 
                Map.of("id", TITAN_EMBEDDING_G1.getId(),
                        "payload", ), 
                Result::resultAsString);
        System.out.println("s = " + s);
    }
    // todo - test payload as a string..


    @Test
    public void test2() {
        String s = db.executeTransactionally("call apoc.ml.bedrock('amazon.titan-embed-text-v1', '{\"inputText\": \"Prova\" }' )", Map.of(), Result::resultAsString);
        System.out.println("s = " + s);
    }
    
    // todo - try another model id
    
    
    
    // todo - payload can be map or string??
    
    
    
    // todo - try with Authorization
    
    
    // todo - try with jsonPath?
    
    
    // todo - try with custom url in config...
}
