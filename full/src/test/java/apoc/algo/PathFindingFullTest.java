package apoc.algo;

import apoc.bolt.Bolt;
import apoc.diff.DiffFull;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.nustaq.serialization.util.test;

import java.util.Map;

import static apoc.algo.AlgoUtil.SETUP_GEO;
import static apoc.util.TestUtil.testResult;

public class PathFindingFullTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, DiffFull.class, PathFindingFull.class, Bolt.class);
    }

    // tests empty--> CALL apoc.diff.graphs("MATCH (p) RETURN p", "MATCH (p) RETURN p", {source: {params: {id: 1}},  dest: {target: {type: 'DATABASE', value: 'neo4j'}}, findById: true})
    // test empty --> CALL apoc.diff.graphs("MATCH (p) RETURN p", "MATCH (p) RETURN p", {dest: {target: {type: 'DATABASE', value: 'neo4j'}}, findById: true})
    
    @Test
    public void bbb() {
        db.executeTransactionally("CREATE (n:Something {a: 2})-[:KNOWS]->(:JJJ)");
//        db.executeTransactionally("create constraint for (n:Something) require n.a is unique");
        System.out.println("res - " +
//                db.executeTransactionally("CALL apoc.diff.graphs(\"MATCH (p) RETURN p\", \"MATCH (p) RETURN p\", {dest: {target: {type: 'DATABASE', value: 'neo4j'}}, findById: true})", Map.of(),
                db.executeTransactionally("CALL apoc.diff.graphs(\"MATCH p=()-[:KNOWS]->() RETURN p\", \"MATCH p=()-[:KNOWS]->() RETURN p\", {dest: {target: {type: 'DATABASE', value: 'neo4j'}}, findById: true})", Map.of(),
                        Result::resultAsString));
        
    }    
    
    @Test
    public void ccc() {
        db.executeTransactionally("CREATE (n:Something {a: 2})-[:KNOWS]->(:JJJ)");
//        db.executeTransactionally("create constraint for (n:Something) require n.a is unique");
        System.out.println("res - " +
//                db.executeTransactionally("CALL apoc.diff.graphs(\"MATCH (p) RETURN p\", \"MATCH (p) RETURN p\", {dest: {target: {type: 'DATABASE', value: 'neo4j'}}, findById: true})", Map.of(),
                db.executeTransactionally("CALL apoc.diff.graphs(\"MATCH p=()-[:KNOWS]->() RETURN p\", \"MATCH p=()-[:KNOWS]->() RETURN p\", {dest: {target: {type: 'DATABASE', value: 'neo4j'}}, findById: true})", Map.of(),
                        Result::resultAsString));
        
    }
    
    @Test
    public void testAStarWithPoint() {
        db.executeTransactionally(SETUP_GEO);
        testResult(db,
                "MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'}) " +
                        "CALL apoc.algo.aStarWithPoint(from, to, 'DIRECT', 'dist', 'coords') yield path, weight " +
                        "RETURN path, weight" ,
                AlgoUtil::assertAStarResult
        );
    }
}
