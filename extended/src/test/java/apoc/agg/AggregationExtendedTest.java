package apoc.agg;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AggregationExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, AggregationExtended.class);
        
//        try (Transaction tx = db.beginTx()) {
            db.executeTransactionally("UNWIND range(0,20) AS id CREATE (:Person {id: 'index' + id})");
//            tx.commit();
//        }
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void test() {
        testCall(db,
                "MATCH (n:Person) RETURN apoc.agg.row(n.id, '$curr = \"index10\"') AS row",
                (row) -> {
                    assertEquals(10L, row.get("row"));
                });
    }
}
