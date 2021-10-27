package apoc.periodic;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.io.ByteUnit;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.stream.IntStream;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertTrue;

// created separated test to not affect PeriodicTest through dbms.memory.transaction.database_max_size=10M
public class PeriodicTruncateTest {
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.memory_transaction_database_max_size, ByteUnit.mebiBytes( 10 ));

    @Before
    public void initDb() {
        TestUtil.registerProcedure(db, Periodic.class);
    }

    @Test(expected = QueryExecutionException.class)
    public void testShouldFailsWithTransactionMaxSize() {
        IntStream.range(0, 5)
                .forEach(i -> db.executeTransactionally("unwind range(0, 999) as range create (:Node)-[:REL]->(:Other)"));

        try {
            testCall(db, "call apoc.periodic.truncate", r -> {});
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains("dbms.memory.transaction.database_max_size threshold reached"));
            throw e;
        }
    }
}
