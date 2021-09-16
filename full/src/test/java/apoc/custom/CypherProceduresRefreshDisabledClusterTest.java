package apoc.custom;

import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class CypherProceduresRefreshDisabledClusterTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() ->  cluster = TestContainerUtil
                .createEnterpriseCluster(3, 1, Collections.emptyMap(), 
                        MapUtil.stringMap("apoc.custom.procedures.refresh", "100", "apoc.custom.procedures.check", "false")),
                Exception.class);
        Assume.assumeNotNull(cluster);
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test(expected = RuntimeException.class)
    public void shouldNotRecreateCustomFunctionsOnOtherClusterMembers() throws InterruptedException {
        // given
        try(Session session = cluster.getDriver().session()) {
            session.writeTransaction(tx -> tx.run("call apoc.custom.asFunction('answer1', 'RETURN 42 as answer')")); // we create a function
        }
        
        try(Session session = cluster.getDriver().session()) {
            TestContainerUtil.testCall(session, "return custom.answer1() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        }

        Thread.sleep(1000);

        try {
            TestContainerUtil.testCallInReadTransaction(cluster.getSession(), "call custom.answer1()", (row) -> fail("Procedure should not exists"));
        } catch (Exception e) {
            String expectedMessage = "There is no procedure with the name `custom.answer1` registered for this database instance. " +
                    "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.";
            assertEquals(expectedMessage, e.getMessage());
            throw e;
        }
    }
}
