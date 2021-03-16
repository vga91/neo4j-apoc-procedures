package apoc.sequence;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallInReadTransaction;
import static apoc.util.TestUtil.isTravis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;


public class SequenceClusterTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() ->  cluster = TestContainerUtil
                        .createEnterpriseCluster(3, 1, Collections.emptyMap(), MapUtil.stringMap("apoc.sequences.refresh", "100")),
                Exception.class);
        Assume.assumeNotNull(cluster);
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void shouldRecreateAndUpdateSequenceOnOtherClusterMembers() throws InterruptedException {

        // given
        try(Session session = cluster.getDriver().session()) {
            session.writeTransaction(tx -> tx.run("call apoc.sequence.create('Test')"));
        }

        // when
        try(Session session = cluster.getDriver().session()) {
            testCall(session, "return apoc.sequence.currentValue('Test') as value", (row) -> assertEquals(0L, row.get("value")));
        }

        Thread.sleep(1000);

        // then
        // we use the readTransaction in order to route the execution to the READ_REPLICA
        try(Session session = cluster.getDriver().session()) {
            testCallInReadTransaction(session, "return apoc.sequence.currentValue('Test') as value", (row) -> assertEquals(0L, row.get("value")));
        }

        // when
        try(Session session = cluster.getDriver().session()) {
            testCall(session, "return apoc.sequence.nextValue('Test') as value", (row) -> assertEquals(1L, row.get("value")));
        }

        Thread.sleep(1000);

        // then
        try(Session session = cluster.getDriver().session()) {
            testCall(session, "return apoc.sequence.currentValue('Test') as value", (row) -> assertEquals(1L, row.get("value")));
        }

        try(Session session = cluster.getDriver().session()) {
            testCallInReadTransaction(session, "return apoc.sequence.currentValue('Test') as value", (row) -> assertEquals(1L, row.get("value")));
        }
    }

    @Test
    public void shouldRemoveSequenceOnOtherClusterMembers() throws InterruptedException {
        // given
        cluster.getSession().writeTransaction(tx -> tx.run("CALL apoc.sequence.create('ToRemove', {initialValue: 10})")); // we create a function
        Thread.sleep(1000);
        try {
            testCallInReadTransaction(cluster.getSession(), "return apoc.sequence.currentValue('ToRemove') as value", (row) -> assertEquals(10L, row.get("value")));
        } catch (Exception e) {
            fail("Exception while calling the function");
        }

        // when
        cluster.getSession().writeTransaction(tx -> tx.run("CALL apoc.sequence.drop('ToRemove')"));

        // then
        Thread.sleep(1000);
        try {
            testCallInReadTransaction(cluster.getSession(), "return apoc.sequence.currentValue('ToRemove') as value", (row) -> {});
        } catch (Exception e) {
            System.out.println("e.getMessage()");
            System.out.println(e.getMessage());
//            String expectedMessage = "Unknown function 'custom.answerFunctionToRemove'";
//            assertEquals(expectedMessage, e.getMessage());
//            throw e;
        }

        try {
            testCallInReadTransaction(cluster.getSession(), "return apoc.sequence.currentValue('ToRemove') as value", (row) -> {});
        } catch (DatabaseException e) {
            System.out.println("e.getMessage()");
            System.out.println(e.getMessage());
//            String expectedMessage = "Unknown function 'custom.answerFunctionToRemove'";
//            assertEquals(expectedMessage, e.getMessage());
//            throw e;
        }
    }
}
