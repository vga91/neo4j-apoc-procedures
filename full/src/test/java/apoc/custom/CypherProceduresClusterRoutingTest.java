package apoc.custom;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.custom.CypherProceduresHandler.CUSTOM_PROCEDURES_REFRESH;
import static apoc.util.ClusterTestUtil.checkCorrectRoutingForEachMembers;
import static apoc.util.ClusterTestUtil.checkLeadershipBalanced;
import static apoc.util.TestContainerUtil.singleResultFirstColumn;
import static apoc.uuid.UUIDTest.UUID_TEST_REGEXP;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CypherProceduresClusterRoutingTest {
    private static final int NUM_CORES = 4;
    private static TestcontainersCausalCluster cluster;
    private static Session clusterSession;
    private static List<Neo4jContainerExtension> members;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster(NUM_CORES, 0, Collections.emptyMap(),
                        Map.of("NEO4J_dbms_routing_enabled", "true",
                                CUSTOM_PROCEDURES_REFRESH, "1000"
                        ));

        clusterSession = cluster.getSession();
        members = cluster.getClusterMembers();

        assertEquals(NUM_CORES, members.size());
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember111() {
        // wait until members are balanced, i.e. the system LEADER and the neo4j LEADER aren't in the same member
        checkLeadershipBalanced(clusterSession);

        checkCorrectRoutingForEachMembers(members, (session, container) -> {
            String clusterProcedure = container.getContainerName();
            System.out.println("clusterProcedure = " + clusterProcedure);
            clusterProcedure = clusterProcedure.replace("/", "");
            final String query = String.format(
                    "USE SYSTEM CALL apoc.custom.installProcedure('neo4j', '%s() :: (answer::INT)','RETURN 42 as answer')",
                    clusterProcedure);
            session.writeTransaction(tx -> tx.run(query));
        });

        assertEventually(() -> {
                    String countProcs = "SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'custom' RETURN count(*)";
                    return (Long) singleResultFirstColumn(cluster.getSession(), countProcs);
                },
                (value) -> value == members.size(), 10L, TimeUnit.SECONDS);

        for (Neo4jContainerExtension member : members) {
            assertEventually(() -> {
                        String query = format("CALL custom.%s",
                                member.getContainerName().replace("/", "")
                        );
                        Result r = clusterSession.run(query);
                        assertTrue(r.hasNext());
                        assertEquals(42L, r.single().get("answer").asLong());
                        return true;
                    },
                    (value) -> value, 10L, TimeUnit.SECONDS);
        }
    }
}
