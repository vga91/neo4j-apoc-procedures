package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static apoc.trigger.Trigger.SYS_NON_LEADER_ERROR;
import static apoc.trigger.TriggerNewProcedures.TRIGGER_NOT_ROUTED_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TriggerClusterRoutingTest {

    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster(3, 1, Collections.emptyMap(), Map.of(
                        "NEO4J_dbms_routing_enabled", "true",
                        "apoc.trigger.enabled", "true"
                ));
        System.out.println("TriggerClusterRoutingTest.setupCluster");
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void testTriggerInstallAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1',{})";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR);
    }

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.add($name, 'RETURN 1',{})";
        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR);
    }

    private static void triggerInSysLeaderMemberCommon(String query, String triggerNotRoutedError) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(4, members.size());
        for (Neo4jContainerExtension container: members){
            // we skip READ_REPLICA members
            final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
            try (final Session session = container.getSession()) {
                if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || session == null) {
                    continue;
                }
                if (sysIsLeader(session)) {
                    session.run(query, Map.of("name", UUID.randomUUID().toString()));
                } else {
                    try {
                        TestContainerUtil.testCall(session, query,
                                Map.of("name", UUID.randomUUID().toString()),
                                row -> fail("Should fail because of non leader trigger addition"));
                    } catch (RuntimeException e) {
                        String errorMsg = e.getMessage();
                        assertTrue("The actual message is: " + errorMsg, errorMsg.contains(triggerNotRoutedError));
                    }
                }
            }
        }
    }

    private static boolean sysIsLeader(Session session) {
        final String systemRole = TestContainerUtil.singleResultFirstColumn(session, "CALL dbms.cluster.role('system')");
        return "LEADER".equals(systemRole);
    }

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember1() {
        final String name = "addTriggerInNeo";
        final String query = "CALL apoc.trigger.add($name, 'RETURN 1',{})";
        testTriggerAgainstNeo4jProtocol(name, query);
    }

    @Test
    public void testTriggerInstallAllowedOnlyInSysLeaderMember1() {
        final String name = "installTriggerInNeo";
        final String query = "CALL apoc.trigger.install($name, 'RETURN 1',{})";
        testTriggerAgainstNeo4jProtocol(name, query);
    }

    private static void testTriggerAgainstNeo4jProtocol(String name, String query) {
        System.out.println("cluster.getURI().getPath() = " + cluster.getURI().getPath());
//        try {
            for (Neo4jContainerExtension member: cluster.getClusterMembers()) {
                final String neo4jUrl = member.getBoltUrl().replace("bolt://", "neo4j://");
                System.out.println("neo4jUrl = " + neo4jUrl);
                final Driver driver = GraphDatabase.driver(neo4jUrl, AuthTokens.basic("neo4j", "apoc"));
                final Session session = driver.session(SessionConfig.forDatabase("neo4j"));

                try {
                    session.run("call apoc.trigger.add(\"prova\", \"return 1\", {})");
                } catch (Exception e) {
                    System.out.println("Te.getMessage() = " + e.getMessage());
                }

                assertFalse(session.run("call apoc.trigger.list() yield name where name = $name return name", Map.of("name", name)).hasNext());
                
                session.close();
                driver.close();
            }
//            neo4jContainerExtension.getBoltUrl()
//            neo4jContainerExtension.run(query, Map.of("name", name));
//        } catch (RuntimeException e) {
//            System.out.println("Te.getMessage() = " + e.getMessage());
//        }
        
    }

    @Test
    public void testTriggerRemoveAllowedOnlyInSysLeaderMember1() {
        final String name = "removeTriggerInNeo";
        final String query = "CALL apoc.trigger.remove($name)";

        try (final Session session = cluster.getClusterMembers().stream()
                .map(Neo4jContainerExtension::getSession)
                .filter(TriggerClusterRoutingTest::sysIsLeader)
                        .findAny().orElse(null)) {
            if (session != null) {
                session.run(query, Map.of("name", UUID.randomUUID().toString())); 
            }
            // todo... trigger remove
        }

        testRemoveTriggerAgainstNeo4jProtocol(name, query);
    }

    @Test
    public void testTriggerDropAllowedOnlyInSysLeaderMember1() {
        final String name = "dropTriggerInNeo";
        final String query = "CALL apoc.trigger.install($name, 'RETURN 1',{})";
        testRemoveTriggerAgainstNeo4jProtocol(name, query);
    }

    private static void testRemoveTriggerAgainstNeo4jProtocol(String name, String query) {
        try {
            cluster.getSession().run(query, Map.of("name", name));
        } catch (RuntimeException e) {
            System.out.println("Te.getMessage() = " + e.getMessage());
        }
        assertFalse(cluster.getSession()
                .run("call apoc.trigger.list() yield name where name = $name return name", Map.of("name", name)).hasNext());
    }
}
