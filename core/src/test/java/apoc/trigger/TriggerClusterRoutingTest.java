package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        
        cluster.getClusterMembers().forEach(member -> {
            final String logs = member.getLogs();
            System.out.println("XXXmember = " + member);
            System.out.println("YYYlogs = " + logs);
        });
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void testTriggerInstallAllowedOnlyInSysLeaderMember() {
        System.out.println("TriggerClusterRoutingTest.testTriggerInstallAllowedOnlyInSysLeaderMember");
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
    @Ignore
    public void testTriggerAddAllowedOnlyInSysLeaderMember1() {
        final String name = "addTriggerInNeo";
        final String query = "CALL apoc.trigger.add($name, 'RETURN 1',{})";
        testTriggerAgainstNeo4jProtocol(name, query);
    }

    @Test
//    @Ignore
    public void testTriggerInstallAllowedOnlyInSysLeaderMember1() {
        final String name = "installTriggerInNeo";
        final String query = "CALL apoc.trigger.install($name, 'RETURN 1',{})";
        testTriggerAgainstNeo4jProtocol(name, query);
    }

    private static void testTriggerAgainstNeo4jProtocol(String name, String query) {
        System.out.println("cluster.getURI().getPath() = " + cluster.getURI().getPath());


        if (!cluster.sidecar.isRunning()) {
            System.out.println("sidecar not running...");
//            return;
        }
        try (final Session session1 = cluster.getDriver().session()) {
            try {
                session1.run("call apoc.trigger.add(\"prova\", \"return 1\", {})");
            } catch (Exception e) {
                System.out.println("KKKKKK.getMessage() = " + e.getMessage());
            }

            final boolean name1 = session1.run("call apoc.trigger.list() yield name where name = $name return name", Map.of("name", name)).hasNext();
            System.out.println("name1 = " + name1);
        }
        
//        try {
//            for (Neo4jContainerExtension member: cluster.getClusterMembers()) {
//
//                final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
//                if (readReplica.equals(member.getEnvMap().get("NEO4J_dbms_mode"))) {
//                    continue;
//                }
//                
//                System.out.println("member.getContainerName() = " + member.getContainerName());
//                System.out.println("member.getSession() = " + member.getSession());
//                
//                final String neo4jUrl;
//                try {
//                    neo4jUrl = member.getBoltUrl().replace("bolt://", "neo4j://"); 
//                } catch (Exception e) {
//                    System.out.println("getBoltUrle.getMessage() = " + e.getMessage());
//                    continue;
//                }
////                final String neo4jUrl = member.getBoltUrl().replace("bolt://", "neo4j://");
//                final String envBolt = member.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
//                System.out.println("envBolt = " + envBolt);
//                System.out.println("neo4jUrl = " + neo4jUrl);
//                final Driver driver = GraphDatabase.driver("neo4j://" + envBolt, AuthTokens.basic("neo4j", "apoc"),
//                        Config.builder().withResolver(i -> Set.of()).build());
//                final Session session = driver.session();
////                final Session session = driver.session(SessionConfig.forDatabase("neo4j"));
//
//                try {
//                    session.run("call apoc.trigger.add(\"prova\", \"return 1\", {})");
//                } catch (Exception e) {
//                    System.out.println("Te.getMessage() = " + e.getMessage());
//                }
//
//                // todo - decomment...
////                assertFalse(session.run("call apoc.trigger.list() yield name where name = $name return name", Map.of("name", name)).hasNext());
//                
//                
//                // todo -try-with-res
//                session.close();
//                driver.close();
//            }
//            neo4jContainerExtension.getBoltUrl()
//            neo4jContainerExtension.run(query, Map.of("name", name));
//        } catch (RuntimeException e) {
//            System.out.println("Te.getMessage() = " + e.getMessage());
//        }
        
    }

    @Test
    @Ignore
    public void testTriggerRemoveAllowedOnlyInSysLeaderMember1() {
        final String name = "removeTriggerInNeo";
        final String query = "CALL apoc.trigger.remove($name)";

        try (final Session session = cluster.getClusterMembers().stream()
                .map(Neo4jContainerExtension::getSession)
                .filter(Objects::nonNull)
                .filter(TriggerClusterRoutingTest::sysIsLeader)
                        .findAny().orElse(null)) {
            if (session != null) {
                session.run(query, Map.of("name", UUID.randomUUID().toString())); 
            }
            // todo... trigger remove
        }

//        testRemoveTriggerAgainstNeo4jProtocol(name, query);
    }

//    @Test
//    public void testTriggerDropAllowedOnlyInSysLeaderMember1() {
//        final String name = "dropTriggerInNeo";
//        final String query = "CALL apoc.trigger.install($name, 'RETURN 1',{})";
//        testRemoveTriggerAgainstNeo4jProtocol(name, query);
//    }

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
