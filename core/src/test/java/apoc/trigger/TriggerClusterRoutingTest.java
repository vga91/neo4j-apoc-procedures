package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static apoc.trigger.Trigger.SYS_NON_LEADER_ERROR;
import static apoc.trigger.TriggerNewProcedures.TRIGGER_NOT_ROUTED_ERROR;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertEquals;
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
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    // TODO: making sure that a session against "system" can install triggers

    // TODO: making sure that a session against "system" can drop triggers
    
    // TODO: fabric tests

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.add($name, 'RETURN 1', {})";
        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR, GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerRemoveAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.remove($name)";
        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR, GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerInstallAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR, GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testTriggerDropAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.drop('neo4j', $name)";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR, GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
    }

    private static void triggerInSysLeaderMemberCommon(String query, String triggerNotRoutedError, String dbName) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(4, members.size());
        for (Neo4jContainerExtension container: members) {
            // we skip READ_REPLICA members
            final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
            final Driver driver = container.getDriver();
            if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || driver == null) {
                continue;
            }
            Session session = driver.session(SessionConfig.forDatabase(dbName));
            if (sysIsLeader(session)) {
                final String name = UUID.randomUUID().toString();
                testCall( session, query,
                        Map.of("name", name),
                        row -> assertEquals(name, row.get("name")) );
            } else {
                try {
                    testCall(session, query,
                            Map.of("name", UUID.randomUUID().toString()),
                            row -> fail("Should fail because of non leader trigger addition"));
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(triggerNotRoutedError));
                }
            }
        }
    }

    private static boolean sysIsLeader(Session session) {
        final String systemRole = TestContainerUtil.singleResultFirstColumn(session, "CALL dbms.cluster.role('system')");
        return "LEADER".equals(systemRole);
    }
    
    @Test
    public void testTriggerNewProcsAllowedOnlyWithAdmin() {
        cluster.getSession().run("CREATE USER nonadmin SET PASSWORD \"test\" SET PASSWORD CHANGE NOT REQUIRED");

        for (Neo4jContainerExtension container: cluster.getClusterMembers()) {
            // todo - in this way if it works
//            withDbSession(container, "neo4j", session -> {
//                failsWithNonAdminUser(session, "apoc.trigger.add", "call apoc.trigger.add('abc', 'return 1', {})");
//                failsWithNonAdminUser(session, "apoc.trigger.remove", "call apoc.trigger.remove('abc')");
//                failsWithNonAdminUser(session, "apoc.trigger.removeAll", "call apoc.trigger.removeAll()");
//                failsWithNonAdminUser(session, "apoc.trigger.pause", "call apoc.trigger.pause('abc')");
//                failsWithNonAdminUser(session, "apoc.trigger.resume", "call apoc.trigger.resume('abc')");
//            });
            
            try (final Driver driver = GraphDatabase.driver(container.getBoltUrl(), AuthTokens.basic("neo4j", "test")); 
                 Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {

                failsWithNonAdminUser(session, "apoc.trigger.add", "call apoc.trigger.add('abc', 'return 1', {})");
                failsWithNonAdminUser(session, "apoc.trigger.remove", "call apoc.trigger.remove('abc')");
                failsWithNonAdminUser(session, "apoc.trigger.removeAll", "call apoc.trigger.removeAll()");
                failsWithNonAdminUser(session, "apoc.trigger.pause", "call apoc.trigger.pause('abc')");
                failsWithNonAdminUser(session, "apoc.trigger.resume", "call apoc.trigger.resume('abc')");
            }
            
            try (final Driver driver = GraphDatabase.driver(container.getBoltUrl(), AuthTokens.basic("neo4j", "test")); 
                 Session session = driver.session(SessionConfig.forDatabase("system"))) {

                failsWithNonAdminUser(session, "apoc.trigger.install", "call apoc.trigger.install('neo4j', 'qwe', 'return 1', {})");
                failsWithNonAdminUser(session, "apoc.trigger.drop", "call apoc.trigger.drop('neo4j', 'qwe')");
                failsWithNonAdminUser(session, "apoc.trigger.dropAll", "call apoc.trigger.dropAll('neo4j', )");
                failsWithNonAdminUser(session, "apoc.trigger.stop", "call apoc.trigger.stop('neo4j', 'qwe')");
                failsWithNonAdminUser(session, "apoc.trigger.start", "call apoc.trigger.start('neo4j', 'qwe')");
            }
        }
    }
    
    private void withDbSession(Neo4jContainerExtension container, String dbName, Consumer<Session> runnable) {
        try (final Driver driver = GraphDatabase.driver(container.getBoltUrl(), AuthTokens.basic("neo4j", "test"));
             Session session = driver.session(SessionConfig.forDatabase(dbName))) {
            runnable.accept(session);
        }
    }
            
    private void failsWithNonAdminUser(Session session, String procName, String query) {
        try {
            testCall(session, query, 
                    row -> fail("Should fail because of non admin user") );
        } catch (Exception e) {
            String actual = e.getMessage();
            final String expected = String.format("Executing admin procedure '%s' permission has not been granted for user 'nonadmin'",
                    procName);
            assertTrue(actual.contains(expected));
        }
    }

}
