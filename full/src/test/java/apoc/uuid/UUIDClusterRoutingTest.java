package apoc.uuid;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.util.TestContainerUtil.*;
import static apoc.uuid.UUIDTest.UUID_TEST_REGEXP;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class UUIDClusterRoutingTest {

    public static final int NUM_CORES = 4;
    private static TestcontainersCausalCluster cluster;
    private static Session clusterSession;
    private static List<Neo4jContainerExtension> members;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster(NUM_CORES, 0, Collections.emptyMap(),
                        Map.of(
                                "NEO4J_dbms_routing_enabled", "true",
                                APOC_UUID_ENABLED, "true",
                                APOC_UUID_REFRESH, "1000"
                ));

        clusterSession = cluster.getSession();

        members = cluster.getClusterMembers();

        assertEquals(NUM_CORES, members.size());
    }

    @AfterClass
    public static void bringDownCluster() {
        cluster.close();
    }

//    @Test
//    public void testTriggerAddAllowedOnlyInSysLeaderMember1uu() throws InterruptedException {
//        final String query = "CALL apoc.uuid.install('ClusterLabel', {})";
//
//        try (Driver driver = GraphDatabase.driver("neo4j://localhost:7688", AuthTokens.basic("neo4j", "foobar"))) {
//                driver.session().writeTransaction(tx -> tx.run(query));
//            }
//    }

    // TODO: fabric tests once the @SystemOnlyProcedure annotation is added to Neo4j

    // todo - replication tests if possible

    // todo - common
    private static void checkLeadershipBalanced() {
        assertEventually(() -> {
                    String query = "CALL dbms.cluster.overview() YIELD databases\n" +
                            "WITH databases.neo4j AS neo4j, databases.system AS system\n" +
                            "WHERE neo4j = 'LEADER' OR system = 'LEADER'\n" +
                            "RETURN count(*)";
//                    long l = (long) singleResultFirstColumn(clusterSession, query);
//                    System.out.println("l = " + l);
                    return (long) singleResultFirstColumn(clusterSession, query);
                },
                (value) -> value == 2L, 30L, TimeUnit.SECONDS);
    }


    // todo - common
    private void testFabric(BiConsumer<Session, Neo4jContainerExtension> sessionConsumer) {

        for (Neo4jContainerExtension container: members) {
            // Bolt (routing) url
            String neo4jUrl = "neo4j://localhost:" + container.getMappedPort(7687);

            try (Driver driver = GraphDatabase.driver(neo4jUrl, container.getAuth());
                 Session session = driver.session()) {
                sessionConsumer.accept(session, container);
            }
        }
    }

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember1() throws InterruptedException {
//        final String query = "CALL apoc.uuid.install($label, {})";

        // todo... inside the members foreach
//        cluster.getSession().writeTransaction(tx -> tx.run("CREATE CONSTRAINT FOR (n:ClusterLabel1) REQUIRE n.uuid IS UNIQUE"));
//        cluster.getSession().writeTransaction(tx -> tx.run("CREATE CONSTRAINT FOR (n:ClusterLabel2) REQUIRE n.uuid IS UNIQUE"));
//        cluster.getSession().writeTransaction(tx -> tx.run("CREATE CONSTRAINT FOR (n:ClusterLabel3) REQUIRE n.uuid IS UNIQUE"));
//
//        cluster.getSession().writeTransaction(tx -> tx.run("CREATE (n:ClusterLabel1)"));
//        cluster.getSession().writeTransaction(tx -> tx.run("CREATE (n:ClusterLabel2)"));
//        cluster.getSession().writeTransaction(tx -> tx.run("CREATE (n:ClusterLabel3)"));

//        cluster.getSession().writeTransaction(tx -> tx.run(query));


        // wait until members are balanced, i.e. the system LEADER and the neo4j LEADER aren't in the same member
        checkLeadershipBalanced();

//
//        List<String> labels = IntStream.range(0, members.size())
//                .mapToObj(idx -> "ClusterLabel" + idx)
//                .collect(Collectors.toList());

//        Neo4jContainerExtension container = members.get(1);
//        String clusterLabel = "ClusterLabel";


        testFabric((session, container) -> {
            try {
                String label = container.getContainerName();
                session.writeTransaction(tx -> tx.run(format("CREATE CONSTRAINT FOR (n:`%s`) REQUIRE n.uuid IS UNIQUE", label)));

//                Thread.sleep(5000);
                String query = "CALL apoc.uuid.install($label, {})";
//                final String query = "USE SYSTEM CALL apoc.uuid.create('neo4j', $label, {})";
                session.writeTransaction(tx -> tx.run(query,
                        Map.of("label", label) )
                );
                System.out.println("nonErrore...");
            } catch (Exception e) {
                System.out.println("ERRORE e.getMessage() = " + e.getMessage());
            }
        });

//        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
//        assertEquals(NUM_CORES, members.size());
//        for (Neo4jContainerExtension container: members) {
//
//            boolean running = container.isRunning();
//            System.out.println("running = " + running);
//            System.out.println("container.getEnvMap() = " + container.getEnvMap());
//            String url = container.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
//            System.out.println("url = " + url);
//
//            // todo - try using public URI getURI() { in TestContainersCausalClusterUtils
//            String neo4jUrl = "neo4j://localhost:" + container.getMappedPort(7687);// container.getBoltUrl().replace("bolt://", "neo4j://");
//            System.out.println("neo4jUrl = " + neo4jUrl);
////            try (Driver driver = container.getDriver()) {
//            try (Driver driver = GraphDatabase.driver(neo4jUrl, container.getAuth());
//                 Session session = driver.session()) {
//                String label = container.getContainerName();
//
//
//                session.writeTransaction(tx -> tx.run(format("CREATE CONSTRAINT FOR (n:`%s`) REQUIRE n.uuid IS UNIQUE", label)));
//
////                Thread.sleep(5000);
//                String query = "CALL apoc.uuid.install($label, {})";
////                final String query = "USE SYSTEM CALL apoc.uuid.create('neo4j', $label, {})";
//                session.writeTransaction(tx -> tx.run(query,
//                        Map.of("label", label) )
//                );
//                System.out.println("nonErrore...");
//            } catch (Exception e) {
//                System.out.println("ERRORE e.getMessage() = " + e.getMessage());
//            }
//        }

        assertEventually(
                () -> (Long) singleResultFirstColumn(cluster.getSession(), "CALL apoc.uuid.list() YIELD label RETURN count(label)"),
                (value) -> value == members.size(), 10L, TimeUnit.SECONDS);


        for (Neo4jContainerExtension container: members) {

            String neo4jUrl = "neo4j://localhost:" + container.getMappedPort(7687);// container.getBoltUrl().replace("bolt://", "neo4j://");
            System.out.println("neo4jUrl = " + neo4jUrl);
            try (Driver driver = GraphDatabase.driver(neo4jUrl, container.getAuth());
                 Session session = driver.session()) {
                session.writeTransaction(tx -> tx.run(format("CREATE (n:`%s`)", container.getContainerName())));
                System.out.println("nonErrore...");
            } catch (Exception e) {
                System.out.println("ERRORE e.getMessage() = " + e.getMessage());
            }
        }

        for (Neo4jContainerExtension member : members) {

            assertEventually(() -> {
                String query = format("MATCH (n:`%s`) RETURN n.uuid AS uuid", member.getContainerName());
                Result r = clusterSession.run(query);
                assertTrue(r.hasNext());
                assertThat(r.single().get("uuid").asString(), Matchers.matchesRegex(UUID_TEST_REGEXP));
                return true;
            },
            (value) -> value, 10L, TimeUnit.SECONDS);

//            testCallInReadTransaction(clusterSession, format("MATCH (n:`%s`) RETURN n.uuid AS uuid", member.getContainerName()),
//                    r -> assertThat((String) r.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP)));
        }

//        testCallInReadTransaction(cluster.getSession(), "MATCH (n:ClusterLabel1) RETURN n.uuid AS uuid",
//                r -> assertThat((String) r.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP)));
//        testCallInReadTransaction(cluster.getSession(), "MATCH (n:ClusterLabel2) RETURN n.uuid AS uuid",
//                r -> assertThat((String) r.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP)));
//        testCallInReadTransaction(cluster.getSession(), "MATCH (n:ClusterLabel3) RETURN n.uuid AS uuid",
//                r -> assertThat((String) r.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP)));


//        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR, DEFAULT_DATABASE_NAME);
    }




//    @Test
//    public void testTriggerAddAllowedOnlyInSysLeaderMember() {
//        final String query = "CALL apoc.trigger.add($name, 'RETURN 1', {})";
//        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR, DEFAULT_DATABASE_NAME);
//    }
//
//    @Test
//    public void testTriggerRemoveAllowedOnlyInSysLeaderMember() {
//        final String query = "CALL apoc.trigger.remove($name)";
//        triggerInSysLeaderMemberCommon(query, SYS_NON_LEADER_ERROR, DEFAULT_DATABASE_NAME);
//    }
//
//    @Test
//    public void testUuidCreateAllowedOnlyInSysLeaderMember() {
//        final String query = "CALL apoc.uuid.create('neo4j', $name, 'RETURN 1', {})";
//        triggerInSysLeaderMemberCommon(query, PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME);
//    }
//
//    @Test
//    public void testUuidDropAllowedOnlyInSysLeaderMember() {
//        final String query = "CALL apoc.uuid.drop('neo4j', $name)";
//        triggerInSysLeaderMemberCommon(query, PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME, true,
//                (session, name) -> testCallEmpty(session, query, Map.of("name", name)));
//    }
//
//    @Test
//    public void testTriggerShowAllowedOnlyInSysLeaderMember() {
//        final String query = "CALL apoc.uuid.show('neo4j')";
//        final BiConsumer<Session, String> testTrigger = (session, name) -> testCallEmpty(session, query, Collections.emptyMap());
//        triggerInSysLeaderMemberCommon(query, PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME, true, testTrigger);
//    }

    // todo - uuid show test
    private static void triggerInSysLeaderMemberCommon(String query, String triggerNotRoutedError, String dbName) {
        final BiConsumer<Session, String> testTrigger = (session, label) -> testCall(session, query,
                Map.of("label", label),
                row -> assertEquals(label, row.get("label")));
        triggerInSysLeaderMemberCommon(query, triggerNotRoutedError, dbName, false, testTrigger);
    }


    private static void triggerInSysLeaderMemberCommon(String query, String triggerNotRoutedError, String dbName, boolean nonWriteOperation, BiConsumer<Session, String> testTrigger) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(NUM_CORES, members.size());
        for (Neo4jContainerExtension container: members) {
            // we skip READ_REPLICA members with write operations
            final Driver driver = nonWriteOperation
                    ? container.getDriver()
                    : getDriverIfNotReplica(container);
            if (driver == null) {
                continue;
            }
            Session session = driver.session(SessionConfig.forDatabase(dbName));
            if (nonWriteOperation || sysIsLeader(session)) {
                final String name = UUID.randomUUID().toString();
                testTrigger.accept(session, name);
            } else {
                try {
                    testCall(session, query,
                            Map.of("label", UUID.randomUUID().toString()),
                            row -> fail("Should fail because of non leader trigger addition"));
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(triggerNotRoutedError));
                }
            }
        }
    }

    private static Driver getDriverIfNotReplica(Neo4jContainerExtension container) {
        final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
        final Driver driver = container.getDriver();
        if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || driver == null) {
            return null;
        }
        return driver;
    }

    private static boolean sysIsLeader(Session session) {
        final String systemRole = TestContainerUtil.singleResultFirstColumn(session, "CALL dbms.cluster.role('system')");
        return "LEADER".equals(systemRole);
    }

}
