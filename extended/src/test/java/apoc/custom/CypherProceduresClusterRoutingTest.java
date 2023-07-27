package apoc.custom;

import apoc.util.ExtendedTestContainerUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import apoc.util.collection.Iterators;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static apoc.custom.CypherProceduresHandler.CUSTOM_PROCEDURES_REFRESH;
//import static apoc.util.ClusterTestUtil.connectWithRoutingForEachMembers;
//import static apoc.util.ClusterTestUtil.checkLeadershipBalanced;
import static apoc.util.ExtendedTestContainerUtil.singleResultFirstColumn;
import static apoc.util.SystemDbUtil.PROCEDURE_NOT_ROUTED_ERROR;
import static apoc.util.SystemDbUtil.SYS_NON_LEADER_ERROR;
import static apoc.util.ExtendedTestContainerUtil.checkLeadershipBalanced;
import static apoc.util.ExtendedTestContainerUtil.queryForEachMembers;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static apoc.util.TestContainerUtil.testResult;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CypherProceduresClusterRoutingTest {
    private static final int NUM_CORES = 4;
    private static TestcontainersCausalCluster cluster;
    private static Session clusterSession;
    private static List<Neo4jContainerExtension> members;

    // todo - https://github.com/neo4j-contrib/neo4j-apoc-procedures/pull/3555/files

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster(List.of(TestContainerUtil.ApocPackage.EXTENDED),
                        NUM_CORES, 0,
                        Collections.emptyMap(),
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
    public void testSetupAndDropUuidWithUseSystemClause() {
        isLeadershipBalanced();

        // no matter if the leader labels are balanced, or not.
        // The procedure should always work correctly
        queryForEachMembers(members, (session, container) -> {
            String label = container.getContainerName();

            // create note to be populated because of `addToExistingNodes` config
            session.writeTransaction(tx -> tx.run(format("CREATE (n:`%s`)", label)));

            final String query = "USE SYSTEM CALL apoc.custom.installProcedure($label)";
            Map<String, Object> params = Map.of("label", label);
            session.writeTransaction(tx -> tx.run(query, params));
        });

        String countUuids = "CALL apoc.uuid.list() YIELD label RETURN count(*)";
        assertEventually(() -> (long) singleResultFirstColumn(cluster.getSession(), countUuids),
                (value) -> value == members.size(), 20L, TimeUnit.SECONDS);

        queryForEachMembers(members, (session, container) -> {
            session.writeTransaction(tx -> tx.run(format("CREATE (n:`%s`)", container.getContainerName())));
        });

        for (Neo4jContainerExtension member : members) {
            assertEventually(() -> {
                        String query = format("MATCH (n:`%s`) RETURN n.uuid AS uuid", member.getContainerName());
                        // 2 nodes with uuid: one created via `addToExistingNodes` and the other one via transaction listener
                        Result res = clusterSession.run(query);
//                        Record node = res.next();
//                        assertIsUUID(node.get("uuid").asString());
//                        node = res.next();
//                        assertIsUUID(node.get("uuid").asString());
                        return !res.hasNext();
                    },
                    (val) -> val, 20L, TimeUnit.SECONDS);
        }

        // drop the previus custom procedure
        queryForEachMembers(members, (session, container) -> {
            String query = "USE SYSTEM CALL apoc.uuid.drop($label)";
            Map<String, Object> params = Map.of("label", container.getContainerName());
            session.writeTransaction(tx -> tx.run(query, params));
        });

        assertEventually(() -> (long) singleResultFirstColumn(cluster.getSession(), countUuids),
                (value) -> value == 0L, 20L, TimeUnit.SECONDS);

    }

    @Test
    public void testInstallUudiInClusterViaUseSystemShouldFail() {
        boolean leadershipBalanced = isLeadershipBalanced();

        queryForEachMembers(members, (session, container) -> {
            try {
                String label = container.getContainerName();
//                session.writeTransaction(tx -> tx.run(format("CREATE CONSTRAINT IF NOT EXISTS FOR (n:`%s`) REQUIRE n.uuid IS UNIQUE", label)));
                String query = "CALL apoc.custom.declareProcedure($label, {})";

                session.writeTransaction(tx -> tx.run(query,
                        Map.of("label", label) )
                );
                // the error happens only if sys db leader is in a different core than the neo4j db leader
                if (leadershipBalanced) {
                    fail("Should fail because it's not possible to write via deprecated procedure");
                }
            } catch (Exception e) {
                assertThat(e.getMessage(), containsString(SYS_NON_LEADER_ERROR));
            }
        });
    }

    private boolean isLeadershipBalanced() {
        // wait until members are balanced, i.e. the system LEADER and the neo4j LEADER aren't in the same member.
        // Some time, although causal_clustering.leadership_balancing=NO_BALANCING,
        // the system db leader is placed in the same core as the neo4j db leader
        try {
            checkLeadershipBalanced(clusterSession);
            return true;
        } catch (ConditionTimeoutException e) {
            return false;
        }
    }


    @Test
    public void testUuidSetupAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.uuid.setup($label)";
        customInSysLeaderMemberCommon(query, PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME,
                (session, label) -> {
                    Map<String, Object> params = Map.of("label", label);
                    testCall(session, query, params,
                            row -> assertEquals(label, row.get("label"))
                    );
                    session.writeTransaction(tx -> tx.run("CALL apoc.uuid.drop($label)", params));
                }
        );
    }

    @Test
    public void testUuidDropAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.uuid.drop($label)";
        customInSysLeaderMemberCommon(query, PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME,
                (session, label) -> testCallEmpty(session, query, Map.of("label", label))
        );
    }

    @Test
    public void testUuidShowAllowedInAllSysLeaderMembers() {
        final String query = "CALL apoc.uuid.show";
        final BiConsumer<Session, String> testUuidShow = (session, name) -> testResult(session, query, Iterators::count);
        customInSysLeaderMemberCommon(query, PROCEDURE_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME, testUuidShow, true);
    }

    // todo - ClusterTestUtil with UUID?
    private static void customInSysLeaderMemberCommon(String query, String uuidNotRoutedError, String dbName, BiConsumer<Session, String> testUuid) {
        customInSysLeaderMemberCommon(query, uuidNotRoutedError, dbName, testUuid, false);
    }

    private static void customInSysLeaderMemberCommon(String query, String uuidNotRoutedError, String dbName, BiConsumer<Session, String> testUuid, boolean readOnlyOperation) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(NUM_CORES, members.size());
        final String label = UUID.randomUUID().toString();
        clusterSession.writeTransaction(tx -> tx.run(format("CREATE CONSTRAINT IF NOT EXISTS FOR (n:`%s`) REQUIRE n.uuid IS UNIQUE", label)));
        for (Neo4jContainerExtension container: members) {
            // we skip READ_REPLICA members with write operations
            // instead, we consider all members with a read only operations
            final Driver driver = readOnlyOperation
                    ? container.getDriver()
                    : getDriverIfNotReplica(container);
            if (driver == null) {
                continue;
            }
            Session session = driver.session(SessionConfig.forDatabase(dbName));
            boolean isWriter = dbIsWriter(dbName, session, getBoltAddress(container));
            if (readOnlyOperation || isWriter) {
                testUuid.accept(session, label);
            } else {
                try {
                    testCall(session, query,
                            Map.of("label", UUID.randomUUID().toString()),
                            row -> fail("Should fail because of non leader custom procedure/function addition"));
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(uuidNotRoutedError));
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

    private static String getBoltAddress(Neo4jContainerExtension instance) {
        return instance.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
    }

    private static boolean dbIsWriter(String dbName, Session session, String boltAddress) {
        return session.run( "SHOW DATABASE $dbName WHERE address = $boltAddress",
                        Map.of("dbName", dbName, "boltAddress", boltAddress) )
                .single().get("writer")
                .asBoolean();
    }

    // todo - remove
    private static boolean sysIsLeader(Session session) {
        final String systemRole = singleResultFirstColumn(session, "CALL dbms.cluster.role('system')");
        return "LEADER".equals(systemRole);
    }
}