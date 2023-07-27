package apoc.util;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.awaitility.core.ConditionTimeoutException;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import static apoc.util.TestContainerUtil.copyFilesToPlugin;
import static apoc.util.TestContainerUtil.executeGradleTasks;
import static org.junit.Assert.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ExtendedTestContainerUtil
{
    public static TestcontainersCausalCluster createEnterpriseCluster( List<TestContainerUtil.ApocPackage> apocPackages, int numOfCoreInstances, int numberOfReadReplica, Map<String, Object> neo4jConfig, Map<String, String> envSettings) {
        return TestcontainersCausalCluster.create(apocPackages, numOfCoreInstances, numberOfReadReplica, Duration.ofMinutes(4), neo4jConfig, envSettings);
    }

    public static <T> T singleResultFirstColumn(Session session, String cypher) {
        return (T) session.writeTransaction(tx -> tx.run(cypher).single().fields().get(0).value().asObject());
    }

    public static void testCallInReadTransaction(Session session, String call, Consumer<Map<String, Object>> consumer) {
        TestContainerUtil.testCallInReadTransaction(session, call, null, consumer);
    }

    public static void addExtraDependencies() {
        File extraDepsDir = new File(TestContainerUtil.baseDir, "extra-dependencies");
        // build the extra-dependencies
        executeGradleTasks(extraDepsDir, "buildDependencies");

        // add all extra deps to the plugin docker folder
        final File directory = new File(extraDepsDir, "build/allJars");
        final IOFileFilter instance = new WildcardFileFilter("*.jar");
        copyFilesToPlugin(directory, instance, TestContainerUtil.pluginsFolder);
    }

    /**
     * Check if the system LEADER and the neo4j LEADER are located in different cores
     *
     * @param session
     */
    public static void checkLeadershipBalanced(Session session) throws ConditionTimeoutException {
        assertEventually(() -> {
                    String query = "CALL dbms.cluster.overview() YIELD databases\n" +
                            "WITH databases.neo4j AS neo4j, databases.system AS system\n" +
                            "WHERE neo4j = 'LEADER' OR system = 'LEADER'\n" +
                            "RETURN count(*)";
                    try {
                        long count = singleResultFirstColumn(session, query);
                        assertEquals(2L, count);
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                },
                (value) -> value, 30L, TimeUnit.SECONDS);
    }

    /**
     * Open a routing session for each cluster core member
     *
     * @param members
     * @param sessionConsumer
     */
    public static void queryForEachMembers(List<Neo4jContainerExtension> members,
                                           BiConsumer<Session, Neo4jContainerExtension> sessionConsumer) {

        for (Neo4jContainerExtension container: members) {
            // Bolt (routing) url
            String neo4jUrl = "neo4j://localhost:" + container.getMappedPort(7687);

            try (Driver driver = GraphDatabase.driver(neo4jUrl, AuthTokens.basic("neo4j", container.getAdminPassword()));
                 Session session = driver.session()) {
                sessionConsumer.accept(session, container);
            }
        }
    }

}
