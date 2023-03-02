package apoc.util;

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static apoc.util.TestContainerUtil.singleResultFirstColumn;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ClusterTestUtil {

    public static void checkLeadershipBalanced(Session session) {
        assertEventually(() -> {
                    String query = "CALL dbms.cluster.overview() YIELD databases\n" +
                            "WITH databases.neo4j AS neo4j, databases.system AS system\n" +
                            "WHERE neo4j = 'LEADER' OR system = 'LEADER'\n" +
                            "RETURN count(*)";
//                    long l = (long) singleResultFirstColumn(session, query);
//                    System.out.println("l = " + l);
                    return (long) singleResultFirstColumn(session, query);
                },
                (value) -> value == 2L, 30L, TimeUnit.SECONDS);
    }

    public static void connectWithRoutingForEachMembers(List<Neo4jContainerExtension> members,
                                                        BiConsumer<Session, Neo4jContainerExtension> sessionConsumer) {

        for (Neo4jContainerExtension container: members) {
            // Bolt (routing) url
            String neo4jUrl = "neo4j://localhost:" + container.getMappedPort(7687);

            try (Driver driver = GraphDatabase.driver(neo4jUrl, container.getAuth());
                 Session session = driver.session()) {
                sessionConsumer.accept(session, container);
            }
        }
    }
}
