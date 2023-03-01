package apoc.util;

import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;

public class SystemDbTestUtil {
    public static final long TIMEOUT = 10L;
    public static final long PROCEDURE_DEFAULT_REFRESH = 2000;

//    public static void awaitFunctionalityDiscovered(GraphDatabaseService db, String name, String expected) {
//        awaitFunctionalityDiscovered(db, name, expected, false);
//    }

    public static void awaitFunctionalityDiscovered(GraphDatabaseService db, String name, String expected/*, boolean paused*/) {
        String call = "CALL apoc.trigger.list() YIELD name, query, paused WHERE name = $name RETURN query";
        testCallEventually(db, call,
                Map.of("name", name),
                row -> {
                    assertEquals(expected, row.get("query"));
//                    assertEquals(paused, row.get("paused"));
                }, TIMEOUT);
    }
}
