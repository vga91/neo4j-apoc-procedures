package apoc.util;

import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;

public class SystemDbTestUtil {
    public static final long TIMEOUT = 10L;
    public static final long PROCEDURE_DEFAULT_REFRESH = 2000;
}
