package apoc.custom;

import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.IOException;
import java.util.Map;

import static apoc.custom.CypherProceduresHandler.*;
import static apoc.custom.CypherProceduresHandler.PREFIX;
import static apoc.util.DbmsTestUtil.startDbWithApocConfs;
import static apoc.util.SystemDbTestUtil.PROCEDURE_DEFAULT_REFRESH;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;

public class CypherProcedureTestUtil {
    public static DatabaseManagementService startDbWithCustomApocConfs(TemporaryFolder storeDir) throws IOException {
        return startDbWithApocConfs(storeDir,
                CUSTOM_PROCEDURES_REFRESH + "=" + PROCEDURE_DEFAULT_REFRESH);
    }

    public static void awaitCustomFuncDiscovered(GraphDatabaseService db, String label) {
        awaitCustomDiscovered(db, FUNCTION, label, null);
    }

    public static void awaitCustomProcDiscovered(GraphDatabaseService db, String label) {
        awaitCustomDiscovered(db, PROCEDURE, label, null);
    }

    public static void awaitCustomDiscovered(GraphDatabaseService db, String type, String name, String expectedSignature) {
        String call = "SHOW " + type+ " YIELD name, signature WHERE name CONTAINS $name RETURN signature";
        testCallEventually(db, call,
                Map.of("name", PREFIX + "." + name),
                row -> {
                    if (expectedSignature != null) {
                        assertEquals(expectedSignature, row.get("signature"));
                    }
                }, TIMEOUT);
    }
}
