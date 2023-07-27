package apoc.custom;

import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.IOException;
import java.util.Map;

import static apoc.custom.CypherProceduresHandler.*;
import static apoc.custom.CypherProceduresHandler.PREFIX;
import static apoc.util.DbmsTestUtil.startDbWithApocConfigs;
import static apoc.util.SystemDbTestUtil.PROCEDURE_DEFAULT_REFRESH;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestUtil.testCallCountEventually;
import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;

public class CypherProcedureTestUtil {
    public static DatabaseManagementService startDbWithCustomApocConfs(TemporaryFolder storeDir) throws IOException {
        return startDbWithApocConfigs(storeDir,
                Map.of(CUSTOM_PROCEDURES_REFRESH, PROCEDURE_DEFAULT_REFRESH)
        );
    }

    public static void awaitCustomFuncDiscovered(GraphDatabaseService db, String name) {
        awaitCustomDiscovered(db, FUNCTION, name, null);
    }

    public static void awaitCustomProcDiscovered(GraphDatabaseService db, String name) {
        awaitCustomDiscovered(db, PROCEDURE, name, null);
    }

//    public static void awaitCustomDiscovered(GraphDatabaseService db, String type, String name, String expectedSignature) {
//        String call = "CALL apoc.custom.list() YIELD name, signature WHERE name = $name RETURN signature";
////        String call = "SHOW " + type+ " YIELD name, signature WHERE name CONTAINS $name RETURN signature";
//        testCallEventually(db, call,
//                Map.of("name", PREFIX + "." + name),
//                row -> {
//                    if (expectedSignature != null) {
//                        assertEquals(expectedSignature, row.get("signature"));
//                    }
//                }, TIMEOUT);
//    }

    public static void awaitCustomDiscovered(GraphDatabaseService db, String type, String name, String expectedSignature) {
        String call = "CALL apoc.custom.list() YIELD name WHERE name = $name RETURN *";
//        String call = "SHOW " + type+ " YIELD name, signature WHERE name CONTAINS $name RETURN signature";
        testCallCountEventually(db, call,
                Map.of("name", /*PREFIX + "." + */name),
                1,
                TIMEOUT);
//                row -> {
//                    if (expectedSignature != null) {
//                        assertEquals(expectedSignature, row.get("signature"));
//                    }
//                }, TIMEOUT);
    }
}