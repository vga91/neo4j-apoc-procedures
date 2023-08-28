package apoc.custom;

import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.IOException;
import java.util.Map;

import static apoc.custom.CypherProceduresHandler.*;
import static apoc.util.DbmsTestUtil.startDbWithApocConfigs;
import static apoc.util.SystemDbTestUtil.PROCEDURE_DEFAULT_REFRESH;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestUtil.testCallCountEventually;

public class CypherProcedureTestUtil {
    public static DatabaseManagementService startDbWithCustomApocConfs(TemporaryFolder storeDir) throws IOException {
        return startDbWithApocConfigs(storeDir,
                Map.of(CUSTOM_PROCEDURES_REFRESH, PROCEDURE_DEFAULT_REFRESH)
        );
    }
}