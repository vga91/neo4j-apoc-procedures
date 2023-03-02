package apoc.uuid;

import apoc.ApocConfig;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.util.SystemDbTestUtil.PROCEDURE_DEFAULT_REFRESH;
import static apoc.util.TestUtil.waitDbsAvailable;
import static apoc.uuid.UUIDTestUtils.awaitUuidDiscovered;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;
import static org.junit.Assert.assertEquals;

public class UUIDRestartTest {
    // todo - maybe create UUIDTestUtil

    @Rule
    public TemporaryFolder store_dir = new TemporaryFolder();

    private GraphDatabaseService db;
    private GraphDatabaseService sysDb;
    private DatabaseManagementService databaseManagementService;

    @Before
    public void setUp() throws IOException {
        final File conf = store_dir.newFile("apoc.conf");
        try (FileWriter writer = new FileWriter(conf)) {
            writer.write(String.join("\n",
                    APOC_UUID_REFRESH + "=" + PROCEDURE_DEFAULT_REFRESH,
                    APOC_UUID_ENABLED +  "=true"));
        }
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + store_dir.getRoot().getAbsolutePath());

        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        TestUtil.registerProcedure(db, UUIDNewProcedures.class, Uuid.class);
    }

    @After
    public void tearDown() {
        databaseManagementService.shutdown();
    }

    private void restartDb() {
        databaseManagementService.shutdown();
        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
    }

    @Test
    public void testTriggerViaInstallRunsAfterRestart() {
        final String label = "myTrigger";
        final Map<String, Object> params = Map.of("label", label);
        final String triggerQuery = "CALL apoc.trigger.install('neo4j', 'myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";

        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', $label)", params);
        awaitUuidDiscovered(db, label);
//        TestUtil.testCall(gbs, query, params, row -> {});
//        runnable.run();

        db.executeTransactionally("CREATE (p:Person {id:1})");
        TestUtil.testCall(db, "match (n:Person{id:1}) return n.uuid as uuid",
                r -> assertEquals(1L, r.get("uuid")));

        restartDb();

        db.executeTransactionally("CREATE (p:Person{id:2, trigger: 0})");
        TestUtil.testCall(db, "match (n:Person{id:1}) return n.uuid as uuid",
                r -> assertEquals(1L, r.get("uuid")));
        TestUtil.testCall(db, "match (n:Person{id:2}) return n.uuid as uuid",
                r -> assertEquals(1L, r.get("uuid")));

    }

//    private void testTriggerWorksBeforeAndAfterRestart(GraphDatabaseService gbs, /*String query,*/ Map<String, Object> params, Runnable runnable) {
//        TestUtil.testCall(gbs, query, params, row -> {});
//        runnable.run();
//
//        db.executeTransactionally("CREATE (p:Person{id:1, trigger: 0})");
//        TestUtil.testCall(db, "match (n:Person{id:1}) return n.trigger as trigger",
//                r -> assertEquals(1L, r.get("trigger")));
//
//        restartDb();
//
//        db.executeTransactionally("CREATE (p:Person{id:2, trigger: 0})");
//        TestUtil.testCall(db, "match (n:Person{id:1}) return n.trigger as trigger",
//                r -> assertEquals(1L, r.get("trigger")));
//        TestUtil.testCall(db, "match (n:Person{id:2}) return n.trigger as trigger",
//                r -> assertEquals(1L, r.get("trigger")));
//    }
}
