package apoc.uuid;

import apoc.ApocSettings;
import apoc.SystemLabels;
import apoc.util.TestUtil;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.uuid.UUIDTest.UUID_TEST_REGEXP;
import static apoc.uuid.UUIDTest.assertResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UUIDRestartTest {
    
    @Rule
    public final TemporaryFolder STORE_DIR = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;

    @Before
    public void setUp() throws InterruptedException {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot().toPath())
                .setConfig(ApocSettings.apoc_uuid_enabled, true)
                .build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, Uuid.class);
    }

    @After
    public void tearDown() {
        databaseManagementService.shutdown();
    }

    private void restartDb() {
        databaseManagementService.shutdown();
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot().toPath())
                .setConfig(ApocSettings.apoc_uuid_enabled, true)
                .build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, Uuid.class);
        assertTrue(db.isAvailable(1000));
    }

    @Test
    public void testUUIDAfterRestart() {
        final String systemDb = "system";
        
        db.executeTransactionally("CREATE CONSTRAINT ON (person:Person) ASSERT person.restartUuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Person', {addToExistingNodes: false, uuidProperty: 'restartUuid', addToSetLabels: true})");

        db.executeTransactionally("CREATE (p:Person {foo: 'one'})");
        TestUtil.testCall(db, "MATCH (a:Person {foo:'one'}) RETURN a.restartUuid as uuid",
                row -> TestCase.assertTrue(((String) row.get("uuid")).matches(UUID_TEST_REGEXP)));

        restartDb();
        
        testCall(db, "CALL apoc.uuid.list()",
                (row) -> assertResult(row, "Person", true, 
                        map("uuidProperty", "restartUuid", "addToSetLabels", true)));

        db.executeTransactionally("CREATE (p:Person {foo: 'two'})");
        TestUtil.testCall(db, "MATCH (a:Person {foo: 'two'}) RETURN a.restartUuid as uuid",
                row -> TestCase.assertTrue(((String) row.get("uuid")).matches(UUID_TEST_REGEXP)));

        // change uuid property
        db.executeTransactionally("CREATE CONSTRAINT ON (person:Person) ASSERT person.anotherUuid IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Person', {addToExistingNodes: false, uuidProperty: 'anotherUuid', addToSetLabels: true})");

        db.executeTransactionally("CREATE (p:Person {foo: 'three'})");
        TestUtil.testCall(db, "MATCH (a:Person {foo:'three'}) RETURN a.anotherUuid as uuid",
                row -> TestCase.assertTrue(((String) row.get("uuid")).matches(UUID_TEST_REGEXP)));

        try (final Transaction tx = databaseManagementService.database(systemDb).beginTx()) {
            assertEquals(1L, tx.findNodes(SystemLabels.ApocUuid).stream().count());
        }

        restartDb();

        db.executeTransactionally("CREATE (p:Person {foo: 'four'})");
        TestUtil.testCall(db, "MATCH (a:Person {foo:'four'}) RETURN a.anotherUuid as uuid",
                row -> TestCase.assertTrue(((String) row.get("uuid")).matches(UUID_TEST_REGEXP)));
        
        try (final Transaction tx = databaseManagementService.database(systemDb).beginTx()) {
            assertEquals(1L, tx.findNodes(SystemLabels.ApocUuid).stream().count());
        }
    }
}
