package apoc.uuid;

import apoc.create.Create;
import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;

import java.io.File;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.util.DbmsTestUtil.startDbWithApocConfs;
import static apoc.util.SystemDbTestUtil.*;
import static apoc.util.SystemDbUtil.*;
import static apoc.util.TestUtil.*;
import static apoc.uuid.UUIDTest.UUID_TEST_REGEXP;
import static apoc.uuid.UUIDTest.assertResult;
import static apoc.uuid.UUIDTestUtils.*;
import static apoc.uuid.UuidConfig.*;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.MapUtil.map;


public class UUIDNewProceduresTest {
    private static final File directory = new File("target/conf");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void beforeClass() throws Exception {
        databaseManagementService = startDbWithUuidApocConfs(storeDir);

        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        TestUtil.registerProcedure(sysDb, UUIDNewProcedures.class);
        TestUtil.registerProcedure(db, Uuid.class, Create.class, Periodic.class);
    }

    @AfterClass
    public static void afterClass() {
        databaseManagementService.shutdown();
    }

    @After
    public void after() throws Exception {
        sysDb.executeTransactionally("CALL apoc.uuid.dropAll('neo4j')");
        testCallCountEventually(db, "CALL apoc.uuid.list", 0, TIMEOUT);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // todo - or create a GraphDatabaseService db in @Before instead of @BeforeClass
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.commit();
        }
    }

    //
    // test cases taken and adapted from UUIDTest.java
    //

    @Test
    public void testUUID() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (p:Person) ASSERT p.uuid IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Person')");
        UUIDTestUtils.awaitUuidDiscovered(db, "Person");

        // when
        db.executeTransactionally("CREATE (p:Person{name:'Daniel'})-[:WORK]->(c:Company{name:'Neo4j'})");

        // then
        try (Transaction tx = db.beginTx()) {
            Node company = (Node) tx.execute("MATCH (c:Company) return c").next().get("c");
            assertTrue(!company.hasProperty("uuid"));
            Node person = (Node) tx.execute("MATCH (p:Person) return p").next().get("p");
            assertTrue(person.getAllProperties().containsKey("uuid"));

            assertTrue(person.getAllProperties().get("uuid").toString().matches(UUID_TEST_REGEXP));
            tx.commit();
        }
    }

    @Test
    public void testUUIDWithSetLabel() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (p:Mario) ASSERT p.uuid IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Mario', {addToSetLabels: true}) YIELD label RETURN label");
        awaitUuidDiscovered(db, "Mario", DEFAULT_UUID_PROPERTY, true);

        // when
        db.executeTransactionally("CREATE (p:Luigi {foo:'bar'}) SET p:Mario");
        // then
        TestUtil.testCall(db, "MATCH (a:Luigi:Mario) RETURN a.uuid as uuid",
                row -> assertIsUUID(row.get("uuid")));

        // - set after creation
        db.executeTransactionally("CREATE (:Peach)");
        // when
        db.executeTransactionally("MATCH (p:Peach) SET p:Mario");
        // then
        TestUtil.testCall(db, "MATCH (a:Peach:Mario) RETURN a.uuid as uuid",
                row -> assertIsUUID(row.get("uuid")));

        TestUtil.testCall(sysDb, "CALL apoc.uuid.drop('neo4j', 'Mario')",
                (row) -> assertResult(row, "Mario", false,
                        Util.map(UUID_PROPERTY_KEY, "uuid", ADD_TO_SET_LABELS_KEY, true)));
    }

    @Test
    public void testUUIDWithoutRemovedUuid() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Test') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Test");

        // when
        db.executeTransactionally("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})"); // Create the uuid manually and except is the same after the trigger

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid")); // Check if the uuid if the same when created
            tx.commit();
        }
    }

    @Test
    public void testUUIDSetUuidToEmptyAndRestore() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Test') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Test");

        db.executeTransactionally("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})");

        // when
        db.executeTransactionally("MATCH (t:Test) SET t.uuid = ''");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void testUUIDDeleteUuidAndRestore() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Test') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Test");
        db.executeTransactionally("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})");

        // when
        db.executeTransactionally("MATCH (t:Test) remove t.uuid");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void testUUIDSetUuidToEmpty() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Test') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Test");

        db.executeTransactionally("CREATE (n:Test:Empty {name:'empty'})");

        // when
        db.executeTransactionally("MATCH (t:Test:Empty) SET t.uuid = ''");

        // then
        testCall(db, "MATCH (n:Empty) return n.uuid AS uuid",
                (row) -> assertIsUUID(row.get("uuid"))
        );
//        try (Transaction tx = db.beginTx()) {
//            Node n = (Node) tx.execute("MATCH (n:Empty) return n").next().get("n");
//            assertTrue(n.getAllProperties().containsKey("uuid"));
//            assertTrue(n.getAllProperties().get("uuid").toString().matches(UUID_TEST_REGEXP));
//            tx.commit();
//        }
    }

    @Test
    public void testUUIDList() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE");

        // when
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Bar') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Bar");

        // then
        TestUtil.testCall(db, "CALL apoc.uuid.list()",
                (row) -> assertResult(row, "Bar", true,
                        Util.map(UUID_PROPERTY_KEY, "uuid", ADD_TO_SET_LABELS_KEY, false)));
    }

    @Test
    public void testUUIDListAddToExistingNodes() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE");
        db.executeTransactionally("UNWIND Range(1,10) as i CREATE(bar:Bar{id: i})");

        // when
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Bar')");
        UUIDTestUtils.awaitUuidDiscovered(db, "Bar");

        // then
        List<String> uuidList = TestUtil.firstColumn(db, "MATCH (n:Bar) RETURN n.uuid AS uuid");
        assertEquals(10, uuidList.size());
        assertTrue(uuidList.stream().allMatch(uuid -> uuid.matches(UUID_TEST_REGEXP)));
    }

    @Test
    public void testAddRemoveUuid() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.foo IS UNIQUE");

        // when
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Test', {uuidProperty: 'foo'}) YIELD label RETURN label");
        awaitUuidDiscovered(db, "Test", "foo", DEFAULT_ADD_TO_SET_LABELS);

        // then
        TestUtil.testCall(db, "CALL apoc.uuid.list()",
                (row) -> assertResult(row, "Test", true,
                        Util.map(UUID_PROPERTY_KEY, "foo", ADD_TO_SET_LABELS_KEY, false)));
        TestUtil.testCall(sysDb, "CALL apoc.uuid.drop('neo4j', 'Test')",
                (row) -> assertResult(row, "Test", false,
                        Util.map(UUID_PROPERTY_KEY, "foo", ADD_TO_SET_LABELS_KEY, false)));
    }

    @Test
    public void testNotAddToExistingNodes() {
        // givenhttps://larus-ba.it/privacy-policy/
        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");

        // when
        db.executeTransactionally("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Person', {addToExistingNodes: false}) YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Person");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (person:Person) return person").next().get("person");
            assertFalse(n.getAllProperties().containsKey("uuid"));
            tx.commit();
        }
    }

    @Test
    public void testAddToExistingNodes() throws InterruptedException {
        // given
        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");

        // when
        db.executeTransactionally("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Person') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Person");

        Thread.sleep(5000);

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (person:Person) return person").next().get("person");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertIsUUID(n.getAllProperties().get("uuid").toString());
            tx.commit();
        }

//         then
//        testCall(db, "MATCH (c:Person) RETURN c.uuid AS uuid",
//                (row) -> assertThat((String) row.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP)));
    }

    @Test
    public void testAddToExistingNodesBatchResult() {
        // given
        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");

        // when
        db.executeTransactionally("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE");

        // then
        try (Transaction tx = sysDb.beginTx()) {
            // todo - it's right?
            long total = (Long) tx.execute(
                            "CALL apoc.uuid.create('neo4j', 'Person') YIELD label, installed, properties, batchComputationResult " +
                                    "RETURN batchComputationResult.total as total")
                    .next()
                    .get("total");
            assertEquals(1, total);
            tx.commit();
        }
    }

    @Test
    public void testRemoveAllUuid() {
        // given
        db.executeTransactionally("CREATE CONSTRAINT ON (test:Test) ASSERT test.foo IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Bar') YIELD label RETURN label");
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Test', {addToExistingNodes: false, uuidProperty: 'foo'}) YIELD label RETURN label");
        testCallCountEventually(db, "CALL apoc.uuid.list", 2, TIMEOUT);

        // when
        TestUtil.testResult(sysDb, "CALL apoc.uuid.dropAll('neo4j')",
                (result) -> {
                    // then
                    Map<String, Object> row = result.next();
                    assertResult(row, "Bar", false,
                            Util.map(UUID_PROPERTY_KEY, "uuid", ADD_TO_SET_LABELS_KEY, false));
                    row = result.next();
                    assertResult(row, "Test", false,
                            Util.map(UUID_PROPERTY_KEY, "foo", ADD_TO_SET_LABELS_KEY, false));
                });

        testCallCountEventually(db, "CALL apoc.uuid.list", 0, TIMEOUT);
    }

    @Test(expected = RuntimeException.class)
    public void testAddWithError() {
        try {
            // when
            sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Wrong') YIELD label RETURN label");
        } catch (RuntimeException e) {
            // then
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("No constraint found for label: Wrong, please add the constraint with the following : `CREATE CONSTRAINT ON (wrong:Wrong) ASSERT wrong.uuid IS UNIQUE`", except.getMessage());
            throw e;
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAddWithErrorAndCustomField() {
        try {
            // when
            sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', 'Wrong', {uuidProperty: 'foo'}) YIELD label RETURN label");
        } catch (RuntimeException e) {
            // then
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("No constraint found for label: Wrong, please add the constraint with the following : `CREATE CONSTRAINT ON (wrong:Wrong) ASSERT wrong.foo IS UNIQUE`", except.getMessage());
            throw e;
        }
    }

    //
    // new test cases
    //

    @Test
    public void testUuidShow() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Show1) REQUIRE n.uuid IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Show2) REQUIRE n.uuid IS UNIQUE");

        String label1 = "Show1";
        String label2 = "Show2";
        String query = "MATCH (c:TestShow) SET c.count = 1";

        testCall(sysDb, "CALL apoc.uuid.create('neo4j', $name)",
                map("name", label1),
                r -> assertEquals(label1, r.get("label")));

        testCall(sysDb, "CALL apoc.uuid.create('neo4j', $name)",
                map("name", label2),
                r -> assertEquals(label2, r.get("label")));

        // not updated
        testResult(sysDb, "CALL apoc.uuid.show('neo4j')",
                map("query", query, "name", label1),
                res -> {
                    Map<String, Object> row = res.next();
                    assertEquals(label1, row.get("label"));
                    Map<String, Object> defaultProperties = Map.of(UUID_PROPERTY_KEY, DEFAULT_UUID_PROPERTY,
                            ADD_TO_SET_LABELS_KEY, DEFAULT_ADD_TO_SET_LABELS);
                    assertEquals(defaultProperties, row.get("properties"));
                    row = res.next();
                    assertEquals(label2, row.get("label"));
                    assertEquals(defaultProperties, row.get("properties"));
                    assertFalse(res.hasNext());
                });
    }

    @Test
    public void testInstallTriggerInUserDb() {
        try {
            testCall(db, "CALL apoc.uuid.create('neo4j', 'AnotherLabel')",
                    r -> fail("Should fail because of user db execution"));
        } catch (QueryExecutionException e) {
            assertThat(e.getMessage(), Matchers.containsString(PROCEDURE_NOT_ROUTED_ERROR));
        }
    }

    // TODO - it should be removed/ignored in 5.x, due to Util.validateQuery(..) removal
    @Test
    public void testInstallTriggerInWrongDb() {
        try {
            testCall(sysDb, "CALL apoc.uuid.create('notExistent', 'DbNotExistent')",
                    r -> fail("Should fail because of database not found"));
        } catch (QueryExecutionException e) {
            assertThat(e.getMessage(), Matchers.containsString(DatabaseNotFoundException.class.getName()));
        }
    }

    @Test
    public void testShowTriggerInUserDb() {
        try {
            testCall(db, "CALL apoc.uuid.show('neo4j')",
                    r -> fail("Should fail because of user db execution"));
        } catch (QueryExecutionException e) {
            assertThat(e.getMessage(), Matchers.containsString(NON_SYS_DB_ERROR));
        }
    }

    @Test
    public void testInstallTriggerInSystemDb() {
        try {
            testCall(sysDb, "CALL apoc.uuid.create('system', 'LabelInSystem')",
                    r -> fail("Should fail because of system db pointing"));
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), Matchers.containsString(BAD_TARGET_ERROR));
        }
    }


    // todo - change assertTrue(...) with assertThat((String) row.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP))


    // todo - needed?
    @Test
    public void testEventualConsistencyWithMultipleListeners() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:EventualLabel) REQUIRE n.uuid IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:EventualLabelTwo) REQUIRE n.uuid IS UNIQUE");

        final String label = "EventualLabel";


        // this does nothing, just to test consistency with multiple uuids
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', $label)",
                map("label", label) );

        // create uuid
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', $label)",
                map("label", label));
        UUIDTestUtils.awaitUuidDiscovered(db, label);

        // check uuid
        db.executeTransactionally("CREATE (n:EventualLabel)");
        testCall(db, "MATCH (c:EventualLabel) RETURN c.uuid AS uuid",
                (row) -> assertIsUUID(row.get("uuid"))
        );

        // this does nothing, just to test consistency with multiple uuids
        String labelTwo = "EventualLabelTwo";
        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', $label)",
                map("label", labelTwo) );
        UUIDTestUtils.awaitUuidDiscovered(db, labelTwo);
        testCallCount(db, "CALL apoc.uuid.list", 2);

        // check uuid
        db.executeTransactionally("CREATE (n:EventualLabelTwo)");
        testCall(db, "MATCH (c:EventualLabelTwo) RETURN c.uuid as uuid",
                (row) -> assertIsUUID(row.get("uuid"))
        );

        // check uuids
        db.executeTransactionally("CREATE (n:EventualLabel {id: 2})");
        testCall(db, "MATCH (c:EventualLabel {id: 2}) RETURN c.uuid as uuid",
                (row) -> assertIsUUID(row.get("uuid"))
        );
    }
}
