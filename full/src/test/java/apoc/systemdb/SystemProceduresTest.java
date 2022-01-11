package apoc.systemdb;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;

public class SystemProceduresTest {

    @ClassRule
    public static TemporaryFolder STORE_DIR = new TemporaryFolder();

    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void setUp() throws Exception {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        TestUtil.registerProcedure(db, SystemProcedures.class);
    }

    @AfterClass
    public static void tearDown() {
        databaseManagementService.shutdown();
    }
    

    @Test
    public void testGetGraph() {
        testCall(db, "CALL apoc.systemdb.create.node(['Uno', 'Due'], {alpha: 'beta'})", result -> {
            NodeEntity node = (NodeEntity) result.get("node");
            assertEquals(Set.of("Uno", "Due"), Iterables.stream(node.getLabels()).map(Label::name).collect(Collectors.toSet()));
            assertEquals(Map.of("alpha", "beta"), node.getAllProperties());
        });
    }

    @Test
    public void testMergeNode() {
        try(Transaction tx = db.beginTx()) {
            final Node node = tx.createNode(label("Merge"), label("Node"));
            node.setProperty("alpha", "beta");
            tx.commit();
        }
        assertNumEntities(1L);

        // merge previous node
        testCall(db, "CALL apoc.systemdb.merge.node(['Merge', 'Node'], {alpha: 'beta'}, {gamma: 'delta'})", result -> {
            NodeEntity node = (NodeEntity) result.get("node");
            assertEquals(Set.of("Merge", "Node"), Iterables.stream(node.getLabels()).map(Label::name).collect(Collectors.toSet()));
            assertEquals(Map.of("alpha", "beta"), node.getAllProperties());
        });
        assertNumEntities(1L);

        // create new node due to not matching 'mergeKeys' 
        testCall(db, "CALL apoc.systemdb.merge.node(['Merge', 'Node'], {alpha: 'omicron'}, {gamma: 'delta'})", result -> {
            NodeEntity node = (NodeEntity) result.get("node");
            assertEquals(Set.of("Merge", "Node"), Iterables.stream(node.getLabels()).map(Label::name).collect(Collectors.toSet()));
            assertEquals(Map.of("alpha","omicron", "gamma", "delta"), node.getAllProperties());
        });
        assertNumEntities(2L);
    }
    
    @Test
    public void testFails() {
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        failsInNeo4jDb("CALL apoc.systemdb.create.node(['Uno', 'Due'], {alpha: 'beta'})");
        failsInNeo4jDb("CALL apoc.systemdb.merge.node(['Merge', 'Node'], {alpha: 'beta'}, {gamma: 'delta'})");
    }

    private void assertNumEntities(long expected) {
        try (Transaction tx = db.beginTx()) {
            assertEquals(expected, Iterators.count(tx.findNodes(label("Merge"))));
        }
    }

    private void failsInNeo4jDb(String s) {
        try {
            testCall(db, s, r -> fail());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Can not use an entity from another database"));
        }
    }

}
