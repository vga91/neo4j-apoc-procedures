package apoc.util;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.custom.CypherProcedures;
import apoc.dv.DataVirtualizationCatalog;
import apoc.periodic.Periodic;
import apoc.trigger.Trigger;
import apoc.uuid.Uuid;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.ApocConfig.apocConfig;
import static apoc.ApocSettings.apoc_trigger_enabled;
import static apoc.ApocSettings.apoc_uuid_enabled;
import static apoc.util.SystemDbUtil.KEY_THIS_DB;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class StoreThisDbFullTest {
    @Rule
    public TemporaryFolder storeDir = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;
    private File file;
    
    @Before
    public void setUp() throws Exception {
        startDb();
        file = storeDir.newFile("apoc.conf");
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + storeDir.getRoot().getAbsolutePath());
    }

    private void restartDb() {
        databaseManagementService.shutdown();
        startDb();
    }

    private void startDb() {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath())
                .setConfig(apoc_trigger_enabled, true)
                .setConfig(apoc_uuid_enabled, true)
                .setConfig(GraphDatabaseSettings.auth_enabled, true)
                .build();
        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000));
        TestUtil.registerProcedure(db, Trigger.class, Uuid.class, Periodic.class, CypherProcedures.class, DataVirtualizationCatalog.class);
    }

    // todo - fare @After in cui faccio cose...

    @Test
    public void testUuid() throws IOException {
        db.executeTransactionally("CREATE CONSTRAINT ON (p:Person) ASSERT p.alpha IS UNIQUE");
        final String label = "Person";
        final String propertyName = "alpha";
        final boolean addToSetLabel = true;
        db.executeTransactionally("CALL apoc.uuid.install($label, {uuidProperty: $propertyName,addToSetLabels:$addToSetLabels}) YIELD label RETURN label",
                Map.of("label", label, "propertyName", propertyName, "addToSetLabels", addToSetLabel));

        try (FileWriter writer = new FileWriter(file)) {
            writer.write(KEY_THIS_DB + "=true");
        }

        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            final Iterator<Node> nodes = nodeUuidIterator(tx);
            nodeUuidAssertions(nodes.next(), label, propertyName, addToSetLabel);
            assertFalse(nodes.hasNext());
        }

        try (Transaction tx = db.beginTx()) {
            assertFalse(nodeUuidIterator(tx).hasNext());
        }

        restartDb();

        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            assertFalse(nodeUuidIterator(tx).hasNext());
        }

        try (Transaction tx = db.beginTx()) {
            final Iterator<Node> nodes = nodeUuidIterator(tx);
            nodeUuidAssertions(nodes.next(), label, propertyName, addToSetLabel);
            assertFalse(nodes.hasNext());
        }
    }
    
    

    @Test
    public void testVirtualizeCSV() throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(KEY_THIS_DB + "=true");
        }

        final String name = "csv_vr";
        final String url = getUrlFileName("test.csv").toString();
        final String desc = "person's details";
        final String query = "map.name = $name and map.age = $age";
        List<String> labels = List.of("Person");
        Map<String, Object> map = Map.of("type", "CSV",
                "url", url, "query", query,
                "desc", desc,
                "labels", labels);

        Map<String, Object> mapResult = new HashMap<>(map);
        mapResult.put("params", List.of("$name", "$age"));
        mapResult.put("name", name);

        dataVirtualizationCommon(name, map, mapResult);
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            final Iterator<Node> nodes = nodeDvIterator(tx);
            nodeDvAssertions(nodes.next(), name, mapResult);
            assertFalse(nodes.hasNext());
        }

        try (Transaction tx = db.beginTx()) {
            assertFalse(nodeDvIterator(tx).hasNext());
        }

        restartDb();
        dataVirtualizationCommon(name, map, mapResult);

        try (Transaction tx = db.beginTx()) {
            final Iterator<Node> nodes = nodeDvIterator(tx);
            nodeDvAssertions(nodes.next(), name, mapResult);
            assertFalse(nodes.hasNext());
        }

        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            assertFalse(nodeDvIterator(tx).hasNext());
        }
    }

    @Test
    public void testCustomProceduresFunctions() throws IOException {
        
        customProcsCommon();
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            final Iterator<Node> nodes = nodeCustomIterator(tx);
            nodeCustomAssertions(nodes.next(), "function desc");
            nodeCustomAssertions(nodes.next(), "procedure desc");
            assertFalse(nodes.hasNext());
        }

        try (Transaction tx = db.beginTx()) {
            assertFalse(nodeDvIterator(tx).hasNext());
        }


        try (FileWriter writer = new FileWriter(file)) {
            writer.write(KEY_THIS_DB + "=true");
        }
        restartDb();

        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            assertFalse(nodeDvIterator(tx).hasNext());
        }

        try (Transaction tx = db.beginTx()) {
            final Iterator<Node> nodes = nodeCustomIterator(tx);
            nodeCustomAssertions(nodes.next(), "function desc");
            nodeCustomAssertions(nodes.next(), "procedure desc");
            assertFalse(nodes.hasNext());

        }
        customProcsCommon();
    }

    private void customProcsCommon() {
        db.executeTransactionally("CALL apoc.custom.declareProcedure('double(input::INT) :: (answer::INT)', 'RETURN $input * 2 AS answer', 'read', 'procedure desc')");
        testCall(db, "CALL custom.double(4);", (r) -> assertEquals(8L, r.get("answer")));
        db.executeTransactionally("CALL apoc.custom.declareFunction('double(input::INT) :: INT', 'RETURN $input * 2 AS answer', false, 'function desc')");
        testCall(db, "RETURN custom.double(4) AS answer", (r) -> assertEquals(8L, r.get("answer")));
        testCallCount(db, "call apoc.custom.list", 2);
    }

    private void dataVirtualizationCommon(String name, Map<String, Object> map, Map<String, Object> mapResult) {
        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map),
                r -> assertEquals(mapResult, r));

        testCall(db, "CALL apoc.dv.catalog.list()",
                r -> assertEquals(mapResult, r));
    }
    
    // todo - common method in full
    private Iterator<Node> nodeIterator(Transaction tx, Label label, String propSort) {
        return tx.findNodes(label,
                SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME).stream()
                .sorted(Comparator.comparing(i -> (String) i.getProperty(propSort)))
//                .sorted(Comparator.comparing(i -> (String) i.getProperty(SystemPropertyKeys.name.name())))
                .iterator();
    }

    private Iterator<Node> nodeUuidIterator(Transaction tx) {
        return nodeIterator(tx, SystemLabels.ApocUuid, SystemPropertyKeys.name.name());
    }

    // todo - questo privato che richiama il common
    private Iterator<Node> nodeDvIterator(Transaction tx) {
        return nodeIterator(tx, SystemLabels.DataVirtualizationCatalog, SystemPropertyKeys.propertyName.name());
    }

    private Iterator<Node> nodeCustomIterator(Transaction tx) {
        return nodeIterator(tx, SystemLabels.ApocCypherProcedures, SystemPropertyKeys.description.name());
    }

    private void nodeDvAssertions(Node node, String name, Map<String, Object> data) {
        assertEquals(name, node.getProperty(SystemPropertyKeys.name.name()));
        assertEquals(data, JsonUtil.parse((String) node.getProperty(SystemPropertyKeys.data.name()), "", Map.class));
    }

    private void nodeCustomAssertions(Node node, String desc) {
        assertEquals("double", node.getProperty(SystemPropertyKeys.name.name()));
        assertArrayEquals(new Object[]{"custom"}, (Object[]) node.getProperty(SystemPropertyKeys.prefix.name()));
        assertEquals(desc, node.getProperty(SystemPropertyKeys.description.name()));
        assertEquals("RETURN $input * 2 AS answer", node.getProperty(SystemPropertyKeys.statement.name()));
        assertEquals(List.of(Map.of("name","input", "type", "INTEGER?")), JsonUtil.parse((String) node.getProperty(SystemPropertyKeys.inputs.name()), "", List.class));
        if (node.hasLabel(SystemLabels.Function)) {
            assertEquals("INTEGER?", node.getProperty(SystemPropertyKeys.output.name()));
            assertEquals(false, node.getProperty(SystemPropertyKeys.forceSingle.name()));
        } else {
            assertEquals(List.of(Map.of("name","answer", "type", "INTEGER?")), JsonUtil.parse((String) node.getProperty(SystemPropertyKeys.outputs.name()), "", List.class));
        }
    }

    private void nodeUuidAssertions(Node node, String label, String propertyName, boolean addToSetLabel) {
        assertEquals(label, node.getProperty(SystemPropertyKeys.label.name()));
        assertEquals(propertyName, node.getProperty(SystemPropertyKeys.propertyName.name()));
        assertEquals(addToSetLabel, node.getProperty(SystemPropertyKeys.addToSetLabel.name()));
//        assertEquals(statementBaz, JsonUtil.parse((String) node.getProperty(SystemPropertyKeys.data.name()), "", Map.class));
    }
}
