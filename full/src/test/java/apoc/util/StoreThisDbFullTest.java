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
import org.neo4j.cypher.internal.expressions.functions.E;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
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
import java.util.function.Consumer;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.ApocConfig.apocConfig;
import static apoc.ApocSettings.apoc_trigger_enabled;
import static apoc.ApocSettings.apoc_uuid_enabled;
import static apoc.create.Create.setProperties;
import static apoc.util.SystemDbUtil.KEY_THIS_DB;
import static apoc.util.TestUtil.getUrlFileName;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testResult;
import static apoc.util.TestUtil.writeFile;
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
    
    @Test
    public void testUuid() throws IOException {
        db.executeTransactionally("CREATE CONSTRAINT ON (p:Person) ASSERT p.alpha IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT ON (p:UuidAnother) ASSERT p.propTwo IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT ON (p:UuidNew) ASSERT p.propThree IS UNIQUE");
        final String label = "Person";
        final String propertyName = "alpha";
        final boolean addToSetLabel = true;
        final boolean addToSetLabelOverrode = false;
        db.executeTransactionally("CALL apoc.uuid.install($label, {uuidProperty: $propertyName,addToSetLabels:$addToSetLabels}) YIELD label RETURN label",
                Map.of("label", label, "propertyName", propertyName, "addToSetLabels", addToSetLabel));

        final String label2 = "UuidAnother";
        final String propertyName2 = "propTwo";

        db.executeTransactionally("CALL apoc.uuid.install($label, {uuidProperty: $propertyName,addToSetLabels:$addToSetLabels}) YIELD label RETURN label",
                Map.of("label", label2, "propertyName", propertyName2, "addToSetLabels", addToSetLabel));

        writeFile(file, KEY_THIS_DB + "=true");

        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            final Iterator<Node> nodes = nodeUuidIterator(tx);
            nodeUuidAssertions(nodes.next(), label, propertyName, addToSetLabel);
            nodeUuidAssertions(nodes.next(), label2, propertyName2, addToSetLabel);
            assertFalse(nodes.hasNext());
        }
        final String label3 = "UuidNew";
        final String propertyName3 = "propThree";

        try (Transaction tx = db.beginTx()) {
            assertFalse(nodeUuidIterator(tx).hasNext());

            // mock a new uuid creation, this takes precedence over the other uuid 
            final Node node = tx.createNode(SystemLabels.ApocUuid);
            node.setProperty(SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME);
            node.setProperty(SystemPropertyKeys.label.name(), label2);
            node.setProperty(SystemPropertyKeys.propertyName.name(), propertyName2);
            node.setProperty(SystemPropertyKeys.addToSetLabel.name(), addToSetLabelOverrode);

            // mock a new uuid
            final Node nodeBaz = tx.createNode(SystemLabels.ApocUuid);
            nodeBaz.setProperty(SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME);
            nodeBaz.setProperty(SystemPropertyKeys.label.name(), label3);
            nodeBaz.setProperty(SystemPropertyKeys.propertyName.name(), propertyName3);
            nodeBaz.setProperty(SystemPropertyKeys.addToSetLabel.name(), addToSetLabel);
            
            tx.commit();
        }

        restartDb();

        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            assertFalse(nodeUuidIterator(tx).hasNext());
        }

        try (Transaction tx = db.beginTx()) {
            final Iterator<Node> nodes = nodeUuidIterator(tx);
            nodeUuidAssertions(nodes.next(), label, propertyName, addToSetLabel);
            nodeUuidAssertions(nodes.next(), label2, propertyName2, addToSetLabelOverrode);
            nodeUuidAssertions(nodes.next(), label3, propertyName3, addToSetLabel);
            assertFalse(nodes.hasNext());
        }
    }
    
    
    // todo - test come sopra con altra feature
    
    

    @Test
    public void testVirtualizeCSV() throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(KEY_THIS_DB + "=true");
        }

        final String name = "csv_vr";
        final String url = "mockUrl.csv";
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

        final String nameTwo = "name_anothercsv_v";
//        Map<String, Object> map = new HashMap<>()
        Map<String, Object> mapResultTwo = new HashMap<>(mapResult);
        mapResultTwo.put("name", nameTwo);
        
        // create dv
        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", name, "map", map),
                r -> assertEquals(mapResult, r));
        
        // create dv to be overrode 
        testCall(db, "CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", nameTwo, "map", map),
                r -> assertEquals(mapResultTwo, r));

        dataVirtualizationCommon(r -> {
            Map<String, Object> next = r.next();
            next = r.next();
//            next = r.next();
            assertFalse(r.hasNext());
        });
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            final Iterator<Node> nodes = nodeDvIterator(tx);
            nodeDvAssertions(nodes.next(), name, mapResult);
            nodeDvAssertions(nodes.next(), nameTwo, mapResultTwo);
            assertFalse(nodes.hasNext());
        }

        Map<String, Object> mapOverride = new HashMap<>(mapResult);
        mapOverride.put("desc", "overrode desc");

        final String nameThree = "name_three";
        try (Transaction tx = db.beginTx()) {
            assertFalse(nodeDvIterator(tx).hasNext());

            // mock a new dv creation, this takes precedence over the other dv 
            final Node node = tx.createNode(SystemLabels.DataVirtualizationCatalog);
            node.setProperty(SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME);
            node.setProperty(SystemPropertyKeys.name.name(), nameTwo);
            node.setProperty(SystemPropertyKeys.data.name(), JsonUtil.writeValueAsString(mapOverride));

            // mock a new dv
            final Node nodeBaz = tx.createNode(SystemLabels.DataVirtualizationCatalog);
            nodeBaz.setProperty(SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME);
            nodeBaz.setProperty(SystemPropertyKeys.name.name(), nameThree);
            nodeBaz.setProperty(SystemPropertyKeys.data.name(), JsonUtil.writeValueAsString(mapResultTwo));
            
            tx.commit();
        }

        restartDb();
        dataVirtualizationCommon(r -> {
            Map<String, Object> next = r.next();
            next = r.next();
            next = r.next();
            assertFalse(r.hasNext());
        });

        try (Transaction tx = db.beginTx()) {
            final Iterator<Node> nodes = nodeDvIterator(tx);
            nodeDvAssertions(nodes.next(), name, mapResult);
            nodeDvAssertions(nodes.next(), nameTwo, mapOverride);
            nodeDvAssertions(nodes.next(), nameThree, mapResultTwo);
            assertFalse(nodes.hasNext());
        }

        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            assertFalse(nodeDvIterator(tx).hasNext());
        }
    }

    @Test
    public void testCustomProceduresFunctions() throws IOException {

        db.executeTransactionally("CALL apoc.custom.declareFunction('name2(input::INT) :: INT', 'RETURN $input * 2 AS answer', false, 'function desc')");
        testCall(db, "RETURN custom.name2(4) AS answer", (r) -> assertEquals(8L, r.get("answer")));


        db.executeTransactionally("CALL apoc.custom.declareProcedure('double(input::INT) :: (answer::INT)', 'RETURN $input * 2 AS answer', 'read', 'procedure desc')");
        db.executeTransactionally("CALL apoc.custom.declareFunction('double(input::INT) :: INT', 'RETURN $input * 2 AS answer', false, 'function desc')");
        customProcsCommon();
        testCallCount(db, "call apoc.custom.list", 3);
        
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            final Iterator<Node> nodes = nodeCustomIterator(tx);
            nodes.next();
            nodes.next();
            nodes.next();
//            nodeCustomAssertions(nodes.next(), "function desc");
//            nodeCustomAssertions(nodes.next(), "function desc");
//            nodeCustomAssertions(nodes.next(), "procedure desc");
            assertFalse(nodes.hasNext());

            final Iterator<Node> nodeIterator = nodeCustomMetaIterator(tx);
            assertTrue(nodeIterator.next().hasProperty(SystemPropertyKeys.lastUpdated.name()));
            assertFalse(nodes.hasNext());
        }

        final Map<String, Object> nodeTwoProps = Map.of(
                SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME,
                SystemPropertyKeys.name.name(), "name2",
                SystemPropertyKeys.prefix.name(), new String[]{"custom"},
                SystemPropertyKeys.description.name(), "desc override",
                SystemPropertyKeys.statement.name(), "return 10 as ten",
                SystemPropertyKeys.inputs.name(), Util.toJson(List.of(Map.of("name", "nameInput", "type", "INTEGER?"))),
                SystemPropertyKeys.output.name(), "INTEGER?",
                SystemPropertyKeys.forceSingle.name(), true
        );
        final Map<String, Object> nodeThreeProps = Map.of(
                SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME,
                SystemPropertyKeys.name.name(), "name3",
                SystemPropertyKeys.prefix.name(), new String[]{"custom"},
                SystemPropertyKeys.description.name(), "desc another",
                SystemPropertyKeys.statement.name(), "return $nameInput as nameOut",
                SystemPropertyKeys.inputs.name(), Util.toJson(List.of(Map.of("name", "nameInput", "type", "INTEGER?"))),
                SystemPropertyKeys.outputs.name(), Util.toJson(List.of(Map.of("name", "nameOut", "type", "INTEGER?"))),
                SystemPropertyKeys.forceSingle.name(), true
        );
        try (Transaction tx = db.beginTx()) {
            // todo - check Meta also...
            assertFalse(nodeCustomIterator(tx).hasNext());
            assertFalse(nodeCustomMetaIterator(tx).hasNext());

            // mock a new customFun creation, this takes precedence over the other customFun 
            final Node node = tx.createNode(SystemLabels.ApocCypherProcedures, SystemLabels.Function);
//            try { 
                setProperties(node, nodeTwoProps);
//            } catch (Exception e) {
//                System.out.println("StoreThisDbFullTest.testCustomProceduresFunctions");
//            }

            // mock a new customFun
            final Node nodeBaz = tx.createNode(SystemLabels.ApocCypherProcedures, SystemLabels.Procedure);
            setProperties(nodeBaz, nodeThreeProps);
            
            tx.commit();
        }


        try (FileWriter writer = new FileWriter(file)) {
            writer.write(KEY_THIS_DB + "=true");
        }
        restartDb();

        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            assertFalse(nodeCustomIterator(tx).hasNext());
        }

        try (Transaction tx = db.beginTx()) {
            final Iterator<Node> nodes = nodeCustomIterator(tx);
//            nodeCustomAssertions(nodes.next(), nodeTwoProps);
//            nodeCustomAssertions(nodes.next(), nodeThreeProps);
            nodes.next();
            nodes.next();
            nodes.next();
            nodes.next(); 
//            nodeCustomAssertions(nodes.next(), "function desc");
//            nodeCustomAssertions(nodes.next(), "procedure desc");
            assertFalse(nodes.hasNext());

            final Iterator<Node> nodeIterator = nodeCustomMetaIterator(tx);
            assertTrue(nodeIterator.next().hasProperty(SystemPropertyKeys.lastUpdated.name()));
            assertFalse(nodes.hasNext());
        }
        
        customProcsCommon();
        testCallCount(db, "call apoc.custom.list", 4);
    }

    private void customProcsCommon() {
        testCall(db, "CALL custom.double(4);", (r) -> assertEquals(8L, r.get("answer")));
        testCall(db, "RETURN custom.double(4) AS answer", (r) -> assertEquals(8L, r.get("answer")));
        
    }

    private void dataVirtualizationCommon(Consumer<Result> resultConsumer) {

//        final Consumer<Result> resultConsumer = r -> {
//            Map<String, Object> next = r.next();
//            next = r.next();
//            next = r.next();
//            assertFalse(r.hasNext());
//        };
        testResult(db, "CALL apoc.dv.catalog.list() yield name, type, url, desc, labels, query, params " +
                        "return * order by name",
                resultConsumer);
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
        return nodeIterator(tx, SystemLabels.ApocUuid, SystemPropertyKeys.label.name());
    }

    // todo - questo privato che richiama il common
    private Iterator<Node> nodeDvIterator(Transaction tx) {
        return nodeIterator(tx, SystemLabels.DataVirtualizationCatalog, SystemPropertyKeys.name.name());
    }

    private Iterator<Node> nodeCustomMetaIterator(Transaction tx) {
        return nodeIterator(tx, SystemLabels.ApocCypherProceduresMeta, SystemPropertyKeys.lastUpdated.name());
    }

    private Iterator<Node> nodeCustomIterator(Transaction tx) {
        return nodeIterator(tx, SystemLabels.ApocCypherProcedures, SystemPropertyKeys.description.name());
    }

    private void nodeDvAssertions(Node node, String name, Map<String, Object> data) {
        assertEquals(name, node.getProperty(SystemPropertyKeys.name.name()));
        assertEquals(data, JsonUtil.parse((String) node.getProperty(SystemPropertyKeys.data.name()), "", Map.class));
    }

    private void nodeCustomAssertions(Node node, Map<String, Object> props) {
        final Map<String, Object> allProperties = node.getAllProperties();
        final HashMap<String, Object> stringObjectHashMap = new HashMap<>(props);
        stringObjectHashMap.remove("prefix");
        allProperties.remove("prefix");
        assertEquals(stringObjectHashMap, allProperties);
    }

    private void nodeCustomAssertions1(Node node, String desc) {
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
