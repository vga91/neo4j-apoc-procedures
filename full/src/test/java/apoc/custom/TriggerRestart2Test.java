package apoc.custom;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.trigger.Trigger;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.ApocSettings.apoc_trigger_enabled;
//import static apoc.MockApocSettings.apoc_trigger_enabled2;
//import static apoc.custom.TriggerRestart2Test.MockApocSettings.apoc_trigger_enabled2;
import static apoc.util.SystemDbUtil.KEY_THIS_DB;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TriggerRestart2Test {
    
    @Rule
    public TemporaryFolder storeDir = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;
    
    
    @Before
    public void setUp() throws Exception {
        startDb();
    }

    private void restartDb() throws IOException {
        databaseManagementService.shutdown();
        startDb();
    }

    private void startDb() {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath())
                .setConfig(apoc_trigger_enabled, true)
                .build();
        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000)); // TODO - DECOMMENTARE E CREARE COMMON METHOD
        TestUtil.registerProcedure(db, Trigger.class, CypherProcedures.class);
    }

    @Test
    public void testTriggerRunsAfterRestart() throws Exception {
        
        // create apoc.conf via sun.java.command, because with embedded db, via apocConfig() the configs are recognized too late
        final File file = storeDir.newFile("apoc.conf");
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + storeDir.getRoot().getAbsolutePath());
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(KEY_THIS_DB + "=true");
        }
        
        String name = "myTrigger";
        String statement = "unwind $createdNodes as n set n.trigger=true";
        
        db.executeTransactionally("CALL apoc.trigger.add($name, $statement, {phase:'before'})",
                Map.of("name", name, "statement", statement));

        db.executeTransactionally("CREATE (p:Person{id:1})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 1);

        String nameOther = "other";
        final String statementOtherBefore = "RETURN 'something' as row";
        final String statementOtherAfter = "RETURN 1 as res";
        db.executeTransactionally("CALL apoc.trigger.add($name, $statement, {phase:'after'}, {params: {alpha: 'beta'}})",
                Map.of("name", nameOther, "statement", statementOtherBefore));


        TestUtil.testCallCount(db, "call apoc.trigger.list()", Collections.emptyMap(), 2);

        final String phaseBefore = Util.toJson(Map.of("phase", "before"));
        try (final Transaction tx = ApocConfig.apocConfig().getSystemDb().beginTx()) {
            final Iterator<Node> nodes = withIterator(tx, SystemLabels.ApocTrigger);
            nodeAssertions(nodes.next(),
                    name, statement, phaseBefore, "{}");
            nodeAssertions(nodes.next(),
                    nameOther, statementOtherBefore, Util.toJson(Map.of("phase", "after")), Util.toJson(Map.of("alpha", "beta")));
            assertFalse(nodes.hasNext());

            final Iterator<Node> nodesMeta = withIterator(tx, SystemLabels.ApocTriggerMeta);
            assertTrue(nodesMeta.next().hasProperty(SystemPropertyKeys.lastUpdated.name()));
            assertFalse(nodesMeta.hasNext());
        }

        final String nameBaz = "baz";
        final String statementBaz = "RETURN 3 as res";
        final String paramsBaz = Util.toJson(Map.of("baz", 1L));

        final String paramsOtherAfter = Util.toJson(Map.of("foo", "bar"));
        try (final Transaction tx = db.beginTx()) {

            assertFalse(withIterator(tx, SystemLabels.ApocTrigger).hasNext());
            assertFalse(withIterator(tx, SystemLabels.ApocTriggerMeta).hasNext());

            // mock a trigger creation, this takes precedence over the other trigger
            final Node node = tx.createNode(SystemLabels.ApocTrigger);
            node.setProperty(SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME);
            node.setProperty(SystemPropertyKeys.name.name(), nameOther);
            node.setProperty(SystemPropertyKeys.statement.name(), statementOtherAfter);
            node.setProperty(SystemPropertyKeys.selector.name(), phaseBefore);
            node.setProperty(SystemPropertyKeys.params.name(), paramsOtherAfter);
            node.setProperty(SystemPropertyKeys.paused.name(), false);

            // mock a new trigger
            final Node nodeBaz = tx.createNode(SystemLabels.ApocTrigger);
            nodeBaz.setProperty(SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME);
            nodeBaz.setProperty(SystemPropertyKeys.name.name(), nameBaz);
            nodeBaz.setProperty(SystemPropertyKeys.statement.name(), statementBaz);
            nodeBaz.setProperty(SystemPropertyKeys.selector.name(), phaseBefore);
            nodeBaz.setProperty(SystemPropertyKeys.params.name(), paramsBaz);
            nodeBaz.setProperty(SystemPropertyKeys.paused.name(), false);
            
            tx.commit();
        }

        restartDb();

        try (final Transaction tx = ApocConfig.apocConfig().getSystemDb().beginTx()) {
            assertFalse(withIterator(tx, SystemLabels.ApocTrigger).hasNext());
            assertFalse(withIterator(tx, SystemLabels.ApocTriggerMeta).hasNext());
        }
        
        try (final Transaction tx = db.beginTx()) {
            final Iterator<Node> nodes = withIterator(tx, SystemLabels.ApocTrigger);
            nodeAssertions(nodes.next(), 
                    nameBaz, statementBaz, phaseBefore, paramsBaz);
            nodeAssertions(nodes.next(), 
                    name, statement, phaseBefore, "{}");
            nodeAssertions(nodes.next(), 
                    nameOther, statementOtherAfter, phaseBefore, paramsOtherAfter);
            assertFalse(nodes.hasNext());

            final Iterator<Node> nodesMeta = withIterator(tx, SystemLabels.ApocTriggerMeta);
            assertTrue(nodesMeta.next().hasProperty(SystemPropertyKeys.lastUpdated.name()));
            assertFalse(nodesMeta.hasNext());
        }

        TestUtil.testCallCount(db, "call apoc.trigger.list()", Collections.emptyMap(), 3);
        
        db.executeTransactionally("CREATE (p:Person{id:2})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 2);
    }

    private void nodeAssertions(Node node, String nameBaz, String statementBaz, String selectorBaz, String paramsBaz) {
        assertEquals(nameBaz, node.getProperty(SystemPropertyKeys.name.name()));
        assertEquals(statementBaz, node.getProperty(SystemPropertyKeys.statement.name()));
        assertEquals(selectorBaz, node.getProperty(SystemPropertyKeys.selector.name()));
        assertEquals(paramsBaz, node.getProperty(SystemPropertyKeys.params.name()));
        assertEquals(false, node.getProperty(SystemPropertyKeys.paused.name()));
    }

    // test con config specifica
    // test solo con funzionalità

    private Iterator<Node> withIterator(Transaction tx, Label label) {//}, Consumer<ResourceIterator<Node>> consumer) {
        return tx.findNodes(label,
                SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME).stream()
                .sorted(Comparator.comparing(i -> (String) i.getProperty(SystemPropertyKeys.name.name())))
                .iterator();
        
//        try (ResourceIterator<Node> nodes = tx.findNodes(label, 
//                SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME)) {
//            consumer.accept(nodes);
//        }
    }

}
