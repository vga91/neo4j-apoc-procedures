package apoc.trigger;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
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
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.ApocSettings.apoc_trigger_enabled;
import static apoc.util.SystemDbUtil.KEY_THIS_DB;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.writeFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;


public class StoreThisDbCoreTest {

    @Rule
    public TemporaryFolder storeDir = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;
    private File file;
    

    @Before
    public void setUp() throws Exception {
        startDb();

        // create apoc.conf via sun.java.command, because with embedded db, via apocConfig() the configs are recognized too late
        file = storeDir.newFile("apoc.conf");
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + storeDir.getRoot().getAbsolutePath());
    }

    // todo - @After if needed

    private void restartDb() {
        databaseManagementService.shutdown();
        startDb();
    }

    // todo - test util
    private void startDb() {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath())
                .setConfig(apoc_trigger_enabled, true)
                .build();
        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000));
        TestUtil.registerProcedure(db, Trigger.class);
    }

    @Test
    public void testTriggerRunsAfterRestart() throws Exception {
        writeFile(file, KEY_THIS_DB + "=true");

        String name = "myTrigger";
        String statement = "unwind $createdNodes as n set n.trigger=true";

        db.executeTransactionally("CALL apoc.trigger.add($name, $statement, {phase:'before'})",
                Map.of("name", name, "statement", statement));

        db.executeTransactionally("CREATE (p:Person{id:1})");
        testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 1);

        String nameOther = "other";
        final String statementOtherBefore = "RETURN 'something' as row";
        final String statementOtherAfter = "RETURN 1 as res";
        db.executeTransactionally("CALL apoc.trigger.add($name, $statement, {phase:'after'}, {params: {alpha: 'beta'}})",
                Map.of("name", nameOther, "statement", statementOtherBefore));


        testCallCount(db, "call apoc.trigger.list()", Collections.emptyMap(), 2);

        final String phaseBefore = Util.toJson(Map.of("phase", "before"));
        try (final Transaction tx = ApocConfig.apocConfig().getSystemDb().beginTx()) {
            final Iterator<Node> nodes = nodeIterator(tx, SystemLabels.ApocTrigger);
            nodeAssertions(nodes.next(),
                    name, statement, phaseBefore, "{}");
            nodeAssertions(nodes.next(),
                    nameOther, statementOtherBefore, Util.toJson(Map.of("phase", "after")), Util.toJson(Map.of("alpha", "beta")));
            assertFalse(nodes.hasNext());

            final Iterator<Node> nodesMeta = nodeIterator(tx, SystemLabels.ApocTriggerMeta);
            assertTrue(nodesMeta.next().hasProperty(SystemPropertyKeys.lastUpdated.name()));
            assertFalse(nodesMeta.hasNext());
        }

        final String nameBaz = "baz";
        final String statementBaz = "RETURN 3 as res";
        final String paramsBaz = Util.toJson(Map.of("baz", 1L));

        final String paramsOtherAfter = Util.toJson(Map.of("foo", "bar"));
        try (final Transaction tx = db.beginTx()) {

            assertFalse(nodeIterator(tx, SystemLabels.ApocTrigger).hasNext());
            assertFalse(nodeIterator(tx, SystemLabels.ApocTriggerMeta).hasNext());

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
            assertFalse(nodeIterator(tx, SystemLabels.ApocTrigger).hasNext());
            assertFalse(nodeIterator(tx, SystemLabels.ApocTriggerMeta).hasNext());
        }

        try (final Transaction tx = db.beginTx()) {
            final Iterator<Node> nodes = nodeIterator(tx, SystemLabels.ApocTrigger);
            nodeAssertions(nodes.next(),
                    nameBaz, statementBaz, phaseBefore, paramsBaz);
            nodeAssertions(nodes.next(),
                    name, statement, phaseBefore, "{}");
            nodeAssertions(nodes.next(),
                    nameOther, statementOtherAfter, phaseBefore, paramsOtherAfter);
            assertFalse(nodes.hasNext());

            final Iterator<Node> nodesMeta = nodeIterator(tx, SystemLabels.ApocTriggerMeta);
            assertTrue(nodesMeta.next().hasProperty(SystemPropertyKeys.lastUpdated.name()));
            assertFalse(nodesMeta.hasNext());
        }

        testCallCount(db, "call apoc.trigger.list()", Collections.emptyMap(), 3);

        db.executeTransactionally("CREATE (p:Person{id:2})");
        testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 2);
    }

    private void nodeAssertions(Node node, String nameBaz, String statementBaz, String selectorBaz, String paramsBaz) {
        assertEquals(nameBaz, node.getProperty(SystemPropertyKeys.name.name()));
        assertEquals(statementBaz, node.getProperty(SystemPropertyKeys.statement.name()));
        assertEquals(selectorBaz, node.getProperty(SystemPropertyKeys.selector.name()));
        assertEquals(paramsBaz, node.getProperty(SystemPropertyKeys.params.name()));
        assertEquals(false, node.getProperty(SystemPropertyKeys.paused.name()));
    }

    // todo - test with specific config
    
    // todo - test with single functionality


    private Iterator<Node> nodeIterator(Transaction tx, Label label) {
        return tx.findNodes(label,
                SystemPropertyKeys.database.name(), DEFAULT_DATABASE_NAME).stream()
                .sorted(Comparator.comparing(i -> (String) i.getProperty(SystemPropertyKeys.name.name())))
                .iterator();

    }

}
