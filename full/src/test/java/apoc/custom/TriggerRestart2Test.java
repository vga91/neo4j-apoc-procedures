package apoc.custom;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.trigger.Trigger;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.ApocSettings.apoc_trigger_enabled;
//import static apoc.MockApocSettings.apoc_trigger_enabled2;
//import static apoc.custom.TriggerRestart2Test.MockApocSettings.apoc_trigger_enabled2;
import static apoc.util.SystemDbUtil.KEY_CURRENT_DB;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

public class TriggerRestart2Test {


    @Rule
    public TemporaryFolder store_dir = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;


    @ServiceProvider
    public static class MockApocSettings implements SettingsDeclaration {
        public MockApocSettings() { }

        public static final Setting<Boolean> apoc_trigger_enabled2 = newBuilder(KEY_CURRENT_DB, BOOL, true).build();
    }

//    @Rule TODO - DECOMMENTARE FORSE
//    public DbmsRule db = new ImpermanentDbmsRule()
////                .withSetting(newBuilder(APOC_TRIGGER_ENABLED, BOOL, false).build(), true)
//                .withSetting(apoc_trigger_enabled, true)
//                .withSetting(apoc_trigger_enabled2, true);
    
    
    
//    = new ImpermanentDbmsRule()
//            .withSetting(newBuilder(APOC_TRIGGER_ENABLED, BOOL, false).build(), true)
//            .withSetting(apoc_trigger_enabled2, true);
//            .withSetting(apoc_trigger_enabled, true);  // need to use settings here, apocConfig().setProperty in `setUp` is too late

//    @BeforeClass
//    public static void before() throws Exception {
//        final MockApocSettings mockApocSettings = new MockApocSettings();
//
//        final File file1 = store_dir.newFolder("conf");
//        final File file = store_dir.newFile("conf" + File.separator + "apoc.conf");
//        System.setProperty(SUN_JAVA_COMMAND, file1.getAbsolutePath());
//        try (FileWriter writer = new FileWriter(file)) {
//            writer.write(KEY_CURRENT_DB + "=true");
//        }
//    }
    
    
    
    @Before
    public void setUp() throws Exception {
//                .setConfig(newBuilder(KEY_CURRENT_DB, BOOL, true).build(), true)
        startDb();


//        final MockApocSettings mockApocSettings = new MockApocSettings();
//        db = new ImpermanentDbmsRule()
////                .withSetting(newBuilder(APOC_TRIGGER_ENABLED, BOOL, false).build(), true)
//                .withSetting(apoc_trigger_enabled, true)
//                .withSetting(apoc_trigger_enabled2, true);
        
//        db.startLazily();
//        db.isAvailable(5000);
//        TestUtil.registerProcedure(db, Trigger.class, CypherProcedures.class);

        
        
        
//        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath())
//                .setConfig(newBuilder(KEY_CURRENT_DB, BOOL, false).build(), true)
//                .build();
//        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
//        assertTrue(db.isAvailable(5000));

//        ApocConfig.apocConfig().setProperty(KEY_CURRENT_DB, true);

//        ApocConfig.apocConfig().setProperty("apoc.trigger.enabled", "true");
//        ApocConfig.apocConfig().setProperty("apoc.storecurrentdb", true);
//        ApocConfig.apocConfig().setProperty("apoc.storecurrentdb", true);
    }

    private void startDb() {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath())
                .setConfig(apoc_trigger_enabled, true)
                .build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000)); // TODO - DECOMMENTARE E CREARE COMMON METHOD
        TestUtil.registerProcedure(db, Trigger.class, CypherProcedures.class);
    }

//    @After
//    public void tearDown() {
//        db.shutdown();
//    }

    // todo - provare a mettere un parametro. Se 
    private void restartDb() throws IOException {
//        ApocConfig.apocConfig().setProperty(KEY_CURRENT_DB, true);
        databaseManagementService.shutdown();
        startDb();
//        db.restartDatabase();// databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
//        ApocConfig.apocConfig().setProperty(KEY_CURRENT_DB, true);
    }

    @Test
    public void testTriggerRunsAfterRestart() throws Exception {
        
        // create apoc.conf
        final File file1 = store_dir.newFolder("conf");
        final File file = store_dir.newFile("conf" + File.separator + "apoc.conf");
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + file1.getAbsolutePath());
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(KEY_CURRENT_DB + "=true");
        }
        
        
//        ApocConfig.apocConfig().setProperty(KEY_CURRENT_DB, true);

//        db.execute("CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})");
        TestUtil.testResult(db, "CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})",
                result -> {
                    Map<String, Object> single = Iterators.single(result);
                    System.out.println(single);
                });
        db.executeTransactionally("CREATE (p:Person{id:1})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 1);

        restartDb();

//        // TODO: 14/07/22 to delete 
        try (final Transaction transaction = ApocConfig.apocConfig().getSystemDb().beginTx()) {
            final ResourceIterator<Node> nodes = transaction.findNodes(SystemLabels.ApocTrigger, SystemPropertyKeys.database.name(), "neo4j");
            final Node next = nodes.next();
            System.out.println("TriggerRestartTest " + next);
            transaction.commit();
        }

        TestUtil.testCallCount(db, "call apoc.trigger.list()", Collections.emptyMap(), 1);
        
        db.executeTransactionally("CREATE (p:Person{id:2})");
        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 2);
    }

    // test con config specifica

}
