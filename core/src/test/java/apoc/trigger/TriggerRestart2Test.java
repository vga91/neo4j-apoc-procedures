//package apoc.trigger;
//
//import apoc.ApocConfig;
//import apoc.SystemLabels;
//import apoc.util.TestUtil;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TemporaryFolder;
//import org.neo4j.configuration.GraphDatabaseSettings;
//import org.neo4j.dbms.api.DatabaseManagementService;
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.Node;
//import org.neo4j.graphdb.ResourceIterator;
//import org.neo4j.graphdb.Transaction;
//import org.neo4j.internal.helpers.collection.Iterators;
//import org.neo4j.test.TestDatabaseManagementServiceBuilder;
//import org.neo4j.test.rule.DbmsRule;
//import org.neo4j.test.rule.ImpermanentDbmsRule;
//
//import java.io.IOException;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//
//import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
//import static apoc.ApocSettings.apoc_trigger_enabled;
//import static apoc.util.SystemDbUtil.KEY_CURRENT_DB;
//import static org.junit.Assert.assertTrue;
//import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
//import static org.neo4j.configuration.SettingImpl.newBuilder;
//import static org.neo4j.configuration.SettingValueParsers.BOOL;
//
//public class TriggerRestart2Test {
//    
//    @Rule
//    public DbmsRule db = new ImpermanentDbmsRule()
//            .withSetting(newBuilder(APOC_TRIGGER_ENABLED, BOOL, false).build(), true)
//            .withSetting(newBuilder(KEY_CURRENT_DB, BOOL, false).build(), true);
////            .withSetting(apoc_trigger_enabled, true);  // need to use settings here, apocConfig().setProperty in `setUp` is too late
//
//    @Before
//    public void setUp() {
////        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath())
////                .setConfig(newBuilder(KEY_CURRENT_DB, BOOL, false).build(), true)
////                .build();
////        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
////        assertTrue(db.isAvailable(5000));
//
//        ApocConfig.apocConfig().setProperty(KEY_CURRENT_DB, true);
//
//        ApocConfig.apocConfig().setProperty("apoc.trigger.enabled", "true");
////        ApocConfig.apocConfig().setProperty("apoc.storecurrentdb", true);
////        ApocConfig.apocConfig().setProperty("apoc.storecurrentdb", true);
//        TestUtil.registerProcedure(db, Trigger.class, CypherProcedure.class);
//    }
//
//    @After
//    public void tearDown() {
//        db.shutdown();
//    }
//
//    // todo - provare a mettere un parametro. Se 
//    private void restartDb() throws IOException {
////        ApocConfig.apocConfig().setProperty(KEY_CURRENT_DB, true);
////        databaseManagementService.shutdown();
////        databaseManagementService = new TestDatabaseManagementServiceBuilder(store_dir.getRoot().toPath())
////                .setConfig(newBuilder(KEY_CURRENT_DB, BOOL, false).build(), true)
////                .build();
//        db.restartDatabase();// databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
//        ApocConfig.apocConfig().setProperty(KEY_CURRENT_DB, true);
//        assertTrue(db.isAvailable(5000));
//    }
//
//    @Test
//    public void testTriggerRunsAfterRestart() throws Exception {
////        ApocConfig.apocConfig().setProperty(KEY_CURRENT_DB, true);
//        
////        db.execute("CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})");
//        TestUtil.testResult(db, "CALL apoc.trigge" +
//                        "r.add('myTrigger', 'unwind $createdNodes as n set n.trigger=true', {phase:'before'})",
//                result -> {
//                    Map<String, Object> single = Iterators.single(result);
//                    System.out.println(single);
//                });
//        db.executeTransactionally("CREATE (p:Person{id:1})");
//        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 1);
//
//        restartDb();
//        System.out.println("restarted = ");
//
////        // TODO: 14/07/22 to delete 
//        try (final Transaction transaction = ApocConfig.apocConfig().getSystemDb().beginTx()) {
//            final ResourceIterator<Node> nodes = transaction.findNodes(SystemLabels.ApocTrigger);
//            final Node next = nodes.next();
//            System.out.println("TriggerRestartTest " + next);
//            transaction.commit();
//        }
//
//        db.executeTransactionally("CREATE (p:Person{id:2})");
//        TestUtil.testCallCount(db, "match (n:Person{trigger:true}) return n", Collections.emptyMap(), 2);
//    }
//    
//    // test con config specifica
//
//}
