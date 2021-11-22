package apoc.systemdb;

import apoc.ApocConfig;
import apoc.custom.CypherProcedures;
import apoc.cypher.CypherExtended;
import apoc.dv.DataVirtualizationCatalog;
import apoc.periodic.Periodic;
import apoc.trigger.Trigger;
import apoc.util.TestUtil;
import apoc.uuid.Uuid;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.custom.CypherProceduresHandler.FUNCTION;
import static apoc.custom.CypherProceduresHandler.PROCEDURE;
import static apoc.systemdb.SystemDbConfig.FEATURES_KEY;
import static apoc.systemdb.SystemDbConfig.FILENAME_KEY;
import static apoc.systemdb.SystemDbConfig.TRIGGERS;
import static apoc.systemdb.SystemDbConfig.UUIDS;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.load_csv_file_url_root;

public class SystemDbTest {
    private static File directory = new File("target/import");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());
    
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(ApocConfig.APOC_EXPORT_FILE_ENABLED, true);
        apocConfig().setProperty(ApocConfig.APOC_UUID_ENABLED, true);
        apocConfig().setProperty(ApocConfig.APOC_TRIGGER_ENABLED, true);
        TestUtil.registerProcedure(db, SystemDb.class, Trigger.class, CypherProcedures.class, Uuid.class, Periodic.class, DataVirtualizationCatalog.class, CypherExtended.class);
    }

    @Test
    public void testGetGraph() throws Exception {
        TestUtil.testResult(db, "CALL apoc.systemdb.graph() YIELD nodes, relationships RETURN nodes, relationships", result -> {
            Map<String, Object> map = Iterators.single(result);
            List<Node> nodes = (List<Node>) map.get("nodes");
            List<Relationship> relationships = (List<Relationship>) map.get("relationships");
            assertEquals(4, nodes.size());
            assertEquals(2, nodes.stream().filter( node -> "Database".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(1, nodes.stream().filter( node -> "User".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(1, nodes.stream().filter( node -> "Version".equals(Iterables.single(node.getLabels()).name())).count());
            Set<String> names = nodes.stream().map(node -> (String)node.getProperty("name")).filter(Objects::nonNull).collect(Collectors.toSet());
            org.hamcrest.MatcherAssert.assertThat( names, Matchers.containsInAnyOrder("neo4j", "system"));

            assertTrue(relationships.isEmpty());
        });
    }

    @Test
    public void testExecute() {
        TestUtil.testResult(db, "CALL apoc.systemdb.execute('SHOW DATABASES') YIELD row RETURN row", result -> {
            List<Map<String, Object>> rows = Iterators.asList(result.columnAs("row"));
            // removed key "systemDefault"
            org.hamcrest.MatcherAssert.assertThat(rows, Matchers.containsInAnyOrder(
                    MapUtil.map("name", "system", "default", false, "currentStatus", "online", "role", "standalone", "requestedStatus", "online", "error", "", "address", "localhost:7687", "requestedStatus", "online", "home", false),
                    MapUtil.map("name", "neo4j", "default", true, "currentStatus", "online", "role", "standalone", "requestedStatus", "online", "error", "", "address", "localhost:7687", "requestedStatus", "online", "home", true)
            ));
        });
    }

    @Test
    public void testExecuteMultipleStatements() {
        // we have two databases, so asking twice returns 4
        assertEquals(4, TestUtil.count(db, "CALL apoc.systemdb.execute(['SHOW DATABASES','SHOW DATABASES'])"));
    }

    @Test
    public void testWriteStatements() {
        // count exhaust the result - this is important here
        TestUtil.count(db, "CALL apoc.systemdb.execute([\"CREATE USER dummy SET PASSWORD '123'\"])");

        assertEquals(2l, TestUtil.count(db, "CALL apoc.systemdb.execute('SHOW USERS')"));
    }
    
    @Test
    public void testExportMetadata() {
        String procName = "procName";
        String funName = "funName";
        String declareFunName = "declareFoo";
        String declareProcName = "declareBar";
        String triggerNameOne = "alpha";
        String triggerNameTwo = "beta";
        // create features
        db.executeTransactionally("CALL apoc.trigger.add($name,'RETURN $alpha', {phase: 'after'}, {params: {alpha: 1} })", Map.of("name", triggerNameOne));
        db.executeTransactionally("CALL apoc.trigger.add($name,'RETURN 1', null)", Map.of("name", triggerNameTwo));
        db.executeTransactionally("CALL apoc.trigger.pause($name)", Map.of("name", triggerNameTwo));
        
        db.executeTransactionally(String.format("CALL apoc.custom.declareFunction('%s(val = 2 :: INTEGER) :: NODE ', 'MATCH (t:Target {value : $val}) RETURN t')", declareFunName));
        db.executeTransactionally(String.format("CALL apoc.custom.declareProcedure('%s(one = 2 ::INTEGER?, two = 3 :: INTEGER?) :: (sum :: INTEGER) ', 'RETURN $one + $two as sum')", declareProcName));
        db.executeTransactionally("CALL apoc.custom.asProcedure($name,'RETURN $input as answer','read',[['answer','number']],[['input','int','42']], 'Procedure that answer to the Ultimate Question of Life, the Universe, and Everything')",
                Map.of("name", procName));
        db.executeTransactionally("CALL apoc.custom.asFunction($name,'RETURN $input as answer','long', [['input','number']], false)", 
                Map.of("name", funName));
        
        db.executeTransactionally("CREATE CONSTRAINT person_cons ON (p:Person) ASSERT p.alpha IS UNIQUE");
        db.executeTransactionally("CALL apoc.uuid.install('Person', {addToSetLabels: true, uuidProperty: 'alpha'})");
        
        Map<String, Object> dvMap = Map.of("type", "CSV",
                "url", "file://myUrl",
                "query", "map.name = $name and map.age = $age",
                "desc", "person's details",
                "labels", List.of("Person"));
        final String dvName = "test_dv";

        db.executeTransactionally("CALL apoc.dv.catalog.add($name, $map)",
                Map.of("name", dvName, "map", dvMap));


        allFeaturesAssertions(procName, funName, declareFunName, declareProcName, triggerNameOne, triggerNameTwo, dvMap);

        TestUtil.testCallEmpty(db, "CALL apoc.systemdb.export.metadata()", Map.of());
        
        db.executeTransactionally("CALL apoc.trigger.removeAll");
        db.executeTransactionally("CALL apoc.custom.removeProcedure($name)", Map.of("name", declareProcName));
        db.executeTransactionally("CALL apoc.custom.removeProcedure($name)", Map.of("name", procName));
        db.executeTransactionally("CALL apoc.custom.removeFunction($name)", Map.of("name", funName));
        db.executeTransactionally("CALL apoc.custom.removeFunction($name)", Map.of("name", declareFunName));
        db.executeTransactionally("CALL apoc.uuid.removeAll");
        db.executeTransactionally("CALL apoc.dv.catalog.remove($name)", Map.of("name", dvName));


        // check all features removed
        List.of("CALL apoc.trigger.list", "CALL apoc.custom.list", "CALL apoc.uuid.list", "CALL apoc.dv.catalog.list")
                .forEach(query -> TestUtil.testCallEmpty(db, query, Collections.emptyMap()));

        db.executeTransactionally("CALL apoc.cypher.runFiles($files)",
                Map.of("files", List.of("metadata.customProcedures.neo4j.cypher", "metadata.dvCatalogs.neo4j.cypher", "metadata.triggers.neo4j.cypher", "metadata.uuids.neo4j.cypher")));

        allFeaturesAssertions(procName, funName, declareFunName, declareProcName, triggerNameOne, triggerNameTwo, dvMap);
        
        // -- with config and uuid constrain dropped
        db.executeTransactionally("DROP CONSTRAINT ON (p:Person) ASSERT p.alpha IS UNIQUE");
        TestUtil.testCallEmpty(db, "CALL apoc.systemdb.export.metadata($config)", 
                Map.of("config", Map.of(FILENAME_KEY, "custom", FEATURES_KEY, Set.of(UUIDS, TRIGGERS))));

        db.executeTransactionally("CALL apoc.uuid.removeAll");
        db.executeTransactionally("CALL apoc.trigger.removeAll");

        // check features removed
        List.of("CALL apoc.trigger.list", "CALL apoc.uuid.list").forEach(query -> TestUtil.testCallEmpty(db, query, Collections.emptyMap()));

        db.executeTransactionally("CALL apoc.cypher.runSchemaFile($file)", Map.of("file", "custom.uuids.schema.neo4j.cypher"));
        db.executeTransactionally("CALL apoc.cypher.runFiles($files)", Map.of("files", List.of("custom.triggers.neo4j.cypher", "custom.uuids.neo4j.cypher")));
        assertionUuidAndTrigger(triggerNameOne, triggerNameTwo);
    }

    private void allFeaturesAssertions(String procName, String funName, String declareFunName, String declareProcName, String triggerOne, String triggerTwo, Map<String, Object> map) {
        Map<String, Object> mapExpected = new HashMap<>(map);
        mapExpected.put("params", List.of("$name", "$age"));
        mapExpected.put("name", "test_dv");

        testCall(db, "CALL apoc.dv.catalog.list()", row -> assertEquals(mapExpected, row));

        TestUtil.testResult(db, "call apoc.custom.list() YIELD name, type, outputs, inputs, description, forceSingle, mode RETURN * ORDER BY name", (res) -> {
            Map<String, Object> value = res.next();
            assertEquals(declareProcName, value.get("name"));
            assertEquals(PROCEDURE, value.get("type"));
            assertEquals(asList(asList("sum", "integer")), value.get("outputs"));
            assertEquals(asList(asList("one", "integer", "2"), asList("two", "integer", "3")), value.get("inputs"));
            assertEquals("", value.get("description"));
            assertNull(value.get("forceSingle"));
            assertEquals("read", value.get("mode"));
            
            value = res.next();
            assertEquals(declareFunName, value.get("name"));
            assertEquals(FUNCTION, value.get("type"));
            assertEquals("node", value.get("outputs"));
            assertEquals(asList(asList("val", "integer", "2")), value.get("inputs"));
            assertEquals("", value.get("description"));
            assertFalse((Boolean) value.get("forceSingle"));
            assertNull(value.get("mode"));
            
            value = res.next();
            assertEquals(funName, value.get("name"));
            assertEquals(FUNCTION, value.get("type"));
            assertEquals("integer", value.get("outputs"));
            assertEquals(asList(asList("input", "number")), value.get("inputs"));
            assertEquals("", value.get("description"));
            assertFalse((Boolean) value.get("forceSingle"));
            assertNull(value.get("mode"));
            
            value = res.next();
            assertEquals(procName, value.get("name"));
            assertEquals(PROCEDURE, value.get("type"));
            assertEquals(asList(asList("answer", "number")), value.get("outputs"));
            assertEquals(asList(asList("input", "integer", "42")), value.get("inputs"));
            assertEquals("Procedure that answer to the Ultimate Question of Life, the Universe, and Everything", value.get("description").toString());
            assertNull(value.get("forceSingle"));
            assertEquals("read", value.get("mode"));
            assertFalse(res.hasNext());
        });

        assertionUuidAndTrigger(triggerOne, triggerTwo);
    }

    private void assertionUuidAndTrigger(String triggerOne, String triggerTwo) {
        TestUtil.testResult(db, "CALL apoc.trigger.list() YIELD name, query, selector, params, installed, paused RETURN * ORDER BY name", res -> {
            Map<String, Object> value = res.next();
            assertEquals(triggerOne, value.get("name"));
            assertEquals("RETURN $alpha", value.get("query"));
            assertEquals(Map.of("phase", "after"), value.get("selector"));
            assertEquals(Map.of("alpha", 1L), value.get("params"));
            assertEquals(true, value.get("installed"));
            assertEquals(false, value.get("paused"));
            value = res.next();
            assertEquals(triggerTwo, value.get("name"));
            assertEquals("RETURN 1", value.get("query"));
            assertNull(value.get("selector"));
            assertEquals(Collections.emptyMap(), value.get("params"));
            assertEquals(true, value.get("installed"));
            assertEquals(true, value.get("paused"));
            assertFalse(res.hasNext());
        });

        TestUtil.testCall(db, "CALL apoc.uuid.list", row -> {
            assertEquals("Person", row.get("label"));
            assertEquals(Map.of("uuidProperty", "alpha", "addToSetLabels", true), row.get("properties"));
        });
    }

}
