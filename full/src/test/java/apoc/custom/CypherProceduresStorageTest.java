package apoc.custom;

import apoc.path.PathExplorer;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProceduresStorageTest {

    @Rule
    public TemporaryFolder STORE_DIR = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;

    @Before
    public void setUp() throws Exception {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, CypherProcedures.class, PathExplorer.class);
    }

    private void restartDb() throws IOException {
        databaseManagementService.shutdown();
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000));
        TestUtil.registerProcedure(db, CypherProcedures.class, PathExplorer.class);
    }
    @Test
    public void registerSimpleStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        restartDb();
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("answer", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
    }

    @Test
    public void registerSimpleStatementFunction123() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('sumFun', 'RETURN $input1 + $input2 as answer','int',[['input1', 'int'], ['input2', 'int']])");
        extracted2(false);
    }

    @Test
    public void registerSimpleStatementFunction12() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('sumFun','RETURN $input1 + $input2 as answer', 'int',[['input1', 'int', 'null'], ['input2', 'int', 'null']])");
        extracted2(true);
    }

    @Test
    public void registerSimpleStatementFunction122() throws Exception {
        db.executeTransactionally("call apoc.custom.declareFunction('sumFun(input1 = null::INT, input2 = null::INT) :: INT','RETURN $input1 + $input2 AS answer')");
        extracted2(true);
    }

    @Test
    public void registerSimpleStatementFunction1() throws Exception {
        db.executeTransactionally("call apoc.custom.declareFunction('sumFun(input1::INT, input2::INT) :: INT','RETURN $input1 + $input2 AS answer')");
        extracted2(false);
    }

    @Test
    public void registerSimpleStatement123() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('sum','RETURN $input1 + $input2 AS answer','read',[['answer','int']],[['input1', 'int'], ['input2', 'int']])");
        extracted(false);
    }

    @Test
    public void registerSimpleStatement12() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('sum','RETURN $input1 + $input2 AS answer','read',[['answer','int']],[['input1', 'int', 'null'], ['input2', 'int', 'null']])");
        extracted(true);
    }

    @Test
    public void registerSimpleStatement122() throws Exception {
        db.executeTransactionally("call apoc.custom.declareProcedure('sum(input1 = null::INT, input2 = null::INT) :: (answer::INT)','RETURN $input1 + $input2 AS answer')");
        extracted(true);

    }

    @Test
    public void registerSimpleStatement1() throws Exception { 
        db.executeTransactionally("call apoc.custom.declareProcedure('sum(input1::INT, input2::INT) :: (answer::INT)','RETURN $input1 + $input2 AS answer')");
        extracted( false);
    }

    private void extracted2(boolean isDefaultNull) throws IOException {
        String expectedSignature = isDefaultNull 
                ? "custom.sumFun(input1 = null :: INTEGER?, input2 = null :: INTEGER?) :: (INTEGER?)"
                : "custom.sumFun(input1 :: INTEGER?, input2 :: INTEGER?) :: (INTEGER?)";
        asd(isDefaultNull, expectedSignature);
        restartDb();
        asd(isDefaultNull, expectedSignature);
    }

    private void asd(boolean isDefaultNull, String expectedSignature) {
        TestUtil.testCall(db, "SHOW FUNCTIONS YIELD signature, name WHERE name = 'custom.sumFun' RETURN DISTINCT signature",
                r -> assertEquals(expectedSignature, r.get("signature")));
        TestUtil.testCall(db, "RETURN custom.sumFun(40, 2) as row", (row) -> assertEquals(42L, row.get("row")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("sumFun", row.get("name"));
            assertEquals("function", row.get("type"));
        });
        if (isDefaultNull) {
            TestUtil.testCall(db, "RETURN custom.sumFun()", (row) -> assertNull(row.get("answer")));
        } else {
            try {
                TestUtil.testCall(db, "RETURN custom.sumFun()", (row) -> fail("Should fail because of missing params"));
            } catch (RuntimeException e) {
                assertTrue(e.getMessage().contains("Function call does not provide the required number of arguments: expected 2 got 0"));
            }
        }
    }

    private void extracted(boolean isDefaultNull) throws IOException {
        String expectedSignature = isDefaultNull
                ? "custom.sum(input1 = null :: INTEGER?, input2 = null :: INTEGER?) :: (answer :: INTEGER?)"
                : "custom.sum(input1 :: INTEGER?, input2 :: INTEGER?) :: (answer :: INTEGER?)";
        TestUtil.testCall(db, "SHOW PROCEDURES YIELD signature, name WHERE name = 'custom.sum'",
                r -> assertEquals(expectedSignature, r.get("signature")));
        TestUtil.testCall(db, "call custom.sum(40, 2)", (row) -> assertEquals(42L, row.get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("sum", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
        restartDb();
        TestUtil.testCall(db, "SHOW PROCEDURES YIELD signature, name WHERE name = 'custom.sum' RETURN DISTINCT signature",
                r -> assertEquals(expectedSignature, r.get("signature")));
        TestUtil.testCall(db, "call custom.sum(40, 2)", (row) -> assertEquals(42L, row.get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("sum", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
        if (isDefaultNull) {
            TestUtil.testCall(db, "call custom.sum()", (row) -> assertNull(row.get("answer")));
        } else {
            try {
                TestUtil.testCall(db, "call custom.sum()", (row) -> fail("Should fail because of missing params"));
            } catch (RuntimeException e) {
                assertTrue(e.getMessage().contains("Procedure call does not provide the required number of arguments: got 0 expected at least 2"));
            }
        }
    }

    @Test
    public void registerSimpleFunctionWithDotInName() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('foo.bar.baz','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.foo.bar.baz() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
        restartDb();
        TestUtil.testCall(db, "return custom.foo.bar.baz() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
    }

    @Test
    public void registerSimpleProcedureWithDotInName() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('foo.bar.baz','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.foo.bar.baz()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
        restartDb();
        TestUtil.testCall(db, "call custom.foo.bar.baz()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("procedure", row.get("type"));
        });
    }

    @Test
    public void registerSimpleStatementConcreteResults() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer','read',[['answer','long']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $answer as answer')");
        restartDb();
        TestUtil.testCall(db, "call custom.answer({answer:42})", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',null,[['input','number']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer(42)", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatement() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypes() throws Exception {
        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','read',null," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']])");
        restartDb();
        TestUtil.testCall(db, "call custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2}))", (row) -> assertEquals(9, ((List)((Map)row.get("row")).get("data")).size()));
    }

    @Test
    public void registerSimpleStatementFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        restartDb();
        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("answer", row.get("name"));
            assertEquals("function", row.get("type"));
        });
    }

    @Test
    public void registerSimpleStatementFunctionWithDotInName() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('foo.bar.baz','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.foo.bar.baz() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
        restartDb();
        TestUtil.testCall(db, "return custom.foo.bar.baz() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
            assertEquals("foo.bar.baz", row.get("name"));
            assertEquals("function", row.get("type"));
        });
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42 as answer','long')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunctionUnnamedResultColumn() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatementFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $answer as answer','long')");
        restartDb();
        TestUtil.testCall(db, "return custom.answer({answer:42}) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatementFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $input as answer','long',[['input','number']])");
        restartDb();
        TestUtil.testCall(db, "return custom.answer(42) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypesFunction() throws Exception {
        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','list of any'," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']], true)");
        restartDb();
        TestUtil.testCall(db, "return custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2})) as data", (row) -> assertEquals(9, ((List)row.get("data")).size()));
    }

    @Test
    public void testIssue1744() throws Exception {
        db.executeTransactionally("CREATE (:Area {name: 'foo'})-[:CURRENT]->(:VantagePoint {alpha: 'beta'})");
        db.executeTransactionally("CALL apoc.custom.asProcedure('vantagepoint_within_area',\n" +
            "  \"MATCH (start:Area {name: $areaName} )\n" +
            "    CALL apoc.path.expand(start,'CONTAINS>|<SEES|CURRENT','',0,100) YIELD path\n" +
            "    UNWIND nodes(path) as node\n" +
            "    WITH node\n" +
            "    WHERE node:VantagePoint\n" +
            "    RETURN DISTINCT node as resource\",\n" +
            "  'read',\n" +
            "  [['resource','NODE']],\n" +
            "  [['areaName', 'STRING']],\n" +
            "  \"Get vantage points within an area and all included areas\");");

        // function analogous to procedure
        db.executeTransactionally("CALL apoc.custom.asFunction('vantagepoint_within_area',\n" +
            "  \"MATCH (start:Area {name: $areaName} )\n" +
            "    CALL apoc.path.expand(start,'CONTAINS>|<SEES|CURRENT','',0,100) YIELD path\n" +
            "    UNWIND nodes(path) as node\n" +
            "    WITH node\n" +
            "    WHERE node:VantagePoint\n" +
            "    RETURN DISTINCT node as resource\",\n" +
            "  'read',\n" +
            "  [['areaName', 'STRING']]);");

        testCallIssue1744();
        restartDb();

        final String logFileContent = Files.readString(new File(FileUtils.getLogDirectory(), "debug.log").toPath());
        assertFalse(logFileContent.contains("Could not register function: custom.vantagepoint_within_area"));
        assertFalse(logFileContent.contains("Could not register procedure: custom.vantagepoint_within_area"));
        testCallIssue1744();
    }

    private void testCallIssue1744() {
        TestUtil.testCall(db, "CALL custom.vantagepoint_within_area('foo')", this::assertCallIssue1744);
        TestUtil.testCall(db, "RETURN custom.vantagepoint_within_area('foo') as resource", this::assertCallIssue1744);
    }

    private void assertCallIssue1744(Map<String, Object> row) {
        final Node resource = (Node) row.get("resource");
        assertEquals("VantagePoint", resource.getLabels().iterator().next().name());
        assertEquals("beta", resource.getProperty("alpha"));
    }
}
