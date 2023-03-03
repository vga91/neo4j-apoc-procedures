package apoc.custom;

import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.custom.CypherProcedureTestUtil.*;
import static apoc.custom.CypherProcedures.ERROR_MISMATCHED_INPUTS;
import static apoc.custom.CypherProcedures.ERROR_MISMATCHED_OUTPUTS;
import static apoc.custom.CypherProceduresHandler.*;
import static apoc.custom.Signatures.SIGNATURE_SYNTAX_ERROR;
import static apoc.util.SystemDbTestUtil.PROCEDURE_DEFAULT_REFRESH;
import static apoc.util.SystemDbTestUtil.TIMEOUT;
import static apoc.util.TestUtil.*;
import static apoc.util.TestUtil.testCall;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class CypherProceduresNewProceduresTest {
    // TODO - mettere assertThrow invece di thrown

    private static final File directory = new File("target/conf");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void beforeClass() throws Exception {
        databaseManagementService = startDbWithCustomApocConfs(storeDir);
        
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        // todo - Nodes.class and Schemas.class needed?
        TestUtil.registerProcedure(sysDb, CypherProceduresNewProcedures.class);
        TestUtil.registerProcedure(db, CypherProcedures.class,
                // todo - necessario?
                BuiltInDbmsProcedures.class );
    }

    @AfterClass
    public static void afterClass() {
        databaseManagementService.shutdown();
    }

    @After
    public void after() throws Exception {
        sysDb.executeTransactionally("CALL apoc.custom.dropAll('neo4j')");
        testCallCountEventually(db, "CALL apoc.custom.list", 0, TIMEOUT);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // todo - or create a GraphDatabaseService db in @Before instead of @BeforeClass
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.commit();
        }
    }

    //
    // test cases taken and adapted from CypherProceduresTest.java
    //

    @Test
    public void registerSimpleStatement() throws Exception {
        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'answer2() :: (answer::INT)','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.answer2()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerSimpleStatementWithOneChar() throws Exception {
        TestUtil.testFail(db, "CALL apoc.custom.installProcedure('neo4j', 'b() :: (answer::INT)','RETURN 42 as answer')", QueryExecutionException.class);
    }

//    @Test
//    public void overrideSingleCallStatement() throws Exception {
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
//        TestUtil.testCall(db, "call custom.answer() yield row return row", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 43 as answer')");
//        TestUtil.testCall(db, "call custom.answer() yield row return row", (row) -> assertEquals(43L, ((Map)row.get("row")).get("answer")));
//    }
//
//    @Test
//    public void registerSimpleStatementConcreteResults() throws Exception {
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer','read',[['answer','long']])");
//        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
//    }
//
//    @Test
//    public void registerParameterStatement() throws Exception {
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $answer as answer')");
//        TestUtil.testCall(db, "call custom.answer({answer:42})", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
//    }
//
//    @Test
//    public void registerConcreteParameterStatement() throws Exception {
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',null,[['input','number']])");
//        TestUtil.testCall(db, "call custom.answer(42)", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
//    }
//
//    @Test
//    public void registerConcreteParameterAndReturnStatement() throws Exception {
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']])");
//        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
//    }

    @Test
    public void testValidationProceduresIssue2654() {
        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'doubleProc(input::INT) :: (answer::INT)', 'RETURN $input * 2 AS answer')");
        TestUtil.testCall(db, "CALL custom.doubleProc(4);", (r) -> assertEquals(8L, r.get("answer")));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'testValTwo(input::INT) :: (answer::INT)', 'RETURN $input ^ 2 AS answer')");
        TestUtil.testCall(db, "CALL custom.testValTwo(4);", (r) -> assertEquals(16D, r.get("answer")));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'testValThree(input::MAP, power :: LONG) :: (answer::INT)', 'RETURN $input.a ^ $power AS answer')");
        TestUtil.testCall(db, "CALL custom.testValThree({a: 2}, 3);", (r) -> assertEquals(8D, r.get("answer")));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', $signature, $query)",
                Map.of("signature", "testValFour(input::INT, power::NUMBER) :: (answer::INT)",
                        "query", "UNWIND range(0, $power) AS power RETURN $input ^ power AS answer"));

        TestUtil.testResult(db, "CALL custom.testValFour(2, 3)",
                (r) -> assertEquals(List.of(1D, 2D, 4D, 8D), Iterators.asList(r.columnAs("answer"))));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', $signature, $query)",
                Map.of("signature", "multiProc(input::LOCALDATETIME, minus::INT) :: (first::INT, second:: STRING, third::DATETIME)",
                        "query", "WITH $input AS input RETURN input.year - $minus AS first, toString(input) as second, input as third"));

        TestUtil.testCall(db, "CALL custom.multiProc(localdatetime('2020'), 3);", (r) -> {
            assertEquals(2017L, r.get("first"));
            assertEquals("2020-01-01T00:00:00", r.get("second"));
            assertEquals(LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0), r.get("third"));
        });
    }

    @Test
    public void testValidationFunctionsIssue2654() {
        sysDb.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'double(input::INT) :: INT', 'RETURN $input * 2 AS answer')");
        awaitCustomFuncDiscovered(db, "double");

        TestUtil.testCall(db, "RETURN custom.double(4) AS answer", (r) -> assertEquals(8L, r.get("answer")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'testValOne(input::INT) :: INT', 'RETURN $input ^ 2 AS answer')");
        awaitCustomFuncDiscovered(db, "double");

        TestUtil.testCall(db, "RETURN custom.testValOne(3) as result", (r) -> assertEquals(9D, r.get("result")));

        sysDb.executeTransactionally("CALL apoc.custom.installFunction('neo4j', $signature, $query)",
                Map.of("signature", "multiFun(point:: POINT, input ::DATETIME, duration :: DURATION, minus = 1 ::INT) :: STRING",
                        "query", "RETURN toString($duration) + ', ' + toString($input.epochMillis - $minus) + ', ' + toString($point) as result"));
        awaitCustomFuncDiscovered(db, "multiFun");

        TestUtil.testCall(db, "RETURN custom.multiFun(point({x: 1, y:1}), datetime('2020'), duration('P5M1DT12H')) as result",
                (r) -> assertEquals("P5M1DT12H, 1577836799999, point({x: 1.0, y: 1.0, crs: 'cartesian'})", r.get("result")));
    }

//    @Test
//    public void testAllParameterTypes() throws Exception {
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','read',null," +
//                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']])");
//        TestUtil.testCall(db, "call custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2}))", (row) -> assertEquals(9, ((List)((Map)row.get("row")).get("data")).size()));
//    }

    @Test
    public void  testDeclareFunctionReturnTypes() {
        // given
        db.executeTransactionally("UNWIND range(1, 4) as val CREATE (i:Target {value: val});");

        // when
        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'ret_node(val :: INTEGER) :: NODE ', 'MATCH (t:Target {value : $val}) RETURN t')");
        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'ret_node_list(val :: INTEGER) :: LIST OF NODE ', 'MATCH (t:Target {value : $val}) RETURN [t]')");
        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'ret_map(val :: INTEGER) :: MAP ', 'RETURN {value : $val} as value')");
        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'ret_map_list(val :: INTEGER) :: LIST OF MAP ', 'RETURN [{value : $val}] as value')");

        // then
        TestUtil.testResult(db, "RETURN custom.ret_node(1) AS val", (result) -> {
            Node node = result.<Node>columnAs("val").next();
            assertTrue(node.hasLabel(Label.label("Target")));
            assertEquals(1L, node.getProperty("value"));
        });
        TestUtil.testResult(db, "RETURN custom.ret_node_list(2) AS val", (result) -> {
            List<List<Node>> nodes = result.<List<List<Node>>>columnAs("val").next();
            assertEquals(1, nodes.size());
            Node node = nodes.get(0).get(0);
            assertTrue(node.hasLabel(Label.label("Target")));
            assertEquals(2L, node.getProperty("value"));
        });
        TestUtil.testResult(db, "RETURN custom.ret_map(3) AS val", (result) -> {
            Map<String, Map<String, Object>> map = result.<Map<String, Map<String, Object>>>columnAs("val").next();
            assertEquals(1, map.size());
            assertEquals(3L, map.get("value").get("value"));
        });
        TestUtil.testResult(db, "RETURN custom.ret_map_list(4) AS val", (result) -> {
            List<Map<String, List<Map<String, Object>>>> list = result.<List<Map<String, List<Map<String, Object>>>>>columnAs("val").next();
            assertEquals(1, list.size());
            assertEquals(1, list.get(0).size());
            assertEquals(4L, list.get(0).get("value").get(0).get("value"));
        });
    }

//    @Test
//    public void testStatementReturningNode() throws Exception {
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','create path=(node)-[relationship:FOO]->() return node, relationship, path','write', [['node','Node'], ['relationship','RELATIONSHIP'], ['path','PATH']], [])");
//        TestUtil.testCall(db, "call custom.answer()", (row) -> {});
//    }
//
//    @Test
//    public void testWrongMode() {
//        assertProcedureFails("The query execution type is READ_WRITE, but you provided mode READ.\n" +
//                        "Supported modes are [READ, WRITE, WRITE, SCHEMA, DBMS]",
//                "call apoc.custom.asProcedure('answer','create path=(node)-[relationship:FOO]->() return node, relationship, path','read', [['node','Node'], ['relationship','RELATIONSHIP'], ['path','PATH']], [])");
//    }

    @Test
    public void registerSimpleStatementFunction() throws Exception {
        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'answer2() :: STRING','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.answer2() as row", (row) -> assertEquals(42L, row.get("row")));
    }

    @Test
    public void registerSimpleStatementFunctionWithOneChar() throws Exception {
        final String procedureSignature = "b() :: STRING";
        assertProcedureFails(String.format(SIGNATURE_SYNTAX_ERROR, procedureSignature),
                "CALL apoc.custom.installFunction('neo4j', '" + procedureSignature + "','RETURN 42 as answer')");
    }

//    @Test
//    public void registerSimpleStatementConcreteResultsFunction() throws Exception {
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42 as answer','long')");
//        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
//    }
//
//    @Test
//    public void registerSimpleStatementConcreteResultsFunctionUnnamedResultColumn() throws Exception {
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
//        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
//    }
//
//    @Test
//    public void registerParameterStatementFunction() throws Exception {
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $answer as answer','long')");
//        TestUtil.testCall(db, "return custom.answer({answer:42}) as answer", (row) -> assertEquals(42L, row.get("answer")));
//    }
//
//    @Test
//    public void registerConcreteParameterAndReturnStatementFunction() throws Exception {
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $input.a as answer','long',[['input','map']])");
//        TestUtil.testCall(db, "return custom.answer({a: 42}) as answer", (row) -> assertEquals(42L, row.get("answer")));
//    }
//
//    @Test
//    public void testAllParameterTypesFunction() throws Exception {
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','list of any'," +
//                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']], true)");
//        TestUtil.testCall(db, "return custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2})) as data", (row) -> assertEquals(9, ((List)row.get("data")).size()));
//    }
//
//    @Test
//    public void shouldRegisterSimpleStatementWithDescription() throws Exception {
//        // given
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer', 'read', null, null, 'Answer to the Ultimate Question of Life, the Universe, and Everything')");
//
//        // when
//        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
//
//        // then
//        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
//            assertEquals("Answer to the Ultimate Question of Life, the Universe, and Everything", row.get("description"));
//            assertEquals("procedure", row.get("type"));
//        });
//    }
//
//    @Test
//    public void shouldRegisterSimpleStatementFunctionDescription() throws Exception {
//        // given
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42 as answer', '', null, false, 'Answer to the Ultimate Question of Life, the Universe, and Everything')");
//
//        // when
//        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
//
//        // then
//        TestUtil.testCall(db, "call apoc.custom.list()", row -> {
//            assertEquals("Answer to the Ultimate Question of Life, the Universe, and Everything", row.get("description"));
//            assertEquals("function", row.get("type"));
//        });
//    }
//
//    @Test
//    public void shouldListAllProceduresAndFunctions() throws Exception {
//        // given
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']], 'Procedure that answer to the Ultimate Question of Life, the Universe, and Everything')");
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN $input as answer','long', [['input','number']], false)");
//        // System.out.println(db.execute("call apoc.custom.list").resultAsString());
//
//        // when
//        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
//            // then
//            assertTrue(row.hasNext());
//            while (row.hasNext()){
//                Map<String, Object> value = row.next();
//                assertTrue(value.containsKey("type"));
//                assertTrue(FUNCTION.equals(value.get("type")) || PROCEDURE.equals(value.get("type")));
//
//                if(PROCEDURE.equals(value.get("type"))){
//                    assertEquals("answer", value.get("name"));
//                    assertEquals(asList(asList("answer", "number")), value.get("outputs"));
//                    assertEquals(asList(asList("input", "integer", "42")), value.get("inputs"));
//                    assertEquals("Procedure that answer to the Ultimate Question of Life, the Universe, and Everything", value.get("description").toString());
//                    assertNull(value.get("forceSingle"));
//                    assertEquals("read", value.get("mode"));
//                }
//
//                if(FUNCTION.equals(value.get("type"))){
//                    assertEquals("answer", value.get("name"));
//                    assertEquals("integer", value.get("outputs"));
//                    assertEquals(asList(asList("input", "number")), value.get("inputs"));
//                    assertEquals("", value.get("description"));
//                    assertFalse((Boolean) value.get("forceSingle"));
//                    assertNull(value.get("mode"));
//                }
//            }
//        });
//    }

    @Test
    public void shouldProvideAnEmptyList() throws Exception {
        // when
        TestUtil.testResult(db, "call apoc.custom.list", (row) ->
                // then
                assertFalse(row.hasNext())
        );
    }

//    @Test
//    public void shouldRemoveTheCustomProcedure() throws Exception {
//        thrown.expect(QueryExecutionException.class);
//        thrown.expectMessage("There is no procedure with the name `custom.answer` registered for this database instance. " +
//                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.");
//        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));
//
//        // given
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
//        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
//
//        // when
//        db.executeTransactionally("call apoc.custom.removeProcedure('answer')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        // then
//        TestUtil.count(db, "call custom.answer()");
//    }
//
//    @Test
//    public void shouldOverrideAndRemoveTheCustomFunctionWithDotInName() throws Exception {
//        thrown.expect(QueryExecutionException.class);
//        thrown.expectMessage("Unknown function 'custom.a.b.c'");
//        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));
//
//        // given
//        db.executeTransactionally("CALL apoc.custom.asFunction('a.b.c','RETURN 42 as answer')");
//        TestUtil.testCall(db, "return custom.a.b.c() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//        db.executeTransactionally("CALL apoc.custom.asFunction('a.b.c','RETURN 43 as answer')");
//        TestUtil.testCall(db, "return custom.a.b.c() as row", (row) -> assertEquals(43L, ((Map)((List)row.get("row")).get(0)).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        // when
//        db.executeTransactionally("call apoc.custom.removeFunction('a.b.c')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        // then
//        TestUtil.count(db, "RETURN custom.a.b.c()");
//    }

//    @Test
//    public void shouldOverrideAndRemoveTheCustomProcedureWithDotInName() throws Exception {
//        thrown.expect(QueryExecutionException.class);
//        thrown.expectMessage("There is no procedure with the name `custom.a.b.c` registered for this database instance. " +
//                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.");
//        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));
//
//        // given
//        db.executeTransactionally("call apoc.custom.asProcedure('a.b.c','RETURN 42 as answer')");
//        TestUtil.testCall(db, "call custom.a.b.c()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//        db.executeTransactionally("call apoc.custom.asProcedure('a.b.c','RETURN 43 as answer')");
//        TestUtil.testCall(db, "call custom.a.b.c()", (row) -> assertEquals(43L, ((Map)row.get("row")).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        // when
//        db.executeTransactionally("call apoc.custom.removeProcedure('a.b.c')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        // then
//        TestUtil.count(db, "call custom.a.b.c()");
//    }

//    @Test
//    public void shouldRemoveTheCustomFunctionWithDotInName() throws Exception {
//        thrown.expect(QueryExecutionException.class);
//        thrown.expectMessage("Unknown function 'custom.a.b.c'");
//        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));
//
//        // given
//        db.executeTransactionally("CALL apoc.custom.asFunction('a.b.c','RETURN 42 as answer')");
//        TestUtil.testCall(db, "return custom.a.b.c() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
//
//        // when
//        db.executeTransactionally("call apoc.custom.removeFunction('a.b.c')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        // then
//        TestUtil.count(db, "RETURN custom.a.b.c()");
//    }

//    @Test
//    public void shouldRemoveTheCustomProcedureWithDotInName() throws Exception {
//        thrown.expect(QueryExecutionException.class);
//        thrown.expectMessage("There is no procedure with the name `custom.a.b.c` registered for this database instance. " +
//                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.");
//        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));
//
//        // given
//        db.executeTransactionally("call apoc.custom.asProcedure('a.b.c','RETURN 42 as answer')");
//        TestUtil.testCall(db, "call custom.a.b.c()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
//
//        // when
//        db.executeTransactionally("call apoc.custom.removeProcedure('a.b.c')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        // then
//        TestUtil.count(db, "call custom.a.b.c()");
//    }

//    @Test
//    public void shouldOverrideCustomFunctionWithDotInNameOnlyIfWithSameNamespaceAndFinalName() throws Exception {
//
//        // given
//        db.executeTransactionally("call apoc.custom.asFunction('a.b.name','RETURN 42 as answer')");
//        TestUtil.testCall(db, "return custom.a.b.name() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        db.executeTransactionally("call apoc.custom.asFunction('a.b.name','RETURN 34 as answer')");
//        TestUtil.testCall(db, "return custom.a.b.name() as row", (row) -> assertEquals(34L, ((Map)((List)row.get("row")).get(0)).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        db.executeTransactionally("call apoc.custom.asFunction('x.z.name','RETURN 12 as answer')");
//        TestUtil.testCall(db, "return custom.x.z.name() as row", (row) -> assertEquals(12L, ((Map)((List)row.get("row")).get(0)).get("answer")));
//        TestUtil.testCall(db, "return custom.a.b.name() as row", (row) -> assertEquals(34L, ((Map)((List)row.get("row")).get(0)).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
//            assertTrue(row.hasNext());
//            Map<String, Object> mapFirst = row.next();
//            assertEquals("a.b.name", mapFirst.get("name"));
//            assertEquals("RETURN 34 as answer", mapFirst.get("statement"));
//            assertEquals(FUNCTION, mapFirst.get("type"));
//            assertTrue(row.hasNext());
//            Map<String, Object> mapSecond = row.next();
//            assertEquals("x.z.name", mapSecond.get("name"));
//            assertEquals("RETURN 12 as answer", mapSecond.get("statement"));
//            assertEquals(FUNCTION, mapSecond.get("type"));
//            assertFalse(row.hasNext());
//        });
//
//        db.executeTransactionally("call apoc.custom.removeFunction('a.b.name')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
//            assertTrue(row.hasNext());
//            Map<String, Object> map = row.next();
//            assertEquals("x.z.name", map.get("name"));
//            assertEquals("RETURN 12 as answer", map.get("statement"));
//            assertEquals(FUNCTION, map.get("type"));
//            assertFalse(row.hasNext());
//        });
//
//        db.executeTransactionally("call apoc.custom.removeFunction('x.z.name')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        TestUtil.testCallEmpty(db, "call apoc.custom.list()", Collections.emptyMap());
//    }

//    @Test
//    public void shouldOverrideCustomProcedureWithDotInNameOnlyIfWithSameNamespaceAndFinalName() throws Exception {
//
//        // given
//        db.executeTransactionally("call apoc.custom.asProcedure('a.b.name','RETURN 42 as answer')");
//        TestUtil.testCall(db, "call custom.a.b.name()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        db.executeTransactionally("call apoc.custom.asProcedure('a.b.name','RETURN 34 as answer')");
//        TestUtil.testCall(db, "call custom.a.b.name()", (row) -> assertEquals(34L, ((Map)row.get("row")).get("answer")));
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        db.executeTransactionally("call apoc.custom.asProcedure('x.z.name','RETURN 12 as answer')");
//        TestUtil.testCall(db, "call custom.x.z.name()", (row) -> assertEquals(12L, ((Map)row.get("row")).get("answer")));
//        TestUtil.testCall(db, "call custom.a.b.name()", (row) -> assertEquals(34L, ((Map)row.get("row")).get("answer")));
//
//        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
//            assertTrue(row.hasNext());
//            Map<String, Object> mapFirst = row.next();
//            assertEquals("a.b.name", mapFirst.get("name"));
//            assertEquals("RETURN 34 as answer", mapFirst.get("statement"));
//            assertEquals(PROCEDURE, mapFirst.get("type"));
//            assertTrue(row.hasNext());
//            Map<String, Object> mapSecond = row.next();
//            assertEquals("x.z.name", mapSecond.get("name"));
//            assertEquals("RETURN 12 as answer", mapSecond.get("statement"));
//            assertEquals(PROCEDURE, mapSecond.get("type"));
//            assertFalse(row.hasNext());
//        });
//
//        db.executeTransactionally("call apoc.custom.removeProcedure('a.b.name')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
//            assertTrue(row.hasNext());
//            Map<String, Object> map = row.next();
//            assertEquals("x.z.name", map.get("name"));
//            assertEquals("RETURN 12 as answer", map.get("statement"));
//            assertEquals(PROCEDURE, map.get("type"));
//            assertFalse(row.hasNext());
//        });
//
//        db.executeTransactionally("call apoc.custom.removeProcedure('x.z.name')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        TestUtil.testCallEmpty(db, "call apoc.custom.list()", Collections.emptyMap());
//    }

//    @Test
//    public void shouldRemoveTheCustomFunction() throws Exception {
//        thrown.expect(QueryExecutionException.class);
//        thrown.expectMessage("Unknown function 'custom.answer'");
//        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));
//
//        // given
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
//        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
//
//        // when
//        db.executeTransactionally("call apoc.custom.removeFunction('answer')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//
//        // then
//        TestUtil.count(db, "return custom.answer()");
//    }

//    @Test
//    public void shouldOverwriteAndRemoveCustomProcedure() throws Exception {
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42')");
//        db.executeTransactionally("call apoc.custom.asProcedure('answer','RETURN 42')");
//        assertEquals("Expecting one procedure listed", 1, TestUtil.count(db, "call apoc.custom.list()"));
//        db.executeTransactionally("call apoc.custom.removeProcedure('answer')");
//    }

//    @Test
//    public void shouldOverwriteAndRemoveCustomFunction() throws Exception {
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
//        assertEquals("Expecting one function listed", 1, TestUtil.count(db, "call apoc.custom.list()"));
//        db.executeTransactionally("call apoc.custom.removeFunction('answer')");
//    }

//    @Test
//    public void shouldRemovalOfProcedureNodeDeactivate() {
//        thrown.expect(QueryExecutionException.class);
//        thrown.expectMessage("There is no procedure with the name `custom.answer` registered for this database instance. " +
//                "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.");
//        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));
//
//        //given
//        db.executeTransactionally("call apoc.custom.asProcedure('answer', 'RETURN 42 as answer')");
//        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
//
//        // remove the node in systemdb
//        GraphDatabaseService systemDb = databaseManagementService.database("system");
//        try (Transaction tx = systemDb.beginTx()) {
//            Node node = tx.findNode(SystemLabels.ApocCypherProcedures, SystemPropertyKeys.name.name(), "answer");
//            node.delete();
//            tx.commit();
//        }
//
//        // refresh procedures
//        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(RegisterComponentFactory.RegisterComponentLifecycle.class);
//        CypherProceduresHandler cypherProceduresHandler = (CypherProceduresHandler) registerComponentLifecycle.getResolvers().get(CypherProceduresHandler.class).get(db.databaseName());
//        cypherProceduresHandler.restoreProceduresAndFunctions();
//
//        // when
//        TestUtil.count(db, "call custom.answer()");
//    }

//    @Test
//    public void shouldRemovalOfFunctionNodeDeactivate() {
//        thrown.expect(QueryExecutionException.class);
//        thrown.expectMessage("Unknown function 'custom.answer'");
//        thrown.expect(new StatusCodeMatcher("Neo.ClientError.Statement.SyntaxError"));
//
//        //given
//        db.executeTransactionally("call apoc.custom.asFunction('answer','RETURN 42','long')");
//
//        long answer = TestUtil.singleResultFirstColumn(db, "return custom.answer()");
//        assertEquals(42L, answer);
//
//        // remove the node in systemdb
//        GraphDatabaseService systemDb = databaseManagementService.database("system");
//        try (Transaction tx = systemDb.beginTx()) {
//            Node node = tx.findNode(SystemLabels.ApocCypherProcedures, SystemPropertyKeys.name.name(), "answer");
//            node.delete();
//            tx.commit();
//        }
//
//        // refresh procedures
//        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(RegisterComponentFactory.RegisterComponentLifecycle.class);
//        CypherProceduresHandler cypherProceduresHandler = (CypherProceduresHandler) registerComponentLifecycle.getResolvers().get(CypherProceduresHandler.class).get(db.databaseName());
//        cypherProceduresHandler.restoreProceduresAndFunctions();
//
//        // when
//        TestUtil.singleResultFirstColumn(db, "return custom.answer()");
//    }

    @Test
    public void testIssue2605() {
        db.executeTransactionally("CREATE (n:Test {id: 1})-[:has]->(:Log), (n)-[:has]->(:System)");
        String query = "MATCH (node:Test)-[:has]->(log:Log) WHERE node.id = $id WITH node \n" +
                "MATCH (node)-[:has]->(log:System) RETURN log, node";
        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'testIssue2605(id :: INTEGER ) :: (log :: NODE, node :: NODE)', $query, 'read')", Map.of("query", query));

        // check query
        TestUtil.testCall(db, "call custom.testIssue2605(1)", (row) -> {
            assertEquals(List.of(Label.label("Test")), ((Node) row.get("node")).getLabels());
            assertEquals(List.of(Label.label("System")), ((Node) row.get("log")).getLabels());
        });

        // UNION ALL github issue case
        db.executeTransactionally("CREATE (n:ExampleNode {id: 1}), (:OtherExampleNode {identifier: '1'})");
        String query2 = "MATCH (:ExampleNode)\n" +
                " OPTIONAL MATCH (o:OtherExampleNode {identifier:$exampleId})\n" +
                " RETURN o.identifier as value\n" +
                " UNION ALL\n" +
                " MATCH (n:ExampleNode)\n" +
                " OPTIONAL MATCH (o:OtherExampleNode {identifier:$exampleId})\n" +
                " RETURN o.identifier as value";
        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'exampleTest(exampleId::STRING) ::(value::STRING)', $query, 'read')", Map.of("query", query2));

        // check query
        final String identifier = "1";
        TestUtil.testResult(db, "call custom.exampleTest($id)", Map.of("id", identifier), (r) -> {
            assertEquals(identifier, r.next().get("value"));
            assertEquals(identifier, r.next().get("value"));
            assertFalse(r.hasNext());
        });
    }

    @Test
    public void shouldFailWithMismatchedParameters() {
        // input mismatch
        assertProcedureFails(ERROR_MISMATCHED_INPUTS,
                "call apoc.custom.installFunction('neo4j', 'double(wrong::INT) :: INT','RETURN $input*2 as answer')");
        assertProcedureFails(ERROR_MISMATCHED_INPUTS,
                "call apoc.custom.installProcedure('neo4j', 'sum(input::INT, invalid::INT) :: (answer::INT)','RETURN $first + $second AS answer')");
        // output mismatch
        assertProcedureFails(ERROR_MISMATCHED_OUTPUTS,
                "call apoc.custom.installProcedure('neo4j', 'sum(first::INT, second::INT) :: (something::INT)','RETURN $first + $second AS answer')");
    }

//    @Test(expected = QueryExecutionException.class)
//    public void shouldCreateAVoidProcedure() {
//        // I create a function to pass later in VOID query
//        final String functionName = "toDelete";
//        final String queryFunction = String.format("RETURN custom.%s() AS num", functionName);
//        db.executeTransactionally("call apoc.custom.asFunction('" + functionName + "', 'return 10', 'INT')");
//        testCall(db, queryFunction, row -> assertEquals(10L, row.get("num")));
//
//        // now I create a custom procedure with VOID return
//        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', 'myVoidProc(name :: STRING) :: VOID','call apoc.custom.removeFunction($name)')");
//        db.executeTransactionally("CALL custom.myVoidProc('" + functionName + "')");
//        db.executeTransactionally("call db.clearQueryCaches()");
//        testCall(db, queryFunction, row -> fail("Should fail because of unknown function"));
//    }

    @Test
    public void shouldDeclareProcedureWithDefaultListAndMaps() {
        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', 'procWithFloatList(minScore = [1.1,2.2,3.3] :: LIST OF FLOAT) :: (res :: BOOLEAN, first :: FLOAT)',\n" +
                "    'return size($minScore) < 4 as res, $minScore[0] as first')");
        testCall(db, "call custom.procWithFloatList", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals(1.1D, (double) row.get("first"), 0.1D);
        });
        testCall(db, "call custom.procWithFloatList([9.1, 2.6, 3.1, 4.3, 5.5])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals(9.1D, (double) row.get("first"), 0.1D);
        });

        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', 'procWithIntList(minScore = [1,2,3] :: LIST OF INT) :: (res :: BOOLEAN, first :: FLOAT)',\n" +
                "    'return size($minScore) < 4 as res, toInteger($minScore[0]) as first')");
        testCall(db, "call custom.procWithIntList", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals(1L, row.get("first"));
        });
        testCall(db, "call custom.procWithIntList([9,2,3,4,5])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals(9L, row.get("first"));
        });

        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', 'procWithListString(minScore = [\"1\",\"2\",\"3\"] :: LIST OF STRING) :: (res :: BOOLEAN, first :: FLOAT)',\n" +
                "    'return size($minScore) < 4 as res, $minScore[0] + \" - suffix\" as first ')");
        testCall(db, "call custom.procWithListString", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals("1 - suffix", row.get("first"));
        });
        testCall(db, "call custom.procWithListString(['aaa','bbb','ccc','ddd','eee'])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals("aaa - suffix", row.get("first"));
        });

        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', 'procWithListPlainString(minScore = [1, 2, 3] :: LIST OF STRING) :: (res :: BOOLEAN, first :: FLOAT)',\n" +
                "    'return size($minScore) < 4 as res, $minScore[0] + \" - suffix\" as first ')");
        testCall(db, "call custom.procWithListPlainString", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals("1 - suffix", row.get("first"));
        });
        testCall(db, "call custom.procWithListPlainString(['aaa','bbb','ccc','ddd','eee'])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals("aaa - suffix", row.get("first"));
        });

        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', \"procWithListStringQuoted(minScore = ['1','2','3'] :: LIST OF STRING) :: (res :: BOOLEAN, first :: FLOAT)\",\n" +
                "    'return size($minScore) < 4 as res, $minScore[0] + \" - suffix\" as first ')");
        testCall(db, "call custom.procWithListStringQuoted", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals("1 - suffix", row.get("first"));
        });
        testCall(db, "call custom.procWithListStringQuoted(['aaa','bbb','ccc','ddd','eee'])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals("aaa - suffix", row.get("first"));
        });

        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', 'procWithListStringVars(minScore = [true,false,null] :: LIST OF STRING) :: (res :: BOOLEAN, first :: STRING)',\n" +
                "    'return size($minScore) < 4 as res, $minScore[0] as first ')");
        testCall(db, "call custom.procWithListStringVars", (row) -> {
            assertEquals(true, row.get("res"));
            assertEquals("true", row.get("first"));
        });
        testCall(db, "call custom.procWithListStringVars(['aaa','bbb','ccc','ddd','eee'])", (row) -> {
            assertEquals(false, row.get("res"));
            assertEquals("aaa", row.get("first"));
        });

        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', 'procWithMapList(minScore = {aa: 1, bb: \"2\"} :: MAP) :: (res :: MAP, first :: ANY)',\n" +
                "    'return $minScore as res, $minScore[\"a\"] as first ')");
        testCall(db, "call custom.procWithMapList", (row) -> {
            assertEquals(Map.of("aa", 1L, "bb", "2"), row.get("res"));
        });
        testCall(db, "call custom.procWithMapList({c: true})", (row) -> {
            assertEquals(Map.of("c", true), row.get("res"));
        });
    }

    @Test
    public void shouldDeclareFunctionWithDefaultListAndMaps() {
        db.executeTransactionally("call apoc.custom.installFunction('neo4j', 'funWithFloatList(minScore = [1.1,2.2,3.3] :: LIST OF FLOAT) :: FLOAT',\n" +
                "    'return $minScore[0]')");
        testCall(db, "RETURN custom.funWithFloatList() AS res",
                (row) -> assertEquals(1.1D, (double) row.get("res"), 0.1D));
        testCall(db, "RETURN custom.funWithFloatList([9.1, 2.6, 3.1, 4.3, 5.5]) AS res",
                (row) -> assertEquals(9.1D, (double) row.get("res"), 0.1D));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'funWithIntList(minScore = [1,2,3] :: LIST OF INT) :: BOOLEAN',\n" +
                "    'return size($minScore) < 4')");
        testCall(db, "RETURN custom.funWithIntList() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCall(db, "RETURN custom.funWithIntList([9,2,3,4,5]) AS res",
                (row) -> assertEquals(false, row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'funWithListString(minScore = [\"1\",\"2\",\"3\"] :: LIST OF STRING) :: BOOLEAN',\n" +
                "    'return size($minScore) < 4')");
        testCall(db, "RETURN custom.funWithListString() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCall(db, "RETURN custom.funWithListString(['aaa','bbb','ccc','ddd','eee']) AS res",
                (row) -> assertEquals(false, row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'funWithListStringPlain(minScore = [1, 2, 3] :: LIST OF STRING) :: BOOLEAN',\n" +
                "    'return size($minScore) < 4')");
        testCall(db, "RETURN custom.funWithListStringPlain() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCall(db, "RETURN custom.funWithListStringPlain(['aaa','bbb','ccc','ddd','eee']) AS res",
                (row) -> assertEquals(false, row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', \"funWithListStringQuoted(minScore = ['1','2','3'] :: LIST OF STRING) :: BOOLEAN\",\n" +
                "    'return size($minScore) < 4')");
        testCall(db, "RETURN custom.funWithListStringQuoted() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCall(db, "RETURN custom.funWithListStringQuoted(['aaa','bbb','ccc','ddd','eee']) AS res",
                (row) -> assertEquals(false, row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'funWithListStringVars(minScore = [true,false,null] :: LIST OF STRING) :: BOOLEAN',\n" +
                "    'return size($minScore) < 4')");
        testCall(db, "RETURN custom.funWithListStringVars() AS res",
                (row) -> assertEquals(true, row.get("res")));
        testCall(db, "RETURN custom.funWithListStringVars(['aaa','bbb','ccc','ddd','eee']) AS res",
                (row) -> assertEquals(false, row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'funWithMapList(minScore = {aa: 1, bb: \"2\"} :: MAP) :: MAP',\n" +
                "    'return $minScore AS mapRes')");
        testCall(db, "RETURN custom.funWithMapList() AS res",
                (row) -> assertEquals(Map.of("mapRes", Map.of("aa", 1L, "bb", "2")), row.get("res")));
        testCall(db, "RETURN custom.funWithMapList({c: true}) AS res",
                (row) -> assertEquals(Map.of("mapRes", Map.of("c", true)), row.get("res")));
    }

    @Test
    public void shouldDeclareProcedureWithDefaultString() {
        String query = "RETURN $minScore + ' - suffix' as res";
        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', \"procWithSingleQuotedText(minScore=' foo \\\" bar '::STRING)::(res::STRING)\", $query)",
                Map.of("query", query));
        testCall(db, "CALL custom.procWithSingleQuotedText", (row) -> {
            assertEquals(" foo \" bar  - suffix", row.get("res"));
        });

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'procWithDoubleQuotedText(minScore=\" foo \\' bar \"::STRING) :: (res::STRING)', $query)",
                Map.of("query", query));
        testCall(db, "CALL custom.procWithDoubleQuotedText", (row) -> {
            assertEquals(" foo ' bar  - suffix", row.get("res"));
        });
        testCall(db, "CALL custom.procWithDoubleQuotedText('myText')", (row) -> assertEquals("myText - suffix", row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'procWithPlainText(minScore = plainText :: STRING) :: (res::STRING)', $query)",
                Map.of("query", query));
        testCall(db, "CALL custom.procWithPlainText", (row) -> assertEquals("plainText - suffix", row.get("res")));
        testCall(db, "CALL custom.procWithPlainText('myText')", (row) -> assertEquals("myText - suffix", row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'procWithStringNull(minScore = null :: STRING) :: (res :: STRING)', $query)",
                Map.of("query", query));
        testCall(db, "CALL custom.procWithStringNull", (row) -> assertNull(row.get("res")));
        testCall(db, "CALL custom.procWithStringNull('other')", (row) -> assertEquals("other - suffix", row.get("res")));
    }

    @Test
    public void shouldDeclareFunctionWithDefaultString() {
        String query = "RETURN $minScore + ' - suffix' as res";
        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', \"funWithSingleQuotedText(minScore=' foo \\\" bar '::STRING):: STRING\", $query)",
                Map.of("query", query));
        testCall(db, "RETURN custom.funWithSingleQuotedText() AS res", (row) -> {
            assertEquals(" foo \" bar  - suffix", row.get("res"));
        });

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'funWithDoubleQuotedText(minScore=\" foo \\' bar \"::STRING) :: STRING', $query)",
                Map.of("query", query));
        testCall(db, "RETURN custom.funWithDoubleQuotedText() AS res", (row) -> {
            assertEquals(" foo ' bar  - suffix", row.get("res"));
        });
        testCall(db, "RETURN custom.funWithDoubleQuotedText('myText') AS res", (row) -> assertEquals("myText - suffix", row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'funWithPlainText(minScore = plainText :: STRING) :: STRING', $query)",
                Map.of("query", query));
        testCall(db, "RETURN custom.funWithPlainText() AS res", (row) -> assertEquals("plainText - suffix", row.get("res")));
        testCall(db, "RETURN custom.funWithPlainText('myText') AS res", (row) -> assertEquals("myText - suffix", row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'funWithStringNull(minScore = null :: STRING) :: STRING', $query)",
                Map.of("query", query));
        testCall(db, "RETURN custom.funWithStringNull() AS res", (row) -> assertNull(row.get("res")));
        testCall(db, "RETURN custom.funWithStringNull('other') AS res", (row) -> assertEquals("other - suffix", row.get("res")));
    }

    @Test
    public void shouldDeclareProcedureWithDefaultBooleanOrNull() {
        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', 'procWithBool(minScore = true :: BOOLEAN) :: (res :: INT)',\n" +
                "    'RETURN case when $minScore then 1 else 2 end as res')");

        awaitCustomProcDiscovered(db, "procWithBool");

        testCall(db, "call custom.procWithBool", (row) -> assertEquals(1L, row.get("res")));
        testCall(db, "call custom.procWithBool(true)", (row) -> assertEquals(1L, row.get("res")));
        testCall(db, "call custom.procWithBool(false)", (row) -> assertEquals(2L, row.get("res")));

        db.executeTransactionally("call apoc.custom.installProcedure('neo4j', 'procWithNull(minScore = null :: INT) :: (res :: INT)',\n" +
                "    'RETURN $minScore as res')");
        awaitCustomProcDiscovered(db, "procWithNull");
        testCall(db, "call custom.procWithNull", (row) -> assertNull(row.get("res")));
        testCall(db, "call custom.procWithNull(1)", (row) -> assertEquals(1L, row.get("res")));
    }

    @Test
    public void shouldDeclareFunctionWithDefaultBooleanOrNull() {
        db.executeTransactionally("call apoc.custom.installFunction('neo4j', 'funWithBool(minScore = true :: BOOLEAN) :: INT',\n" +
                "    'RETURN case when $minScore then 1 else 2 end as res')");
        testCall(db, "RETURN custom.funWithBool() AS res", (row) -> assertEquals(1L, row.get("res")));
        testCall(db, "RETURN custom.funWithBool(true) AS res", (row) -> assertEquals(1L, row.get("res")));
        testCall(db, "RETURN custom.funWithBool(false) AS res", (row) -> assertEquals(2L, row.get("res")));

        db.executeTransactionally("call apoc.custom.installFunction('neo4j', 'funWithNull(minScore = null :: INT) :: INT',\n" +
                "    'RETURN $minScore as res')");
        testCall(db, "RETURN custom.funWithNull() AS res", (row) -> assertNull(row.get("res")));
        testCall(db, "RETURN custom.funWithNull(1) AS res", (row) -> assertEquals(1L, row.get("res")));

    }

    @Test
    public void shouldFailDeclareFunctionWithDefaultNumberParameters() {
        final String query = "RETURN $base * $exp AS res";
        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'defaultFloatFun(base=2.4::FLOAT,exp=1.2::FLOAT):: INT', $query)",
                Map.of("query", query));
        testCall(db, "RETURN custom.defaultFloatFun() AS res", (row) -> assertEquals(2.4D * 1.2D, (double) row.get("res"), 0.1D));
        testCall(db, "RETURN custom.defaultFloatFun(1.1) AS res", (row) -> assertEquals(1.1D * 1.2D, (double) row.get("res"), 0.1D));
        testCall(db, "RETURN custom.defaultFloatFun(1.5, 7.1) AS res", (row) -> assertEquals(1.5D * 7.1D, (double) row.get("res"), 0.1D));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'defaultDoubleFun(base = 2.4 :: DOUBLE, exp = 1.2 :: DOUBLE):: DOUBLE', $query)",
                Map.of("query", query));
        testCall(db, "RETURN custom.defaultDoubleFun() AS res", (row) -> assertEquals(2.4D * 1.2D, (double) row.get("res"), 0.1D));
        testCall(db, "RETURN custom.defaultDoubleFun(1.1) AS res", (row) -> assertEquals(1.1D * 1.2D, (double) row.get("res"), 0.1D));
        testCall(db, "RETURN custom.defaultDoubleFun(1.5, 7.1) AS res", (row) -> assertEquals(1.5D * 7.1D, (double) row.get("res"), 0.1D));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'defaultIntFun(base = 4 ::INT, exp = 5 :: INT):: INT', $query)",
                Map.of("query", query));
        testCall(db, "RETURN custom.defaultIntFun() AS res", (row) -> assertEquals(4L * 5L, row.get("res")));
        testCall(db, "RETURN custom.defaultIntFun(2) AS res", (row) -> assertEquals(2L * 5L, row.get("res")));
        testCall(db, "RETURN custom.defaultIntFun(3, 7) AS res", (row) -> assertEquals(3L * 7L, row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'defaultLongFun(base = 4 ::LONG, exp = 5 :: LONG):: LONG', $query)",
                Map.of("query", query));
        testCall(db, "RETURN custom.defaultLongFun() AS res", (row) -> assertEquals(4L * 5L, row.get("res")));
        testCall(db, "RETURN custom.defaultLongFun(2) AS res", (row) -> assertEquals(2L * 5L, row.get("res")));
        testCall(db, "RETURN custom.defaultLongFun(3, 7) AS res", (row) -> assertEquals(3L * 7L, row.get("res")));
    }

    @Test
    public void shouldFailDeclareProcedureWithDefaultNumberParameters() {
        final String query = "RETURN $base * $exp AS res";
        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'defaultFloatProc(base=2.4::FLOAT,exp=1.2::FLOAT)::(res::INT)', $query)",
                Map.of("query", query));
        awaitCustomProcDiscovered(db, "defaultFloatProc");

        testCall(db, "CALL custom.defaultFloatProc", (row) -> assertEquals(2.4D * 1.2D, (double) row.get("res"), 0.1D));
        testCall(db, "CALL custom.defaultFloatProc(1.1)", (row) -> assertEquals(1.1D * 1.2D, (double) row.get("res"), 0.1D));
        testCall(db, "CALL custom.defaultFloatProc(1.5, 7.1)", (row) -> assertEquals(1.5D * 7.1D, (double) row.get("res"), 0.1D));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'defaultDoubleProc(base = 2.4 :: DOUBLE, exp = 1.2 :: DOUBLE)::(res::DOUBLE)', $query)",
                Map.of("query", query));
        awaitCustomProcDiscovered(db, "defaultDoubleProc");

        testCall(db, "CALL custom.defaultDoubleProc", (row) -> assertEquals(2.4D * 1.2D, (double) row.get("res"), 0.1D));
        testCall(db, "CALL custom.defaultDoubleProc(1.1)", (row) -> assertEquals(1.1D * 1.2D, (double) row.get("res"), 0.1D));
        testCall(db, "CALL custom.defaultDoubleProc(1.5, 7.1)", (row) -> assertEquals(1.5D * 7.1D, (double) row.get("res"), 0.1D));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'defaultIntProc(base = 4 ::INT, exp = 5 :: INT)::(res::INT)', $query)",
                Map.of("query", query));
        awaitCustomProcDiscovered(db, "defaultIntProc");

        testCall(db, "CALL custom.defaultIntProc", (row) -> assertEquals(4L * 5L, row.get("res")));
        testCall(db, "CALL custom.defaultIntProc(2)", (row) -> assertEquals(2L * 5L, row.get("res")));
        testCall(db, "CALL custom.defaultIntProc(3, 7)", (row) -> assertEquals(3L * 7L, row.get("res")));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'defaultLongProc(base = 4 ::LONG, exp = 5 :: LONG)::(res::LONG)', $query)",
                Map.of("query", query));
        awaitCustomProcDiscovered(db, "defaultLongProc");

        testCall(db, "CALL custom.defaultLongProc", (row) -> assertEquals(4L * 5L, row.get("res")));
        testCall(db, "CALL custom.defaultLongProc(2)", (row) -> assertEquals(2L * 5L, row.get("res")));
        testCall(db, "CALL custom.defaultLongProc(3, 7)", (row) -> assertEquals(3L * 7L, row.get("res")));
    }

    @Test
    public void shouldFailDeclareFunctionAndProcedureWithInvalidParameterTypes() {
        final String procedureStatementInvalidInput = "sum(input:: INVALID) :: (answer::INT)";
        assertProcedureFails(String.format(SIGNATURE_SYNTAX_ERROR, procedureStatementInvalidInput),
                "call apoc.custom.installProcedure('neo4j', '" + procedureStatementInvalidInput + "','RETURN $input AS input')");
        final String functionStatementInvalidInput = "double(input :: INVALID) :: INT";
        assertProcedureFails(String.format(SIGNATURE_SYNTAX_ERROR, functionStatementInvalidInput),
                "call apoc.custom.installFunction('neo4j', '" + functionStatementInvalidInput + "','RETURN $input*2 as answer')");

        final String procedureStatementInvalidOutput = "myProc(input :: INTEGER) :: (sum :: DUNNO)";
        assertProcedureFails(String.format(SIGNATURE_SYNTAX_ERROR, procedureStatementInvalidOutput),
                "call apoc.custom.installProcedure('neo4j', '" + procedureStatementInvalidOutput + "','RETURN $input AS sum')");
        final String functionStatementInvalidOutput = "myFunc(val :: INTEGER) :: DUNNO";
        assertProcedureFails(String.format(SIGNATURE_SYNTAX_ERROR, functionStatementInvalidOutput),
                "CALL apoc.custom.installFunction('neo4j', '" + functionStatementInvalidOutput + "', 'RETURN $val')");
    }

    @Test
    public void shouldCreateFunctionWithDefaultParameters() {
        // default inputs
        db.executeTransactionally("CALL apoc.custom.installFunction('neo4j', 'multiParDeclareFun(params = {} :: MAP) :: INT ', 'RETURN $one + $two as sum')");
        TestUtil.testCall(db, "return custom.multiParDeclareFun({one:2, two: 3}) as row", (row) -> assertEquals(5L, row.get("row")));

        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'multiParDeclareProc(params = {} :: MAP) :: (sum :: INT) ', 'RETURN $one + $two + $three as sum')");
        TestUtil.testCall(db, "call custom.multiParDeclareProc({one:2, two: 3, three: 4})", (row) -> assertEquals(9L, row.get("sum")));

        // default outputs
        db.executeTransactionally("CALL apoc.custom.installProcedure('neo4j', 'declareDefaultOut(one :: INTEGER, two :: INTEGER) :: (row :: MAP) ', 'RETURN $one + $two as sum')");
        TestUtil.testCall(db, "call custom.declareDefaultOut(5, 3)", (row) -> assertEquals(8L, ((Map<String, Object>)row.get("row")).get("sum")));
    }

    @Test
    public void testIssue2032() {
        String functionSignature = "foobar(xx::NODE, y::NODE) ::(NODE)";
        assertProcedureFails(String.format(SIGNATURE_SYNTAX_ERROR, functionSignature),
                "CALL apoc.custom.installFunction('neo4j', '" + functionSignature + "', 'MATCH (n) RETURN n limit 1');");

        String procedureSignature = "testFail(first::INT, s::INT) :: (answer::INT)";
        assertProcedureFails(String.format(SIGNATURE_SYNTAX_ERROR, procedureSignature),
                "call apoc.custom.installProcedure('neo4j', '" + procedureSignature + "','RETURN $first + $s AS answer')");
    }

    @Test
    public void testIssue3349() {
        String procedure = "CALL apoc.custom.installProcedure('neo4j', \n" +
                "  'retFunctionNames() :: (name :: STRING)',\n" +
                "  '\n" +
                "      CALL dbms.functions() YIELD name RETURN name" +
                "  ',\n" +
                "  'DBMS'\n" +
                ");";
        db.executeTransactionally(procedure);
        List<String> functions = db.executeTransactionally("CALL custom.retFunctionNames()", Map.of(), result -> result
                .stream()
                .map(m -> (String) m.get("name"))
                .collect(Collectors.toList()));
        assertFalse(functions.isEmpty());


        String procedureVoid = "CALL apoc.custom.installProcedure('neo4j', \n" +
                "  'setTxMetadata(meta :: MAP) :: VOID',\n" +
                "  '\n" +
                "      CALL tx.setMetaData($meta)" +
                "  ',\n" +
                "  'DBMS'\n" +
                ");";
        db.executeTransactionally(procedureVoid);
        // This should run without exception
        db.executeTransactionally("CALL custom.setTxMetadata($meta)", Map.of(
                "meta", Map.of("foo", "bar")
        ));
    }


    //
    // new test cases
    //

//    @Test
//    public void testUuidShow() {
//        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Show1) REQUIRE n.uuid IS UNIQUE");
//        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Show2) REQUIRE n.uuid IS UNIQUE");
//
//        String label1 = "Show1";
//        String label2 = "Show2";
//        String query = "MATCH (c:TestShow) SET c.count = 1";
//
//        testCall(sysDb, "CALL apoc.uuid.create('neo4j', $name)",
//                map("name", label1),
//                r -> assertEquals(label1, r.get("label")));
//
//        testCall(sysDb, "CALL apoc.uuid.create('neo4j', $name)",
//                map("name", label2),
//                r -> assertEquals(label2, r.get("label")));
//
//        // not updated
//        testResult(sysDb, "CALL apoc.uuid.show('neo4j')",
//                map("query", query, "name", label1),
//                res -> {
//                    Map<String, Object> row = res.next();
//                    assertEquals(label1, row.get("label"));
//                    Map<String, Object> defaultProperties = Map.of(UUID_PROPERTY_KEY, DEFAULT_UUID_PROPERTY,
//                            ADD_TO_SET_LABELS_KEY, DEFAULT_ADD_TO_SET_LABELS);
//                    assertEquals(defaultProperties, row.get("properties"));
//                    row = res.next();
//                    assertEquals(label2, row.get("label"));
//                    assertEquals(defaultProperties, row.get("properties"));
//                    assertFalse(res.hasNext());
//                });
//    }
//
//    @Test
//    public void testInstallTriggerInUserDb() {
//        try {
//            testCall(db, "CALL apoc.uuid.create('neo4j', 'AnotherLabel')",
//                    r -> fail("Should fail because of user db execution"));
//        } catch (QueryExecutionException e) {
//            assertThat(e.getMessage(), Matchers.containsString(PROCEDURE_NOT_ROUTED_ERROR));
//        }
//    }
//
//    // TODO - it should be removed/ignored in 5.x, due to Util.validateQuery(..) removal
//    @Test
//    public void testInstallTriggerInWrongDb() {
//        try {
//            testCall(sysDb, "CALL apoc.uuid.create('notExistent', 'DbNotExistent')",
//                    r -> fail("Should fail because of database not found"));
//        } catch (QueryExecutionException e) {
//            assertThat(e.getMessage(), Matchers.containsString(DatabaseNotFoundException.class.getName()));
//        }
//    }
//
//    @Test
//    public void testShowTriggerInUserDb() {
//        try {
//            testCall(db, "CALL apoc.uuid.show('neo4j')",
//                    r -> fail("Should fail because of user db execution"));
//        } catch (QueryExecutionException e) {
//            assertThat(e.getMessage(), Matchers.containsString(NON_SYS_DB_ERROR));
//        }
//    }
//
//    @Test
//    public void testInstallTriggerInSystemDb() {
//        try {
//            testCall(sysDb, "CALL apoc.uuid.create('system', 'LabelInSystem')",
//                    r -> fail("Should fail because of system db pointing"));
//        } catch (RuntimeException e) {
//            assertThat(e.getMessage(), Matchers.containsString(BAD_TARGET_ERROR));
//        }
//    }
//
//
//    // todo - change assertTrue(...) with assertThat((String) row.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP))
//
//
//    // todo - needed?
//    @Test
//    public void testEventualConsistencyWithMultipleListeners() {
//        db.executeTransactionally("CREATE CONSTRAINT FOR (n:EventualLabel) REQUIRE n.uuid IS UNIQUE");
//        db.executeTransactionally("CREATE CONSTRAINT FOR (n:EventualLabelTwo) REQUIRE n.uuid IS UNIQUE");
//
//        final String label = "EventualLabel";
//
//
//        // this does nothing, just to test consistency with multiple uuids
//        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', $label)",
//                map("label", label) );
//
//        // create uuid
//        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', $label)",
//                map("label", label));
//        awaitFunctionalityDiscovered(label);
//
//        // check uuid
//        db.executeTransactionally("CREATE (n:EventualLabel)");
//        testCall(db, "MATCH (c:EventualLabel) RETURN c.uuid AS uuid",
//                (row) -> assertThat((String) row.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP)));
//
//        // this does nothing, just to test consistency with multiple uuids
//        String labelTwo = "EventualLabelTwo";
//        sysDb.executeTransactionally("CALL apoc.uuid.create('neo4j', $label)",
//                map("label", labelTwo) );
//        awaitFunctionalityDiscovered(labelTwo);
//        testCallCount(db, "CALL apoc.uuid.list", 2);
//
//        // check uuid
//        db.executeTransactionally("CREATE (n:EventualLabelTwo)");
//        testCall(db, "MATCH (c:EventualLabelTwo) RETURN c.uuid as uuid",
//                (row) -> assertThat((String) row.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP)));
//
//        // check uuids
//        db.executeTransactionally("CREATE (n:EventualLabel {id: 2})");
//        testCall(db, "MATCH (c:EventualLabel {id: 2}) RETURN c.uuid as uuid",
//                (row) -> assertThat((String) row.get("uuid"), Matchers.matchesRegex(UUID_TEST_REGEXP)));
//
//    }

    private void assertProcedureFails(String expectedMessage, String query) {
        CypherProceduresTest.assertProcedureFails(db, expectedMessage, query);
    }

}
