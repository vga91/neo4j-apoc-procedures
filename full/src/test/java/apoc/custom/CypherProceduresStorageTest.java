package apoc.custom;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        TestUtil.registerProcedure(db, CypherProcedures.class);
    }

    private void restartDb() throws IOException {
        databaseManagementService.shutdown();
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot().toPath()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000));
        TestUtil.registerProcedure(db, CypherProcedures.class);
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


    @Test(expected = QueryExecutionException.class)
    public void shouldOverloadAndRemoveCorrectlyTheProcedure() throws IOException {
        db.executeTransactionally("call apoc.custom.asProcedure('test_ghost','RETURN 100 as result')");

        TestUtil.testCall(db, "call custom.test_ghost", res -> {
            final Map<String, Object> row = (Map<String, Object>) res.get("row");
            assertEquals(100L, row.get("result"));
        });
        db.executeTransactionally("call apoc.custom.asProcedure('test_ghost','RETURN $count as result','read',[['result','int']],[['count','int']])");

        TestUtil.testCall(db, "call custom.test_ghost(15)", res -> assertEquals(15L, res.get("result")));
        db.executeTransactionally("call db.clearQueryCaches()");

        try {
            db.executeTransactionally("call custom.test_ghost");
            fail("Should fails because of 'Expected parameter'");
        } catch (QueryExecutionException e) {
            assertEquals("Expected parameter(s): count", e.getMessage());
        }
        TestUtil.testCall(db, "call apoc.custom.list", row -> {
            assertEquals(asList(asList("count", "integer")), row.get("inputs"));
            assertEquals(asList(asList("result", "integer")), row.get("outputs"));
            assertEquals("test_ghost", row.get("name"));
            assertEquals("RETURN $count as result", row.get("statement"));
            assertEquals("procedure", row.get("type"));
        });
        TestUtil.testResult(db, "call apoc.custom.removeProcedure('test_ghost')", res -> {});
        db.executeTransactionally("call db.clearQueryCaches()");

        TestUtil.testCallEmpty(db, "call apoc.custom.list", Collections.emptyMap());
        try {
            TestUtil.count(db, "call custom.test_ghost(1)");
            fail("Should fails because of 'unknown procedure'");
        } catch (QueryExecutionException e) {
            final String expectedMsg = "There is no procedure with the name `custom.test_ghost` registered for this database instance. " +
                    "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.";
            assertEquals(expectedMsg, e.getMessage());
        }

        restartDb();

        TestUtil.testCallEmpty(db, "call apoc.custom.list", Collections.emptyMap());
        try {
            TestUtil.count(db, "call custom.test_ghost(1)");
        } catch (QueryExecutionException e) {
            final String expectedMsg = "There is no procedure with the name `custom.test_ghost` registered for this database instance. " +
                    "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.";
            assertEquals(expectedMsg, e.getMessage());
            throw e;
        }
    }

    @Test(expected = QueryExecutionException.class)
    public void shouldOverloadAndRemoveCorrectlyTheFunction() throws IOException {
        db.executeTransactionally("call apoc.custom.asFunction('test_ghost','RETURN 100 as result')");

        TestUtil.testCall(db, "return custom.test_ghost() as row", res -> {
            final Map<String, Object> row = (Map<String, Object>) ((List) res.get("row")).get(0);
            assertEquals(100L, row.get("result"));
        });
        db.executeTransactionally("call apoc.custom.asFunction('test_ghost','RETURN $count as result','long',[['count','number']])");

        TestUtil.testCall(db, "return custom.test_ghost(15) as result", res -> assertEquals(15L, res.get("result")));
        db.executeTransactionally("call db.clearQueryCaches()");

        try {
            db.executeTransactionally("return custom.test_ghost() as row");
            fail("Should fails because of wrong 'required number of arguments'");
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains("Function call does not provide the required number of arguments: expected 1 got 0"));
        }
        TestUtil.testCall(db, "call apoc.custom.list", row -> {
            assertEquals(asList(asList("count", "number")), row.get("inputs"));
            assertEquals("integer", row.get("outputs"));
            assertEquals("test_ghost", row.get("name"));
            assertEquals("RETURN $count as result", row.get("statement"));
            assertEquals("function", row.get("type"));
        });
        TestUtil.testResult(db, "call apoc.custom.removeFunction('test_ghost')", res -> {});
        db.executeTransactionally("call db.clearQueryCaches()");

        final String expectedMsg = "Unknown function 'custom.test_ghost'";

        TestUtil.testCallEmpty(db, "call apoc.custom.list", Collections.emptyMap());
        try {
            TestUtil.count(db, "return custom.test_ghost(1)");
            fail("Should fails because of 'unknown function'");
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(expectedMsg));
        }

        restartDb();

        TestUtil.testCallEmpty(db, "call apoc.custom.list", Collections.emptyMap());
        try {
            TestUtil.count(db, "return custom.test_ghost(1)");
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(expectedMsg));
            throw e;
        }
    }
}
