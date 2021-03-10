package apoc.sequence;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static apoc.sequence.SequenceTest.assertSequenceConstraintSizeWithDbms;
import static apoc.util.TestUtil.singleResultFirstColumn;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testFail;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SequenceStorageTest {

    @Rule
    public TemporaryFolder STORE_DIR = new TemporaryFolder();

    private GraphDatabaseService db;
    private DatabaseManagementService databaseManagementService;

    @Before
    public void setUp() throws Exception {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, Sequence.class);
    }

    private void restartDb() {
        databaseManagementService.shutdown();
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000));
        TestUtil.registerProcedure(db, Sequence.class);
    }

    @Test
    public void storeSequenceAndIncrementAfterRestart() throws IOException {
        db.executeTransactionally("CALL apoc.sequence.create('test', {initialValue: 1})");

        assertSequenceConstraintSize(1);
        restartDb();

        assertSequenceConstraintSize(1);
        long actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.currentValue('test')");
        assertEquals(1L, actualValue);

        actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.nextValue('test')");
        assertEquals(2L, actualValue);

        db.executeTransactionally("CALL apoc.sequence.drop('test')");
        assertSequenceConstraintSize(0);
    }

    @Test
    public void storeSequenceAfterIncrement() throws IOException {
        db.executeTransactionally("CALL apoc.sequence.create('test.Increment', {initialValue: 3})");
        final String queryCurrentValue = "RETURN apoc.sequence.currentValue('test.Increment')";
        long actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(3L, actualValue);

        actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.nextValue('test.Increment')");
        assertEquals(4L, actualValue);

        restartDb();

        actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(4L, actualValue);

        db.executeTransactionally("CALL apoc.sequence.drop('test.Increment')");
    }

    @Test
    public void dropSequenceAfterRestart() throws IOException {
        db.executeTransactionally("CALL apoc.sequence.create('dropAfterRestart', {initialValue: 5})");

        long actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.nextValue('dropAfterRestart')");
        assertEquals(6L, actualValue);

        final String queryCurrentValue = "RETURN apoc.sequence.currentValue('dropAfterRestart')";
        actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(6L, actualValue);

        restartDb();

        actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(6L, actualValue);

        db.executeTransactionally("CALL apoc.sequence.drop('dropAfterRestart')");

        testFail(db, queryCurrentValue, QueryExecutionException.class);
    }

    @Test
    public void shouldOverrideSequenceWithSameName() throws IOException {
        testCall(db, "CALL apoc.sequence.create('sameName', {initialValue: 1})", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(1L, row.get("value"));
        });
        restartDb();

        final String queryCurrentValue = "RETURN apoc.sequence.currentValue('sameName')";
        long actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(1L, actualValue);

        testCall(db, "CALL apoc.sequence.create('sameName', {initialValue: 3})", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(3L, row.get("value"));
        });
        testCall(db, "CALL apoc.sequence.create('sameName', {initialValue: 6})", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(6L, row.get("value"));
        });
        restartDb();
        actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(6L, actualValue);

        actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.nextValue('sameName')");
        assertEquals(7L, actualValue);

        testCall(db, "CALL apoc.sequence.list", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(7L, row.get("value"));
        });
        testCallEmpty(db, "CALL apoc.sequence.drop('sameName')", emptyMap());
    }

    @Test
    public void shouldReturnTheListAfterRestart() throws IOException {
        testCall(db, "CALL apoc.sequence.create('one', {initialValue: 1})", row -> {
            assertEquals("one", row.get("name"));
            assertEquals(1L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.create('two', {initialValue: 11})", row -> {
            assertEquals("two", row.get("name"));
            assertEquals(11L, row.get("value"));
        });

        testResult(db, "CALL apoc.sequence.list", result -> {
            List<Map<String, Object>> list = Iterators.asList(result);
            assertEquals(2, list.size());
            list.forEach(item -> {
                List<String> expectedNames = List.of("one", "two");
                List<Long> expectedValues = List.of(1L, 11L);
                assertTrue(expectedNames.contains(item.get("name")));
                assertTrue(expectedValues.contains(item.get("value")));
            });
        });

        restartDb();

        testResult(db, "CALL apoc.sequence.list", result -> {
            List<Map<String, Object>> list = Iterators.asList(result);
            assertEquals(2, list.size());
            list.forEach(item -> {
                List<String> expectedNames = List.of("one", "two");
                List<Long> expectedValues = List.of(1L, 11L);
                assertTrue(expectedNames.contains(item.get("name")));
                assertTrue(expectedValues.contains(item.get("value")));
            });
        });

        testCall(db, "CALL apoc.sequence.create('three', {initialValue: 22})", row -> {
            assertEquals("three", row.get("name"));
            assertEquals(22L, row.get("value"));
        });

        testResult(db, "CALL apoc.sequence.drop('two')", result -> {
            List<Map<String, Object>> list = Iterators.asList(result);
            assertEquals(2, list.size());
            list.forEach(item -> {
                List<String> expectedNames = List.of("one", "three");
                List<Long> expectedValues = List.of(1L, 22L);
                assertTrue(expectedNames.contains(item.get("name")));
                assertTrue(expectedValues.contains(item.get("value")));
            });
        });

        long actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.nextValue('one')");
        assertEquals(2L, actualValue);

        restartDb();

        testResult(db, "CALL apoc.sequence.list", result -> {
            List<Map<String, Object>> list = Iterators.asList(result);
            assertEquals(2, list.size());
            list.forEach(item -> {
                List<String> expectedNames = List.of("one", "three");
                List<Long> expectedValues = List.of(2L, 22L);
                assertTrue(expectedNames.contains(item.get("name")));
                assertTrue(expectedValues.contains(item.get("value")));
            });
        });

        testCall(db, "CALL apoc.sequence.drop('one')", row -> {
            assertEquals("three", row.get("name"));
            assertEquals(22L, row.get("value"));
        });

        testCallEmpty(db, "CALL apoc.sequence.drop('three')", emptyMap());
    }

    private void assertSequenceConstraintSize(int expectedSize) {
        assertSequenceConstraintSizeWithDbms(expectedSize, databaseManagementService.database(SYSTEM_DATABASE_NAME));
    }
}
