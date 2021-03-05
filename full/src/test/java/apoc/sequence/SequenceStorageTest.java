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

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testFail;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    private void restartDb() throws IOException {
        databaseManagementService.shutdown();
        databaseManagementService = new TestDatabaseManagementServiceBuilder(STORE_DIR.getRoot()).build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        assertTrue(db.isAvailable(1000));
        TestUtil.registerProcedure(db, Sequence.class);
    }

    @Test
    public void storeSequenceAndIncrementAfterRestart() throws IOException {
        db.executeTransactionally("CALL apoc.sequence.create('test', 1)");
        restartDb();
        TestUtil.testCall(db, "CALL apoc.sequence.currentValue('test')", row -> {
            assertEquals("test", row.get("name"));
            assertEquals(1L, row.get("value"));
        });
        TestUtil.testCall(db, "CALL apoc.sequence.nextValue('test')", row -> {
            assertEquals("test", row.get("name"));
            assertEquals(2L, row.get("value"));
        });
        db.executeTransactionally("CALL apoc.sequence.drop('test')");
    }

    @Test
    public void storeSequenceAfterIncrement() throws IOException {
        db.executeTransactionally("CALL apoc.sequence.create('test.Increment', 3)");
        db.executeTransactionally("CALL apoc.sequence.nextValue('test.Increment')");
        restartDb();
        TestUtil.testCall(db, "CALL apoc.sequence.currentValue('test.Increment')", row -> {
            assertEquals("test.Increment", row.get("name"));
            assertEquals(4L, row.get("value"));
        });
        db.executeTransactionally("CALL apoc.sequence.drop('test.Increment')");
    }

    @Test
    public void cancelSequenceAfterDrop() throws IOException {
        db.executeTransactionally("CALL apoc.sequence.create('testAfterDrop', 5)");
        db.executeTransactionally("CALL apoc.sequence.nextValue('testAfterDrop')");
        TestUtil.testCall(db, "CALL apoc.sequence.currentValue('testAfterDrop')", row -> {
            assertEquals("testAfterDrop", row.get("name"));
            assertEquals(6L, row.get("value"));
        });
        restartDb();

        testFail(db, "CALL apoc.sequence.currentValue('testIncrement')", QueryExecutionException.class);
    }

    @Test
    public void shouldOverrideSequenceWithSameName() throws IOException {
        testCall(db, "CALL apoc.sequence.create('sameName', 1)", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(1L, row.get("value"));
        });
        restartDb();
        testCall(db, "CALL apoc.sequence.currentValue('sameName')", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(1L, row.get("value"));
        });
        testCall(db, "CALL apoc.sequence.create('sameName', 3)", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(3L, row.get("value"));
        });
        testCall(db, "CALL apoc.sequence.create('sameName', 6)", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(6L, row.get("value"));
        });
        restartDb();
        testCall(db, "CALL apoc.sequence.currentValue('sameName')", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(6L, row.get("value"));
        });
        testCall(db, "CALL apoc.sequence.nextValue('sameName')", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(7L, row.get("value"));
        });
        testCall(db, "CALL apoc.sequence.list", row -> {
            assertEquals("sameName", row.get("name"));
            assertEquals(7L, row.get("value"));
        });
        testCallEmpty(db, "CALL apoc.sequence.drop('sameName')", emptyMap());
    }

    @Test
    public void shouldReturnTheListAfterRestart() throws IOException {
        testCall(db, "CALL apoc.sequence.create('one', 1)", row -> {
            assertEquals("one", row.get("name"));
            assertEquals(1L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.create('two', 11)", row -> {
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

        testCall(db, "CALL apoc.sequence.create('three', 22)", row -> {
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

        testCall(db, "CALL apoc.sequence.nextValue('one')", row -> {
            assertEquals("one", row.get("name"));
            assertEquals(2L, row.get("value"));
        });

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

}
