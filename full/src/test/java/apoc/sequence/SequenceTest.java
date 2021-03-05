package apoc.sequence;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testFail;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SequenceTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setup() {
        TestUtil.registerProcedure(db, Sequence.class);
    }

    @Test
    public void createSequences() {
        db.executeTransactionally("CALL apoc.sequence.create('testOne', 11)");
        testCall(db, "CALL apoc.sequence.currentValue('testOne')", row -> {
            assertEquals("testOne", row.get("name"));
            assertEquals(11L, row.get("value"));
        });

        db.executeTransactionally("CALL apoc.sequence.create('testTwo')");
        testCall(db, "CALL apoc.sequence.currentValue('testTwo')", row -> {
            assertEquals("testTwo", row.get("name"));
            assertEquals(0L, row.get("value"));
        });

        db.executeTransactionally("CALL apoc.sequence.drop('testOne')");
        db.executeTransactionally("CALL apoc.sequence.drop('testTwo')");
    }

    @Test
    public void dropSequence() {
        db.executeTransactionally("CALL apoc.sequence.create('test', 1)");

        testCallEmpty(db, "CALL apoc.sequence.drop('test')", emptyMap());
    }

    @Test
    public void incrementSequences() {
        db.executeTransactionally("CALL apoc.sequence.create('test', 1)");
        testCall(db, "CALL apoc.sequence.currentValue('test')", row -> {
            assertEquals("test", row.get("name"));
            assertEquals(1L, row.get("value"));
        });
    }

    @Test
    public void shouldOverrideSequenceWithSameName() {
        db.executeTransactionally("CALL apoc.sequence.create('test', 1)");
        testCall(db, "CALL apoc.sequence.currentValue('test')", row -> {
            assertEquals("test", row.get("name"));
            assertEquals(1L, row.get("value"));
        });

        db.executeTransactionally("CALL apoc.sequence.create('test', 5)");
        testCall(db, "CALL apoc.sequence.currentValue('test')", row -> {
            assertEquals("test", row.get("name"));
            assertEquals(5L, row.get("value"));
        });

        db.executeTransactionally("CALL apoc.sequence.create('test')");
        testCall(db, "CALL apoc.sequence.currentValue('test')", row -> {
            assertEquals("test", row.get("name"));
            assertEquals(0L, row.get("value"));
        });

        db.executeTransactionally("CALL apoc.sequence.drop('test')");

        testCallEmpty(db, "CALL apoc.sequence.list", emptyMap());
    }

    @Test
    public void shouldFailIfNotExists() {
        testFail(db, "CALL apoc.sequence.currentValue('notExistent')", QueryExecutionException.class);
        testFail(db, "CALL apoc.sequence.nextValue('notExistent')", QueryExecutionException.class);
        testFail(db, "CALL apoc.sequence.drop('notExistent')", QueryExecutionException.class);
    }

    @Test
    public void shouldFailAfterDropAndNotAfterRecreate() {

        testCall(db, "CALL apoc.sequence.create('custom')", row -> {
            assertEquals("custom", row.get("name"));
            assertEquals(0L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.currentValue('custom')", row -> {
            assertEquals("custom", row.get("name"));
            assertEquals(0L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.nextValue('custom')", row -> {
            assertEquals("custom", row.get("name"));
            assertEquals(1L, row.get("value"));
        });

        testCallEmpty(db, "CALL apoc.sequence.drop('custom')", emptyMap());

        testFail(db, "CALL apoc.sequence.currentValue('custom')", QueryExecutionException.class);
        testFail(db, "CALL apoc.sequence.nextValue('custom')", QueryExecutionException.class);
        testFail(db, "CALL apoc.sequence.drop('custom')", QueryExecutionException.class);

        testCall(db, "CALL apoc.sequence.create('custom', 5)", row -> {
            assertEquals("custom", row.get("name"));
            assertEquals(5L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.currentValue('custom')", row -> {
            assertEquals("custom", row.get("name"));
            assertEquals(5L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.nextValue('custom')", row -> {
            assertEquals("custom", row.get("name"));
            assertEquals(6L, row.get("value"));
        });

        // remove the node in systemdb manually
        GraphDatabaseService systemDb = db.getManagementService().database("system");
        try (Transaction tx = systemDb.beginTx()) {
            Node node = tx.findNode(SystemLabels.Sequence, SystemPropertyKeys.name.name(), "custom");
            node.delete();
            tx.commit();
        }

        testFail(db, "CALL apoc.sequence.currentValue('custom')", QueryExecutionException.class);
    }

    @Test
    public void shouldListCurrentSequences() {
        testCallEmpty(db, "CALL apoc.sequence.list", emptyMap());

        testCall(db, "CALL apoc.sequence.create('foo')", row -> {
            assertEquals("foo", row.get("name"));
            assertEquals(0L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.create('bar', 1)", row -> {
            assertEquals("bar", row.get("name"));
            assertEquals(1L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.create('baz', 2)", row -> {
            assertEquals("baz", row.get("name"));
            assertEquals(2L, row.get("value"));
        });

        testResult(db, "CALL apoc.sequence.list", result -> {
            List<Map<String, Object>> list = Iterators.asList(result);
            assertEquals(3, list.size());
            list.forEach(item -> {
                List<String> expectedNames = List.of("foo", "bar", "baz");
                List<Long> expectedValues = List.of(0L, 1L, 2L);
                assertTrue(expectedNames.contains(item.get("name")));
                assertTrue(expectedValues.contains(item.get("value")));
            });
        });

        testResult(db, "CALL apoc.sequence.drop('foo')", result -> {
            List<Map<String, Object>> list = Iterators.asList(result);
            assertEquals(2, list.size());
            list.forEach(item -> {
                List<String> expectedNames = List.of("bar", "baz");
                List<Long> expectedValues = List.of(1L, 2L);
                assertTrue(expectedNames.contains(item.get("name")));
                assertTrue(expectedValues.contains(item.get("value")));
            });
        });

        testCall(db, "CALL apoc.sequence.drop('bar')", row -> {
            assertEquals("baz", row.get("name"));
            assertEquals(2L, row.get("value"));
        });

        testCallEmpty(db, "CALL apoc.sequence.drop('baz')", emptyMap());
    }
}
