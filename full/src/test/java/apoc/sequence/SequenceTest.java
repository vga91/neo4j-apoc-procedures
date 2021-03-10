package apoc.sequence;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.sequence.Sequence.SEQUENCE_CONSTRAINT_PREFIX;
import static apoc.util.TestUtil.singleResultFirstColumn;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testFail;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SequenceTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setup() {
        TestUtil.registerProcedure(db, Sequence.class);
    }

    @Test
    public void createSequences() {
        db.executeTransactionally("CALL apoc.sequence.create('testOne', {initialValue: 11})");
        long actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.currentValue('testOne')");
        assertEquals(11L, actualValue);

        assertSequenceConstraintSize(1);

        db.executeTransactionally("CALL apoc.sequence.create('testTwo')");
        actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.currentValue('testTwo')");
        assertEquals(0L, actualValue);

        // it already exists
        assertSequenceConstraintSize(1);

        db.executeTransactionally("CALL apoc.sequence.create('testThree', {constraintPropertyName: 'custom'})");
        actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.currentValue('testThree')");
        assertEquals(0L, actualValue);

        // create another constraint
        assertSequenceConstraintSize(2);

        db.executeTransactionally("CALL apoc.sequence.drop('testOne')");
        db.executeTransactionally("CALL apoc.sequence.drop('testTwo')");

        // remains 'custom' constraint
        assertSequenceConstraintSize(1);

        db.executeTransactionally("CALL apoc.sequence.drop('testThree')");
        assertSequenceConstraintSize(0);
    }

    @Test
    public void createSequencesWithoutConstraint() {
        db.executeTransactionally("CALL apoc.sequence.create('withoutConstraint', {initialValue: 22, createConstraint: false})");
        long actualValueTwo = singleResultFirstColumn(db, "RETURN apoc.sequence.currentValue('withoutConstraint')");
        assertEquals(22L, actualValueTwo);
        assertSequenceConstraintSize(0);

        db.executeTransactionally("CALL apoc.sequence.create('withConstraint', {initialValue: 11})");
        long actualValueOne = singleResultFirstColumn(db, "RETURN apoc.sequence.currentValue('withConstraint')");
        assertEquals(11L, actualValueOne);
        assertSequenceConstraintSize(1);

        db.executeTransactionally("CALL apoc.sequence.drop('withoutConstraint')");
        assertSequenceConstraintSize(1);

        db.executeTransactionally("CALL apoc.sequence.drop('withConstraint')");
        assertSequenceConstraintSize(0);
    }

    @Test
    public void dropSequence() {
        db.executeTransactionally("CALL apoc.sequence.create('test', {initialValue: 1})");
        testCallEmpty(db, "CALL apoc.sequence.drop('test')", emptyMap());
    }

    @Test
    public void dropSequenceWithoutConstraint() {
        db.executeTransactionally("CALL apoc.sequence.create('test', {initialValue: 1})");
        assertSequenceConstraintSize(1);

        testCallEmpty(db, "CALL apoc.sequence.drop('test', {dropConstraint: false})", emptyMap());
        assertSequenceConstraintSize(1);

        db.executeTransactionally("CALL apoc.sequence.create('test', {initialValue: 1, createConstrain: false})");
        assertSequenceConstraintSize(1);

        testCallEmpty(db, "CALL apoc.sequence.drop('test')", emptyMap());
        assertSequenceConstraintSize(0);
    }

    @Test
    public void incrementSequences() {
        db.executeTransactionally("CALL apoc.sequence.create('test', {initialValue: 1})");

        long actualValue = singleResultFirstColumn(db, "RETURN apoc.sequence.currentValue('test')");
        assertEquals(1L, actualValue);
    }

    @Test
    public void shouldOverrideSequenceWithSameName() {
        db.executeTransactionally("CALL apoc.sequence.create('test', {initialValue: 1})");


        final String queryCurrentValue = "RETURN apoc.sequence.currentValue('test')";
        long actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(1L, actualValue);

        db.executeTransactionally("CALL apoc.sequence.create('test', {initialValue: 5})");

        actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(5L, actualValue);

        db.executeTransactionally("CALL apoc.sequence.create('test')");

        actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(0L, actualValue);

        db.executeTransactionally("CALL apoc.sequence.drop('test')");

        testCallEmpty(db, "CALL apoc.sequence.list", emptyMap());
    }
    @Test
    public void shouldFailIfNotExists() {
        testFail(db, "RETURN apoc.sequence.currentValue('notExistent')", QueryExecutionException.class);
        testFail(db, "RETURN apoc.sequence.nextValue('notExistent')", QueryExecutionException.class);
        testFail(db, "CALL apoc.sequence.drop('notExistent')", QueryExecutionException.class);
    }

    @Test
    public void shouldFailAfterDropAndNotAfterRecreate() {

        testCall(db, "CALL apoc.sequence.create('custom')", row -> {
            assertEquals("custom", row.get("name"));
            assertEquals(0L, row.get("value"));
        });

        final String queryCurrentValue = "RETURN apoc.sequence.currentValue('custom')";
        final String queryNextValue = "RETURN apoc.sequence.nextValue('custom')";

        long actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(0L, actualValue);

        actualValue = singleResultFirstColumn(db, queryNextValue);
        assertEquals(1L, actualValue);

        final String queryDrop = "CALL apoc.sequence.drop('custom')";
        testCallEmpty(db, queryDrop, emptyMap());

        testFail(db, queryCurrentValue, QueryExecutionException.class);

        testFail(db, queryNextValue, QueryExecutionException.class);
        testFail(db, queryDrop, QueryExecutionException.class);

        testCall(db, "CALL apoc.sequence.create('custom', {initialValue: 5})", row -> {
            assertEquals("custom", row.get("name"));
            assertEquals(5L, row.get("value"));
        });

        actualValue = singleResultFirstColumn(db, queryCurrentValue);
        assertEquals(5L, actualValue);

        actualValue = singleResultFirstColumn(db, queryNextValue);
        assertEquals(6L, actualValue);

        // remove the node in systemdb manually
        GraphDatabaseService systemDb = db.getManagementService().database(SYSTEM_DATABASE_NAME);
        try (Transaction tx = systemDb.beginTx()) {
            Node node = tx.findNode(SystemLabels.Sequence, SystemPropertyKeys.name.name(), "custom");
            node.delete();
            tx.commit();
        }
        try (Transaction tx = systemDb.beginTx()) {
            tx.schema().getConstraintByName(SEQUENCE_CONSTRAINT_PREFIX + "custom").drop();
            tx.commit();
        }

        testCallEmpty(db, queryDrop, emptyMap());
    }

    @Test
    public void shouldListCurrentSequences() {
        testCallEmpty(db, "CALL apoc.sequence.list", emptyMap());

        testCall(db, "CALL apoc.sequence.create('foo')", row -> {
            assertEquals("foo", row.get("name"));
            assertEquals(0L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.create('bar', {initialValue: 1})", row -> {
            assertEquals("bar", row.get("name"));
            assertEquals(1L, row.get("value"));
        });

        testCall(db, "CALL apoc.sequence.create('baz', {initialValue: 2})", row -> {
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

    private static void assertSequenceConstraintSize(int expectedSize) {
        assertSequenceConstraintSizeWithDbms(expectedSize, db.getManagementService().database(SYSTEM_DATABASE_NAME));
    }

    protected static void assertSequenceConstraintSizeWithDbms(int expectedSize, GraphDatabaseService systemDb) {
        try (Transaction tx = systemDb.beginTx()) {
            int actualSize = Iterables.count(tx.schema().getConstraints(SystemLabels.Sequence));
            assertEquals(expectedSize, actualSize);
            tx.commit();
        }
    }
}
