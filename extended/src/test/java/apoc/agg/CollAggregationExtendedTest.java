package apoc.agg;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class CollAggregationExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, CollAggregationExtended.class);
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }


    @Test
    public void testAnyAllWithAllFalses() {
        List<Boolean> list = asList(false, false, false, false, false);
        Consumer<Map<String, Object>> rowConsumer = (row) -> {
            assertEquals(false, row.get("any"));
            assertEquals(false, row.get("all"));
        };

        testAnyAllCommon(list, rowConsumer);
    }

    @Test
    public void testAnyAll() {
        List<Boolean> list = asList(true, false, false, true, false);
        Consumer<Map<String, Object>> rowConsumer = (row) -> {
            assertEquals(true, row.get("any"));
            assertEquals(false, row.get("all"));
        };

        testAnyAllCommon(list, rowConsumer);
    }

    @Test
    public void testAnyAllWithAllTrues() {
        List<Boolean> list = asList(true, true, true);
        Consumer<Map<String, Object>> rowConsumer = (row) -> {
            assertEquals(true, row.get("any"));
            assertEquals(true, row.get("all"));
        };

        testAnyAllCommon(list, rowConsumer);
    }

    @Test
    public void testAnyAllWithNullAndTrue() {
        List<Boolean> list = asList(null, true, null, true);
        Consumer<Map<String, Object>> rowConsumer = (row) -> {
            assertEquals(true, row.get("any"));
            assertNull(row.get("all"));
        };

        testAnyAllCommon(list, rowConsumer);
    }

    @Test
    public void testAnyAllWithNullAndFalse() {
        List<Boolean> list = asList(null, false, null, false);
        Consumer<Map<String, Object>> rowConsumer = (row) -> {
            assertNull(row.get("any"));
            assertEquals(false, row.get("all"));
        };

        testAnyAllCommon(list, rowConsumer);
    }

    private static void testAnyAllCommon(List<Boolean> list, Consumer<Map<String, Object>> rowConsumer) {
        testCall(db, "UNWIND $list as value \n" +
                     "RETURN apoc.agg.any(value) as any, apoc.agg.all(value) as all",
                Map.of("list", list),
                rowConsumer);

        // compare with any and all Cypher functions, as need to be conceptually consistent
        testCall(db, "RETURN all(variable IN $list WHERE variable) as all, any(variable IN $list WHERE variable) as any",
                Map.of("list", list),
                rowConsumer);
    }
}
