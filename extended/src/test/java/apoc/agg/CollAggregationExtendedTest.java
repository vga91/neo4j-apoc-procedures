package apoc.agg;

import static apoc.agg.CollAggregationExtended.BITWISE_OPERATOR_NOT_DEFINED;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    
    @Test
    public void testBitwise() {
        long first = 0b0011_1100L;
        long second = 0b0000_1101L;
        long third = 2_100L;
        List<Long> list = List.of(first, second, third);
        
        try {
            testBitwiseCommon(list, 0, null);
            fail("Should fail since the operator is null");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(BITWISE_OPERATOR_NOT_DEFINED));
        }

        long expected = first >> second >> third;
        testBitwiseCommon(list, expected, ">>");
        testBitwiseCommon(list, expected, "right shift");
        
        long expectedLeftShift = first << second << third;
        testBitwiseCommon(list, expectedLeftShift, "<<");
        testBitwiseCommon(list, expectedLeftShift, "left shift");
        
        long expectedRightShiftUnsigned = first >>> second >>> third;
        testBitwiseCommon(List.of(first, second), expectedRightShiftUnsigned, ">>>");
        testBitwiseCommon(List.of(first, second), expectedRightShiftUnsigned, "right shift unsigned");

        long expectedAnd = first & second & third;
        testBitwiseCommon(list, expectedAnd, "&");
        testBitwiseCommon(list, expectedAnd, "AND");
        
        long expectedOr = first | second | third;
        testBitwiseCommon(list, expectedOr, "OR");
        testBitwiseCommon(list, expectedOr, "|");
        
        long expectedXor = first ^ second ^ third;
        testBitwiseCommon(list, expectedXor, "XOR");
        testBitwiseCommon(list, expectedXor, "^");
    }

    private static void testBitwiseCommon(List<Long> list, long expected, String operator) {
        testCall(db, "UNWIND $list as value \n" +
                     "RETURN apoc.agg.bitwise(value, $operator) as bitwise",
                map("list", list, "operator", operator),
                row -> assertEquals(expected, row.get("bitwise")));
    }

    @Test
    public void testBinaryString() {
        List<Long> list = List.of(0b0011_1100L, 0b0000_1101L, 2_100L);
        testCall(db, "UNWIND $list as value \n" +
                     "RETURN apoc.agg.binaryString(value) as result",
                map("list", list),
                row -> {
                    List<Object> expected = List.of("111100","1101","100000110100");
                    assertEquals(expected, row.get("result"));
                });
    }

    @Test
    public void testStatisticalOperation() {
        testStatisticalCommon("SUM", 123.5D);
        testStatisticalCommon("SUM_OF_SQUARES", 123.5D);
        testStatisticalCommon("PRODUCT", 123.5D);
        testStatisticalCommon("SUM_OF_LOGS", 123.5D);
        testStatisticalCommon("MIN", 123.5D);
        testStatisticalCommon("MAX", 123.5D);
        testStatisticalCommon("MEAN", 123.5D);
        testStatisticalCommon("VARIANCE", 123.5D);
        testStatisticalCommon("PERCENTILE", 123.5D);
        testStatisticalCommon("GEOMETRIC_MEAN", 123.5D);
        testStatisticalCommon("SKEWNESS", 123.5D);
        testStatisticalCommon("STANDARD_DEVIATION", 123.5D);
        testStatisticalCommon("SECOND_MOMENT", 123.5D);
        testStatisticalCommon("KURTOSIS", 123.5D);
        testStatisticalCommon("SEMI_VARIANCE", 123.5D);
    }

    private static void testStatisticalCommon(String operation, Double expected) {
        List<Double> list = List.of(3.11D, 8.22D, 10.3D, 17.1D, 17.98D, 22.0D);
        testCall(db, "UNWIND $list as value \n" +
                     "RETURN apoc.agg.statisticalOperation(value, $operation) as result",
                map("list", list, "operation", operation),
                row -> {
                    // TODO - COMPLETE ASSERTIONS
                    System.out.println("operation = " + operation + " -- row.get(\"result\") = " + row.get("result"));
//                    assertEquals(expected, row.get("result"));
                });
    }
}
