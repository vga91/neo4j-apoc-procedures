package apoc.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static apoc.agg.Rollup.NULL_ROLLUP;
import static apoc.util.MapUtil.map;

public class RollupTestUtil {
    public static final String CATEGORY_ID = "CategoryID";
    public static final String SUPPLIER_ID = "SupplierID";
    public static final String ANOTHER_ID = "anotherID";
    
    public static final String sumFloat = "SUM(otherNum)";
    public static final String countFloat = "COUNT(otherNum)";
    public static final String avgFloat = "AVG(otherNum)";
    
    public static final String sumPrice = "SUM(Price)";
    public static final String countPrice = "COUNT(Price)";
    public static final String avgPrice = "AVG(Price)";

    static List<Map> getRollupTripleGroup() {
        
        return new ArrayList<>() {{
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 1L, ANOTHER_ID, 0L,
                        sumFloat, 0.5, countFloat, 1L, sumPrice, 19L, avgFloat, 0.5, countPrice, 1L, avgPrice, 19.0));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 1L, ANOTHER_ID, 1L,
                        sumFloat, 0.3, countFloat, 1L, sumPrice, 18L, avgFloat, 0.3, countPrice, 1L, avgPrice, 18.0));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.8, countFloat, 2L, sumPrice, 37L, avgFloat, 0.4, countPrice, 2L, avgPrice, 18.5));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, 1L, ANOTHER_ID, 1L,
                        sumFloat, 0.6, countFloat, 1L, sumPrice, 10L, avgFloat, 0.6, countPrice, 1L, avgPrice, 10.0));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.6, countFloat, 1L, sumPrice, 10L, avgFloat, 0.6, countPrice, 1L, avgPrice, 10.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 2.0999999999999996, countFloat, 4L, sumPrice, 65L, avgFloat, 0.5249999999999999, countPrice, 4L, avgPrice, 16.25));
            add(
                map(CATEGORY_ID, null, SUPPLIER_ID, 1L, ANOTHER_ID, 1L,
                        sumFloat, 0.7, countFloat, 1L, sumPrice, 18L, avgFloat, 0.7, countPrice, 1L, avgPrice, 18.0));
            add(
                map(CATEGORY_ID, null, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.7, countFloat, 1L, sumPrice, 18L, avgFloat, 0.7, countPrice, 1L, avgPrice, 18.0));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 4L, ANOTHER_ID, 0L,
                        sumFloat, 0.6, countFloat, 1L, sumPrice, 31L, avgFloat, 0.6, countPrice, 1L, avgPrice, 31.0));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 4L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.6, countFloat, 1L, sumPrice, 31L, avgFloat, 0.6, countPrice, 1L, avgPrice, 31.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 4L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.6, countFloat, 1L, sumPrice, 31L, avgFloat, 0.6, countPrice, 1L, avgPrice, 31.0));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 5L, ANOTHER_ID, 1L,
                        sumFloat, 0.2, countFloat, 1L, sumPrice, 21L, avgFloat, 0.2, countPrice, 1L, avgPrice, 21.0));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 5L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.2, countFloat, 1L, sumPrice, 21L, avgFloat, 0.2, countPrice, 1L, avgPrice, 21.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 5L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.2, countFloat, 1L, sumPrice, 21L, avgFloat, 0.2, countPrice, 1L, avgPrice, 21.0));
            add(
                map(CATEGORY_ID, 7L, SUPPLIER_ID, 6L, ANOTHER_ID, 1L,
                        sumFloat, 0.6, countFloat, 1L, sumPrice, 23L, avgFloat, 0.6, countPrice, 1L, avgPrice, 23.0));
            add(
                map(CATEGORY_ID, 7L, SUPPLIER_ID, 6L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.6, countFloat, 1L, sumPrice, 23L, avgFloat, 0.6, countPrice, 1L, avgPrice, 23.0));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 6L, ANOTHER_ID, 1L,
                        sumFloat, 0.5, countFloat, 1L, sumPrice, 6L, avgFloat, 0.5, countPrice, 1L, avgPrice, 6.0));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 6L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.5, countFloat, 1L, sumPrice, 6L, avgFloat, 0.5, countPrice, 1L, avgPrice, 6.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 6L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 1.1, countFloat, 2L, sumPrice, 29L, avgFloat, 0.55, countPrice, 2L, avgPrice, 14.5));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                        sumFloat, 0.7, countFloat, 1L, sumPrice, 17L, avgFloat, 0.7, countPrice, 1L, avgPrice, 17.0));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.7, countFloat, 1L, sumPrice, 17L, avgFloat, 0.7, countPrice, 1L, avgPrice, 17.0));
            add(
                map(CATEGORY_ID, 6L, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                        sumFloat, 0.8, countFloat, 1L, sumPrice, 39L, avgFloat, 0.8, countPrice, 1L, avgPrice, 39.0));
            add(
                map(CATEGORY_ID, 6L, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.8, countFloat, 1L, sumPrice, 39L, avgFloat, 0.8, countPrice, 1L, avgPrice, 39.0));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                        sumFloat, 0.9, countFloat, 1L, sumPrice, 63L, avgFloat, 0.9, countPrice, 1L, avgPrice, 63.0));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.9, countFloat, 1L, sumPrice, 63L, avgFloat, 0.9, countPrice, 1L, avgPrice, 63.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 2.4, countFloat, 3L, sumPrice, 119L, avgFloat, 0.7999999999999999, countPrice, 3L, avgPrice, 39.666666666666664));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 8L, ANOTHER_ID, 0L,
                        sumFloat, 0.2, countFloat, 1L, sumPrice, 9L, avgFloat, 0.2, countPrice, 1L, avgPrice, 9.0));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 8L, ANOTHER_ID, 1L,
                        sumFloat, 0.5, countFloat, 1L, sumPrice, 81L, avgFloat, 0.5, countPrice, 1L, avgPrice, 81.0));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 8L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.7, countFloat, 2L, sumPrice, 90L, avgFloat, 0.35, countPrice, 2L, avgPrice, 45.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 8L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.7, countFloat, 2L, sumPrice, 90L, avgFloat, 0.35, countPrice, 2L, avgPrice, 45.0));
            add(
                map(CATEGORY_ID, 5L, SUPPLIER_ID, 9L, ANOTHER_ID, 0L,
                        sumFloat, 0.9, countFloat, 1L, sumPrice, 9L, avgFloat, 0.9, countPrice, 1L, avgPrice, 9.0));
            add(
                map(CATEGORY_ID, 5L, SUPPLIER_ID, 9L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.9, countFloat, 1L, sumPrice, 9L, avgFloat, 0.9, countPrice, 1L, avgPrice, 9.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 9L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.9, countFloat, 1L, sumPrice, 9L, avgFloat, 0.9, countPrice, 1L, avgPrice, 9.0));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 10L, ANOTHER_ID, 1L,
                        sumFloat, 0.2, countFloat, 1L, sumPrice, 5L, avgFloat, 0.2, countPrice, 1L, avgPrice, 5.0));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 10L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.2, countFloat, 1L, sumPrice, 5L, avgFloat, 0.2, countPrice, 1L, avgPrice, 5.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 10L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.2, countFloat, 1L, sumPrice, 5L, avgFloat, 0.2, countPrice, 1L, avgPrice, 5.0));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 11L, ANOTHER_ID, 0L,
                        sumFloat, 0.2, countFloat, 1L, sumPrice, 31L, avgFloat, 0.2, countPrice, 1L, avgPrice, 31.0));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 11L, ANOTHER_ID, 1L,
                        sumFloat, 0.1, countFloat, 1L, sumPrice, 14L, avgFloat, 0.1, countPrice, 1L, avgPrice, 14.0));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 11L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.30000000000000004, countFloat, 2L, sumPrice, 45L, avgFloat, 0.15000000000000002, countPrice, 2L, avgPrice, 22.5));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 11L, ANOTHER_ID, 0L,
                        sumFloat, 0.7, countFloat, 1L, sumPrice, 44L, avgFloat, 0.7, countPrice, 1L, avgPrice, 44.0));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 11L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.7, countFloat, 1L, sumPrice, 44L, avgFloat, 0.7, countPrice, 1L, avgPrice, 44.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 11L, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 1.0, countFloat, 3L, sumPrice, 89L, avgFloat, 0.3333333333333333, countPrice, 3L, avgPrice, 29.666666666666668));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 10.6, countFloat, 20L, sumPrice, 675L, avgFloat, 0.53, countPrice, 20L, avgPrice, 33.75));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, null, ANOTHER_ID, 0L,
                        sumFloat, 0.8, countFloat, 1L, sumPrice, 199L, avgFloat, 0.8, countPrice, 1L, avgPrice, 199.0));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, null, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.8, countFloat, 1L, sumPrice, 199L, avgFloat, 0.8, countPrice, 1L, avgPrice, 199.0));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, null, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 1.4, countFloat, 2L, sumPrice, 217L, avgFloat, 0.7, countPrice, 2L, avgPrice, 108.5));
            add(
                map(CATEGORY_ID, null, SUPPLIER_ID, null, ANOTHER_ID, 0L,
                        sumFloat, 0.6, countFloat, 1L, sumPrice, 18L, avgFloat, 0.6, countPrice, 1L, avgPrice, 18.0));
            add(
                map(CATEGORY_ID, null, SUPPLIER_ID, null, ANOTHER_ID, NULL_ROLLUP,
                        sumFloat, 0.6, countFloat, 1L, sumPrice, 18L, avgFloat, 0.6, countPrice, 1L, avgPrice, 18.0));
        }};
    }

    static List<Map> getRollupTripleGroupTwo() {
        return new ArrayList<>() {{
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 1L, ANOTHER_ID, 0L,
                        avgFloat, 0.5, sumPrice, 19L, countPrice, 1L, sumFloat, 0.5, avgPrice, 19.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 1L, ANOTHER_ID, 1L,
                        avgFloat, 0.3, sumPrice, 18L, countPrice, 1L, sumFloat, 0.3, avgPrice, 18.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.4, sumPrice, 37L, countPrice, 2L, sumFloat, 0.8, avgPrice, 18.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 10L, ANOTHER_ID, 1L,
                        avgFloat, 0.2, sumPrice, 5L, countPrice, 1L, sumFloat, 0.2, avgPrice, 5.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 10L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.2, sumPrice, 5L, countPrice, 1L, sumFloat, 0.2, avgPrice, 5.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.3333333333333333, sumPrice, 42L, countPrice, 3L, sumFloat, 1.0, avgPrice, 14.0, countFloat, 3L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, 1L, ANOTHER_ID, 1L,
                        avgFloat, 0.6, sumPrice, 10L, countPrice, 1L, sumFloat, 0.6, avgPrice, 10.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.6, sumPrice, 10L, countPrice, 1L, sumFloat, 0.6, avgPrice, 10.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.7, sumPrice, 209L, countPrice, 2L, sumFloat, 1.4, avgPrice, 104.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, null, ANOTHER_ID, 0L,
                        avgFloat, 0.8, sumPrice, 199L, countPrice, 1L, sumFloat, 0.8, avgPrice, 199.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, null, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.8, sumPrice, 199L, countPrice, 1L, sumFloat, 0.8, avgPrice, 199.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                        avgFloat, 0.7, sumPrice, 17L, countPrice, 1L, sumFloat, 0.7, avgPrice, 17.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.7, sumPrice, 17L, countPrice, 1L, sumFloat, 0.7, avgPrice, 17.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 8L, ANOTHER_ID, 0L,
                        avgFloat, 0.2, sumPrice, 9L, countPrice, 1L, sumFloat, 0.2, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 8L, ANOTHER_ID, 1L,
                        avgFloat, 0.5, sumPrice, 81L, countPrice, 1L, sumFloat, 0.5, avgPrice, 81.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 8L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.35, sumPrice, 90L, countPrice, 2L, sumFloat, 0.7, avgPrice, 45.0, countFloat, 2L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 11L, ANOTHER_ID, 0L,
                        avgFloat, 0.2, sumPrice, 31L, countPrice, 1L, sumFloat, 0.2, avgPrice, 31.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 11L, ANOTHER_ID, 1L,
                        avgFloat, 0.1, sumPrice, 14L, countPrice, 1L, sumFloat, 0.1, avgPrice, 14.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 11L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.15000000000000002, sumPrice, 45L, countPrice, 2L, sumFloat, 0.30000000000000004, avgPrice, 22.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.33999999999999997, sumPrice, 152L, countPrice, 5L, sumFloat, 1.7, avgPrice, 30.4, countFloat, 5L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 5L, ANOTHER_ID, 1L,
                        avgFloat, 0.2, sumPrice, 21L, countPrice, 1L, sumFloat, 0.2, avgPrice, 21.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 5L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.2, sumPrice, 21L, countPrice, 1L, sumFloat, 0.2, avgPrice, 21.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 11L, ANOTHER_ID, 0L,
                        avgFloat, 0.7, sumPrice, 44L, countPrice, 1L, sumFloat, 0.7, avgPrice, 44.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 11L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.7, sumPrice, 44L, countPrice, 1L, sumFloat, 0.7, avgPrice, 44.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.44999999999999996, sumPrice, 65L, countPrice, 2L, sumFloat, 0.8999999999999999, avgPrice, 32.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, 5L, SUPPLIER_ID, 9L, ANOTHER_ID, 0L,
                        avgFloat, 0.9, sumPrice, 9L, countPrice, 1L, sumFloat, 0.9, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 5L, SUPPLIER_ID, 9L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.9, sumPrice, 9L, countPrice, 1L, sumFloat, 0.9, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 5L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.9, sumPrice, 9L, countPrice, 1L, sumFloat, 0.9, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 6L, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                        avgFloat, 0.8, sumPrice, 39L, countPrice, 1L, sumFloat, 0.8, avgPrice, 39.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 6L, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.8, sumPrice, 39L, countPrice, 1L, sumFloat, 0.8, avgPrice, 39.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 6L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.8, sumPrice, 39L, countPrice, 1L, sumFloat, 0.8, avgPrice, 39.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 7L, SUPPLIER_ID, 6L, ANOTHER_ID, 1L,
                        avgFloat, 0.6, sumPrice, 23L, countPrice, 1L, sumFloat, 0.6, avgPrice, 23.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 7L, SUPPLIER_ID, 6L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.6, sumPrice, 23L, countPrice, 1L, sumFloat, 0.6, avgPrice, 23.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 7L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.6, sumPrice, 23L, countPrice, 1L, sumFloat, 0.6, avgPrice, 23.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 4L, ANOTHER_ID, 0L,
                        avgFloat, 0.6, sumPrice, 31L, countPrice, 1L, sumFloat, 0.6, avgPrice, 31.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 4L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.6, sumPrice, 31L, countPrice, 1L, sumFloat, 0.6, avgPrice, 31.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 6L, ANOTHER_ID, 1L,
                        avgFloat, 0.5, sumPrice, 6L, countPrice, 1L, sumFloat, 0.5, avgPrice, 6.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 6L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.5, sumPrice, 6L, countPrice, 1L, sumFloat, 0.5, avgPrice, 6.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                        avgFloat, 0.9, sumPrice, 63L, countPrice, 1L, sumFloat, 0.9, avgPrice, 63.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.9, sumPrice, 63L, countPrice, 1L, sumFloat, 0.9, avgPrice, 63.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.6666666666666666, sumPrice, 100L, countPrice, 3L, sumFloat, 2.0, avgPrice, 33.333333333333336, countFloat, 3L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.53, sumPrice, 675L, countPrice, 20L, sumFloat, 10.6, avgPrice, 33.75, countFloat, 20L));
            add(
                map(CATEGORY_ID, null, SUPPLIER_ID, 1L, ANOTHER_ID, 1L,
                        avgFloat, 0.7, sumPrice, 18L, countPrice, 1L, sumFloat, 0.7, avgPrice, 18.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, null, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.7, sumPrice, 18L, countPrice, 1L, sumFloat, 0.7, avgPrice, 18.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, null, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.6499999999999999, sumPrice, 36L, countPrice, 2L, sumFloat, 1.2999999999999998, avgPrice, 18.0, countFloat, 2L));
            add(
                map(CATEGORY_ID, null, SUPPLIER_ID, null, ANOTHER_ID, 0L,
                        avgFloat, 0.6, sumPrice, 18L, countPrice, 1L, sumFloat, 0.6, avgPrice, 18.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, null, SUPPLIER_ID, null, ANOTHER_ID, NULL_ROLLUP,
                        avgFloat, 0.6, sumPrice, 18L, countPrice, 1L, sumFloat, 0.6, avgPrice, 18.0, countFloat, 1L));
        }};
    }

    static List<Map> getCubeTripleGroupTwo() {
        return new ArrayList<>() {{
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 1L, ANOTHER_ID, 0L,
                    avgFloat, 0.5, sumPrice, 19L, countPrice, 1L, sumFloat, 0.5, avgPrice, 19.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 1L, ANOTHER_ID, 1L,
                    avgFloat, 0.3, sumPrice, 18L, countPrice, 1L, sumFloat, 0.3, avgPrice, 18.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.4, sumPrice, 37L, countPrice, 2L, sumFloat, 0.8, avgPrice, 18.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 10L, ANOTHER_ID, 1L,
                    avgFloat, 0.2, sumPrice, 5L, countPrice, 1L, sumFloat, 0.2, avgPrice, 5.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, 10L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.2, sumPrice, 5L, countPrice, 1L, sumFloat, 0.2, avgPrice, 5.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 0L,
                    avgFloat, 0.5, sumPrice, 19L, countPrice, 1L, sumFloat, 0.5, avgPrice, 19.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 1L,
                    avgFloat, 0.25, sumPrice, 23L, countPrice, 2L, sumFloat, 0.5, avgPrice, 11.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, 1L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.3333333333333333, sumPrice, 42L, countPrice, 3L, sumFloat, 1.0, avgPrice, 14.0, countFloat, 3L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, 1L, ANOTHER_ID, 1L,
                    avgFloat, 0.6, sumPrice, 10L, countPrice, 1L, sumFloat, 0.6, avgPrice, 10.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.6, sumPrice, 10L, countPrice, 1L, sumFloat, 0.6, avgPrice, 10.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 0L,
                    avgFloat, 0.8, sumPrice, 398L, countPrice, 2L, sumFloat, 1.6, avgPrice, 199.0, countFloat, 2L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 1L,
                    avgFloat, 0.6, sumPrice, 10L, countPrice, 1L, sumFloat, 0.6, avgPrice, 10.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 2L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.7333333333333334, sumPrice, 408L, countPrice, 3L, sumFloat, 2.2, avgPrice, 136.0, countFloat, 3L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                    avgFloat, 0.7, sumPrice, 17L, countPrice, 1L, sumFloat, 0.7, avgPrice, 17.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.7, sumPrice, 17L, countPrice, 1L, sumFloat, 0.7, avgPrice, 17.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 8L, ANOTHER_ID, 0L,
                    avgFloat, 0.2, sumPrice, 9L, countPrice, 1L, sumFloat, 0.2, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 8L, ANOTHER_ID, 1L,
                    avgFloat, 0.5, sumPrice, 81L, countPrice, 1L, sumFloat, 0.5, avgPrice, 81.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 8L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.35, sumPrice, 90L, countPrice, 2L, sumFloat, 0.7, avgPrice, 45.0, countFloat, 2L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 11L, ANOTHER_ID, 0L,
                    avgFloat, 0.2, sumPrice, 31L, countPrice, 1L, sumFloat, 0.2, avgPrice, 31.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 11L, ANOTHER_ID, 1L,
                    avgFloat, 0.1, sumPrice, 14L, countPrice, 1L, sumFloat, 0.1, avgPrice, 14.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, 11L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.15000000000000002, sumPrice, 45L, countPrice, 2L, sumFloat, 0.30000000000000004, avgPrice, 22.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 0L,
                    avgFloat, 0.2, sumPrice, 40L, countPrice, 2L, sumFloat, 0.4, avgPrice, 20.0, countFloat, 2L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 1L,
                    avgFloat, 0.43333333333333335, sumPrice, 112L, countPrice, 3L, sumFloat, 1.3, avgPrice, 37.333333333333336, countFloat, 3L));
            add(
                map(CATEGORY_ID, 3L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.33999999999999997, sumPrice, 152L, countPrice, 5L, sumFloat, 1.7, avgPrice, 30.4, countFloat, 5L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 5L, ANOTHER_ID, 1L,
                    avgFloat, 0.2, sumPrice, 21L, countPrice, 1L, sumFloat, 0.2, avgPrice, 21.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 5L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.2, sumPrice, 21L, countPrice, 1L, sumFloat, 0.2, avgPrice, 21.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 11L, ANOTHER_ID, 0L,
                    avgFloat, 0.7, sumPrice, 44L, countPrice, 1L, sumFloat, 0.7, avgPrice, 44.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, 11L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.7, sumPrice, 44L, countPrice, 1L, sumFloat, 0.7, avgPrice, 44.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 0L,
                    avgFloat, 0.7, sumPrice, 44L, countPrice, 1L, sumFloat, 0.7, avgPrice, 44.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 1L,
                    avgFloat, 0.2, sumPrice, 21L, countPrice, 1L, sumFloat, 0.2, avgPrice, 21.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 4L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.44999999999999996, sumPrice, 65L, countPrice, 2L, sumFloat, 0.8999999999999999, avgPrice, 32.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, 5L, SUPPLIER_ID, 9L, ANOTHER_ID, 0L,
                    avgFloat, 0.9, sumPrice, 9L, countPrice, 1L, sumFloat, 0.9, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 5L, SUPPLIER_ID, 9L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.9, sumPrice, 9L, countPrice, 1L, sumFloat, 0.9, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 5L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 0L,
                    avgFloat, 0.9, sumPrice, 9L, countPrice, 1L, sumFloat, 0.9, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 5L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.9, sumPrice, 9L, countPrice, 1L, sumFloat, 0.9, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 6L, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                    avgFloat, 0.8, sumPrice, 39L, countPrice, 1L, sumFloat, 0.8, avgPrice, 39.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 6L, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.8, sumPrice, 39L, countPrice, 1L, sumFloat, 0.8, avgPrice, 39.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 6L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 1L,
                    avgFloat, 0.8, sumPrice, 39L, countPrice, 1L, sumFloat, 0.8, avgPrice, 39.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 6L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.8, sumPrice, 39L, countPrice, 1L, sumFloat, 0.8, avgPrice, 39.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 7L, SUPPLIER_ID, 6L, ANOTHER_ID, 1L,
                    avgFloat, 0.6, sumPrice, 23L, countPrice, 1L, sumFloat, 0.6, avgPrice, 23.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 7L, SUPPLIER_ID, 6L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.6, sumPrice, 23L, countPrice, 1L, sumFloat, 0.6, avgPrice, 23.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 7L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 1L,
                    avgFloat, 0.6, sumPrice, 23L, countPrice, 1L, sumFloat, 0.6, avgPrice, 23.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 7L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.6, sumPrice, 23L, countPrice, 1L, sumFloat, 0.6, avgPrice, 23.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 4L, ANOTHER_ID, 0L,
                    avgFloat, 0.6, sumPrice, 31L, countPrice, 1L, sumFloat, 0.6, avgPrice, 31.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 4L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.6, sumPrice, 31L, countPrice, 1L, sumFloat, 0.6, avgPrice, 31.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 6L, ANOTHER_ID, 1L,
                    avgFloat, 0.5, sumPrice, 6L, countPrice, 1L, sumFloat, 0.5, avgPrice, 6.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 6L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.5, sumPrice, 6L, countPrice, 1L, sumFloat, 0.5, avgPrice, 6.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                    avgFloat, 0.9, sumPrice, 63L, countPrice, 1L, sumFloat, 0.9, avgPrice, 63.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.9, sumPrice, 63L, countPrice, 1L, sumFloat, 0.9, avgPrice, 63.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 0L,
                    avgFloat, 0.6, sumPrice, 31L, countPrice, 1L, sumFloat, 0.6, avgPrice, 31.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 1L,
                    avgFloat, 0.7, sumPrice, 69L, countPrice, 2L, sumFloat, 1.4, avgPrice, 34.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, 8L, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.6666666666666666, sumPrice, 100L, countPrice, 3L, sumFloat, 2.0, avgPrice, 33.333333333333336, countFloat, 3L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 1L, ANOTHER_ID, 0L,
                    avgFloat, 0.5, sumPrice, 19L, countPrice, 1L, sumFloat, 0.5, avgPrice, 19.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 1L, ANOTHER_ID, 1L,
                    avgFloat, 0.575, sumPrice, 64L, countPrice, 4L, sumFloat, 2.3, avgPrice, 16.0, countFloat, 4L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 1L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.5599999999999999, sumPrice, 83L, countPrice, 5L, sumFloat, 2.8, avgPrice, 16.6, countFloat, 5L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 4L, ANOTHER_ID, 0L,
                    avgFloat, 0.6, sumPrice, 31L, countPrice, 1L, sumFloat, 0.6, avgPrice, 31.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 4L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.6, sumPrice, 31L, countPrice, 1L, sumFloat, 0.6, avgPrice, 31.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 5L, ANOTHER_ID, 1L,
                    avgFloat, 0.2, sumPrice, 21L, countPrice, 1L, sumFloat, 0.2, avgPrice, 21.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 5L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.2, sumPrice, 21L, countPrice, 1L, sumFloat, 0.2, avgPrice, 21.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 6L, ANOTHER_ID, 1L,
                    avgFloat, 0.55, sumPrice, 29L, countPrice, 2L, sumFloat, 1.1, avgPrice, 14.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 6L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.55, sumPrice, 29L, countPrice, 2L, sumFloat, 1.1, avgPrice, 14.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 7L, ANOTHER_ID, 1L,
                    avgFloat, 0.7999999999999999, sumPrice, 119L, countPrice, 3L, sumFloat, 2.4, avgPrice, 39.666666666666664, countFloat, 3L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 7L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.7999999999999999, sumPrice, 119L, countPrice, 3L, sumFloat, 2.4, avgPrice, 39.666666666666664, countFloat, 3L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 8L, ANOTHER_ID, 0L,
                    avgFloat, 0.2, sumPrice, 9L, countPrice, 1L, sumFloat, 0.2, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 8L, ANOTHER_ID, 1L,
                    avgFloat, 0.5, sumPrice, 81L, countPrice, 1L, sumFloat, 0.5, avgPrice, 81.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 8L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.35, sumPrice, 90L, countPrice, 2L, sumFloat, 0.7, avgPrice, 45.0, countFloat, 2L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 9L, ANOTHER_ID, 0L,
                    avgFloat, 0.9, sumPrice, 9L, countPrice, 1L, sumFloat, 0.9, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 9L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.9, sumPrice, 9L, countPrice, 1L, sumFloat, 0.9, avgPrice, 9.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 10L, ANOTHER_ID, 1L,
                    avgFloat, 0.2, sumPrice, 5L, countPrice, 1L, sumFloat, 0.2, avgPrice, 5.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 10L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.2, sumPrice, 5L, countPrice, 1L, sumFloat, 0.2, avgPrice, 5.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 11L, ANOTHER_ID, 0L,
                    avgFloat, 0.44999999999999996, sumPrice, 75L, countPrice, 2L, sumFloat, 0.8999999999999999, avgPrice, 37.5, countFloat, 2L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 11L, ANOTHER_ID, 1L,
                    avgFloat, 0.1, sumPrice, 14L, countPrice, 1L, sumFloat, 0.1, avgPrice, 14.0, countFloat, 1L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, 11L, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.3333333333333333, sumPrice, 89L, countPrice, 3L, sumFloat, 1.0, avgPrice, 29.666666666666668, countFloat, 3L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 0L,
                    avgFloat, 0.5916666666666667, sumPrice, 613L, countPrice, 12L, sumFloat, 7.1, avgPrice, 51.083333333333336, countFloat, 12L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, 1L,
                    avgFloat, 0.5230769230769231, sumPrice, 333L, countPrice, 13L, sumFloat, 6.8, avgPrice, 25.615384615384617, countFloat, 13L));
            add(
                map(CATEGORY_ID, NULL_ROLLUP, SUPPLIER_ID, NULL_ROLLUP, ANOTHER_ID, NULL_ROLLUP,
                    avgFloat, 0.5559999999999999, sumPrice, 946L, countPrice, 25L, sumFloat, 13.899999999999999, avgPrice, 37.84, countFloat, 25L)); 
        }};
    }
}
