package apoc.agg;

import apoc.Extended;
import apoc.util.Util;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ComparatorUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.compare.ComparableUtils;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.Entity;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.utils.ValueMath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/*
ROLLUP ORACLE:

* Regular aggregation rows that would be produced by GROUP BY without using ROLLUP
* First-level subtotals aggregating across Department for each combination of Time and Region
* Second-level subtotals aggregating across Region and Department for each Time value
* A grand total row Table 20-2 ROLLUP Aggregation across Three Dimensions
•	Time	•	Region	•	Department	•	Profit
•	1996	•	Central	•	VideoRental	•	75,000
•	1996	•	Central	•	VideoSales	•	74,000
•	1996	•	Central	•	[NULL]	•	149,000
•	1996	•	East	•	VideoRental	•	89,000
•	1996	•	East	•	VideoSales	•	115,000
•	1996	•	East	•	[NULL]	•	204,000
•	1996	•	West	•	VideoRental	•	87,000
•	1996	•	West	•	VideoSales	•	86,000


  SELECT Time, Region, Department,
   sum(Profit) AS Profit FROM sales
   GROUP BY ROLLUP(Time, Region, Dept)

 */


@Extended
public class Rollup {
    public static final String NULL_ROLLUP = "NULL";
    
    // TODO - description --> apoc.agg.rollup(<ANY>, [groupKeys], [aggKeys]) --> aggKeys are the one like sum(Profit)
    @UserAggregationFunction("apoc.agg.rollup")
    @Description("Return a multi-dimensional aggregation")
    public RollupFunction rollup() {
        return new RollupFunction();
    }
    
    public static class RollupFunction {
        // Function to generate all combinations of a list with "TEST" as a placeholder
        public static <T> List<List<T>> generateCombinationsWithPlaceholder(List<T> elements) {
            List<List<T>> result = new ArrayList<>();
            generateCombinationsWithPlaceholder(elements, 0, new ArrayList<>(), result);
            return result;
        }

        // Helper function for generating combinations recursively
        private static <T> void generateCombinationsWithPlaceholder(List<T> elements, int index, List<T> current, List<List<T>> result) {
            if (index == elements.size()) {
                result.add(new ArrayList<>(current));
                return;
            }

            current.add(elements.get(index));
            generateCombinationsWithPlaceholder(elements, index + 1, current, result);
            current.remove(current.size() - 1);

            // Add "NULL" as a combination placeholder
            current.add((T) NULL_ROLLUP);
            generateCombinationsWithPlaceholder(elements, index + 1, current, result);
            current.remove(current.size() - 1);
        }
        
        private final Map<String, Object> result = new HashMap<>();

        private final Map<List<Object>, Map<String, Number>> rolledUpData = new HashMap<>();
        private List<String> groupKeysRes = null;
        
        @UserAggregationUpdate
        public void aggregate(
                @Name("value") Object value,
                // todo - rename to groupKeys
                @Name(value = "groupKeys") List<String> groupKeys,
                @Name(value = "aggKeys") List<String> aggKeys,
                @Name(value = "config", defaultValue = "{}")  Map<String, Object> config) {

            boolean cube = Util.toBoolean(config.get("cube"));
            
            Entity entity = (Entity) value;
            
            if (groupKeys.isEmpty()) {
                return;
            }
            groupKeysRes = groupKeys;

            if (cube) {
                // todo - is this right?
                List<List<String>> groupingSets = generateCombinationsWithPlaceholder(groupKeys);

                    for (List<String> groupKey : groupingSets) {
                        List<Object> partialKey = new ArrayList<>();
                        for (String column : groupKey) {
                            partialKey.add(((Entity) value).getProperty(column, NULL_ROLLUP));
                        }
                        if (!rolledUpData.containsKey(partialKey)) {
                            rolledUpData.put(partialKey, new HashMap<>());
                        }
                        extracted(aggKeys, entity, partialKey);
                    }
//                }
                return;
            }
            
            

            List<Object> groupKey = groupKeys.stream()
                    .map(i -> entity.getProperty(i, null))
                    .toList();

            for (int i = 0; i <= groupKey.size(); i++) {
                // todo - add null to remaining elements
                List<Object> partialKey = ListUtils.union(groupKey.subList(0, i), Collections.nCopies(groupKey.size() - i, NULL_ROLLUP));
                if (!rolledUpData.containsKey(partialKey)) {
                    rolledUpData.put(partialKey, new HashMap<>());
                }
                extracted(aggKeys, entity, partialKey);
            }
        }

        private void extracted(List<String> aggKeys, Entity entity, List<Object> partialKey) {
            Map<String, Number> partialResult = rolledUpData.get(partialKey);
            for(var aggKey: aggKeys) {
                if (entity.hasProperty(aggKey)) {
                    Object property = entity.getProperty(aggKey);
                    
                    String countKey = "COUNT(%s)".formatted(aggKey);
                    Number count = partialResult.compute(countKey,
                            ((subKey, subVal) -> {
                                return subVal == null ? 1 : subVal.longValue() + 1;
                            }));

                    String sumKey = "SUM(%s)".formatted(aggKey);
                    String avgKey = "AVG(%s)".formatted(aggKey);

                    if (property instanceof Number numberProp) {
                        Number sum = partialResult.compute(sumKey,
                                ((subKey, subVal) -> {
                                    if (subVal == null) {
                                        if (numberProp instanceof Long longProp) {
                                            return longProp;
                                        }
                                        return numberProp.doubleValue();
                                    }
                                    if (subVal instanceof Long long1
                                        && numberProp instanceof Long long2) {
                                        return long1 + long2;
                                    }
                                    return subVal.doubleValue() + numberProp.doubleValue();
                                }));
                        
                        partialResult.compute(avgKey,
                                ((subKey, subVal) -> {
                                    if (subVal == null) {
                                        if (numberProp instanceof Long longProp) {
                                            return longProp;
                                        }
                                        return numberProp.doubleValue();
                                    }
                                    if (sum instanceof Long longSum
                                        && count instanceof Long longCount) {
                                        return longSum / longCount;
                                    }
                                    return sum.doubleValue() + count.doubleValue();
                                }));
                    }
                }
            }
        }

        /**
         * Transform a Map.of(ListGroupKeys, MapOfAggResults) in a List of Map.of(AggResult + ListGroupKeyToMap)
         */
        @UserAggregationResult
        public Object result() {
            List<HashMap<String, Object>> list = rolledUpData.entrySet().stream()
                    .map(e -> {
                        HashMap<String, Object> map = new HashMap<>();
                        for (int i = 0; i < groupKeysRes.size(); i++) {
                            map.put(groupKeysRes.get(i), e.getKey().get(i));
                        }
                        map.putAll(e.getValue());
                        return map;
                    })
                    .sorted((m1, m2) -> {
                        for (String key : groupKeysRes) {
                            Object value1 = m1.get(key);
                            Object value2 = m2.get(key);
                            int cmp = compareValues(value1, value2);
                            if (cmp != 0) {
                                return cmp;
                            }
                        }
                        return 0;
                    })
                    .toList();

//            Comparator.comparing(i -> 
//            ComparatorUtils.chainedComparator(C
            
//            list.sort(
            return list;
//            return rolledUpData;
        }

// todo - used this instead of apoc.coll.sortMulti since 
        private static int compareValues(Object value1, Object value2) {
            if (value1 == null && value2 == null) {
                return 0;
            } else if (value1 == null) {
                return 1;
            } else if (value2 == null) {
                return -1;
            } else if (NULL_ROLLUP.equals(value1) && NULL_ROLLUP.equals(value2)) {
                return 0;
            } else if (NULL_ROLLUP.equals(value1)) {
                return 1;
            } else if (NULL_ROLLUP.equals(value2)) {
                return -1;
            } else if (value1 instanceof Comparable && value2 instanceof Comparable) {
                try {
                    return ((Comparable<Object>) value1).compareTo(value2);
                } catch (Exception e) {
                    // e.g. different data types, like int and strings
                    return 0;
                }

            } else {
                return 0;
            }
        }
    }
    
    
    /*
    
INSERT INTO ProductsAltro VALUES(SupplierID: 1, CategoryID: 1, random_int: 1, Price: 18, randomNum: 0.3});
INSERT INTO ProductsAltro VALUES(SupplierID: 1, CategoryID: 1, random_int: 0, Price: 19, randomNum: 0.5});
INSERT INTO ProductsAltro VALUES(SupplierID: 1, CategoryID: 2, random_int: 1, Price: 10, randomNum: 0.6});
INSERT INTO ProductsAltro VALUES(SupplierID: 4, CategoryID: 8, random_int: 0, Price: 31, randomNum: 0.6});
INSERT INTO ProductsAltro VALUES(SupplierID: 5, CategoryID: 4, random_int: 1, Price: 21, randomNum: 0.2});
INSERT INTO ProductsAltro VALUES(SupplierID: 6, CategoryID: 8, random_int: 1, Price: 6, randomNum: 0.5});
INSERT INTO ProductsAltro VALUES(SupplierID: 6, CategoryID: 7, random_int: 1, Price: 23, randomNum: 0.6});
INSERT INTO ProductsAltro VALUES(SupplierID: 7, CategoryID: 3, random_int: 1, Price: 17, randomNum: 0.7});
INSERT INTO ProductsAltro VALUES(SupplierID: 7, CategoryID: 6, random_int: 1, Price: 39, randomNum: 0.8});
INSERT INTO ProductsAltro VALUES(SupplierID: 7, CategoryID: 8, random_int: 1, Price: 63, randomNum: 0.9});
INSERT INTO ProductsAltro VALUES(SupplierID: 8, CategoryID: 3, random_int: 0, Price: 9, randomNum: 0.2});
INSERT INTO ProductsAltro VALUES(SupplierID: 8, CategoryID: 3, random_int: 1, Price: 81, randomNum: 0.5});
INSERT INTO ProductsAltro VALUES(SupplierID: 9, CategoryID: 5, random_int: 0, Price: 9, randomNum: 0.9});
INSERT INTO ProductsAltro VALUES(SupplierID: 10, CategoryID: 1, random_int: 1, Price: 5, randomNum: 0.2});
INSERT INTO ProductsAltro VALUES(SupplierID: 11, CategoryID: 3, random_int: 1, Price: 14, randomNum: 0.1});
INSERT INTO ProductsAltro VALUES(SupplierID: 11, CategoryID: 3, random_int: 0, Price: 31, randomNum: 0.2});
INSERT INTO ProductsAltro VALUES(SupplierID: 11, CategoryID: 4, random_int: 0, Price: 44, randomNum: 0.7});
INSERT INTO ProductsAltro VALUES(SupplierID: 1, CategoryID: NULL, random_int: 1, Price: 18, randomNum: 0.7});
INSERT INTO ProductsAltro VALUES(SupplierID: NULL, CategoryID: NULL, random_int: 0, Price: 18, randomNum: 0.6});
INSERT INTO ProductsAltro VALUES(SupplierID: NULL, CategoryID: 2, random_int: 0, Price: 199, randomNum: 0.8});  
    
    
,1, 0.3
,0, 0.5
,1, 0.6
,0, 0.6
,1, 0.2
,1, 0.5
,1, 0.6
,1, 0.7
,1, 0.8
,1, 0.9
,0, 0.2
,1, 0.5
,0, 0.9
,1, 0.2    
,1, 0.1    
,0, 0.2   
,0, 0.7 
,1, 0.7 
,0, 0.6 
,0, 0.8 
 
 
 import java.util.*;
0
public class GroupByRollupWithAggregations {

    public static void main(String[] args) {
        List<Map<String, Object>> data = new ArrayList<>();

        // Populate data
        Map<String, Object> row1 = new HashMap<>();
        row1.put("category", "A");
        row1.put("subcategory", "X");
        row1.put("type", "P");
        row1.put("value", 10);
        data.add(row1);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("category", "A");
        row2.put("subcategory", "X");
        row2.put("type", "Q");
        row2.put("value", 20);
        data.add(row2);

        Map<String, Object> row3 = new HashMap<>();
        row3.put("category", "A");
        row3.put("subcategory", "Y");
        row3.put("type", "P");
        row3.put("value", 30);
        data.add(row3);

        Map<String, Object> row4 = new HashMap<>();
        row4.put("category", "A");
        row4.put("subcategory", "Y");
        row4.put("type", "Q");
        row4.put("value", 40);
        data.add(row4);

        Map<String, Object> row5 = new HashMap<>();
        row5.put("category", "B");
        row5.put("subcategory", "X");
        row5.put("type", "P");
        row5.put("value", 50);
        data.add(row5);

        Map<String, Object> row6 = new HashMap<>();
        row6.put("category", "B");
        row6.put("subcategory", "X");
        row6.put("type", "Q");
        row6.put("value", 60);
        data.add(row6);

        Map<String, Object> row7 = new HashMap<>();
        row7.put("category", "B");
        row7.put("subcategory", "Y");
        row7.put("type", "P");
        row7.put("value", 70);
        data.add(row7);

        Map<String, Object> row8 = new HashMap<>();
        row8.put("category", "B");
        row8.put("subcategory", "Y");
        row8.put("type", "Q");
        row8.put("value", 80);
        data.add(row8);

        // Simulate GROUP BY ROLLUP with multiple aggregation functions
        Map<List<String>, Map<String, Integer>> rolledUpData = new HashMap<>();
        for (Map<String, Object> row : data) {
            List<String> groupKey = new ArrayList<>();
            groupKey.add((String) row.get("category"));
            groupKey.add((String) row.get("subcategory"));
            groupKey.add((String) row.get("type"));

            for (int i = 0; i <= groupKey.size(); i++) {
                List<String> partialKey = new ArrayList<>(groupKey.subList(0, i));
                if (!rolledUpData.containsKey(partialKey)) {
                    rolledUpData.put(partialKey, new HashMap<>());
                }
                Map<String, Integer> partialResult = rolledUpData.get(partialKey);
                partialResult.put("SUM", partialResult.getOrDefault("SUM", 0) + (int) row.get("value"));
                partialResult.put("COUNT", partialResult.getOrDefault("COUNT", 0) + 1);
            }
        }

        // Print rolled up data
        for (Map.Entry<List<String>, Map<String, Integer>> entry : rolledUpData.entrySet()) {
            System.out.println(entry.getKey() + " => " + entry.getValue());
        }
    }
}


    -- Create a table to hold the data
CREATE TABLE data (
    category VARCHAR(255),
    subcategory VARCHAR(255),
    type VARCHAR(255),
    value INT
);

-- Insert data into the table
INSERT INTO data (category, subcategory, type, value) VALUES ('A', 'X', 'P', 10);
INSERT INTO data (category, subcategory, type, value) VALUES ('A', 'X', 'Q', 20);
INSERT INTO data (category, subcategory, type, value) VALUES ('A', 'Y', 'P', 30);
INSERT INTO data (category, subcategory, type, value) VALUES ('A', 'Y', 'Q', 40);
INSERT INTO data (category, subcategory, type, value) VALUES ('B', 'X', 'P', 50);
INSERT INTO data (category, subcategory, type, value) VALUES ('B', 'X', 'Q', 60);
INSERT INTO data (category, subcategory, type, value) VALUES ('B', 'Y', 'P', 70);
INSERT INTO data (category, subcategory, type, value) VALUES ('B', 'Y', 'Q', 80);

-- Perform GROUP BY ROLLUP with multiple aggregation functions
SELECT 
    category,
    subcategory,
    type,
    SUM(value) AS total_sum,
    COUNT(*) AS total_count
FROM 
    data
GROUP BY 
    ROLLUP(category, subcategory, type);


     */
    
    
    
    
    // 
    
    
    
    
    /*
    
    
    
    
mysql> SELECT SupplierID, CategoryID, SUM(Price), AVG(Price) FROM Products GROUP BY SupplierID, CategoryID WITH ROLLUP;

+------------+------------+------------+------------+
| SupplierID | CategoryID | SUM(Price) | AVG(Price) |
+------------+------------+------------+------------+
|       NULL |       NULL |         18 |    18.0000 |
|       NULL |       NULL |         18 |    18.0000 |
|          1 |       NULL |         18 |    18.0000 |
|          1 |          1 |         37 |    18.5000 |
|          1 |          2 |         10 |    10.0000 |
|          1 |       NULL |         65 |    16.2500 |
|          2 |          2 |         81 |    20.2500 |
|          2 |       NULL |         81 |    20.2500 |
|          3 |          2 |         65 |    32.5000 |
|          3 |          7 |         30 |    30.0000 |
|          3 |       NULL |         95 |    31.6667 |
|          4 |          6 |         97 |    97.0000 |
|          4 |          7 |         10 |    10.0000 |
|          4 |          8 |         31 |    31.0000 |
|          4 |       NULL |        138 |    46.0000 |
|          5 |          4 |         59 |    29.5000 |
|          5 |       NULL |         59 |    29.5000 |
|          6 |          2 |         16 |    16.0000 |
|          6 |          7 |         23 |    23.0000 |
|          6 |          8 |          6 |     6.0000 |
|          6 |       NULL |         45 |    15.0000 |
|          7 |          1 |         15 |    15.0000 |
|          7 |          2 |         44 |    44.0000 |
|          7 |          3 |         17 |    17.0000 |
|          7 |          6 |         39 |    39.0000 |
|          7 |          8 |         63 |    63.0000 |
|          7 |       NULL |        178 |    35.6000 |
|          8 |          3 |        113 |    28.2500 |
|          8 |       NULL |        113 |    28.2500 |
|          9 |          5 |         30 |    15.0000 |
|          9 |       NULL |         30 |    15.0000 |
|         10 |          1 |          5 |     5.0000 |
|         10 |       NULL |          5 |     5.0000 |
|         11 |          3 |         89 |    29.6667 |
|         11 |       NULL |         89 |    29.6667 |
|         12 |          1 |          8 |     8.0000 |
|         12 |          2 |         13 |    13.0000 |
|         12 |          5 |         33 |    33.0000 |
|         12 |          6 |        124 |   124.0000 |
|         12 |          7 |         46 |    46.0000 |
|         12 |       NULL |        224 |    44.8000 |
|         13 |          8 |         26 |    26.0000 |
|         13 |       NULL |         26 |    26.0000 |
|         14 |          4 |         80 |    26.6667 |
|         14 |       NULL |         80 |    26.6667 |
|         15 |          4 |         61 |    20.3333 |
|         15 |       NULL |         61 |    20.3333 |
|         16 |          1 |         46 |    15.3333 |
|         16 |       NULL |         46 |    15.3333 |
|         17 |          8 |         60 |    20.0000 |
|         17 |       NULL |         60 |    20.0000 |
|         18 |          1 |        282 |   141.0000 |
|         18 |       NULL |        282 |   141.0000 |
|         19 |          8 |         28 |    14.0000 |
|         19 |       NULL |         28 |    14.0000 |
|         20 |          1 |         46 |    46.0000 |
|         20 |          2 |         19 |    19.0000 |
|         20 |          5 |         14 |    14.0000 |
|         20 |       NULL |         79 |    26.3333 |
|         21 |          8 |         22 |    11.0000 |
|         21 |       NULL |         22 |    11.0000 |
|         22 |          3 |         23 |    11.5000 |
|         22 |       NULL |         23 |    11.5000 |
|         23 |          1 |         18 |    18.0000 |
|         23 |          3 |         36 |    18.0000 |
|         23 |       NULL |         54 |    18.0000 |
|         24 |          5 |          7 |     7.0000 |
|         24 |          6 |         33 |    33.0000 |
|         24 |          7 |         53 |    53.0000 |
|         24 |       NULL |         93 |    31.0000 |
|         25 |          6 |         31 |    15.5000 |
|         25 |       NULL |         31 |    15.5000 |
|         26 |          5 |         58 |    29.0000 |
|         26 |       NULL |         58 |    29.0000 |
|         27 |          8 |         13 |    13.0000 |
|         27 |       NULL |         13 |    13.0000 |
|         28 |          4 |         89 |    44.5000 |
|         28 |       NULL |         89 |    44.5000 |
|         29 |          2 |         29 |    29.0000 |
|         29 |          3 |         49 |    49.0000 |
|         29 |       NULL |         78 |    39.0000 |
|       NULL |       NULL |       2263 |    28.6456 |
+------------+------------+------------+------------+



{SUPPL1, 1, CATID, 1, AVG(PRICE), }

SELECT SupplierID, CategoryID, Price FROM Products;
+------------+------------+-------+
| SupplierID | CategoryID | Price |
+------------+------------+-------+
|          1 |          1 |    18 |
|          1 |          1 |    19 |
|          1 |          2 |    10 |
|          2 |          2 |    22 |
|          2 |          2 |    21 |
|          3 |          2 |    25 |
|          3 |          7 |    30 |
|          3 |          2 |    40 |
|          4 |          6 |    97 |
|          4 |          8 |    31 |
|          5 |          4 |    21 |
|          5 |          4 |    38 |
|          6 |          8 |     6 |
|          6 |          7 |    23 |
|          6 |          2 |    16 |
|          7 |          3 |    17 |
|          7 |          6 |    39 |
|          7 |          8 |    63 |
|          8 |          3 |     9 |
|          8 |          3 |    81 |
|          8 |          3 |    10 |
|          9 |          5 |    21 |
|          9 |          5 |     9 |
|         10 |          1 |     5 |
|         11 |          3 |    14 |
|         11 |          3 |    31 |
|         11 |          3 |    44 |
|         12 |          7 |    46 |
|         12 |          6 |   124 |
|         13 |          8 |    26 |
|         14 |          4 |    13 |
|         14 |          4 |    32 |
|         15 |          4 |     3 |
|         16 |          1 |    14 |
|         16 |          1 |    18 |
|         17 |          8 |    19 |
|         17 |          8 |    26 |
|         18 |          1 |   264 |
|         18 |          1 |    18 |
|         19 |          8 |    18 |
|         19 |          8 |    10 |
|         20 |          5 |    14 |
|         20 |          1 |    46 |
|         20 |          2 |    19 |
|         21 |          8 |    10 |
|         21 |          8 |    12 |
|         22 |          3 |    10 |
|         22 |          3 |    13 |
|         23 |          3 |    20 |
|         23 |          3 |    16 |
|         24 |          7 |    53 |
|         24 |          5 |     7 |
|         24 |          6 |    33 |
|         25 |          6 |     7 |
|         25 |          6 |    24 |
|         26 |          5 |    38 |
|         26 |          5 |    20 |
|         27 |          8 |    13 |
|         28 |          4 |    55 |
|         28 |          4 |    34 |
|         29 |          2 |    29 |
|         29 |          3 |    49 |
|          7 |          2 |    44 |
|         12 |          5 |    33 |
|          2 |          2 |    21 |
|          2 |          2 |    17 |
|         16 |          1 |    14 |
|          8 |          3 |    13 |
|         15 |          4 |    36 |
|          7 |          1 |    15 |
|         15 |          4 |    22 |
|         14 |          4 |    35 |
|         17 |          8 |    15 |
|          4 |          7 |    10 |
|         12 |          1 |     8 |
|         23 |          1 |    18 |
|         12 |          2 |    13 |
|          1 |       NULL |    18 |
|       NULL |       NULL |    18 |
+------------+------------+-------+



     */
    

//    @UserAggregationFunction("apoc.agg.multiStats")
//    @Description("Return a multi-dimensional aggregation")
//    public MultiStatsFunction multiStats() {
//        return new MultiStatsFunction();
//    }
//
//    public static class MultiStatsFunction {
//
//        private final Map<String, Map<String, Map<String, NumberValue>>> result = new HashMap<>();
//        
//        @UserAggregationUpdate
//        public void aggregate(
//                @Name("value") Object value,
//                @Name(value = "keys") List<String> keys) {
//            Entity entity = (Entity) value;
//            
//            // for each prop
//            keys.forEach(key -> {
//                if (entity.hasProperty(key)) {
//                    Object property = entity.getProperty(key);
//                    
//                    result.compute(key, (ignored, v) -> {
//                        Map<String, Map<String, NumberValue>> map = Objects.requireNonNullElseGet(v, HashMap::new);
//                        
//                        map.compute(property.toString(), (propKey, propVal) -> {
//
//                            return getStringNumberValueMap(property, propVal);
//                        });
//
//                        return map;
//                    });
//                }
//            });
//        }
//
//        @UserAggregationResult
//        // apoc.agg.multiStats([key1,key2,key3]) -> Map<Key,Map<agg="sum,count,avg", number>>
//        public Map<String, Map<String, Map<String, NumberValue>>> result() {
//            return result;
//        }
//    }

    // Map<SupplierID, ValueSupplierId, CategoryID, ValueCategoryId, SUM(Price), <VALUE>, AVG(Price), <VALUE>, COUNT(Price), SUM(OtherVal), <VALUE> > 

    // Map<ListaDeiGroupKeys, ALTRI> --> 
    
//    private static Map<String, Object> getStringNumberValueMap(Object property, Map<String, NumberValue> propVal) {
//        Map<String, NumberValue> propMap = Objects.requireNonNullElseGet(propVal, HashMap::new);
//
//        NumberValue count = propMap.compute("count",
//                ((subKey, subVal) -> (NumberValue) ValueUtils.of(subVal == null ? 1 : subVal.longValue() + 1)) );
//
//        AnyValue neo4jValue = ValueUtils.of(property);
//
//        if (neo4jValue instanceof NumberValue numberValue) {
//            NumberValue sum = propMap.compute("sum",
//                    ((subKey, subVal) -> subVal == null ? numberValue : ValueMath.overflowSafeAdd(subVal, numberValue)));
//
//            propMap.compute("avg",
//                    ((subKey, subVal) -> subVal == null ? ValueUtils.asDoubleValue(numberValue.doubleValue()) : sum.dividedBy(count.doubleValue())  ));
//        }
//
//        return propMap;
//    }
    
//    private static Map<String, NumberValue> getStringNumberValueMap(Object property, Map<String, NumberValue> propVal) {
//        Map<String, NumberValue> propMap = Objects.requireNonNullElseGet(propVal, HashMap::new);
//
//        NumberValue count = propMap.compute("count",
//                ((subKey, subVal) -> (NumberValue) ValueUtils.of(subVal == null ? 1 : subVal.longValue() + 1)) );
//
//        AnyValue neo4jValue = ValueUtils.of(property);
//
//        if (neo4jValue instanceof NumberValue numberValue) {
//            NumberValue sum = propMap.compute("sum",
//                    ((subKey, subVal) -> subVal == null ? numberValue : ValueMath.overflowSafeAdd(subVal, numberValue)));
//            
//            propMap.compute("avg",
//                    ((subKey, subVal) -> subVal == null ? ValueUtils.asDoubleValue(numberValue.doubleValue()) : sum.dividedBy(count.doubleValue())  ));
//        }
//
//        return propMap;
//    }
}
