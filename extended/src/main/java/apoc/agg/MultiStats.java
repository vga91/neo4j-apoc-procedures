package apoc.agg;

import apoc.Extended;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Extended
public class MultiStats {


    @UserAggregationFunction("apoc.agg.rollup")
    @Description("Return a multi-dimensional aggregation")
    public RollupFunction rollup() {
        return new RollupFunction();
    }

    public static class RollupFunction {
        private static final String NULL_ROLLUP = "NULL";
        private final Map<String, Object> result = new HashMap<>();
//        private final Map<String, Map<String, Map<String, NumberValue>>> result = new HashMap<>();

        @UserAggregationUpdate
        public void aggregate(
                @Name("value") Object value,
                @Name(value = "groupKeys") List<String> groupKeys,
                @Name(value = "aggKeys") List<String> aggKeys) {
            Entity entity = (Entity) value;
            
            if (groupKeys.isEmpty()) {
                return;
            }
            
            if (entity.hasProperty(groupKeys.get(0))) {
                return;
            }
                
            result.compute(groupKeys.get(0), (i, v) -> {
                result.compute(groupKeys.get(1), (i2, v2) -> {
                    
                });
            });


//            result.compute(NULL_ROLLUP, ()
            
            
            // primo compute
                    // inner compute
                    // 
            // secondo compute
            // terzo compute
            // `NULL`
        }
    }
    
    
    /*
    mysql> SELECT SupplierID, CategoryID, sum(Price), avg(Price), Unit FROM Products GROUP BY SupplierID, CategoryID WITH ROLLUP;
ERROR 1055 (42000): Expression #5 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'Northwind.Products.Unit' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by
mysql> SELECT SupplierID, CategoryID, sum(Price), avg(Price) FROM Products GROUP BY SupplierID, CategoryID WITH ROLLUP;
+------------+------------+------------+------------+
| SupplierID | CategoryID | sum(Price) | avg(Price) |
+------------+------------+------------+------------+
|          1 |          1 |         37 |    18.5000 |
|          1 |          2 |         10 |    10.0000 |
|          1 |       NULL |         47 |    15.6667 |
|          2 |          2 |         81 |    20.2500 |
|          2 |       NULL |         81 |    20.2500 |
     */
    

    @UserAggregationFunction("apoc.agg.multiStats")
    @Description("Return a multi-dimensional aggregation")
    public MultiStatsFunction multiStats() {
        return new MultiStatsFunction();
    }

    public static class MultiStatsFunction {

        private final Map<String, Map<String, Map<String, NumberValue>>> result = new HashMap<>();
        
        @UserAggregationUpdate
        public void aggregate(
                @Name("value") Object value,
                @Name(value = "keys") List<String> keys) {
            Entity entity = (Entity) value; 
            
            // for each prop
            keys.forEach(key -> {
                if (entity.hasProperty(key)) {
                    Object property = entity.getProperty(key);
                    
                    result.compute(key, (ignored, v) -> {
                        Map<String, Map<String, NumberValue>> map = Objects.requireNonNullElseGet(v, HashMap::new);
                        
                        map.compute(property.toString(), (propKey, propVal) -> {

                            return getStringNumberValueMap(property, propVal);
                        });

                        return map;
                    });
                }
            });
        }
        

        @UserAggregationResult
        // apoc.agg.multiStats([key1,key2,key3]) -> Map<Key,Map<agg="sum,count,avg", number>>
        public Map<String, Map<String, Map<String, NumberValue>>> result() {
            return result;
        }
    }

    
    private static Map<String, NumberValue> getStringNumberValueMap(Object property, Map<String, NumberValue> propVal) {
        Map<String, NumberValue> propMap = Objects.requireNonNullElseGet(propVal, HashMap::new);

        NumberValue count = propMap.compute("count",
                ((subKey, subVal) -> (NumberValue) ValueUtils.of(subVal == null ? 1 : subVal.longValue() + 1)) );

        AnyValue neo4jValue = ValueUtils.of(property);

        if (neo4jValue instanceof NumberValue numberValue) {
            NumberValue sum = propMap.compute("sum",
                    ((subKey, subVal) -> subVal == null ? numberValue : ValueMath.overflowSafeAdd(subVal, numberValue)));
            
            propMap.compute("avg",
                    ((subKey, subVal) -> subVal == null ? ValueUtils.asDoubleValue(numberValue.doubleValue()) : sum.dividedBy(count.doubleValue())  ));
        }

        return propMap;
    }
}
