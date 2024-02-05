package apoc.agg;

import apoc.Extended;
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
}
