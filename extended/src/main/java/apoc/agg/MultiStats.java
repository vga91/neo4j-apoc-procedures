package apoc.agg;

import apoc.Extended;
import com.amazonaws.util.NumberUtils;
import org.HdrHistogram.HistogramUtil;
import org.neo4j.graphdb.Entity;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.utils.ValueMath;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Extended
public class MultiStats {
    /*
    For a property you want to have more than one statistic:

size, count, avg, median, ...

which is probably already covered by apoc.agg.stats()

but then, how about multi-dimensional aggregation.

e.g.

apoc.agg.multiStats([key1,key2,key3]) -> Map<Key,Map<agg="sum,count,avg", number>>

e.g.

match (p:Person)
with apoc.agg.multiStats(p, ["wcc","lpa","louvain"]) as data
match (p:Person) 
return p.name, data[toString(p.wcc)].count as size

see
https://community.neo4j.com/t/listing-the-community-size-of-different-community-detection-algorithms-already-calculated/42895/2?u=michael.hunger
     */

    @UserAggregationFunction("apoc.agg.multiStats")
    @Description("TODO...")
    public MultiStatsFunction multiStats() {
        return new MultiStatsFunction();
    }
    
    // todo --> sum,count,avg

    public static class MultiStatsFunction {

//        private Histogram values = new Histogram(3);
//        private DoubleHistogram doubles;
//        private List<Double> percentiles = asList(0.5D, 0.75D, 0.9D, 0.95D, 0.9D, 0.99D);
//        private Number minValue;
        private final Map<String, Map<String, NumberValue>> result = new HashMap<>();

        // --> TODO - sum must be similar to https://neo4j.com/docs/cypher-manual/current/functions/aggregating/#functions-sum
        
        @UserAggregationUpdate
        public void aggregate(
                @Name("value") Object value,
                @Name(value = "keys") List<String> keys,
                // todo...
                @Name(value = "statistics", defaultValue = "['sum','count','avg']") List<String> statistics
                ) {
            // todo - can be also a map, maybe?
            Entity entity = (Entity) value; 
            
            // per ogni prop
            keys.forEach(key -> {
                if (entity.hasProperty(key)) {
                    Object property = entity.getProperty(key);
                    // todo - forse metterlo all'esterno
                    result.compute(key, (ignored, v) -> {
                        Map<String, NumberValue> map;
                        if (v == null) {
                            map = new HashMap<>();
                        } else {
                            map = v;
                        }

                        NumberValue count = map.compute("count", ((subKey, subVal) -> (NumberValue) ValueUtils.of(subVal == null ? 1 : subVal.longValue() + 1)) );
                        
                        if (property instanceof Number propNum) {
//                            NumberValue
//                            
                            NumberValue of = (NumberValue) ValueUtils.of(property);
                            
//                            ValueMath.overflowSafeAdd(
                            
//                            ValueMath.overflowSafeAdd(  )
                            
                            
                            // todo - double and long must be different ?
                            NumberValue sum = map.compute("sum", ((subKey, subVal) -> subVal == null ? of : ValueMath.overflowSafeAdd(subVal, of)));

                            // NB: avg() return always a double
                            NumberValue avg = map.compute("avg", ((subKey, subVal) -> subVal == null ? of : sum.dividedBy(count.doubleValue())  ));
//                            NumberValue avg = map.compute("avg", ((subKey, subVal) -> subVal == null ? of : sum.divideBy (count)  ));
                        }

                        return map;
                    });
                    
//                    Map<String, Number> orDefault = result.getOrDefault(key, new HashMap<>());
//                    orDefault.compute("count", ((k, v) -> v == null ? 1 : v.longValue() + 1));
//                    System.out.println("orDefault = " + orDefault);
                }
            });
//            value.getProperty()
        }
        
        // --> Map<key, Map<key, Map<key, value>> >

        @UserAggregationResult
        // apoc.agg.multiStats([key1,key2,key3]) -> Map<Key,Map<agg="sum,count,avg", number>>
        public Map<String, Map<String, NumberValue>> result() {
            return result;
        }
    }
}
