package apoc.agg;

import apoc.Extended;
import org.HdrHistogram.HistogramUtil;
import org.neo4j.graphdb.Entity;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

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
        private Map<String, Map<String, Number>> result;

        @UserAggregationUpdate
        public void aggregate(
                @Name("value") Entity value,
                @Name(value = "keys") List<String> keys,
                // todo...
                @Name(value = "statistics", defaultValue = "['sum','count','avg']") List<String> statistics
                ) {
            // per ogni prop
            keys.forEach(key -> {
                if (value.hasProperty(key)) {
                    Object property = value.getProperty(key);
                    // todo - forse metterlo all'esterno
                    Map<String, Number> orDefault = result.getOrDefault(key, new HashMap<>());
                    orDefault.compute("count", 
                }
            });
//            value.getProperty()
        }

        @UserAggregationResult
        // apoc.agg.multiStats([key1,key2,key3]) -> Map<Key,Map<agg="sum,count,avg", number>>
        public Map<String, Map<String, Number>> result() {
            return result;
        }
    }
}
