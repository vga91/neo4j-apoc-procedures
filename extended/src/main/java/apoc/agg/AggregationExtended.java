package apoc.agg;

import apoc.Extended;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import java.util.Map;

@Extended
public class AggregationExtended {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @UserAggregationFunction("apoc.agg.row")
    @Description("TODO")
    public RowFunction minItems() {
        return new RowFunction();
    }

    public class RowFunction {
        private boolean found;
        private long index = -1L;

        @UserAggregationUpdate
        public void nth(@Name("value") Object value, @Name("predicate") String predicate, @Name(value = "first", defaultValue = "true") boolean first) {
            if (!found) {
                this.found = db.executeTransactionally("RETURN " + predicate,
                        Map.of("curr", value),
                        result -> Iterators.singleOrNull(result.columnAs(Iterables.single(result.columns()))));
                index++;
            }
        }

        @UserAggregationResult
        public Object result() {
            return index;
        }
    }
    
    // todo --> like apoc.path.expand?? no..
    
    
}
