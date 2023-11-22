package apoc.agg;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.procedure.*;

public class CollAggregationExtended {
        /*
    todo: step 1 https://duckdb.org/docs/sql/aggregates#general-aggregate-functions
        any_value(arg)	OK
        arg_max(arg, val)	NO -> apoc.agg.maxItems()
        arg_min(arg, val)	NO -> apoc.agg.minItems()
        avg(arg)	no -> avg() in Cypher
        bit_and(arg)	Returns the bitwise AND of all bits in a given expression .	bit_and(A)	-
        bit_or(arg)	Returns the bitwise OR of all bits in a given expression.	bit_or(A)	-
        bit_xor(arg)	Returns the bitwise XOR of all bits in a given expression.	bit_xor(A)	-
        bitstring_agg(arg)	Returns a bitstring with bits set for each distinct value.	bitstring_agg(A)	-
        bool_and(arg)	Returns true if every input value is true, otherwise false.	bool_and(A)	-
        bool_or(arg)	Returns true if any input value is true, otherwise false.	bool_or(A)	-
        count(arg)	NO
        favg(arg)	NO
        first(arg)	--> apoc.agg.first
        fsum(arg)	--> sum()
        geomean(arg)	Calculates the geometric mean for all tuples in arg.	geomean(A)	geometric_mean(A)
        histogram(arg)	Returns a MAP of key-value pairs representing buckets and counts.	histogram(A)	-
        last(arg)	--> apoc.agg.last
        list(arg)	--> collect()
        max(arg)	NO -> max()
        min(arg)	NO -> min()
        product(arg)	NO -> apoc.agg.product()
        string_agg(arg, sep)	Concatenates the column string values with a separator	string_agg(S, ',')	group_concat(arg, sep), listagg(arg, sep)
        sum(arg) --> sum()
        
        https://duckdb.org/docs/sql/aggregates#ordered-set-aggregate-functions --> NO percentileCont() and percentileDisc()
        
        TODO IN ANOTHER PR MAYBE:
            https://duckdb.org/docs/sql/aggregates#approximate-aggregates
            https://duckdb.org/docs/sql/aggregates#statistical-aggregates
            
         
     */
    
        /*
    VALID:
    any_value
    bit_and
    bit_or
    bit_xor
    bitstring_agg
    bool_and --> chiamarla apoc.agg.all()
    bool_or --> chiamarla apoc.agg.any()
    geomean --> ??
    histogram --> ??
    string_agg --> apoc.agg.join
    
     */


    @UserAggregationFunction("apoc.agg.any")
    @Description("TODO")
    public AnyFunction any() {
        return new AnyFunction();
    }

    public static class AnyFunction {

        private boolean isNull = false;
        private boolean value = false;

        @UserAggregationUpdate
        public void update(@Name("value") Boolean value) {
            if (!this.value && value == null)  {
                isNull = true;
            } else if (Boolean.TRUE.equals(value)) {
                this.value = true;
                this.isNull = false;
            }
        }

        @UserAggregationResult
        public Boolean result() {
            return isNull ? null : value;
        }
    }

    @UserAggregationFunction("apoc.agg.all")
    @Description("TODO")
    public AllFunction all() {
        return new AllFunction();
    }

    public static class AllFunction {

        private boolean isNull = false;
        private boolean value;

        @UserAggregationUpdate
        public void update(@Name("value") Boolean value) {
            if (this.value && value == null)  {
                isNull = true;
            } else if (Boolean.FALSE.equals(value)) {
                this.value = false;
                isNull = false;
            } else if (Boolean.TRUE.equals(value)) {
                this.value = true;
            }
        }

        @UserAggregationResult
        public Boolean result() {
            return isNull ? null : value;
        }
    }
}
