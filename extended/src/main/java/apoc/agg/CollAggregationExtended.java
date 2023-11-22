package apoc.agg;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.UnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.*;
import org.apache.commons.math3.stat.descriptive.rank.*;
import org.apache.commons.math3.stat.descriptive.summary.*;
import org.neo4j.procedure.*;

public class CollAggregationExtended {
    public static final String BITWISE_OPERATOR_NOT_DEFINED = "Bitwise operator not defined";

    @UserAggregationFunction("apoc.agg.statisticalOperation")
    @Description("TODO")
    public StatisticalOperation statisticalOperation() {
        return new StatisticalOperation();
    }

    public static UnivariateStatistic from(String type) {
        return switch (type.toUpperCase()) {
            case "SUM" -> new Sum();
            case "SUM_OF_SQUARES" -> new SumOfSquares();
            case "PRODUCT" -> new Product();
            case "SUM_OF_LOGS" -> new SumOfLogs();
            case "MIN" -> new Min();
            case "MAX" -> new Max();
            case "MEAN" -> new Mean();
            case "VARIANCE" -> new Variance();
            case "PERCENTILE" -> new Percentile();
            case "GEOMETRIC_MEAN" -> new GeometricMean();
            case "SKEWNESS" -> new Skewness();
            case "STANDARD_DEVIATION" -> new StandardDeviation();
            case "SECOND_MOMENT" -> new SecondMoment();
            case "KURTOSIS" -> new Kurtosis();
            case "SEMI_VARIANCE" -> new SemiVariance();
            default -> throw new RuntimeException("Invalid statistical operation");
        };
    }

    public static class StatisticalOperation {

        private UnivariateStatistic operation;
        private long begin;
        private long length;
        private final List<Double> valueList = new ArrayList<>();

        @UserAggregationUpdate
        public void update(@Name("value") double current, 
                           @Name("operation") String operation,
                           @Name(value = "begin", defaultValue = "-1") long begin,
                           @Name(value = "length", defaultValue = "-1") long length) {
            if (this.operation == null) {
                this.operation = CollAggregationExtended.from(operation);
                this.begin = begin;
                this.length = length;
            }

            this.valueList.add(current);
        }

        @UserAggregationResult
        public double result() {
            double[] doubles = valueList.stream().mapToDouble(Double::doubleValue).toArray();
            if (begin == -1L || length == -1L) {
                return operation.evaluate(doubles);
            }
            return operation.evaluate(doubles, (int) begin, (int) length);
        }
    }
    
    @UserAggregationFunction("apoc.agg.binaryString")
    @Description("TODO")
    public BitStringFunction binaryString() {
        return new BitStringFunction();
    }

    public static class BitStringFunction {

        private final Set<String> value = new HashSet<>();

        @UserAggregationUpdate
        public void update(@Name("value") Long current) {
            String binaryString = Long.toBinaryString(current);
            this.value.add(binaryString);
        }

        @UserAggregationResult
        public List<String> result() {
            return List.copyOf(value);
        }
    }
    
    
    @UserAggregationFunction("apoc.agg.bitwise")
    @Description("TODO")
    public BitwiseFunction bitwise() {
        return new BitwiseFunction();
    }

    public static class BitwiseFunction {

        private Long value;

        @UserAggregationUpdate
        public void update(@Name("value") Long current, @Name("operator") final String operator) {
            this.value = bitwiseOperation(this.value, operator, current);
        }

        @UserAggregationResult
        public Long result() {
            return value;
        }
    }

    /**
     * Similar to `apoc.bitwise.BitwiseOperations.java` (just with switch operator improved)
     * and without `NOT` operator, which it doesn't make much sense with an aggregation function
     */
    public static Long bitwiseOperation(Long a, String operator, Long b) {
        if (a == null) {
            return b;
        }
        if (operator == null || operator.isEmpty()) {
            throw new RuntimeException(BITWISE_OPERATOR_NOT_DEFINED);
        }
        if (!operator.equals("~") && b == null) {
            return null;
        }
        return switch (operator.toLowerCase()) {
            case "&", "and" -> a & b;
            case "|", "or" -> a | b;
            case "^", "xor" -> a ^ b;
            case ">>", "right shift" -> a >> b;
            case ">>>", "right shift unsigned" -> a >>> b;
            case "<<", "left shift" -> a << b;
            default -> throw new RuntimeException("Invalid bitwise operator : '%s'".formatted(operator));
        };
    }
    

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
