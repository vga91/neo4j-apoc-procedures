package apoc.util;

import java.math.BigDecimal;
import java.math.BigInteger;

public class MappingUtil {
    
    public static Object toLongOrString(Object value) {
        if (value instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) value;
            try {
                return bigInteger.longValueExact();
            } catch (ArithmeticException e) {
                return bigInteger.toString();
            }
        }
        if (value instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) value;
            try {
                return bigDecimal.longValueExact();
            } catch (ArithmeticException e) {
                return bigDecimal.toString();
            }
        }
        return Util.toLong(value);
    }
    
    public static Object toDoubleOrString(Object value) {
        if (value instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) value;
            return checkIfFitsScale(bigInteger.doubleValue(), bigInteger.toString());
        }
        if (value instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) value;
            double doubleValue = bigDecimal.doubleValue();
            return checkIfFitsScale(doubleValue, bigDecimal.toPlainString());
        }
        return Util.toDouble(value);
    }

    private static Object checkIfFitsScale(double doubleValue, String numAsString) {
        final boolean fitsScale = doubleValue != Double.POSITIVE_INFINITY
                && doubleValue != Double.NEGATIVE_INFINITY;
        return fitsScale ? doubleValue : numAsString;
    }
}
