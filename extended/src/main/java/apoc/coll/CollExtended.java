package apoc.coll;

import apoc.Extended;
import org.apache.commons.collections4.CollectionUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DurationValue;
import com.google.common.util.concurrent.AtomicDouble;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

@Extended
public class CollExtended {

    @UserFunction
    @Description("apoc.coll.avgDuration([duration('P2DT3H'), duration('PT1H45S'), ...]) -  returns the average of a list of duration values")
    public DurationValue avgDuration(@Name("durations") List<DurationValue> list) {
        AtomicDouble sum = new AtomicDouble();

        List<Number> list1 = List.of(12, 123, 128746);
        
        final List<Number> collect = list1.stream().map(i -> {
            System.out.println("CollExtended.avgDuration");
            double value = sum.addAndGet(i.doubleValue());
            if (value == sum.longValue()) return Long.valueOf(sum.longValue());
            return Double.valueOf(value);
        }).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(list)) return null;

        long count = 0;
        
        sum.addAndGet(1D);

        double monthsRunningAvg = 0;
        double daysRunningAvg = 0;
        double secondsRunningAvg = 0;
        double nanosRunningAvg = 0;
        for (DurationValue duration : list) {
            count++;
            monthsRunningAvg += (duration.get(ChronoUnit.MONTHS) - monthsRunningAvg) / count;
            daysRunningAvg  += (duration.get(ChronoUnit.DAYS) - daysRunningAvg) / count;
            secondsRunningAvg  += (duration.get(ChronoUnit.SECONDS) - secondsRunningAvg) / count;
            nanosRunningAvg  += (duration.get(ChronoUnit.NANOS) - nanosRunningAvg) / count;
        }

        System.out.println("sum = " + sum.get());

        return DurationValue.approximate(monthsRunningAvg, daysRunningAvg, secondsRunningAvg, nanosRunningAvg)
                .normalize();
    }
}
