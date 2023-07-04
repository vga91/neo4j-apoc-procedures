package apoc.load;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.parquet.column.ColumnDescriptor;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.value.MapValue;
import blue.strategic.parquet.Hydrator;

/**
 * Hydrates Parquet records into a {@link Value} containing a map of properties.
 */
final class PropertiesHydrator<R> implements Hydrator<Object[], R> {

    private final Map<String, Integer> index;
    private final Function<Map<String, Object>, R> finisher;

    PropertiesHydrator(List<ColumnDescriptor> columns, Function<Map<String, Object>, R> finisher) {
        this.index = new HashMap<>(columns.size());
        this.finisher = finisher;
        int idx = 0;
        for (ColumnDescriptor d : columns) {
            this.index.put(d.getPath()[d.getPath().length - 1], idx++);
        }
    }

    @Override
    public Object[] start() {
        return new Object[index.size()];
    }

    @Override
    public Object[] add(Object[] target, String heading, Object value) {
        if (index.get(heading) == null) {
            return target;
        }
        if ("kids".equals(heading)/* value != null && value.getClass().isArray()*/) {
            System.out.println("target = " + target);
        }

        try {
            target[index.get(heading)] = value;
        } catch (Exception e) {
            System.out.println("e = " + e);
        }
        return target;
    }

    @Override
    public R finish(Object[] target) {
        return this.index.entrySet().stream()
                .filter(e -> target[e.getValue()] != null)
                .collect(Collectors.collectingAndThen(Collectors.toMap(Map.Entry::getKey, e -> target[e.getValue()]), finisher));
    }
}
