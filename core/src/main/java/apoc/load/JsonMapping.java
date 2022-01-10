package apoc.load;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class JsonMapping extends AbstractMapping {

    public JsonMapping(String name, Map<String, Object> mapping, boolean ignore, List<String> nullValues, ZoneId timezone) {
        super(name, mapping, ignore, nullValues, timezone, true);
    }

    public Object convert(Object value) {
        return value instanceof List ? convertList((List) value) : commonConvertType(value);
    }

    private Object convertList(List<Object> value) {
        return value.stream().map(this::commonConvertType).collect(Collectors.toList());
    }
}