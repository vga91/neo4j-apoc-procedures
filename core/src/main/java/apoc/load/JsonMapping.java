package apoc.load;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonMapping extends AbstractMapping {

    public JsonMapping(String name, LoadImportConfig config) {
        super(name, config);
    }

    public Object convert(Object value) {
        return value instanceof List ? convertList((List) value) : commonConvertType(value);
    }

    private Object convertList(List<Object> value) {
        return value.stream()
                .map(this::commonConvertType)
                .collect(Collectors.toList());
    }
}