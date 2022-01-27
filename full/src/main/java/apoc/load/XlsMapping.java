package apoc.load;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static apoc.util.Util.parseCharFromConfig;
import static java.util.Collections.emptyList;

public class XlsMapping extends AbstractMapping {
    public static final XlsMapping EMPTY = new XlsMapping("", new LoadXlsConfig(null));
    final boolean array;
    final char arraySep;
    private final Pattern arrayPattern;

    public XlsMapping(String name, LoadXlsConfig config) {
        super(name, config);
        this.array = (Boolean) mapping.getOrDefault("array", false);
        this.arraySep = parseCharFromConfig(mapping, "arraySep", config.getArraySep());
        this.arrayPattern = Pattern.compile(String.valueOf(this.arraySep), Pattern.LITERAL);
        this.listSupplier = value -> Arrays.stream(arrayPattern.split((String) value)).map(this::commonConvertType).collect(Collectors.toList());
    }

    public Object convert(Object value) {
        return array ? convertArray(value) : commonConvertType(value);
    }

    private Object convertArray(Object value) {
        if (value == null) return emptyList();
        String[] values = arrayPattern.split(value.toString());
        List<Object> result = new ArrayList<>(values.length);
        for (String v : values) {
            result.add(commonConvertType(v));
        }
        return result;
    }
}
