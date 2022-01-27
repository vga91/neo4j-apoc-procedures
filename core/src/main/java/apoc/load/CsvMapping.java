package apoc.load;

import apoc.export.csv.CsvLoaderConfig;
import apoc.load.util.LoadCsvConfig;
import apoc.meta.Meta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static apoc.util.Util.parseCharFromConfig;


public class CsvMapping extends AbstractMapping {
    public static final CsvMapping EMPTY = new CsvMapping("", new LoadCsvConfig(null));
    final boolean array;

    char arraySep;
    private final Pattern arrayPattern;

    public CsvMapping(String name, LoadImportConfig config) {
        super(name, config);
        final Map<String, Object> mapping = (Map<String, Object>) config.getMapping().getOrDefault(name, Collections.emptyMap());
        this.array = (Boolean) mapping.getOrDefault("array", false);
        this.arraySep = config instanceof LoadCsvConfig 
                ? parseCharFromConfig(mapping, "arraySep", ((LoadCsvConfig) config).getArraySep())
                : parseCharFromConfig(mapping, "arraySep", ((CsvLoaderConfig) config).getArrayDelimiter());
        this.arrayPattern = Pattern.compile(String.valueOf(this.arraySep), Pattern.LITERAL);

        this.listSupplier = value -> Arrays.stream(arrayPattern.split((String) value)).map(this::convertType).collect(Collectors.toList());

        if (this.type == null) {
            // Call this out to the user explicitly because deep inside of LoadCSV and others you will get
            // NPEs that are hard to spot if this is allowed to go through.
            throw new RuntimeException("In specified mapping, there is no type by the name " +
                    mapping.getOrDefault("type", "STRING").toString());
        }
    }

    public Object convert(Object value) {
        final String stringValue = (String) value;
        return array ? convertArray(stringValue) : convertType(stringValue);
    }

    private Object convertArray(String value) {
        String[] values = arrayPattern.split(value);
        List<Object> result = new ArrayList<>(values.length);
        for (String v : values) {
            result.add(convertType(v));
        }
        return result;
    }

    private Object convertType(String value) {
        if (type == Meta.Types.STRING) return value;
        return super.commonConvertType(value);
    }
}