package apoc.load;

import java.util.regex.Pattern;

import static apoc.util.Util.parseCharFromConfig;

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
    }

    public Object convert(Object value) {
        return array 
                ? convertArray(value, arrayPattern, this::commonConvertType) 
                : commonConvertType(value);
    }
}
