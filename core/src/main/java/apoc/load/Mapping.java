package apoc.load;

import apoc.load.util.LoadCsvConfig;
import apoc.meta.Meta;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.DateParseUtil.DB_TEMPORAL_TIMEZONE;
import static apoc.util.DateParseUtil.getTimezoneIfValid;
import static apoc.util.Util.parseCharFromConfig;
import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseSettings.db_temporal_timezone;


public class Mapping extends AbstractMapping {
    public static final Mapping EMPTY = new Mapping("", Collections.emptyMap(), LoadCsvConfig.DEFAULT_ARRAY_SEP, false, null);
    final boolean array;

    final char arraySep;
    private final Pattern arrayPattern;

    public Mapping(String name, Map<String, Object> mapping, char arraySep, boolean ignore, ZoneId zoneId) {
        super(name, mapping, ignore, emptyList(), zoneId);
        this.array = (Boolean) mapping.getOrDefault("array", false);
        this.arraySep = parseCharFromConfig(mapping, "arraySep", arraySep);
        this.arrayPattern = Pattern.compile(String.valueOf(this.arraySep), Pattern.LITERAL);

        this.listSupplier = value -> Arrays.stream(arrayPattern.split((String) value)).map(this::convertType).collect(Collectors.toList());

        if (this.zoneId == null) {
            System.out.println("DB_TEMPORAL_TIMEZONE 1" + DB_TEMPORAL_TIMEZONE);
            System.out.println("DB_TEMPORAL_TIMEZONE 2" + apocConfig().getString(db_temporal_timezone.name()));
            System.out.println("DB_TEMPORAL_TIMEZONE 3" + apocConfig().getString("db.temporal.timezone"));
            System.out.println("DB_TEMPORAL_TIMEZONE 4" + db_temporal_timezone.name());
            // to preserve ImportCsv behavior like neo4j-import-tool
            // we leverage on optionalData, e.g. myProp:time{timezone:+02:00}
            this.zoneId = getTimezoneIfValid(optionalData, apocConfig().getString("db.temporal.timezone"));
        }

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