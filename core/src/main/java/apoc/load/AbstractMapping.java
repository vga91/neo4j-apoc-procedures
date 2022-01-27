package apoc.load;

import apoc.meta.Meta;
import apoc.util.MappingUtil;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static apoc.load.LoadImportConfig.IGNORE_KEY;
import static apoc.load.LoadImportConfig.NULL_VALUES_KEY;
import static apoc.load.LoadImportConfig.TIMEZONE_KEY;
import static apoc.util.DateParseUtil.dateParse;
import static apoc.util.Util.dateFormat;

public abstract class AbstractMapping {
    
    final String name;
    final Collection<String> nullValues;
    final Meta.Types type;
    final String dateFormat;
    final String[] dateParse;
    final boolean ignore;

    Function<Object, Object> listSupplier = null;
    ZoneId zoneId;
    final Map<String, Object> mapping;

    public AbstractMapping(String name, LoadImportConfig config) {
        Map<String, Object> mapping = (Map<String, Object>) config.getMapping().get(name);
        if (mapping == null) {
            mapping = Collections.emptyMap();
        }
        this.mapping = mapping;
        this.name = mapping.getOrDefault("name", name).toString();
        this.ignore = (boolean) mapping.getOrDefault(IGNORE_KEY, config.getIgnore().contains(name));
        this.nullValues = (Collection<String>) mapping.getOrDefault(NULL_VALUES_KEY, config.getNullValues());
        this.type = Meta.Types.from((String) mapping.get("type"));
        this.dateFormat = mapping.getOrDefault("dateFormat", StringUtils.EMPTY).toString();
        this.dateParse = convertFormat(mapping.get("dateParse"));
        this.zoneId = getTimezoneIfValid(mapping, config.getZoneId());
    }
    
    public AbstractMapping(String name, Map<String, Object> mapping, boolean ignore, Collection<String> defaultNullValues, String zoneId, boolean isTypeNull) {
        if (mapping == null) {
            mapping = Collections.emptyMap();
        }
        this.mapping = mapping;
        this.name = mapping.getOrDefault("name", name).toString();
        this.ignore = (boolean) mapping.getOrDefault("ignore", ignore);
        this.nullValues = (Collection<String>) mapping.getOrDefault("nullValues", defaultNullValues);
        this.type = isTypeNull && !mapping.containsKey("type") 
                ? null 
                : Meta.Types.from(mapping.getOrDefault("type", Meta.Types.STRING.name()).toString());
        this.dateFormat = mapping.getOrDefault("dateFormat", StringUtils.EMPTY).toString();
        this.dateParse = convertFormat(mapping.get("dateParse"));
    }
    
    abstract protected <I> I convert(I input);

    public String getName() {
        return name;
    }

    public Meta.Types getType() {
        return type;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public String[] getDateParse() {
        return dateParse;
    }

    private static String[] convertFormat(Object value) {
        if (value == null) return null;
        if (!(value instanceof List)) throw new RuntimeException("Only array of Strings are allowed!");
        List<String> strings = (List<String>) value;
        return strings.toArray(new String[strings.size()]);
    }

    public Object commonConvertType(Object value) {
        if (nullValues.contains(name) || value == null) return null;
        
        if (type == null) {
            return value;
        }
        final boolean isParseNull = dateParse == null;
        switch (type) {
            case POINT:
                // in case of csv we retrieve a String to parse, in case of json directly a Map
                return value instanceof String
                        ? Util.toPoint(Util.fromJson((String) value, Map.class), mapping)
                        : Util.toPoint((Map<String, Object>) value, mapping);
            case STRING:
                if (value instanceof TemporalAccessor && !dateFormat.isEmpty()) {
                    return dateFormat((TemporalAccessor) value, dateFormat);
                }
                if(value instanceof BigDecimal) {
                    return ((BigDecimal) value).toPlainString();
                }
                return value.toString();
            case INTEGER:
                // to handle BigInteger and BigDecimal (jdbc)
                return MappingUtil.toLongOrString(value);
            case FLOAT:
                // to handle BigInteger and BigDecimal (jdbc)
                return MappingUtil.toDoubleOrString(value);
            case BOOLEAN:
                return Util.toBoolean(value);
            case NULL:
                return null;
            case LIST:
                return listSupplier == null ? null : listSupplier.apply(value);
            case DATE:
                // in case of parse null, we leverage Neo4j parsing, to handle e.g. '2018-05-10T10:30[Europe/Berlin]', otherwise we use dateParse
                return isParseNull
                        ? DateValue.parse((String) value).asObjectCopy()
                        : dateParse(value.toString(), LocalDate.class, dateParse);
            case DATE_TIME:
                return isParseNull
                        ? DateTimeValue.parse((String) value, () -> zoneId).asObjectCopy()
                        : dateParse(value.toString(), ZonedDateTime.class, zoneId, dateParse);
            case LOCAL_DATE_TIME:
                return isParseNull
                        ? LocalDateTimeValue.parse((String) value).asObjectCopy()
                        : dateParse(value.toString(), LocalDateTime.class, dateParse);
            case LOCAL_TIME:
                return isParseNull
                        ? LocalTimeValue.parse((String) value).asObjectCopy()
                        : dateParse(value.toString(), LocalTime.class, dateParse);
            case TIME:
                return isParseNull
                        ? TimeValue.parse((String) value, () -> zoneId).asObjectCopy()
                        : dateParse(value.toString(), OffsetTime.class, zoneId, dateParse);
            case DURATION:
                return DurationValue.parse((String) value).asObjectCopy();
            default:
                return value;
        }
    }

    private ZoneId getTimezoneIfValid(Map<String, Object> config, String defaultZone) {
        try {
            return Optional.ofNullable((String) config.getOrDefault(TIMEZONE_KEY, defaultZone))
                    .map(ZoneId::of)
                    .orElse(null);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException(String.format("The timezone field contains an error: %s", e.getMessage()));
        }
    }
}
