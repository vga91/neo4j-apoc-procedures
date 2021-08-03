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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.Util.parseCharFromConfig;
import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseSettings.db_temporal_timezone;


// - riprendere codice di https://github.com/vga91/neo4j-apoc-procedures/pull/74/files

// todo - questo si può riutilizzare in qualche modo?

// todo - fare un AbstractMapping.class che poi viene esteso con super(..) --> tutto tranne robe di array
public class Mapping extends AbstractMapping {
    public static final Mapping EMPTY = new Mapping("", Collections.emptyMap(), LoadCsvConfig.DEFAULT_ARRAY_SEP, false/*, false*/);
    final boolean array;
    
    // todo - arraySep e arrayPattern metterli solo dove serve
    final char arraySep;
    private final Pattern arrayPattern;
    
    // todo - optionalData è una roba di qua, ma forse vale la pena metterlo nell'Abstract..
//    private final Map<String, Object> optionalData;
    

//    private final String[] dateParse;

    
    // todo - provare a mettere i Function<> da qualche altra parte
//    final BiFunction<Pattern, Object, Object> listFunction = (arrayPattern, value) -> Arrays.stream(arrayPattern.split((String) value)).map(this::convertType).collect(Collectors.toList());
    
    public Mapping(String name, Map<String, Object> mapping, char arraySep, boolean ignore) {
        super(name, mapping, ignore, emptyList(), ZoneId.systemDefault()/*, nullValues*/); // todo - implementare nullValues, o forse no
        
//        this.name = mapping.getOrDefault("name", name).toString();
        this.array = (Boolean) mapping.getOrDefault("array", false);
        this.arraySep = parseCharFromConfig(mapping, "arraySep", arraySep);
//        this.type = Meta.Types.from(mapping.getOrDefault("type", "STRING").toString());
        this.arrayPattern = Pattern.compile(String.valueOf(this.arraySep), Pattern.LITERAL);
        
        // todo - necessario il cast a string?
        this.listSupplier = value -> Arrays.stream(arrayPattern.split((String) value)).map(this::convertType).collect(Collectors.toList());

//        this.dateParse = convertFormat(mapping.getOrDefault("dateParse", DEFAULT_DATE_PATTERN));

//        this.listFunction = (arrayPattern, value) -> Arrays.stream(arrayPattern.split((String) value)).map(this::convertType).collect(Collectors.toList());
        
        if (this.type == null) {
            // Call this out to the user explicitly because deep inside of LoadCSV and others you will get
            // NPEs that are hard to spot if this is allowed to go through.
            throw new RuntimeException("In specified mapping, there is no type by the name " +
                    mapping.getOrDefault("type", "STRING").toString());
        }
    }

    // todo - UtilMapping.java
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

    // todo - nella pr del csv import mettere ZoneId.systemDefault() --> db.temporal.timezone
    
    private Object convertType(String value) {
        // todo --> if (nullValues || value == null) return null; va bene per tutti?
//        if (nullValues) return null;
//        if (nullValues.contains(value)) return null;
        // todo - questo va bene qua, non nelle altre cose...
        if (type == Meta.Types.STRING) return value;
        
//        final Supplier<Object> listSupplier = () -> Arrays.stream(arrayPattern.split((String) value)).map(this::convertType).collect(Collectors.toList());
        // todo - forse ha senso passare timezone, per sql tipo
        final Supplier<ZoneId> timezone = () -> ZoneId.of((String) optionalData.getOrDefault("timezone", ZoneId.systemDefault().getId()));
        return this.switchConvertType(value/*, timezone*/);
//        return MappingUtils.convertType(value, this, listSupplier, timezone, optionalData);
        
//        final Supplier<ZoneId> timezone = () -> ZoneId.of((String) optionalData.getOrDefault("timezone", ZoneId.systemDefault().getId()));
//        switch (type) {
//            // todo -> point... posso metterlo in un common util... -> se instance of Map allora ok altrimenti Util.fromJson(...)
//            case POINT:
//                // todo - point supplier oppure instanceOf --> tipo mongoDb...
//                return Util.toPoint(Util.fromJson(value, Map.class), optionalData);
////                return Util.toPoint((Map<String, Object>) value, Collections.emptyMap());
//
//            case STRING:
//                // todo - questo forse va bene rimanerlo
//                // todo - string supplier
////                if (value instanceof TemporalAccessor && !dateFormat.isEmpty()) {
////                    return dateFormat((TemporalAccessor) value, dateFormat);
////                } else {
//                    return value.toString();
////                }
//            case INTEGER:
//                return Util.toLong(value);
//            case FLOAT:
//                return Util.toDouble(value);
//            case BOOLEAN:
//                return Util.toBoolean(value);
//            // todo - fare un test anche con questo
//            case NULL:
//                return null;
////                case LIST:
////                    return Arrays.stream(arrayPattern.split(value.toString())).map(this::convertType).collect(Collectors.toList());
//            case LIST:
//                // todo - list supplier
//                return listFunction.apply(arrayPattern, value);
////                return Arrays.stream(arrayPattern.split(value)).map(this::convertType).collect(Collectors.toList());
//            case DATE:
//                // todo - valutare asObjectCopy() se serve effettivamente... credo di si per l'import
//
//                return dateParse == null
//                        ? DateValue.parse((String) value).asObjectCopy()
//                        : dateParse(value.toString(), LocalDate.class, dateParse);
//            case DATE_TIME:
//                // todo...
//                return dateParse == null
//                        ? DateTimeValue.parse((String) value, timezone).asObjectCopy()
//                        : dateParse(value.toString(), ZonedDateTime.class, dateParse);
//            case LOCAL_DATE_TIME:
////                    return dateParse(value.toString(), LocalDateTime.class, dateParse);
//                return dateParse == null
//                        ? LocalDateTimeValue.parse((String) value).asObjectCopy()
//                        : dateParse(value.toString(), LocalDateTime.class, dateParse);
//            case LOCAL_TIME:
////                    return dateParse(value.toString(), LocalTime.class, dateParse);
//                return dateParse == null
//                        ? LocalTimeValue.parse((String) value).asObjectCopy()
//                        : dateParse(value.toString(), LocalTime.class, dateParse);
//            case TIME:
//                return dateParse == null
//                        ? TimeValue.parse((String) value, timezone).asObjectCopy()
//                        : dateParse(value.toString(), OffsetTime.class, dateParse);
//            case DURATION:
//                // provare DurationValue.parse() vs Duration.parse(..) -> se funzionano entrambi mettere durationParse()
//                
//                // TODO con import csv funziona solo questo... --> vedere con gli altri a sto punto
//                return DurationValue.parse(value).asObjectCopy();
////                return durationParse(value.toString());
//            default:
//                return value;
//        }
            
//            case POINT:
//                return Util.toPoint(Util.fromJson(value, Map.class), optionalData);
//            case LOCAL_DATE_TIME:
//                // asObjectCopy() returns LocalDateTime, 
//                // because in case of array entity.setProperty() fails with LocalDateTimeValue[]
//                return LocalDateTimeValue.parse(value).asObjectCopy();
//            case LOCAL_TIME:
//                return LocalTimeValue.parse(value).asObjectCopy();
//            case DATE_TIME:
//                return dateParse(value.toString(), ZonedDateTime.class, null);
////                return DateTimeValue.parse(value, timezone).asObjectCopy();
//            case TIME:
//                return dateParse(value.toString(), timezone, OffsetTime.class, null);
////                return TimeValue.parse(value, timezone).asObjectCopy();
//            case DATE:
//                return DateValue.parse(value).asObjectCopy();
//            case DURATION:
//                return DurationValue.parse(value);
////            case POINT:
////                return Util.toPoint(Util.fromJson(value, Map.class), optionalData);
////            case LOCAL_DATE_TIME:
////                // asObjectCopy() returns LocalDateTime, 
////                // because in case of array entity.setProperty() fails with LocalDateTimeValue[]
////                return LocalDateTimeValue.parse(value).asObjectCopy();
////            case LOCAL_TIME:
////                return LocalTimeValue.parse(value).asObjectCopy();
////            case DATE_TIME:
////                return DateTimeValue.parse(value, timezone).asObjectCopy();
////            case TIME:
////                return TimeValue.parse(value, timezone).asObjectCopy();
////            case DATE:
////                return DateValue.parse(value).asObjectCopy();
////            case DURATION:
////                return DurationValue.parse(value);
//            case INTEGER:
//                return Util.toLong(value);
//            case FLOAT:
//                return Util.toDouble(value);
//            case BOOLEAN:
//                return Util.toBoolean(value);
//            case NULL:
//                return null;
//            case LIST:
//                return Arrays.stream(arrayPattern.split(value)).map(this::convertType).collect(Collectors.toList());
//            default: return value;
        

        final Supplier<ZoneId> timezone = () -> ZoneId.of((String) optionalData.getOrDefault("timezone", apocConfig().getString(db_temporal_timezone.name())));
        switch (type) {
            case POINT:
                return Util.toPoint(Util.fromJson(value, Map.class), optionalData);
            case LOCAL_DATE_TIME:
                // asObjectCopy() returns LocalDateTime, 
                // because in case of array entity.setProperty() fails with LocalDateTimeValue[]
                return LocalDateTimeValue.parse(value).asObjectCopy();
            case LOCAL_TIME:
                return LocalTimeValue.parse(value).asObjectCopy();
            case DATE_TIME:
                return DateTimeValue.parse(value, timezone).asObjectCopy();
            case TIME:
                return TimeValue.parse(value, timezone).asObjectCopy();
            case DATE:
                return DateValue.parse(value).asObjectCopy();
            case DURATION:
                return DurationValue.parse(value);
            case INTEGER: return Util.toLong(value);
            case FLOAT: return Util.toDouble(value);
            case BOOLEAN: return Util.toBoolean(value);
            case NULL: return null;
            case LIST: return Arrays.stream(arrayPattern.split(value)).map(this::convertType).collect(Collectors.toList());
            default: return value;
        }
    }
    
//    public static Object conversion(Object value) {
//        
//    }
}
