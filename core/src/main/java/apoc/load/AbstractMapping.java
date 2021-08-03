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
import java.util.function.Function;
import java.util.function.Supplier;

import static apoc.util.DateParseUtil.dateParse;
import static apoc.util.Util.dateFormat;

public abstract class AbstractMapping {
    // todo - vale la pena farli privati ma con get-set?
    final String name;
    final Collection<String> nullValues;
    final Meta.Types type;
    final boolean ignore;

    final String dateFormat;
    final String[] dateParse;



    // todo - provare a fare enum eventualmente
    Function<Object, Object> listSupplier = null;
    final Map<String, Object> optionalData;// = Collections.emptyMap();
    final Supplier<ZoneId> timezone;
    
    // TODO ---> ci passo un construttore con defaultMapping, che nei csv sara emptyList, nei Json sara quello che ci passo..

    public AbstractMapping(String name, Map<String, Object> mapping, boolean ignore, Collection<String> defaultNullValues, ZoneId timezone) {
        if (mapping == null) {
            mapping = Collections.emptyMap();
        }
        this.name = mapping.getOrDefault("name", name).toString();
        this.ignore = (boolean) mapping.getOrDefault("ignore", ignore);
        this.nullValues = (Collection<String>) mapping.getOrDefault("nullValues", defaultNullValues);
        this.type = Meta.Types.from(mapping.getOrDefault("type", Meta.Types.STRING.name()).toString());
        this.dateFormat = mapping.getOrDefault("dateFormat", StringUtils.EMPTY).toString();
        this.dateParse = convertFormat(mapping.getOrDefault("dateParse", null));
        this.optionalData = (Map<String, Object>) mapping.getOrDefault("optionalData", Collections.emptyMap()); // todo - e se mettessi pure questo?
        // in case I put config {timezone: [ZONEID]}, i pick this one, otherwise from optionalData (importCsv)
        this.timezone = () -> timezone != null ? timezone : ZoneId.of((String) optionalData.getOrDefault("timezone", ZoneId.systemDefault().getId()));
        
        // todo - valutare optional data in LoadJson.java e negli altri.
        //  forse boh...

        
        // todo - questa mi sembra una cosa specifica di csv... (provare a togliere dopo)
//        if (this.type == null) {
//            // Call this out to the user explicitly because deep inside of LoadCSV and others you will get
//            // NPEs that are hard to spot if this is allowed to go through.
//            throw new RuntimeException("In specified mapping, there is no type by the name " +
//                    mapping.getOrDefault("type", "STRING").toString());
//        }
    }

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

    // todo - necessario metterlo come Object?
    abstract Object convert(Object value);


    private static String[] convertFormat(Object value) {
        if (value == null) return null;
        if (!(value instanceof List)) throw new RuntimeException("Only array of Strings are allowed!");
        List<String> strings = (List<String>) value;
        return strings.toArray(new String[strings.size()]);
    }

    // todo - timezone?
    public Object switchConvertType(Object value/*, Supplier<ZoneId> timezone*/) {
        // todo - valutare sta cosa
        if (nullValues.contains(name) || value == null) return null;
//        if (nullValues.contains(value.toString()) || value == null) return null;
        
        // in case of chars like '\n', with xml import for example
        if (value instanceof String && StringUtils.isBlank((String) value)) {
            return value;
        }

        final boolean isParseNull = dateParse == null;
        switch (type) {
            // todo -> point... posso metterlo in un common util... -> se instance of Map allora ok altrimenti Util.fromJson(...)
            case POINT:
                // todo - a differenza del csv non serve fromJson bla bla...
                return value instanceof String
                        ? Util.toPoint(Util.fromJson((String) value, Map.class), optionalData)
                        : Util.toPoint((Map<String, Object>) value, optionalData);
            case STRING:
                // todo - questo forse va bene rimanerlo
                // todo - string supplier
                if (value instanceof TemporalAccessor && !dateFormat.isEmpty()) {
                    return dateFormat((TemporalAccessor) value, dateFormat);
                } 
                if(value instanceof BigDecimal) {
                    return ((BigDecimal) value).toPlainString();
                }
                return value.toString();
            case INTEGER:
                return MappingUtil.toLongOrString(value);
            case FLOAT:
                return MappingUtil.toDoubleOrString(value);
            case BOOLEAN:
                return Util.toBoolean(value);
            // todo - fare un test anche con questo
            case NULL:
                return null;
            case LIST:
                // todo - list supplier
                return listSupplier == null ? null : listSupplier.apply(value);
            case DATE:
                // todo - valutare asObjectCopy() se serve effettivamente... credo di si per l'import
                return isParseNull
                        ? DateValue.parse((String) value).asObjectCopy()
                        : dateParse(value.toString(), LocalDate.class, dateParse);
            case DATE_TIME:
                return isParseNull
                        ? DateTimeValue.parse((String) value, timezone).asObjectCopy()
                        : dateParse(value.toString(), ZonedDateTime.class, dateParse);
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
                        ? TimeValue.parse((String) value, timezone).asObjectCopy()
                        : dateParse(value.toString(), OffsetTime.class, dateParse);
            case DURATION:
                // TODO con import csv funziona solo questo... --> vedere con gli altri a sto punto
                return DurationValue.parse((String) value).asObjectCopy();
//                return durationParse(value.toString());
            default:
                return value;
        }
    }
}
